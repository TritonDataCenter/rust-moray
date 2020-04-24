/*
 * Copyright 2019 Joyent, Inc.
 */

use rust_fast::{client as fast_client, protocol::FastMessageId};
use serde::ser::Serializer;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use uuid::Uuid;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MorayObject {
    pub bucket: String,
    #[serde(default, deserialize_with = "null_to_zero")]
    pub _count: u64, // TODO: This should probably be an Option<u64>
    pub _etag: String,
    pub _id: u64,
    pub _mtime: u64,
    pub _txn_snap: Option<u64>,
    pub key: String,
    pub value: Value, // Bucket schema dependent
}

///
/// * Undefined: Clobber any object on put
/// * Nulled: An object with the same key must not exist
/// * Specified(String): The object will only be added or overwritten if the
///     etag (String) matches the existing value
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum Etag {
    Undefined,
    Nulled,
    Specified(String),
}

impl Etag {
    fn is_undefined(&self) -> bool {
        self == &Etag::Undefined
    }

    // We can't use "Self" here because enum variants on type aliases are
    // experimental
    pub fn specified_value(&self) -> Option<&String> {
        match self {
            Etag::Undefined | Etag::Nulled => None,
            Etag::Specified(s) => Some(s)
        }
    }
}

impl Serialize for Etag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Etag::Undefined => {
                panic!(
                    "Attempt to serialize undefined etag which should be \
                     skipped"
                );
            }
            Etag::Nulled => serializer.serialize_none(),
            Etag::Specified(etag) => serializer.serialize_str(etag),
        }
    }
}

// TODO:
// * include _value: String = serde_json::to_string(value)
// * add offset,
// * add sort
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MethodOptions {
    pub req_id: String, // UUID as String
    #[serde(skip_serializing_if = "Etag::is_undefined")]
    pub etag: Etag,
    pub headers: Value,
    pub no_count: bool,
    pub sql_only: bool,
    #[serde(rename(serialize = "noCache"))]
    pub no_cache: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
}

/*
impl Serialize for MethodOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MethodOptions", )

    }
}
*/

impl Default for MethodOptions {
    fn default() -> Self {
        Self {
            req_id: Uuid::new_v4().to_string(),
            etag: Etag::Undefined,
            headers: json!({}),
            no_count: false,
            sql_only: false,
            no_cache: true,
            limit: None,
        }
    }
}

impl MethodOptions {
    pub fn set_limit(&mut self, limit: u64) {
        self.limit = Some(limit);
    }

    pub fn unset_limit(&mut self) {
        self.limit = None;
    }
}

/*
 * Could later extend this so that each method maps to a method specific structure which would hold
 * the possible options for this method, and other method specific data.
 */
pub enum Methods {
    Get,
    Find,
    Put,
}

impl Methods {
    fn method(&self) -> String {
        match *self {
            Methods::Get => String::from("getObject"),
            Methods::Find => String::from("findObjects"),
            Methods::Put => String::from("putObject"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PutObjectReturn {
    etag: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BatchPutReturn {
    bucket: String,
    etag: String,
    key: String,
}

fn null_to_zero<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    match opt {
        Some(o) => Ok(o),
        None => Ok(0),
    }
}

fn decode_object<F>(fm_data: &Value, mut cb: F) -> Result<(), Error>
where
    F: FnMut(MorayObject) -> Result<(), Error>,
{
    let result = Ok(());

    if fm_data.is_array() {
        let resp_data: Vec<Value> =
            serde_json::from_value(fm_data.clone()).unwrap();

        resp_data.iter().fold(result, |_r, object_data| {
            serde_json::from_value::<MorayObject>(object_data.clone())
                .map_err(|e| {
                    // TODO: this should propagate error up
                    eprintln!("ERROR: {}", &e);
                    Error::new(ErrorKind::Other, e)
                })
                .and_then(|obj| cb(obj))
        })
    } else {
        assert_eq!(fm_data.is_object(), true);

        serde_json::from_value::<MorayObject>(fm_data.clone())
            .map_err(|e| Error::new(ErrorKind::Other, e))
            .and_then(cb)?;

        result
    }
}

pub fn get_find_objects<F>(
    stream: &mut TcpStream,
    bucket: &str,
    key_filter: &str,
    opts: &MethodOptions,
    method: Methods,
    mut object_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&MorayObject) -> Result<(), Error>,
{
    let obj_method = method.method();
    let arg = json!([bucket, key_filter, opts]);
    let mut msg_id = FastMessageId::new();

    fast_client::send(obj_method, arg, &mut msg_id, stream).and_then(|_| {
        fast_client::receive(stream, |resp| {
            decode_object(&resp.data.d, |obj| object_handler(&obj))
        })
    })?;

    Ok(())
}

pub fn put_object<F>(
    stream: &mut TcpStream,
    bucket: &str,
    key: &str,
    value: Value,
    opts: &MethodOptions,
    mut object_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&str) -> Result<(), Error>,
{
    let arg = json!([bucket, key, value, opts]);
    let mut msg_id = FastMessageId::new();

    fast_client::send(Methods::Put.method(), arg, &mut msg_id, stream)
        .and_then(|_| {
        fast_client::receive(stream, |resp| {
            let arr: Vec<PutObjectReturn> =
                serde_json::from_value(resp.data.d.clone())?;
            if arr.len() != 1 {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("Expected response to be a single element Array, got: {:?}",
                        arr
                    ),
                ));
            }
            object_handler(arr[0].etag.as_str())
        })
    })?;

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
// This serde macro adds the "operation" field to each variant's structure when
// it is serialized.
#[serde(tag = "operation")]
#[serde(rename_all = "camelCase")]
pub enum BatchRequest {
    Put(BatchPutRequest),
    Update(BatchUpdateRequest),
    Delete(BatchDeleteRequest),
    DeleteMany(BatchDeleteManyRequest),
}

// TODO: impl Default for BatchRequest {} (default to Put)

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BatchPutRequest {
    pub bucket: String,
    pub options: MethodOptions,
    pub key: String,
    pub value: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BatchUpdateRequest {
    pub bucket: String,
    pub options: Option<MethodOptions>,
    pub key: String,
    pub fields: Value,
    pub filter: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BatchDeleteRequest {
    pub bucket: String,
    pub options: Option<MethodOptions>,
    pub key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BatchDeleteManyRequest {
    pub bucket: String,
    pub options: Option<MethodOptions>,
    pub filter: String,
}

// Returns Err on EtagConflict and does not call the object_handler
pub fn batch<F>(
    stream: &mut TcpStream,
    requests: &Vec<BatchRequest>,
    opts: &MethodOptions,
    mut batch_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&str) -> Result<(), Error>,
{
    let batch_requests =
        serde_json::to_value(requests.to_owned()).expect("batch requests");
    let arg = json!([batch_requests, opts]);
    let mut msg_id = FastMessageId::new();

    fast_client::send(String::from("batch"), arg, &mut msg_id, stream)
        .and_then(|_| {
            fast_client::receive(stream, |resp| {

                // Expected return value looks like:
                // resp.data.d:
                //  [{
                //      "etags": [
                //          BucketPutReturn{},
                //          BucketPutReturn{},
                //          BucketPutReturn{},
                //          ...
                //      ]
                //  }]
                // TODO: make this generic.  This only works for put returns
                let arr: Vec<Value> =
                    serde_json::from_value(resp.data.d.clone())?;
                if arr.len() != 1 {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Expected response to be a single element Array, got: {:?}",
                                arr
                        ),
                    ));
                }

                let etags: Vec<BatchPutReturn> = serde_json::from_value(
                    arr[0]["etags"].clone())?;

                batch_handler(&serde_json::to_string(&etags)?)

            })
        })?;

    Ok(())
}

mod test {
    use super::*;
    #[test]
    fn batch_test() {
        let mut requests = vec![];
        let value = json!({
            "field 1": "value 1",
            "objectID": "someuuid",
            "number": 4
        });

        let mut stream = TcpStream::connect("localhost:8000").unwrap();

        // TODO: spawn thread to listen on localhost:8080

        requests.push(BatchRequest::Put(BatchPutRequest {
            bucket: String::from("foo bucket"),
            options: MethodOptions::default(),
            key: String::from("somekey"),
            value,
        }));

        requests.push(BatchRequest::DeleteMany(BatchDeleteManyRequest {
            bucket: String::from("foo bucket"),
            options: Some(MethodOptions::default()),
            filter: String::from("(mydelete=filter)"),
        }));

        let opts = MethodOptions::default();
        batch(&mut stream, &requests, &opts, |_| Ok(())).expect("batch");
    }

    #[test]
    fn method_options_test() {
        let etag_string = String::from("Some Special Etag");
        let mut options = MethodOptions::default();

        // Check that the default etag is Etag::Undefined, and that we skip
        // serializing in that case.
        assert_eq!(options.etag, Etag::Undefined);
        let serialized = serde_json::to_value(options.clone()).unwrap();
        assert!(serialized.get("etag").is_none());

        // Etag::Nulled should serialize to Value::Null
        options.etag = Etag::Nulled;
        let serialized = serde_json::to_value(options.clone()).unwrap();
        let null_etag = serialized.get("etag").expect("get Nulled Etag");
        assert_eq!(*null_etag, Value::Null);

        // Etag::Specified(<String>) should serialize to Value::String(<String>)
        options.etag = Etag::Specified(etag_string.clone());
        let serialized = serde_json::to_value(options.clone()).unwrap();
        let specified_etag =
            serialized.get("etag").expect("get Specified Etag");
        assert_eq!(*specified_etag, Value::String(etag_string));
    }
}
