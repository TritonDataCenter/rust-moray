use rust_fast::client as mod_client;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use uuid::Uuid;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
#[serde(tag = "bucket")]
pub enum MorayObject {
    #[serde(alias = "manta")]
    Manta(MantaObject),
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MantaObject {
    #[serde(default, deserialize_with = "null_to_zero")]
    pub _count: u64,
    pub _etag: String,
    pub _id: u64,
    pub _mtime: u64,
    // TODO: _txn_snap:
    pub key: String,
    pub value: MantaObjectValue, // TODO: Could possibly make this an enum with a serde tag as well
}

///
/// * Undefined: Clobber any object on put
/// * Nulled: An object with the same key must not exist
/// * Specified(String): The object will only be added or overwritten if the
///     etag (String) matches the existing value
pub enum Etag {
    Undefined,
    Nulled,
    Specified(String),
}

// TODO: include _value: String = serde_json::to_string(value)
pub struct Options {
    pub req_id: String, // UUID as String
    pub etag: Etag,
    pub headers: Value,
    pub no_count: bool,
    pub sql_only: bool,
    pub no_cache: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            req_id: Uuid::new_v4().to_string(),
            etag: Etag::Undefined,
            headers: json!({}),
            no_count: false,
            sql_only: false,
            no_cache: true,
        }
    }
}

impl Options {
    pub fn new() -> Options {
        Options::default()
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

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MantaObjectValue {
    // TODO:
    // all the content_* fields (and defaults) should have skip_deserializing_if type != "directory"
    #[serde(alias = "contentLength", default)]
    pub content_length: u64,

    #[serde(alias = "contentMD5", default)]
    pub content_md5: String,

    #[serde(alias = "contentType", default)]
    pub content_type: String,

    pub creator: String,
    pub dirname: String,

    #[serde(default)]
    pub etag: String,

    //headers: Map???, // TODO:
    pub key: String,
    pub mtime: u64, //TODO Convert to date?
    pub name: String,

    #[serde(alias = "objectId", default)]
    pub object_id: String,

    pub owner: String,
    pub roles: Vec<String>, // TODO: double check this is a String

    #[serde(default)]
    pub sharks: Vec<MantaObjectShark>,

    #[serde(alias = "type")]
    pub object_type: String, // TODO: represents as a String but is a defacto enum, right?

    pub vnode: u64,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MantaObjectShark {
    pub datacenter: String,
    pub manta_storage_id: String,
}

fn decode_object<F>(fm_data: &Value, mut cb: F) -> Result<(), Error>
where
    F: FnMut(MorayObject) -> Result<(), Error>,
{
    let result = Ok(());

    if fm_data.is_array() {
        let resp_data: Vec<Value> =
            serde_json::from_value(fm_data.clone()).unwrap();

        return resp_data.iter().fold(result, |_r, object_data| {
            serde_json::from_value::<MorayObject>(object_data.clone())
                .map_err(|e| {
                    // TODO: this should propagate error up
                    eprintln!("ERROR: {}", &e);
                    Error::new(ErrorKind::Other, e)
                })
                .and_then(|obj| cb(obj))
        });
    }

    assert_eq!(fm_data.is_object(), true);

    serde_json::from_value::<MorayObject>(fm_data.clone())
        .map_err(|e| Error::new(ErrorKind::Other, e))
        .and_then(|obj| cb(obj))?;

    result
}

pub fn get_find_objects<F>(
    stream: &mut TcpStream,
    bucket: &str,
    key_filter: &str,
    opts: &str, // TODO: Should take Value
    method: Methods,
    mut object_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&MorayObject) -> Result<(), Error>,
{
    let options: Value = serde_json::from_str(opts).unwrap();
    let arg = json!([bucket, key_filter, options]);
    let obj_method = method.method();

    mod_client::send(String::from(obj_method), arg, stream).and_then(|_| {
        mod_client::receive(stream, |resp| {
            decode_object(&resp.data.d, |obj| object_handler(&obj))
        })
    })?;

    Ok(())
}

fn make_options(options: &Options) -> Value {
    let json_value = json!({
        "req_id": options.req_id,
        "headers": options.headers,
        "no_count": options.no_count,
        "sql_only": options.sql_only,
        "noCache": options.no_cache,
    });

    let mut ret = json_value.as_object().unwrap().clone();

    match &options.etag {
        Etag::Undefined => (),
        Etag::Nulled => {
            ret.insert(String::from("etag"), Value::Null);
        }
        Etag::Specified(s) => {
            ret.insert(
                String::from("etag"),
                serde_json::from_str(s.as_str()).unwrap(),
            );
        }
    }

    serde_json::to_value(ret).unwrap()
}

pub fn put_object<F>(
    stream: &mut TcpStream,
    bucket: &str,
    key: &str,
    value: Value,
    opts: &Options,
    mut object_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&Value) -> Result<(), Error>,
{
    let arg = json!([bucket, key, value, make_options(opts)]);

    mod_client::send(Methods::Put.method(), arg, stream).and_then(|_| {
        mod_client::receive(stream, |resp| {
            /*
             * TODO: deserialize the return value which is:
             * Array([Object({"etag": String()})])
             */
            object_handler(&resp.data.d)
        })
    })?;

    Ok(())
}
