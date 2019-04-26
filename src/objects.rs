use rust_fast::client as mod_client;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
#[serde(tag = "bucket")]
pub enum MorayObject {
    #[serde(alias = "manta")]
    Manta(MantaObject),
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MantaObject {
    #[serde(default, deserialize_with = "null_to_zero")]
    _count: u64,
    _etag: String,
    _id: u64,
    _mtime: u64,
    // TODO: _txn_snap:
    pub key: String,
    pub value: MantaObjectValue, // TODO: Could possibly make this an enum with a serde tag as well
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

pub fn put_object<F>(
    stream: &mut TcpStream,
    bucket: &str,
    key: &str,
    value: Value,
    opts: &str,
    mut object_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&Value) -> Result<(), Error>,
{
    let options: Value = serde_json::from_str(opts).unwrap();
    let arg = json!([bucket, key, value, options]);

    mod_client::send(Methods::Put.method(), arg, stream).and_then(|_| {
        mod_client::receive(stream, |resp| {
            // TODO: does putObject always return {"etag": "<etag>"}
            object_handler(&resp.data.d)
        })
    })?;

    Ok(())
}
