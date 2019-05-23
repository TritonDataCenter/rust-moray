/*
 * Copyright 2019 Joyent, Inc.
 */

use rust_fast::client as fast_client;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use uuid::Uuid;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MorayObject {
    pub bucket: String,
    #[serde(default, deserialize_with = "null_to_zero")]
    pub _count: u64,
    pub _etag: String,
    pub _id: u64,
    pub _mtime: u64,
    // TODO: _txn_snap:
    pub key: String,
    pub value: Value, // We don't know what the bucket schema is so we leave that up to the caller
}

///
/// * Undefined: Clobber any object on put
/// * Nulled: An object with the same key must not exist
/// * Specified(String): The object will only be added or overwritten if the
///     etag (String) matches the existing value
#[derive(Debug)]
pub enum Etag {
    Undefined,
    Nulled,
    Specified(String),
}

// TODO:
// * include _value: String = serde_json::to_string(value)
// * add offset,
// * add sort
#[derive(Debug)]
pub struct MethodOptions {
    pub req_id: String, // UUID as String
    pub etag: Etag,
    pub headers: Value,
    pub no_count: bool,
    pub sql_only: bool,
    pub no_cache: bool,
    limit: Option<u64>,
}

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

// TODO: make method specific
fn make_options(options: &MethodOptions) -> Value {
    let json_value = json!({
        "req_id": options.req_id,
        "headers": options.headers,
        "no_count": options.no_count,
        "sql_only": options.sql_only,
    });

    let mut ret = json_value.as_object().unwrap().clone();

    match &options.etag {
        Etag::Undefined => (),
        Etag::Nulled => {
            ret.insert(String::from("etag"), Value::Null);
        }
        Etag::Specified(s) => {
            ret.insert(String::from("etag"), serde_json::to_value(s).unwrap());
        }
    }

    match &options.limit {
        None => (),
        Some(lim) => {
            ret.insert(
                String::from("limit"),
                serde_json::to_value(lim).unwrap(),
            );
        }
    };

    serde_json::to_value(ret).unwrap()
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
    let arg = json!([bucket, key_filter, make_options(opts)]);

    fast_client::send(String::from(obj_method), arg, stream).and_then(
        |_| {
            fast_client::receive(stream, |resp| {
                decode_object(&resp.data.d, |obj| object_handler(&obj))
            })
        },
    )?;

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
    let arg = json!([bucket, key, value, make_options(opts)]);

    fast_client::send(Methods::Put.method(), arg, stream).and_then(|_| {
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
