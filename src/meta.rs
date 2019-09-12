/*
 * Copyright 2019 Joyent, Inc.
 */

use rust_fast::{client as fast_client, protocol::FastMessageId};
use serde_json::{self, json, Value};
use std::io::Error;
use std::net::TcpStream;

/// Make a raw sql query.
///
/// * stmt: The SQL query statement
/// * vals: A vector of values to insert
/// * opts: Query options.  Acceptable formats include:
///     * String or &str: `r#{ "key": <value> }#`
///     * Map<String, serde_json::value::Value>
///     * serde_json::value::Value
///     * Other formats for which serde_json implements the From trait
/// * query_handler: will be called with the response as a
/// &serde_json::value::Value
///
/// Note:  The serde_json::value::Value From trait for Strings and &str's simply
/// encodes them as Value::String's.  For our use case opts must be a JSON
/// object.  So if you pass a String or a &str this function will convert it
/// from a Value::String to a Value::Object and pass that to moray.
pub fn sql<F, V>(
    stream: &mut TcpStream,
    stmt: &str,
    vals: Vec<&str>,
    opts: V,
    mut query_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&Value) -> Result<(), Error>,
    V: Into<Value>,
{
    let opts_tmp: Value = opts.into();

    let options = if opts_tmp.is_string() {
        let s: String = serde_json::from_value(opts_tmp).unwrap();
        let v: Value = serde_json::from_str(s.as_str()).unwrap();
        v
    } else {
        opts_tmp
    };

    let values: Value = json!(vals);
    let args: Value = json!([stmt, values, options]);
    let mut msg_id = FastMessageId::new();

    fast_client::send(String::from("sql"), args, &mut msg_id, stream)
        .and_then(|_| {
            fast_client::receive(stream, |resp| query_handler(&resp.data.d))
        })?;

    Ok(())
}
