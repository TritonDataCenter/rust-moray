/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::client::MorayClient;
use serde_json::{Map, Value};
use std::io::Error;

fn query_handler(resp: &Value) -> Result<(), Error> {
    dbg!(&resp);
    Ok(())
}

fn query_client_string_opts(ip: [u8; 4], port: u16) -> Result<(), Error> {
    let mut mclient = MorayClient::from_parts(ip, port).unwrap();

    // The sql interface does not take 'limit' in opts
    let query = "SELECT * FROM manta limit 10";

    mclient.sql(query, vec![], r#"{}"#, query_handler)
}

fn query_client_map_opts(ip: [u8; 4], port: u16) -> Result<(), Error> {
    let mut mclient = MorayClient::from_parts(ip, port).unwrap();

    // The sql interface does not take 'limit' in opts
    let query = "SELECT * FROM manta limit 10";
    let map = Map::new();

    mclient.sql(query, vec![], map, query_handler)
}

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    println!("Testing SQL method");
    query_client_string_opts(ip_arr, port)?;
    query_client_map_opts(ip_arr, port)?;
    Ok(())
}
