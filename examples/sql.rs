/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::client::MorayClient;
use serde_json::Value;
use std::io::Error;

fn query_handler(resp: &Value) -> Result<(), Error> {
    dbg!(&resp);
    Ok(())
}

fn query_client_fromparts(ip: [u8; 4], port: u16) {
    let mut mclient = MorayClient::from_parts(ip, port).unwrap();
    match mclient.sql("select * from manta", vec![], "{}", query_handler) {
        Ok(()) => (),
        Err(e) => {
            println!("Error: {}", e);
            ()
        }
    }
}

fn main() {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    println!("Testing SQL method");
    query_client_fromparts(ip_arr, port);

}
