/*
 * Copyright 2019 Joyent, Inc.
 */

#[macro_use]
extern crate serde_json;

use moray::buckets;
use moray::client::MorayClient;
use std::io::Error;
use std::sync::Mutex;

use slog::{o, Drain, Logger};

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;
    let opts = buckets::MethodOptions::default();

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    let mut mclient = MorayClient::from_parts(ip_arr, port, log, None)?;
    let bucket_config = json!({
        "index": {
            "aNumber": {
                "type": "number"
            }
        }
    });

    match mclient.create_bucket("rust_test_bucket", bucket_config, opts) {
        Ok(()) => {
            println!("Bucket Created Successfully");
            Ok(())
        }
        Err(e) => {
            eprintln!("Error Creating Bucket");
            Err(e)
        }
    }
}
