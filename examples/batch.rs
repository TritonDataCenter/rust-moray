/*
 * Copyright 2020 Joyent, Inc.
 */

#[macro_use]
extern crate serde_json;

use moray::buckets;
use moray::client::MorayClient;
use moray::objects::{self, BatchPutOp, BatchRequest, Etag};
use slog::{o, Drain, Logger};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::Mutex;

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;
    let opts = objects::MethodOptions::default();
    let bucket_opts = buckets::MethodOptions::default();
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );
    let mut mclient = MorayClient::from_parts(ip_arr, port, log, None)?;
    let bucket_name = "rust_test_bucket";
    let new_etag = String::from("");
    let mut correct_values = HashMap::new();

    correct_values.insert("eulers_number", 2.718);
    correct_values.insert("golden_ratio", 1.618);
    correct_values.insert("circle_constant", 6.28);

    println!("===confirming bucket exists===");
    match mclient.get_bucket(bucket_name, bucket_opts, |b| {
        dbg!(b);
        Ok(())
    }) {
        Err(e) => {
            eprintln!(
                "You must create a bucket named '{}' first. \
                 Run the createbucket example to do so.",
                bucket_name
            );
            let e = Error::new(ErrorKind::Other, e);
            return Err(e);
        }
        Ok(()) => (),
    }

    /* opts.etag defaults to undefined, and will clobber any existing value */
    println!("\n\n===undefined etag===");

    let put_ops: Vec<BatchPutOp> = vec![
        BatchPutOp {
            bucket: bucket_name.to_string(),
            options: opts.clone(),
            key: "circle_constant".to_string(),
            value: json!({"aNumber": 6.28}),
        },
        BatchPutOp {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "eulers_number".to_string(),
            value: json!({"aNumber": 2.718}),
        },
        BatchPutOp {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "golden_ratio".to_string(),
            value: json!({"aNumber": 1.618}),
        },
    ];

    let mut requests = vec![];
    for req in put_ops.iter() {
        requests.push(BatchRequest::Put((*req).clone()));
    }

    mclient.batch(&requests, &opts, |_| Ok(()))?;

    for req in put_ops.iter() {
        mclient
            .get_object(&req.bucket, &req.key, &opts, |o| {
                dbg!(o);
                Ok(())
            })
            .unwrap();
    }

    // Specify an incorrect etag for one of the operations and assert
    // the expected failure.
    println!("======= Specified incorrect etag =======");
    let mut bad_opts = opts.clone();
    bad_opts.etag = Etag::Specified(new_etag);

    let put_ops: Vec<BatchPutOp> = vec![
        BatchPutOp {
            bucket: bucket_name.to_string(),
            options: bad_opts,
            key: "circle_constant".to_string(),
            value: json!({"aNumber": 12.28}),
        },
        BatchPutOp {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "eulers_number".to_string(),
            value: json!({"aNumber": 4.718}),
        },
        BatchPutOp {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "golden_ratio".to_string(),
            value: json!({"aNumber": 2.618}),
        },
    ];

    let mut requests = vec![];

    for req in put_ops.iter() {
        requests.push(BatchRequest::Put((*req).clone()));
    }

    // Assert that specifying the wrong etag for even one of the operations
    // in the batch causes the entire call to fail.
    assert!(mclient.batch(&requests, &opts, |_| Ok(())).is_err());

    // Assert that if one of the operations fails the others are not executed.
    for req in put_ops.iter() {
        mclient
            .get_object(&req.bucket, &req.key, &opts, |o| {
                assert_eq!(
                    correct_values.get(req.key.as_str()).unwrap(),
                    o.value.get("aNumber").unwrap()
                );
                dbg!(o);
                Ok(())
            })
            .unwrap();
    }

    Ok(())
}
