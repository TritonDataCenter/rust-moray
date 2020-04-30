/*
 * Copyright 2019 Joyent, Inc.
 */

#[macro_use]
extern crate serde_json;

use moray::buckets;
use moray::client::MorayClient;
use moray::objects::{
    self, BatchDeleteManyRequest, BatchDeleteRequest, BatchPutRequest,
    BatchRequest, Etag,
};
use slog::{o, Drain, Logger};
use std::io::{Error, ErrorKind};
use std::sync::Mutex;

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    let bucket_name = "rust_test_bucket";
    let mut opts = objects::MethodOptions::default();
    let bucket_opts = buckets::MethodOptions::default();
    let mut new_etag = String::from("");

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    let mut mclient = MorayClient::from_parts(ip_arr, port, log, None)?;

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

    let put_requests: Vec<BatchPutRequest> = vec![
        BatchPutRequest {
            bucket: bucket_name.to_string(),
            options: opts.clone(),
            key: "circle_constant".to_string(),
            value: json!({"aNumber": 6.28}),
        },
        BatchPutRequest {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "eulers_number".to_string(),
            value: json!({"aNumber": 2.718}),
        },
        BatchPutRequest {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "golden_ratio".to_string(),
            value: json!({"aNumber": 1.618}),
        },
    ];

    let mut requests = vec![];

    for req in put_requests.iter() {
        requests.push(BatchRequest::Put((*req).clone()));
    }

    mclient.batch(&requests, &opts, |_| Ok(()))?;

    for req in put_requests.iter() {
        mclient
            .get_object(&req.bucket, &req.key, &opts, |o| {
                dbg!(o);
                Ok(())
            })
            .unwrap();
    }

    println!("============1 Delete, 2 Puts ==============");

    let requests: Vec<BatchRequest> = vec![
        BatchRequest::Delete(BatchDeleteRequest {
            bucket: bucket_name.to_string(),
            options: opts.clone(),
            key: "circle_constant".to_string(),
        }),
        BatchRequest::Put(BatchPutRequest {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "eulers_number".to_string(),
            value: json!({"state": "wrong", "aNumber": 4.718}),
        }),
        BatchRequest::Put(BatchPutRequest {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "golden_ratio".to_string(),
            value: json!({"state": "wrong", "aNumber": 2.618}),
        }),
    ];

    mclient.batch(&requests, &opts, |_| Ok(()))?;

    for req in requests.iter() {
        let (bucket, key) = match req {
            BatchRequest::Put(r) => (&r.bucket, &r.key),
            BatchRequest::Delete(r) => (&r.bucket, &r.key),
            _ => panic!("operation"),
        };

        mclient
            .get_object(bucket, key, &opts, |o| {
                dbg!(o);
                Ok(())
            })
            .unwrap();
    }

    println!("============ DeleteMany ==============");

    let requests: Vec<BatchRequest> = vec![
        BatchRequest::DeleteMany(BatchDeleteManyRequest {
            bucket: bucket_name.to_string(),
            options: opts.clone(),
            filter: String::from("(state=wrong)"),
        }),
        BatchRequest::Put(BatchPutRequest {
            bucket: bucket_name.into(),
            options: opts.clone(),
            key: "avagadro".to_string(),
            value: json!({"aNumber": 6.02214}),
        }),
    ];

    mclient.batch(&requests, &opts, |_| Ok(()))?;

    mclient
        .get_object(bucket_name, "avagadro", &opts, |o| {
            dbg!(o);
            Ok(())
        })
        .unwrap();

    mclient
        .get_object(bucket_name, "golden_ratio", &opts, |o| {
            dbg!(o);
            Ok(())
        })
        .unwrap();

    mclient
        .get_object(bucket_name, "eulers_number", &opts, |o| {
            dbg!(o);
            Ok(())
        })
        .unwrap();

    Ok(())
}
