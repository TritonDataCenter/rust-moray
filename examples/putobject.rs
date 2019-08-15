/*
 * Copyright 2019 Joyent, Inc.
 */

#[macro_use]
extern crate serde_json;

use moray::buckets;
use moray::client::MorayClient;
use moray::objects::{self, Etag};
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
    mclient.put_object(
        "rust_test_bucket",
        "circle_constant",
        json!({"aNumber": 6.28}),
        &opts,
        |o| {
            println!("Put object with undefined etag returns:\n {:?}\n", &o);
            new_etag = o.to_string();
            Ok(())
        },
    )?;

    /*
     * Specifying the etag will ensure that the value is only altered if the
     * etags match.
     */
    println!("\n===specified etag===");
    opts.etag = Etag::Specified(new_etag);
    mclient.put_object(
        "rust_test_bucket",
        "circle_constant",
        json!({"aNumber": 6.2831}),
        &opts,
        |o| {
            println!(
                "Put object (replacement) with etag specified returns:\n \
                 {:?}\n",
                &o
            );
            Ok(())
        },
    )?;

    /*
     * An etag of "null: JSON" will only succeed if the object did not exist
     * previously. Therefore this should fail.
     */
    println!("\n===null etag (should fail)===");
    opts.etag = Etag::Nulled;
    match mclient.put_object(
        "rust_test_bucket",
        "circle_constant",
        json!({"aNumber": 3.14159}),
        &opts,
        |o| {
            dbg!(&o);
            Ok(())
        },
    ) {
        Ok(()) => {
            return Err(Error::new(
                ErrorKind::Other,
                "replacing object with 'Nulled' etag should fail",
            ));
        }
        Err(e) => {
            println!(
                "Attempt to replace exiting object with 'Nulled' etag failed \
                 as expected:\n {}\n",
                e
            );
        }
    }

    /* Object doesn't exist, should pass. */
    println!("\n===null etag (should pass)===");
    opts.etag = Etag::Nulled;
    mclient
        .put_object(
            "rust_test_bucket",
            "viva_la_pi",
            json!({"aNumber": 3.14159}),
            &opts,
            |o| {
                println!(
                    "Put object (new) with Nulled etag returns: \n{:?}",
                    &o
                );
                Ok(())
            },
        )
        .unwrap_or_else(|e| {
            println!(
                "This should have been successful you may need to delete the \
                 existing 'viva_la_pi' object:\n {}",
                e
            );
        });

    // TODO: Delete 'viva_la_pi' object so that this test can be run twice

    Ok(())
}
