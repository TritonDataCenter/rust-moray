/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::client::MorayClient;
use moray::objects::{self, MorayObject};
use std::io::Error;

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    let mut key: String = "".to_string();
    let mut checksum: String = "".to_string();
    let mut oid: String = String::new();
    let mut opts = objects::MethodOptions::default();

    let mut mclient = MorayClient::from_parts(ip_arr, port).unwrap();

    opts.set_limit(10);
    mclient.find_objects(
        "manta",
        "(type=object)",
        &opts,
        |o| {
            match o {
                MorayObject::Manta(mantaobj) => {
                    dbg!(&mantaobj.value.name);
                    if key.len() == 0 {
                        key = mantaobj.key.clone();
                        checksum = mantaobj.value.content_md5.clone();
                        oid = mantaobj.value.object_id.clone();
                    }
                    ()
                }
            }
            Ok(())
        },
    )?;

    let opts = objects::MethodOptions::default();

    mclient.get_object("manta", key.as_str(), &opts, |o| {
        match o {
            MorayObject::Manta(mantaobj) => {
                println!("Found checksum:     {}", &mantaobj.value.content_md5);
                println!("Expected checksum:  {}", &checksum);
                assert_eq!(mantaobj.value.content_md5, checksum);
                ()
            }
        }
        Ok(())
    })?;

    let mut count = 0;
    let filter = format!("(objectId={})", oid);
    mclient.find_objects("manta", filter.as_str(), &opts, |o| {
        count += 1;
        assert_eq!(count, 1, "should only be one result");
        match o {
            MorayObject::Manta(mantaobj) => {
                println!("Found checksum:     {}", &mantaobj.value.content_md5);
                println!("Expected checksum:  {}", &checksum);
                assert_eq!(mantaobj.value.content_md5, checksum);
                ()
            }
        }
        Ok(())
    })
}
