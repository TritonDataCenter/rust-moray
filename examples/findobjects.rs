/*
 * Copyright 2019 Joyent, Inc.
 */

use libmanta::moray as manta;
use moray::client::MorayClient;
use moray::objects;

use slog::{o, Drain, Logger};
use std::io::{Error, ErrorKind};
use std::sync::Mutex;

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 15];
    let port: u16 = 2021;

    let mut key: String = "".to_string();
    let mut checksum: String = "".to_string();
    let mut oid: String = String::new();
    let mut opts = objects::MethodOptions::default();

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );
    let mut mclient = MorayClient::from_parts(ip_arr, port, log, None)?;

    opts.set_limit(10);
    mclient.find_objects("manta", "(type=object)", &opts, |o| {
        if o.bucket != "manta" {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Unknown bucket type {}", &o.bucket),
            ));
        }
        let mobj: manta::MantaObject =
            serde_json::from_value(o.value.clone()).unwrap();
        assert_eq!(mobj.obj_type, String::from("object"));
        dbg!(&mobj.name);
        if key.len() == 0 {
            key = mobj.key.clone();
            checksum = mobj.content_md5.clone();
            oid = mobj.object_id.clone();
        }
        Ok(())
    })?;

    let mut opts = objects::MethodOptions::default();

    mclient.get_object("manta", key.as_str(), &opts, |o| {
        if o.bucket != "manta" {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Unknown bucket type {}", &o.bucket),
            ));
        }
        let manta_obj: manta::ObjectType =
            serde_json::from_value(o.value.clone()).unwrap();
        match manta_obj {
            manta::ObjectType::Object(mobj) => {
                println!("Found checksum:     {}", &mobj.content_md5);
                println!("Expected checksum:  {}", &checksum);
                assert_eq!(mobj.content_md5, checksum);
                ()
            }
            _ => (),
        }
        Ok(())
    })?;

    let mut count = 0;
    let filter = format!("(objectId={})", oid);
    mclient.find_objects("manta", filter.as_str(), &opts, |o| {
        count += 1;
        assert_eq!(count, 1, "should only be one result");
        if o.bucket != "manta" {
            let e = Error::new(
                ErrorKind::Other,
                format!("Unknown bucket type {}", &o.bucket),
            );
            return Err(e);
        }
        let manta_obj: manta::ObjectType =
            serde_json::from_value(o.value.clone()).unwrap();
        match manta_obj {
            manta::ObjectType::Object(mobj) => {
                println!("Found checksum:     {}", &mobj.content_md5);
                println!("Expected checksum:  {}", &checksum);
                assert_eq!(mobj.content_md5, checksum);
                ()
            }
            _ => (),
        }
        Ok(())
    })?;

    opts.set_limit(10);
    mclient.find_objects("manta", "(type=directory)", &opts, |o| {
        assert_eq!(count, 1, "should only be one result");
        if o.bucket != "manta" {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Unknown bucket type {}", &o.bucket),
            ));
        }
        let manta_obj: manta::ObjectType =
            serde_json::from_value(o.value.clone()).unwrap();
        match manta_obj {
            manta::ObjectType::Object(_) => {
                assert!(false, "Found object in directory query");
                ()
            }
            manta::ObjectType::Directory(mdir) => {
                println!("Found directory: {}", mdir.key);
                ()
            }
        }
        Ok(())
    })
}
