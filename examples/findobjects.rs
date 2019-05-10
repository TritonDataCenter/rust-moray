/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::client::MorayClient;
use moray::objects::MorayObject;
use std::io::Error;
use uuid::Uuid;

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    let mut key: String = "".to_string();
    let mut checksum: String = "".to_string();
    let mut oid: String = String::new();

    let mut mclient = MorayClient::from_parts(ip_arr, port).unwrap();

    mclient.find_objects(
        "manta",
        "(type=object)",
        r#"{"limit": 10}"#,
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

    let reqid = format!("{{ \"req_id\": \"{}\", ", Uuid::new_v4());
    let other_opts = r#" "headers": {}, "no_count": false, "sql_only": false, "noCache": true}"#;
    let getopts = format!("{}{}", reqid, other_opts);

    mclient.get_object("manta", key.as_str(), getopts.as_str(), |o| {
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
    mclient.find_objects("manta", filter.as_str(), "{}", |o| {
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
