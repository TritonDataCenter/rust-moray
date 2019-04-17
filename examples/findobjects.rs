/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::client::MorayClient;
use moray::objects::MorayObject;
use std::io::Error;

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    let mut mclient = MorayClient::from_parts(ip_arr, port).unwrap();
    // TODO: try with the "opts" arg as "{limit: 10}"
    mclient.find_objects(
        "manta",
        "(objectId=561e119d-b056-67e8-e078-f541494de358)",
        "{}",
        |o| {
            dbg!(o);
            Ok(())
        },
    )?;

    mclient.find_objects("manta", "(type=object)", r#"{"limit": 10}"#, |o| {
        // dbg!(o);
        match o {
            MorayObject::Manta(mantaobj) => {
                dbg!(&mantaobj.value.name);
                ()
            }
        }
        Ok(())
    })
}
