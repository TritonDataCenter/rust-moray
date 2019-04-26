#[macro_use]
extern crate serde_json;

use moray::client::MorayClient;
use std::io::Error;

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    let mut mclient = MorayClient::from_parts(ip_arr, port)?;
    let bucket_config = json!({
        "index": {
            "aNumber": {
                "type": "number"
            }
        }
    });

    match mclient.create_bucket("rust_test_bucket", bucket_config) {
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
