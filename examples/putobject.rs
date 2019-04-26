#[macro_use]
extern crate serde_json;
use moray::client::MorayClient;
use std::io::{Error, ErrorKind};

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;
    let bucket_name = "rust_test_bucket";

    let mut mclient = MorayClient::from_parts(ip_arr, port)?;

    match mclient.get_bucket(bucket_name, |b| {
        dbg!(b);
        Ok(())
    }) {
        Err(e) => {
            eprintln!(
                "You must create a bucket named '{}' first. \
                 Run createbucket example to do so.",
                bucket_name
            );
            return Err(Error::new(ErrorKind::Other, e));
        }
        Ok(()) => (),
    }

    mclient.put_object(
        "rust_test_bucket",
        "pi_is_a_lie",
        json!({"aNumber": 6.28}),
        r#"{}"#,
        |o| {
            dbg!(o);
            Ok(())
        }
    )
}
