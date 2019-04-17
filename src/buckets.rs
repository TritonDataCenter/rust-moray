use rust_fast::client as mod_client;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use std::io::{Error, ErrorKind};
use std::net::TcpStream;

/*
 * === Buckets ===
 */
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct BucketOptions {
    #[serde(default)]
    version: u32,

    #[serde(alias = "guaranteeOrder", default)]
    guarantee_order: bool,

    #[serde(alias = "syncUpdates", default)]
    sync_updates: bool,
}

// TODO: We should be able to skip this step with per field deserializers
#[derive(Deserialize, Serialize, Debug, Clone)]
struct BucketIntermediate {
    index: String,
    mtime: String, // TODO: format as date
    name: String,
    options: String,
    post: String,
    pre: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct Bucket {
    index: String,
    mtime: String,
    name: String,
    options: BucketOptions,
    post: Vec<String>,
    pre: Vec<String>,
}

fn decode_bucket<F>(fm_data: &Value, mut cb: F) -> Result<(), Error>
where
    F: FnMut(Bucket) -> Result<(), Error>,
{
    let resp_data: Vec<Value> =
        serde_json::from_value(fm_data.clone()).unwrap();

    let result = Ok(());

    resp_data.iter().fold(result, |_r, bucket_data| {
        serde_json::from_value::<BucketIntermediate>(bucket_data.clone())
            .map_err(|e| Error::new(ErrorKind::Other, e))
            .and_then(|bi| {
                cb(Bucket {
                    name: bi.name,
                    index: bi.index,
                    mtime: bi.mtime,
                    options: serde_json::from_str(bi.options.as_str()).unwrap(),
                    post: serde_json::from_str(bi.post.as_str()).unwrap(),
                    pre: serde_json::from_str(bi.pre.as_str()).unwrap(),
                })
            })
    })
}

pub fn list_buckets<F>(
    stream: &mut TcpStream,
    mut bucket_handler: F,
) -> Result<(), Error>
where
    F: FnMut(&Bucket) -> Result<(), Error>,
{
    let empty_arg = serde_json::from_str(r#"[{}]"#).unwrap();

    mod_client::send(String::from("listBuckets"), empty_arg, stream).and_then(
        |_| {
            mod_client::receive(stream, |resp| {
                decode_bucket(&resp.data.d, |b| bucket_handler(&b))
            })
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{quickcheck, Arbitrary, Gen};
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use serde_json::Map;
    use std::iter;

    fn random_string<G: Gen>(g: &mut G, len: usize) -> String {
        iter::repeat(())
            .map(|()| g.sample(Alphanumeric))
            .take(len)
            .collect()
    }

    impl Arbitrary for BucketOptions {
        fn arbitrary<G: Gen>(g: &mut G) -> BucketOptions {
            let version = g.gen::<u32>();
            let guarantee_order = g.gen::<bool>();
            let sync_updates = g.gen::<bool>();

            BucketOptions {
                version,
                guarantee_order,
                sync_updates,
            }
        }
    }

    impl Arbitrary for Bucket {
        fn arbitrary<G: Gen>(g: &mut G) -> Bucket {
            let index_len = g.gen::<u8>() as usize;
            let mtime_len = g.gen::<u8>() as usize;
            let name_len = g.gen::<u8>() as usize;
            let post_len = g.gen::<u8>() as usize;
            let pre_len = g.gen::<u8>() as usize;

            let index = random_string(g, index_len);
            let mtime = random_string(g, mtime_len);
            let name = random_string(g, name_len);
            let options = BucketOptions::arbitrary(g);
            let post = vec![random_string(g, post_len)];
            let pre = vec![random_string(g, pre_len)];

            Bucket {
                index,
                mtime,
                name,
                options,
                post,
                pre,
            }
        }
    }

    fn create_intermediate_bucket(bucket: Bucket) -> BucketIntermediate {
        BucketIntermediate {
            index: bucket.index,
            mtime: bucket.mtime,
            name: bucket.name,
            options: serde_json::to_string(&bucket.options).unwrap(),
            post: serde_json::to_string(&bucket.post).unwrap(),
            pre: serde_json::to_string(&bucket.pre).unwrap(),
        }
    }

    // TODO: Create array of multiple buckets
    quickcheck! {
        fn decode_bucket_test(bucket: Bucket) -> bool {
            let mut pass = false;
            let bucket_clone = bucket.clone();
            let bi = create_intermediate_bucket(bucket);
            let mut map = Map::new();

            dbg!(&bi);
            map.insert(String::from("index"), Value::String(bi.index));
            map.insert(String::from("mtime"), Value::String(bi.mtime));
            map.insert(String::from("name"), Value::String(bi.name));
            map.insert(String::from("options"), Value::String(bi.options));
            map.insert(String::from("post"), Value::String(bi.post));
            map.insert(String::from("pre"), Value::String(bi.pre));

            let obj = Value::Object(map);
            let input = Value::Array(vec![obj]);
            dbg!(&input);
            match decode_bucket(&input, |b| {
                pass = b == bucket_clone;
                Ok(())
            }) {
                Ok(()) => pass,
                Err(_e) => false
            }
        }
    }
}
