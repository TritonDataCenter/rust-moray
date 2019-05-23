/*
 * Copyright 2019 Joyent, Inc.
 */

/* TODO: rust-cueball */
use serde_json::{self, Value};
use std::io::{Error, ErrorKind};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::str::FromStr;

use super::buckets;
use super::meta;
use super::objects;

#[derive(Debug)]
pub struct MorayClient {
    stream: TcpStream,
}

///
/// MorayClient
///
impl MorayClient {
    pub fn new<S: Into<SocketAddr>>(address: S) -> Result<MorayClient, Error> {
        match TcpStream::connect(address.into()) {
            Ok(st) => Ok(MorayClient { stream: st }),
            Err(e) => Err(e),
        }
    }

    pub fn from_parts<I: Into<IpAddr>>(
        ip: I,
        port: u16,
    ) -> Result<MorayClient, Error> {
        match TcpStream::connect(SocketAddr::new(ip.into(), port)) {
            Ok(st) => Ok(MorayClient { stream: st }),
            Err(e) => Err(e),
        }
    }

    pub fn list_buckets<F>(
        &mut self,
        opts: buckets::MethodOptions,
        bucket_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&buckets::Bucket) -> Result<(), Error>,
    {
        buckets::get_list_buckets(
            &mut self.stream,
            "",
            opts,
            buckets::Methods::List,
            bucket_handler,
        )?;
        Ok(())
    }

    pub fn get_bucket<F>(
        &mut self,
        name: &str,
        opts: buckets::MethodOptions,
        bucket_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&buckets::Bucket) -> Result<(), Error>,
    {
        buckets::get_list_buckets(
            &mut self.stream,
            name,
            opts,
            buckets::Methods::Get,
            bucket_handler,
        )?;
        Ok(())
    }

    pub fn get_object<F>(
        &mut self,
        bucket: &str,
        key: &str,
        opts: &objects::MethodOptions,
        object_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&objects::MorayObject) -> Result<(), Error>,
    {
        objects::get_find_objects(
            &mut self.stream,
            bucket,
            key,
            opts,
            objects::Methods::Get,
            object_handler,
        )
    }

    pub fn find_objects<F>(
        &mut self,
        bucket: &str,
        filter: &str,
        opts: &objects::MethodOptions,
        object_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&objects::MorayObject) -> Result<(), Error>,
    {
        objects::get_find_objects(
            &mut self.stream,
            bucket,
            filter,
            opts,
            objects::Methods::Find,
            object_handler,
        )
    }

    pub fn put_object<F>(
        &mut self,
        bucket: &str,
        key: &str,
        value: Value,
        opts: &objects::MethodOptions,
        object_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&str) -> Result<(), Error>,
    {
        objects::put_object(
            &mut self.stream,
            bucket,
            key,
            value,
            opts,
            object_handler,
        )
    }

    pub fn create_bucket(
        &mut self,
        name: &str,
        config: Value,
        opts: buckets::MethodOptions,
    ) -> Result<(), Error> {
        buckets::create_bucket(&mut self.stream, name, config, opts)
    }

    pub fn sql<F, V>(
        &mut self,
        stmt: &str,
        vals: Vec<&str>,
        opts: V,
        query_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&Value) -> Result<(), Error>,
        V: Into<Value>,
    {
        meta::sql(&mut self.stream, stmt, vals, opts, query_handler)
    }
}

impl FromStr for MorayClient {
    type Err = Error;
    fn from_str(s: &str) -> Result<MorayClient, Error> {
        let addr = SocketAddr::from_str(s).expect("Error parsing address");
        match TcpStream::connect(addr) {
            Ok(st) => Ok(MorayClient { stream: st }),
            Err(e) => Err(Error::new(ErrorKind::NotConnected, e)),
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn placeholder() {
        assert_eq!(1, 1);
    }
}
