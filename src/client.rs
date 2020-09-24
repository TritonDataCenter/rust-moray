/*
 * Copyright 2020 Joyent, Inc.
 */

use cueball::backend::Backend;
use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball::connection_pool::ConnectionPool;
use cueball_static_resolver::StaticIpResolver;
use cueball_tcp_stream_connection::TcpStreamWrapper;

use slog::Logger;
use std::ops::DerefMut;

use std::str::FromStr;

use serde_json::{self, Value};
use std::io::{Error, ErrorKind};

use std::net::{IpAddr, SocketAddr};

use super::buckets;
use super::meta;
use super::objects;

#[derive(Clone)]
pub struct MorayClient {
    connection_pool: ConnectionPool<
        TcpStreamWrapper,
        StaticIpResolver,
        fn(&Backend) -> TcpStreamWrapper,
    >,
}

///
/// MorayClient
///
impl MorayClient {
    pub fn new(
        address: SocketAddr,
        log: Logger,
        opts: Option<ConnectionPoolOptions>,
    ) -> Result<MorayClient, Error> {
        let primary_backend = (address.ip(), address.port());
        let resolver = StaticIpResolver::new(vec![primary_backend]);

        let pool_opts = match opts {
            None => ConnectionPoolOptions {
                max_connections: Some(2),
                claim_timeout: Some(5000),
                log: Some(log),
                rebalancer_action_delay: None, // Default 100ms
                decoherence_interval: None,    // Default 300s
                connection_check_interval: None, // Default 30s
            },
            Some(opts) => opts,
        };

        let pool = ConnectionPool::<
            TcpStreamWrapper,
            StaticIpResolver,
            fn(&Backend) -> TcpStreamWrapper,
        >::new(pool_opts, resolver, TcpStreamWrapper::new);

        Ok(MorayClient {
            connection_pool: pool,
        })
    }

    pub fn from_parts<I: Into<IpAddr>>(
        ip: I,
        port: u16,
        log: Logger,
        opts: Option<ConnectionPoolOptions>,
    ) -> Result<MorayClient, Error> {
        Self::new(SocketAddr::new(ip.into(), port), log, opts)
    }

    pub fn list_buckets<F>(
        &mut self,
        opts: buckets::MethodOptions,
        bucket_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&buckets::Bucket) -> Result<(), Error>,
    {
        let mut conn = self
            .connection_pool
            .claim()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        buckets::get_list_buckets(
            &mut (*conn).deref_mut(),
            "",
            opts,
            buckets::Methods::List,
            bucket_handler,
        )
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
        let mut conn = self
            .connection_pool
            .claim()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        buckets::get_list_buckets(
            &mut (*conn).deref_mut(),
            name,
            opts,
            buckets::Methods::Get,
            bucket_handler,
        )
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
        let mut conn = self
            .connection_pool
            .claim()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        objects::get_find_objects(
            &mut (*conn).deref_mut(),
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
        let mut conn = self
            .connection_pool
            .claim()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        objects::get_find_objects(
            &mut (*conn).deref_mut(),
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
        let mut conn = self
            .connection_pool
            .claim()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        objects::put_object(
            &mut (*conn).deref_mut(),
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
        buckets::create_bucket(
            &mut self
                .connection_pool
                .claim()
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                .deref_mut(),
            name,
            config,
            opts,
        )
    }

    pub fn batch<F>(
        &mut self,
        requests: &[objects::BatchRequest],
        opts: &objects::MethodOptions,
        object_handler: F,
    ) -> Result<(), Error>
    where
        F: FnMut(Vec<Value>) -> Result<(), Error>,
    {
        let mut conn = self
            .connection_pool
            .claim()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        objects::batch(&mut (*conn).deref_mut(), requests, opts, object_handler)
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
        meta::sql(
            &mut self
                .connection_pool
                .claim()
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                .deref_mut(),
            stmt,
            vals,
            opts,
            query_handler,
        )
    }

    pub fn from_str(
        s: &str,
        log: Logger,
        opts: Option<ConnectionPoolOptions>,
    ) -> Result<MorayClient, Error> {
        let addr = SocketAddr::from_str(s).expect("Error parsing address");
        Self::new(addr, log, opts)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn placeholder() {
        assert_eq!(1, 1);
    }
}
