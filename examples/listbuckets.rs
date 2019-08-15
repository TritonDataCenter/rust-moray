/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::buckets;
use moray::client::MorayClient;

use slog::{o, Drain, Logger};
use std::io::{Error, ErrorKind};
use std::net::IpAddr;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Mutex;
use trust_dns_resolver::Resolver;

fn client_fromstr(
    addr: &str,
    opts: buckets::MethodOptions,
) -> Result<(), Error> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    let mut mclient = MorayClient::from_str(addr, log, None).unwrap();

    mclient.list_buckets(opts, |b| {
        dbg!(&b);
        Ok(())
    })
}

fn client_sockaddr(
    sockaddr: SocketAddr,
    opts: buckets::MethodOptions,
    log: Logger,
) -> Result<(), Error> {
    let mut mclient = MorayClient::new(sockaddr, log, None).unwrap();
    mclient.list_buckets(opts, |b| {
        dbg!(&b);
        Ok(())
    })
}

fn client_fromparts(
    ip: [u8; 4],
    port: u16,
    opts: buckets::MethodOptions,
    log: Logger,
) -> Result<(), Error> {
    let mut mclient = MorayClient::from_parts(ip, port, log, None).unwrap();
    mclient.list_buckets(opts, |b| {
        dbg!(&b);
        Ok(())
    })
}

fn main() -> Result<(), Error> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    let resolver = Resolver::from_system_conf().unwrap();
    let response = resolver.lookup_ip("1.moray.east.joyent.us")?;
    let ipaddr: Vec<IpAddr> = response.iter().collect();
    dbg!(&ipaddr);
    let ipaddr = ipaddr[0];

    let ip_arr = match ipaddr {
        IpAddr::V4(ip) => ip.octets(),
        _ => {
            return Err(Error::new(ErrorKind::Other, "Need IPv4"));
        }
    };

    let port: u16 = 2021;
    let addr = format!("{}:{}", ipaddr.to_string(), port.to_string().as_str());

    let opts = buckets::MethodOptions::default();
    println!("MorayClient from_str");
    client_fromstr(addr.as_str(), opts.clone())?;

    println!("MorayClient SocketAddr");
    let sockaddr = SocketAddr::from_str(addr.as_str()).unwrap();
    client_sockaddr(sockaddr.clone(), opts.clone(), log.clone())?;

    println!("MorayClient from_parts");
    client_fromparts(ip_arr, port, opts.clone(), log.clone())?;

    Ok(())
}
