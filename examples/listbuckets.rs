/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::buckets;
use moray::client::MorayClient;

use std::io::Error;
use std::net::SocketAddr;
use std::str::FromStr;

fn client_fromstr(
    addr: &str,
    opts: buckets::MethodOptions,
) -> Result<(), Error> {
    let mut mclient = MorayClient::from_str(addr).unwrap();
    mclient.list_buckets(opts, |b| {
        dbg!(&b);
        Ok(())
    })
}

fn client_sockaddr(
    sockaddr: SocketAddr,
    opts: buckets::MethodOptions,
) -> Result<(), Error> {
    let mut mclient = MorayClient::new(sockaddr).unwrap();
    mclient.list_buckets(opts, |b| {
        dbg!(&b);
        Ok(())
    })
}

fn client_fromparts(
    ip: [u8; 4],
    port: u16,
    opts: buckets::MethodOptions,
) -> Result<(), Error> {
    let mut mclient = MorayClient::from_parts(ip, port).unwrap();
    mclient.list_buckets(opts, |b| {
        dbg!(&b);
        Ok(())
    })
}

fn client_reconnect(
    addr: SocketAddr,
    opts: buckets::MethodOptions,
) -> Result<(), Error> {
    let mut mclient = MorayClient::new(addr).unwrap();
    let mut count: u64 = 0;
    mclient.list_buckets(opts.clone(), |_| {
        count += 1;
        Ok(())
    })?;

    println!("Found {} buckets before reconnect", count);
    println!("Reconnecting");

    mclient = mclient.reconnect()?;

    let mut after_count = 0;
    match mclient.list_buckets(opts.clone(), |_| {
        after_count += 1;
        Ok(())
    }) {
        Ok(()) => {
            println!("Found {} buckets after reconnect", after_count);
            assert_eq!(count, after_count, "match counts after reconnect");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    let i: Vec<String> = ip_arr.iter().map(|o| o.to_string()).collect();
    let ip = i.join(".");
    let addr = format!("{}:{}", ip, port.to_string().as_str());

    let opts = buckets::MethodOptions::default();
    println!("MorayClient from_str");
    client_fromstr(addr.as_str(), opts.clone())?;

    println!("MorayClient SocketAddr");
    let sockaddr = SocketAddr::from_str(addr.as_str()).unwrap();
    client_sockaddr(sockaddr.clone(), opts.clone())?;

    println!("MorayClient from_parts");
    client_fromparts(ip_arr, port, opts.clone())?;

    println!("MorayClient reconnect");
    client_reconnect(sockaddr, opts)?;
    println!("MorayClient reconnect success");
    Ok(())
}
