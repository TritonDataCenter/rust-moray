/*
 * Copyright 2019 Joyent, Inc.
 */

use moray::client::MorayClient;
use std::io::Error;
use std::net::SocketAddr;
use std::str::FromStr;

fn client_fromstr(addr: &str) -> Result<(), Error> {
    let mut mclient = MorayClient::from_str(addr).unwrap();
    mclient.list_buckets(|b| {
        dbg!(&b);
        Ok(())
    })
}

fn client_sockaddr(sockaddr: SocketAddr) -> Result<(), Error> {
    let mut mclient = MorayClient::new(sockaddr).unwrap();
    mclient.list_buckets(|b| {
        dbg!(&b);
        Ok(())
    })
}

fn client_fromparts(ip: [u8; 4], port: u16) -> Result<(), Error> {
    let mut mclient = MorayClient::from_parts(ip, port).unwrap();
    mclient.list_buckets(|b| {
        dbg!(&b);
        Ok(())
    })
}

fn main() -> Result<(), Error> {
    let ip_arr: [u8; 4] = [10, 77, 77, 9];
    let port: u16 = 2021;

    let i: Vec<String> = ip_arr.iter().map(|o| o.to_string()).collect();
    let ip = i.join(".");
    let addr = format!("{}:{}", ip, port.to_string().as_str());

    println!("MorayClient from_str");
    client_fromstr(addr.as_str())?;

    println!("MorayClient SocketAddr");
    let sockaddr = SocketAddr::from_str(addr.as_str()).unwrap();
    client_sockaddr(sockaddr)?;

    println!("MorayClient from_parts");
    client_fromparts(ip_arr, port)?;
    Ok(())
}
