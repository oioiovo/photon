//! 两进程示例的服务端：`cargo run --example calc_server -- [addr]`（默认 `127.0.0.1:7001`）。

#[path = "calc_service/mod.rs"]
mod calc;

use std::net::SocketAddr;
use std::sync::Arc;

use calc::{Calculator, register_calc};
use photon::{Endpoint, Server, SessionOptions, TransportOptions, transport::tcp::TcpTransport};

const DEFAULT_ADDR: &str = "127.0.0.1:7001";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = match std::env::args().nth(1) {
        Some(arg) => arg.parse()?,
        None => DEFAULT_ADDR.parse()?,
    };

    let mut server = Server::<TcpTransport>::bind(
        &Endpoint::Tcp(addr),
        TransportOptions::default(),
        SessionOptions::default(),
    )
    .await?;
    register_calc(server.registry(), Arc::new(Calculator))?;

    println!("[server] listening on {addr}");
    server.serve().await?;
    Ok(())
}
