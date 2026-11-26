//! 服务端程序：**唯一**知道业务逻辑的地方。
//!
//! `cargo run -p calc-server -- [addr]`（默认 `127.0.0.1:7001`）。

use std::net::SocketAddr;
use std::sync::Arc;

use calc_api::{CODE_DIVIDE_BY_ZERO, Calc};
use photon::transport::tcp::TcpTransport;
use photon::{Endpoint, RemoteError, RpcError, Server, SessionOptions, TransportOptions};

const DEFAULT_ADDR: &str = "127.0.0.1:7001";

struct Calculator;

impl Calc for Calculator {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        Ok(a + b)
    }

    async fn div(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        if b == 0 {
            // 用户码段：框架原样透传，不解释、不改写。
            return Err(RpcError::Remote(RemoteError {
                code: CODE_DIVIDE_BY_ZERO,
                retryable: false,
                message: "divide by zero".into(),
            }));
        }
        Ok(a / b)
    }
}

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
    calc_api::register_calc(server.registry(), Arc::new(Calculator))?;

    println!("[server] listening on {addr}");
    server.serve().await?;
    Ok(())
}
