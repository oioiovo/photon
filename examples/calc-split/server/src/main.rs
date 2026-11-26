//! 服务端程序：**唯一**知道业务逻辑的地方。
//!
//! `cargo run -p calc-server -- [addr]`（默认 `127.0.0.1:7001`）。

use std::net::SocketAddr;
use std::sync::Arc;

use calc_api::{Calc, CalcError};
use photon::transport::tcp::TcpTransport;
use photon::{Endpoint, Server, SessionOptions, TransportOptions};

const DEFAULT_ADDR: &str = "127.0.0.1:7001";

struct Calculator;

impl Calc for Calculator {
    async fn add(&self, a: i64, b: i64) -> i64 {
        a + b
    }

    async fn div(&self, a: i64, b: i64) -> Result<i64, CalcError> {
        if b == 0 {
            // 业务错误原样走线路：框架只加「这是业务拒绝」的标志位，不解释内容。
            return Err(CalcError::DivideByZero);
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
