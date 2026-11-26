//! 两进程示例的服务端：`cargo run --example calc_server -- [addr]`（默认 `127.0.0.1:7001`）。
//! 真实发布形态请用 `examples/calc-split/`（契约 / 服务端 / 客户端三个 crate）；这里的同一个
//! example target 是「单 package 内最小两进程」，`calc_client` 之所以能只引 `api.rs`，靠的是模块拆分。

// 只有接口半：业务实现不参与客户端的编译，见 `calc_service/api.rs` 顶部说明。
#[path = "calc_service/api.rs"]
mod calc;
// 实现半只在这里、只被服务端引入。
#[path = "calc_service/imp.rs"]
mod calc_imp;

use std::net::SocketAddr;
use std::sync::Arc;

use calc::register_calc;
use calc_imp::Calculator;
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
