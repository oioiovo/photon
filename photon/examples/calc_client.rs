//! 两进程示例的客户端：`cargo run --example calc_client -- [addr]`。
//! 真实发布形态请用 `examples/calc-split/`：那里的 `calc-client` 是独立 crate，依赖树里就没有服务端。
//!
//! 连接是**重试**的：服务端可能比客户端晚几百毫秒起，固定 `sleep` 在不同机器上
//! 会飘（本地绿、CI 红）。最多等 2 s，超时才算失败。

// 客户端**只引接口半**：`Calculator` 的实现不该出现在客户端二进制里。
#[path = "calc_service/api.rs"]
mod calc;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use calc::{CalcClient, describe};
use photon::{
    ConnectOptions, Endpoint, connect,
    transport::tcp::{TcpSink, TcpSource, TcpTransport},
};

const DEFAULT_ADDR: &str = "127.0.0.1:7001";
const CONNECT_BUDGET: Duration = Duration::from_secs(2);
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = match std::env::args().nth(1) {
        Some(arg) => arg.parse()?,
        None => DEFAULT_ADDR.parse()?,
    };

    let client = connect_with_retry(addr).await?;
    println!("[client] connected to {addr}");

    let sum = client.add(2, 3).await?;
    println!("[client] add(2, 3) = {sum}");

    match client.div(1, 0).await {
        Ok(value) => println!("[client] div(1, 0) = {value}"),
        Err(err) => println!("[client] div(1, 0) -> {}", describe(&err)),
    }

    client.handle().close().await?;
    Ok(())
}

async fn connect_with_retry(
    addr: SocketAddr,
) -> Result<CalcClient<TcpSource, TcpSink>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONNECT_BUDGET;
    loop {
        match connect::<TcpTransport>(&Endpoint::Tcp(addr), ConnectOptions::default()).await {
            // 客户端 stub 只泛型化到半身，因此「连接」与「stub」是两步：
            // 换传输只换 `connect::<T>`，stub 与业务代码不动。
            Ok(handle) => return Ok(CalcClient::new(handle)),
            Err(err) if Instant::now() < deadline => {
                eprintln!("[client] 连接未就绪（{err}），{RETRY_INTERVAL:?} 后重试");
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
