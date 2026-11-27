//! 客户端程序：只引契约 crate，看不到任何业务实现。
//!
//! `cargo run -p calc-client -- [addr]`（默认 `127.0.0.1:7001`）。
//!
//! 连接是**重试**的：服务端可能比客户端晚几百毫秒起，固定 `sleep` 在不同机器上会飘。
//! 最多等 2 s，超时才算失败。

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use calc_api::{CalcClient, CalcError, describe};
use spile::{
    CallError, ConnectOptions, Endpoint, connect,
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

    // 这个 crate 的定位是「客户端只依赖契约」的可执行证据，所以结论也要机器可判：
    // 逐条断言，有失败就退出码 1（与 `spile/examples/support/check.rs` 同一口径，
    // 但这里刻意不引 `spile/examples` 下的文件——那是另一个 package 的东西）。
    let mut failures = 0usize;

    let sum = client.add(2, 3).await?;
    if sum == 5 {
        println!("[client] ok   add(2, 3) = 5");
    } else {
        failures += 1;
        println!("[client] FAIL add(2, 3) —— 实际 {sum}，期望 5");
    }

    match client.div(1, 0).await {
        Err(CallError::Rejected(CalcError::DivideByZero)) => {
            println!("[client] ok   div(1, 0) → 业务拒绝 DivideByZero");
        }
        Ok(value) => {
            failures += 1;
            println!("[client] FAIL div(1, 0) —— 居然成功：{value}");
        }
        Err(err) => {
            failures += 1;
            println!("[client] FAIL div(1, 0) —— 实际 {}", describe(&err));
        }
    }

    client.handle().close().await?;

    if failures > 0 {
        println!("[client] {failures} 条失败");
        std::process::exit(1);
    }
    println!("[client] 2/2 全过");
    Ok(())
}

async fn connect_with_retry(
    addr: SocketAddr,
) -> Result<CalcClient<TcpSource, TcpSink>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONNECT_BUDGET;
    loop {
        match connect::<TcpTransport>(&Endpoint::Tcp(addr), ConnectOptions::default()).await {
            // 「连接」与「stub」是两步：换 transport 只换 `connect::<T>`，stub 与业务代码不动。
            Ok(handle) => return Ok(CalcClient::new(handle)),
            Err(err) if Instant::now() < deadline => {
                eprintln!("[client] 连接未就绪（{err}），{RETRY_INTERVAL:?} 后重试");
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
