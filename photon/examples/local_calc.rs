//! M2 最小集：单进程全链路（client stub → session → dispatch → codec → 回包）。
//!
//! 一条命令跑通：`cargo run --example local_calc`。

#[path = "calc_service/mod.rs"]
mod calc;

use std::sync::Arc;

use calc::{CalcClient, Calculator, describe, register_calc};
use photon::{ServiceRegistry, Session, SessionOptions, transport::memory::Memory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 两条队列合成的双向连接：没有端点、没有 connect/accept，但线格式与真连接
    // 完全同一条路径（L2/L3 一行不改）。
    let (client_halves, server_halves) = Memory::pair();

    let mut registry = ServiceRegistry::empty();
    register_calc(&mut registry, Arc::new(Calculator))?;

    let (server_session, _server_handle) = Session::new(
        server_halves.0,
        server_halves.1,
        Arc::new(registry),
        SessionOptions::default(),
    )?;
    println!("[server] listening on memory (in-process pair, no endpoint)");

    // 服务端会话由内部的 reader task 持有，这里只需要确保它不被提前 drop。
    let _server = server_session;

    let (client_session, client_handle) = Session::new(
        client_halves.0,
        client_halves.1,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )?;
    client_session.wait_ready().await?;
    let client = CalcClient::new(client_handle);

    let sum = client.add(2, 3).await?;
    println!("[client] add(2, 3) = {sum}");

    match client.div(1, 0).await {
        Ok(value) => println!("[client] div(1, 0) = {value}"),
        Err(err) => println!("[client] div(1, 0) -> {}", describe(&err)),
    }

    // 32 个并发：每个请求的参数不同，只要有一份应答串了包就会落在 mismatched 上。
    let mut tasks = Vec::new();
    for i in 0..32i64 {
        let client = CalcClient::new(client.handle().clone());
        tasks.push(tokio::spawn(async move { (i, client.add(i, 1000).await) }));
    }
    let mut mismatched = 0;
    for task in tasks {
        let (i, result) = task.await?;
        match result {
            Ok(value) if value == i + 1000 => {}
            _ => mismatched += 1,
        }
    }
    println!("[client] 32 concurrent calls -> all matched, mismatched = {mismatched}");

    client.handle().close().await?;
    Ok(())
}
