//! 单进程全链路：client stub → session → dispatch → codec → 回包。
//!
//! M2 的四种形态之外，M3 的四种形态也在这里同一个会话里跑一遍：
//! 通知（无回包）、超时（客户端准、服务端尽力）、同步 handler、优雅关闭
//! （等在飞清空再收尾）。一条命令：`cargo run --example local_calc`。

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

    // 服务端会话由内部的 reader task 持有；这里留个名字，最后用它的 `close()`
    // 演示「宣告关闭 + 等在飞清空」。
    let server = server_session;

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

    // 通知：不插 pending、不占出站配额，`Ok` 只代表帧进了队列。
    client
        .handle()
        .notify(calc::service_id(), calc::ping_method(), bytes::Bytes::new())
        .await?;
    println!("[client] notify ping (no reply)");

    // 超时：期限挂在客户端上，`slow_op(200)` 在 50 ms 处被本端放弃，
    // 服务端自己的 deadline 也是 50 ms，于是 handler 一并被丢掉。
    let strict = CalcClient::new(client.handle().clone()).with_deadline(50);
    match strict.slow_op(200).await {
        Ok(ms) => println!("[client] slow_op -> Ok({ms})  (超时没生效？)"),
        Err(photon::RpcError::Timeout) => println!("[client] slow_op -> Err(Timeout after 50ms)"),
        Err(err) => println!("[client] slow_op -> {}", describe(&err)),
    }

    // 同步 handler：声明是 `fn`，stub 仍是 async（线上永远异步）。
    let hash = client.fast_hash(b"photon".to_vec()).await?;
    println!("[client] fast_hash(data) = {hash:#018x}   (sync handler, 未 await)");

    // 优雅关闭：留一个在飞的请求，然后由服务端宣告关闭；
    // `close()` 不返回直到 handler 清空，回包也在这之前进了 writer。
    let inflight = tokio::spawn({
        let client = CalcClient::new(client.handle().clone());
        async move { client.slow_op(30).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    server.close().await?;
    let _ = inflight.await?;
    println!("[server] shutdown (in-flight drained)");

    client.handle().close().await?;
    Ok(())
}
