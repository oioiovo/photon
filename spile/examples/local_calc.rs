//! 单进程全链路：client stub → session → dispatch → codec → 回包。
//!
//! 一条命令跑完框架在**一条连接**上能做的事，每一步都自带断言：
//!
//! ```text
//! cargo run --example local_calc
//! ```
//!
//! 覆盖五组：
//!
//! 1. **四种调用形态**：不可失败方法、业务拒绝、32 并发（同一连接上多路复用）、
//!    通知（无回包通道）。
//! 2. **三档声明形态**：`async fn`、同步 `fn`（就地在 worker 上跑完）、
//!    `#[spile(blocking)]` 目标 + 同步客户端（自带线程与 runtime，调用点不写 `await`）。
//! 3. **两个框架能力**：拦截器（客户端链 + 服务端链，逐方法计数）、入站配额
//!    （`max_async_in_flight = 1` 时第二条调用拿 `PeerRejected{code:1}`）。
//! 4. **关闭语义**：留一个在飞请求再 `close()`，`close()` 不返回直到 handler 清空。
//!
//! 失败会打印「期望 / 实际」并以退出码 1 收尾（见 `support/check.rs`）。

#[path = "support/check.rs"]
mod check;
// 单进程示例同时扮演两端，所以接口与实现都引；两进程示例里客户端只引 `api.rs`。
#[path = "calc_service/api.rs"]
mod calc;
#[path = "calc_service/imp.rs"]
mod calc_imp;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use calc::{CalcBlockingClient, CalcClient, describe, describe_call, register_calc};
use calc_imp::Calculator;
use check::Check;
use spile::{
    BoxFuture, Bytes, CallError, CallFailure, ClientCall, ClientInterceptor, ClientNext,
    HandlerError, RpcError, ServerCall, ServerInterceptor, ServerNext, ServiceRegistry, Session,
    SessionOptions, transport::memory::Memory,
};

/// 逐方法计数。拦截器只做一件事：把「这条调用穿过了链」记下来。
///
/// 客户端链与服务端链各持一份，两个数字对不上就说明某一侧的链没被调用——
/// 这正是拦截器最容易被写错的地方（比如漏了通知或漏了业务拒绝路径）。
#[derive(Default)]
struct Counts {
    total: AtomicU64,
    add: AtomicU64,
    div: AtomicU64,
    slow: AtomicU64,
    hash: AtomicU64,
}

impl Counts {
    fn bump(&self, method: Option<&'static str>) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let slot = match method {
            Some("add") => &self.add,
            Some("div") => &self.div,
            Some("slow_op") => &self.slow,
            Some("fast_hash") => &self.hash,
            // 通知与将来的方法都不单独记：`total` 已经覆盖。
            _ => return,
        };
        slot.fetch_add(1, Ordering::Relaxed);
    }

    fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    fn slow(&self) -> u64 {
        self.slow.load(Ordering::Relaxed)
    }
}

struct CountClient(Arc<Counts>);

impl ClientInterceptor for CountClient {
    fn intercept<'a>(
        &'a self,
        call: ClientCall,
        next: ClientNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
        // 先记账再往下走：`next.run` 会消费掉链位置，之后就取不到方法名了。
        self.0.bump(call.method_name());
        next.run(call)
    }
}

struct CountServer(Arc<Counts>);

impl ServerInterceptor for CountServer {
    fn intercept<'a>(
        &'a self,
        call: ServerCall,
        next: ServerNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
        // 记在**进入 handler 之前**：被期限丢掉的那次调用也算「到过服务端」。
        self.0.bump(Some(call.method_name()));
        next.run(call)
    }
}

/// FNV-1a 64。示例自己算一遍，用来验证「同一份 payload 穿过线路后没变」。
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut check = Check::new("local_calc");

    // 两条队列合成的双向连接：没有端点、没有 connect/accept，但线格式与真连接
    // 完全同一条路径（L2/L3 一行不改）。
    let (client_halves, server_halves) = Memory::pair();

    let mut registry = ServiceRegistry::empty();
    register_calc(&mut registry, Arc::new(Calculator))?;

    let client_counts = Arc::new(Counts::default());
    let server_counts = Arc::new(Counts::default());

    let (server_session, _server_handle) = Session::new(
        server_halves.0,
        server_halves.1,
        Arc::new(registry),
        SessionOptions::default().server_interceptor(CountServer(Arc::clone(&server_counts))),
    )?;
    println!("[server] listening on memory (in-process pair, no endpoint)");

    let (client_session, client_handle) = Session::new(
        client_halves.0,
        client_halves.1,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default().client_interceptor(CountClient(Arc::clone(&client_counts))),
    )?;
    client_session.wait_ready().await?;
    let client = CalcClient::new(client_handle.clone());

    // ---- 1. 四种调用形态 -------------------------------------------------
    check.section("四种调用形态");

    let sum = client.add(2, 3).await?;
    check.eq("add(2, 3) = 5（不可失败方法）", sum, 5);

    match client.div(1, 0).await {
        Err(CallError::Rejected(calc::CalcError::DivideByZero)) => {
            check.expect("div(1, 0) → 业务拒绝 DivideByZero", true, "");
        }
        Ok(value) => check.expect("div(1, 0) → 业务拒绝", false, format!("居然成功：{value}")),
        Err(other) => check.expect("div(1, 0) → 业务拒绝", false, describe_call(&other)),
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
    check.eq("32 并发 add：回包不串号", mismatched, 0);

    // 通知：不插 pending、不占出站配额，`Ok` 只代表帧进了队列。
    match client
        .handle()
        .notify(calc::service_id(), calc::ping_method(), Bytes::new())
        .await
    {
        Ok(()) => check.expect("notify ping 发出即成功（没有回包可等）", true, ""),
        Err(err) => check.expect("notify ping 发出即成功", false, describe(&err)),
    }

    // ---- 2. 三档声明形态 ------------------------------------------------
    check.section("三档声明形态：async / 同步 fn / blocking");

    // 超时：期限挂在客户端上，`slow_op(200)` 在 50 ms 处被本端放弃，
    // 服务端自己的 deadline 也是 50 ms，于是 handler 一并被丢掉。
    let strict = CalcClient::new(client.handle().clone()).with_deadline(50);
    match strict.slow_op(200).await {
        Err(RpcError::Timeout) => check.expect("slow_op(200) 在 50 ms 处被本端放弃", true, ""),
        Ok(ms) => check.expect("slow_op(200) 应当超时", false, format!("回了 Ok({ms})")),
        Err(err) => check.expect("slow_op(200) 应当超时", false, describe(&err)),
    }

    // 同步 handler：声明是 `fn`，stub 仍是 async（线上永远异步）。
    let data = b"spile".to_vec();
    let hash = client.fast_hash(data.clone()).await?;
    check.eq(
        "fast_hash 与本地同算法一致（同步 handler 就地跑完）",
        hash,
        fnv1a(&data),
    );

    // 同步客户端：底层是一条自带线程与 runtime 的会话，所以调用点既不需要 runtime
    // 也不需要 `await`。这里另开一对半身，服务端跑在一条 std 线程上——这正好是
    // 「同步库被 async 程序调用」的形状。
    let ((sync_server_src, sync_server_sink), (sync_client_src, sync_client_sink)) = Memory::pair();
    let mut sync_registry = ServiceRegistry::empty();
    register_calc(&mut sync_registry, Arc::new(Calculator))?;
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let sync_server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("同步服务端 runtime");
        rt.block_on(async move {
            let (session, _handle) = Session::new(
                sync_server_src,
                sync_server_sink,
                Arc::new(sync_registry),
                SessionOptions::default(),
            )
            .expect("同步服务端会话");
            session.wait_ready().await.expect("握手");
            let _ = stop_rx.await;
            let _ = session.close().await;
        });
    });

    let blocking = CalcBlockingClient::new(spile::blocking::BlockingSession::from_parts(
        sync_client_src,
        sync_client_sink,
        SessionOptions::default(),
    )?);
    check.eq(
        "blocking add(20, 22) = 42（无 runtime、无 await）",
        blocking.add(20, 22)?,
        42,
    );

    // 外层期限：只保护调用方这一侧（对端不回时不会一直挂着）。
    match blocking
        .with_timeout(Duration::from_millis(30))
        .slow_op(200)
    {
        Err(RpcError::Timeout) => {
            check.expect("blocking slow_op 在 30 ms 处放弃（本端判）", true, "");
        }
        Ok(ms) => check.expect("blocking slow_op 应当超时", false, format!("回了 Ok({ms})")),
        Err(err) => check.expect("blocking slow_op 应当超时", false, describe(&err)),
    }
    let _ = stop_tx.send(());
    sync_server.join().expect("同步服务端线程");

    // ---- 3. 入站配额 ---------------------------------------------------
    check.section("入站配额：max_async_in_flight = 1");

    let ((quota_client_src, quota_client_sink), (quota_server_src, quota_server_sink)) =
        Memory::pair();
    let mut quota_registry = ServiceRegistry::empty();
    register_calc(&mut quota_registry, Arc::new(Calculator))?;
    let (_quota_server, _quota_server_handle) = Session::new(
        quota_server_src,
        quota_server_sink,
        Arc::new(quota_registry),
        // 只留一个异步 handler 名额：第二条请求进不了 handler，应当被回 code=1。
        SessionOptions {
            max_async_in_flight: 1,
            ..SessionOptions::default()
        },
    )?;
    let (quota_client_session, quota_handle) = Session::new(
        quota_client_src,
        quota_client_sink,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )?;
    quota_client_session.wait_ready().await?;
    let quota_client = CalcClient::new(quota_handle.clone());

    let in_flight = tokio::spawn({
        let client = CalcClient::new(quota_handle.clone());
        async move { client.slow_op(120).await }
    });
    // 等到第一条确实进了 handler（占住唯一的名额），再发第二条。
    tokio::time::sleep(Duration::from_millis(20)).await;
    match quota_client.slow_op(1).await {
        Err(RpcError::PeerRejected { code: 1, .. }) => {
            check.expect("第二条调用被配额挡下（对端回 code=1 overloaded）", true, "");
        }
        Ok(ms) => check.expect("第二条调用应被挡下", false, format!("回了 Ok({ms})")),
        Err(err) => check.expect("第二条调用应被挡下", false, describe(&err)),
    }
    check.eq("在飞的第一条照常完成", in_flight.await??, 120);

    // ---- 4. 拦截器账本 -------------------------------------------------
    // 到这里为止的调用都穿过两条链，故意用精确值：链漏了某条路径会立即红。
    // 客户端：add(2,3) 1 + div 1 + 32 并发 add + slow_op(200) 1 + fast_hash 1 = 36
    // 服务端：同样 36（通知不回包，但 handler 照跑），再多 1 条 ping = 37
    check.section("拦截器：逐方法账本");
    check.eq("客户端链总次数", client_counts.total(), 36);
    check.eq("服务端链总次数（多一条通知）", server_counts.total(), 37);
    check.eq("客户端链 slow_op 次数", client_counts.slow(), 1);
    check.eq("服务端链 slow_op 次数", server_counts.slow(), 1);

    // ---- 5. 优雅关闭 ---------------------------------------------------
    check.section("优雅关闭：在飞请求清空后才收尾");

    // 留一个在飞的请求，然后由服务端宣告关闭；
    // `close()` 不返回直到 handler 清空，回包也在这之前进了 writer。
    let inflight = tokio::spawn({
        let client = CalcClient::new(client.handle().clone());
        async move { client.slow_op(30).await }
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    server_session.close().await?;
    check.eq("宣告关闭时在飞的请求仍然拿到了回包", inflight.await??, 30);
    println!("[server] shutdown (in-flight drained)");

    client.handle().close().await?;

    // 关闭之后那条调用同样走过了客户端链，账本 +1；服务端那条也在关闭前进了 handler。
    check.eq("客户端链最终总次数", client_counts.total(), 37);
    check.eq("服务端链最终总次数", server_counts.total(), 38);

    check.finish();
    Ok(())
}
