//! 两进程本地 IPC：同一个二进制，`server` / `client` 两个角色。
//!
//! ```text
//! cargo run --example calc_local -- server          # 一直 serve
//! cargo run --example calc_local -- client          # 跑完一轮断言就退出
//! cargo run --example calc_local -- server --check  # 收一条连接、验完账本、退出 0
//! ```
//!
//! 地址可以跟在角色后面。Unix 上是 socket 文件路径（默认 `/tmp/spile-calc.sock`，
//! 因为 `sun_path` 只有约 104 字节，路径必须浅）；Windows 上它只是 pipe 名的**种子**
//! （默认 `spile-calc`），两端给同一个字符串就落到同一条 pipe。
//!
//! 与 `examples/calc-split` 的 `calc-server` 业务代码逐行相同，只换了 `Endpoint` 与
//! `Transport` 类型——这就是「换 transport 不改业务代码」的可执行证据。
//!
//! **两端都会自检**：客户端逐条断言调用结果（失败退出 1）；`--check` 模式下服务端
//! 靠服务端拦截器记账，等这条连接被对端 `GoAway` 收掉之后比对条数并退出 0。
//! 两个退出码合起来就是「本地 IPC 这条链路在这个进程对之间是通的」。

// 同一个二进制既是服务端又是客户端，所以两半都引。分成两个程序时，客户端那一半
// 只该引 `api.rs`——那个形态见 `examples/calc-split/`（客户端是独立 crate，依赖树里
// 连服务端都看不到）。
#[path = "calc_service/api.rs"]
mod calc;
#[path = "calc_service/imp.rs"]
mod calc_imp;
#[path = "support/check.rs"]
mod check;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use calc::{CalcClient, describe, describe_call, register_calc};
use calc_imp::Calculator;
use check::Check;
use spile::{
    BoxFuture, Bytes, CallError, ConnectOptions, Endpoint, HandlerError, RpcError, ServerCall,
    ServerInterceptor, ServerNext, ServiceRegistry, Session, SessionOptions, Transport, connect,
    transport::local::{LocalSink, LocalSource, LocalTransport},
    transport::{LocalAddr, TransportOptions},
};

/// 默认地址。Unix 的 `sun_path` 上限是 104 字节，所以路径必须浅；Windows 上这只是
/// pipe 名的种子，短一点更省事（最终名字由后端哈希出来，见下）。
#[cfg(unix)]
const DEFAULT_PATH: &str = "/tmp/spile-calc.sock";
#[cfg(windows)]
const DEFAULT_PATH: &str = "spile-calc";

const CONNECT_BUDGET: Duration = Duration::from_secs(2);
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// 客户端在这一轮里会发起的调用条数。服务端 `--check` 用它当期望值。
///
/// 写死成常量而不是「服务端自己觉得够了就行」：两端对同一个账本的期望必须是同一
/// 个数，否则这条断言就成了「服务端觉得收到几条就算几条」。
const EXPECTED_CALLS: u64 = 4;

/// 按方法名计数。放在两进程示例里，它是**唯一**能把「服务端到底进没进 handler」
/// 变成可判定结论的东西：回包只能证明「有事发生」，账本能证明「哪一条发生过」。
#[derive(Default)]
struct Calls {
    add: AtomicU64,
    div: AtomicU64,
    slow: AtomicU64,
    hash: AtomicU64,
}

impl Calls {
    fn bump(&self, method: Option<&'static str>) {
        let slot = match method {
            Some("add") => &self.add,
            Some("div") => &self.div,
            Some("slow_op") => &self.slow,
            Some("fast_hash") => &self.hash,
            _ => return,
        };
        slot.fetch_add(1, Ordering::Relaxed);
    }

    fn total(&self) -> u64 {
        self.add.load(Ordering::Relaxed)
            + self.div.load(Ordering::Relaxed)
            + self.slow.load(Ordering::Relaxed)
            + self.hash.load(Ordering::Relaxed)
    }

    fn summary(&self) -> String {
        format!(
            "add={} div={} slow_op={} fast_hash={}",
            self.add.load(Ordering::Relaxed),
            self.div.load(Ordering::Relaxed),
            self.slow.load(Ordering::Relaxed),
            self.hash.load(Ordering::Relaxed),
        )
    }
}

struct CountServer(Arc<Calls>);

impl ServerInterceptor for CountServer {
    fn intercept<'a>(
        &'a self,
        call: ServerCall,
        next: ServerNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
        self.0.bump(Some(call.method_name()));
        next.run(call)
    }
}

/// FNV-1a 64。客户端本地算一遍，用来验证 payload 穿过线路后没变。
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
    let mut role = None;
    let mut path = None;
    let mut check_mode = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--check" => check_mode = true,
            "server" | "client" if role.is_none() => role = Some(arg),
            other => path = Some(PathBuf::from(other)),
        }
    }
    let role = role.unwrap_or_else(|| "server".into());
    let path = path.unwrap_or_else(|| PathBuf::from(DEFAULT_PATH));

    match role.as_str() {
        "server" => serve(path, check_mode).await,
        "client" => run_client(path).await,
        other => Err(format!("未知角色 {other:?}，只接受 server / client").into()),
    }
}

async fn serve(path: PathBuf, check_mode: bool) -> Result<(), Box<dyn std::error::Error>> {
    let ep = Endpoint::Local(LocalAddr::new(&path));
    let transport_opts = TransportOptions::default();

    let mut registry = ServiceRegistry::empty();
    register_calc(&mut registry, Arc::new(Calculator))?;
    let registry = Arc::new(registry);

    let calls = Arc::new(Calls::default());
    let session_opts =
        SessionOptions::default().server_interceptor(CountServer(Arc::clone(&calls)));

    // 两个后端的共同规则（见 `transport/local/mod.rs`）：「同一个地址只能有一个活的
    // 监听者」。Unix 上这还包括「清掉上次崩溃留下的 socket 文件，且绝不碰用户文件」；
    // Windows 上名字随句柄消失，所以没有残留问题，只有「已有人在听」这一种拒绝。
    //
    // 这里没用框架的 `Server`，而是手写 accept 循环：`--check` 需要一个「这条连接
    // 什么时候结束」的信号，而 `Server::serve` 刻意不暴露它。
    let listener = LocalTransport::bind(&ep, &transport_opts).await?;
    println!("[server] listening on local://{}", path.display());

    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::channel::<u64>(1);
    loop {
        let (source, sink) = match listener.accept(&transport_opts).await {
            Ok(pair) => pair,
            // 单连接层面的失败不该打死 listener：客户端半路 abort 属于常态。
            Err(err) => {
                eprintln!("[server] accept 失败，继续: {err}");
                continue;
            }
        };

        let (session, _handle) =
            Session::new(source, sink, Arc::clone(&registry), session_opts.clone())?;
        println!("[server] 连接已建立");

        let task_calls = Arc::clone(&calls);
        let closed = closed_tx.clone();
        tokio::spawn(async move {
            // 对端 `GoAway` → 入站清空 → 两个 task 收尾，这里才返回。
            session.wait_closed().await;
            println!("[server] 连接已关闭");
            let _ = session.close().await;
            let _ = closed.send(task_calls.total()).await;
        });

        if check_mode {
            // 只服务一条连接：等它优雅收尾，然后比对账本。
            let Some(total) = closed_rx.recv().await else {
                check::fail("服务端没等到连接收尾", "closed 通道已断开");
            };
            println!("[server] 账本：{}（共 {total} 次）", calls.summary());
            if total != EXPECTED_CALLS {
                check::fail(
                    "服务端收到的调用条数",
                    format!("实际 {total}，期望 {EXPECTED_CALLS}"),
                );
            }
            println!("[server] check ok：{total} 次调用，连接已被对端优雅关闭");
            return Ok(());
        }
    }
}

async fn run_client(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let mut check = Check::new("calc_local");
    let ep = Endpoint::Local(LocalAddr::new(&path));
    let client = connect_with_retry(&ep).await?;
    println!("[client] connected to local://{}", path.display());

    check.section("同一套业务代码，走 local 后端");

    check.eq("add(2, 3)", client.add(2, 3).await?, 5);

    match client.div(1, 0).await {
        Err(CallError::Rejected(calc::CalcError::DivideByZero)) => {
            check.expect("div(1, 0) → 业务拒绝 DivideByZero", true, "");
        }
        Ok(value) => check.expect("div(1, 0) → 业务拒绝", false, format!("居然成功：{value}")),
        Err(other) => check.expect("div(1, 0) → 业务拒绝", false, describe_call(&other)),
    }

    let data = b"spile".to_vec();
    check.eq(
        "fast_hash 与本地同算法一致（payload 穿过线路未变）",
        client.fast_hash(data.clone()).await?,
        fnv1a(&data),
    );

    let strict = CalcClient::new(client.handle().clone()).with_deadline(50);
    match strict.slow_op(200).await {
        Err(RpcError::Timeout) => check.expect("slow_op(200) 在 50 ms 处被本端放弃", true, ""),
        Ok(ms) => check.expect("slow_op(200) 应当超时", false, format!("回了 Ok({ms})")),
        Err(err) => check.expect("slow_op(200) 应当超时", false, describe(&err)),
    }

    // 优雅关闭：这条 `GoAway` 就是服务端 `--check` 等的那条收尾信号。
    client.handle().close().await?;
    check.finish();
    Ok(())
}

async fn connect_with_retry(
    ep: &Endpoint,
) -> Result<CalcClient<LocalSource, LocalSink>, Box<dyn std::error::Error>> {
    // 与 `examples/calc-split` 的 `calc-client` 同样按「重试 + 预算」处理时序：服务端可能晚起几百毫秒。
    let deadline = Instant::now() + CONNECT_BUDGET;
    loop {
        match connect::<LocalTransport>(ep, ConnectOptions::default()).await {
            Ok(handle) => return Ok(CalcClient::new(handle)),
            Err(err) if Instant::now() < deadline => {
                eprintln!("[client] 连接未就绪（{err}），{RETRY_INTERVAL:?} 后重试");
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
