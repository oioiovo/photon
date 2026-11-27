//! M4 的第一件东西是**尺子**，不是优化：在同一条会话上量「一次往返要多久」。
//!
//! 不引基准框架（`criterion` 会带进一整棵 dev 依赖树，而这里要的只是
//! p50 / p90 / p99 与 QPS）：用 `Instant` 采样 + 自己排序算分位数。
//!
//! 用法：
//! ```text
//! cargo run --release --example bench
//! cargo run --release --example bench -- tcp 1024 20000 128 0
//! ```
//!
//! 五个位置参数：传输（`memory` / `tcp` / `all`）、payload 字节数、次数、并发数、
//! 请求期限（毫秒，`0` = 不设期限）。**数字只在同一台机器上横比有意义**，
//! 别和文档里的绝对数对线。

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;

use spile::transport::memory::Memory;
use spile::transport::tcp::TcpTransport;
use spile::transport::{Endpoint, MessageSink, MessageSource, Transport, TransportOptions};
use spile::{
    BoxFuture, HandlerError, Service, ServiceRegistry, Session, SessionHandle, SessionOptions,
};

/// 示例服务：原样回显。只在 `service_id = 7`、`method = 1` 上注册。
const SERVICE_ID: u16 = 7;
const METHOD: u64 = 1;
/// 预热次数。第一次调用要等握手 `Ready`、第一次 `dispatch` 要建表，混进样本会污染 p99。
const WARMUP: usize = 200;
/// mpsc 队列深度，与 `memory` 传输的实现常量同量级，免得下限被容量差异影响。
const QUEUE: usize = 1024;

struct Echo;

impl Service for Echo {
    fn call(
        self: Arc<Self>,
        _method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, HandlerError>> {
        Box::pin(async move { Ok(payload) })
    }

    fn method_name(&self, _method: u64) -> Option<&'static str> {
        Some("echo")
    }
}

fn registry() -> Arc<ServiceRegistry> {
    let mut reg = ServiceRegistry::empty();
    reg.insert(SERVICE_ID, Arc::new(Echo))
        .expect("注册不应撞号");
    Arc::new(reg)
}

fn payload_of(len: usize) -> Bytes {
    if len == 0 {
        Bytes::new()
    } else {
        Bytes::from(vec![0xAB; len])
    }
}

/// 分位数（`q` 取 0.0–1.0）。排序一次、查表一次，样本数小到不值得用直方图。
fn percentile(sorted: &[u128], q: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() - 1) as f64) * q).round() as usize;
    sorted[idx]
}

fn us(nanos: u128) -> f64 {
    nanos as f64 / 1000.0
}

fn us_f(nanos: f64) -> f64 {
    nanos / 1000.0
}

fn mean_of(samples: &[u128]) -> f64 {
    samples.iter().sum::<u128>() as f64 / samples.len() as f64
}

/// 顺序往返的**采样本体**：每次 `await` 完再发下一次。
///
/// 注意调用它所在的线程：在 `#[tokio::main]` 的阻塞主线程上跑，每次 `await`
/// 之后线程会真的 park，被唤醒要走一次 futex（本机约 +6 µs）；在 worker 线程
/// 里跑则只在本地队列里换任务。两者都要量，因为用户可能从任意上下文发起调用。
async fn seq_body<S, K>(
    handle: &SessionHandle<S, K>,
    payload_len: usize,
    iters: usize,
    deadline_ms: u32,
) -> Vec<u128>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    let payload = payload_of(payload_len);
    for _ in 0..WARMUP {
        handle
            .call(SERVICE_ID, METHOD, payload.clone(), deadline_ms)
            .await
            .expect("预热调用失败");
    }

    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let reply = handle
            .call(SERVICE_ID, METHOD, payload.clone(), deadline_ms)
            .await
            .expect("调用失败");
        let dt = t0.elapsed().as_nanos();
        assert_eq!(reply, payload, "回显内容必须一致，否则量的是别的东西");
        samples.push(dt);
    }
    black_box(&samples);
    samples
}

/// 理论下限：两条 mpsc + 一个只做回显的 task，**完全不经过 Session / L1 / L2**。
///
/// 差值（`memory` 单跳 − 这一行）就是框架自己（头编解码、pending 表、配额、
/// 每请求 spawn、span）的成本。没有这把尺子，5 µs 到底算好还是坏无从判断。
async fn raw_baseline(payload_len: usize, iters: usize) -> Vec<u128> {
    let (c2s_tx, mut c2s_rx) = mpsc::channel::<Bytes>(QUEUE);
    let (s2c_tx, mut s2c_rx) = mpsc::channel::<Bytes>(QUEUE);
    tokio::spawn(async move {
        while let Some(msg) = c2s_rx.recv().await {
            if s2c_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    let payload = payload_of(payload_len);
    for _ in 0..WARMUP {
        c2s_tx.send(payload.clone()).await.expect("send");
        s2c_rx.recv().await.expect("recv");
    }

    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        c2s_tx.send(payload.clone()).await.expect("send");
        let reply = s2c_rx.recv().await.expect("recv");
        samples.push(t0.elapsed().as_nanos());
        assert_eq!(reply, payload);
    }
    samples
}

/// 拆账用的第二把尺子：**每次迭代都 `spawn` 一个空 task 并 `await` 它**。
///
/// 服务端每请求都要 spawn 一次（外层兜 panic、里层跑 handler），所以「spawn + join」
/// 的单价必须单独知道，否则会话的 5 µs 里哪一块是它、哪一块是框架逻辑说不清。
async fn spawn_baseline(iters: usize) -> Vec<u128> {
    for _ in 0..WARMUP {
        tokio::spawn(async {}).await.expect("join");
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        tokio::spawn(async {}).await.expect("join");
        samples.push(t0.elapsed().as_nanos());
    }
    samples
}

/// 并发吞吐：`conc` 条独立请求流共用一条会话，量墙钟时间。
/// 每次调用仍然按 id 配对，因此这里也顺带证明「高并发下 pending 表不错配」。
async fn conc<S, K>(
    handle: &SessionHandle<S, K>,
    payload_len: usize,
    iters: usize,
    conc: usize,
    deadline_ms: u32,
) -> (f64, usize)
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    let per_task = (iters / conc).max(1);

    let t0 = Instant::now();
    let mut tasks = Vec::with_capacity(conc);
    for _ in 0..conc {
        let handle = handle.clone();
        let payload = payload_of(payload_len);
        tasks.push(tokio::spawn(async move {
            for _ in 0..per_task {
                let reply = handle
                    .call(SERVICE_ID, METHOD, payload.clone(), deadline_ms)
                    .await
                    .expect("并发调用失败");
                assert_eq!(reply, payload);
            }
        }));
    }
    for task in tasks {
        task.await.expect("并发任务 panic");
    }
    let wall = t0.elapsed().as_secs_f64();
    (wall, per_task * conc)
}

fn print_row(label: &str, samples: &[u128]) {
    println!(
        "{label} p50={:>7.2}µs p90={:>7.2}µs p99={:>7.2}µs mean={:>7.2}µs  ≈{:>9.0} 次/秒",
        us(percentile(samples, 0.50)),
        us(percentile(samples, 0.90)),
        us(percentile(samples, 0.99)),
        us_f(mean_of(samples)),
        1.0e9 / mean_of(samples),
    );
}

#[allow(clippy::too_many_arguments)]
async fn measure<S, K>(
    label: &str,
    server: Arc<Session<S, K>>,
    handle: SessionHandle<S, K>,
    payload_len: usize,
    iters: usize,
    conc_n: usize,
    deadline_ms: u32,
    with_raw: bool,
) where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    // ① worker 线程内：这是「框架自身的往返成本」。
    let mut worker = {
        let handle = handle.clone();
        tokio::spawn(async move { seq_body(&handle, payload_len, iters, deadline_ms).await })
            .await
            .expect("采样任务 panic")
    };
    worker.sort_unstable();

    // ② 阻塞主线程上：多付的是一次线程 park/unpark，不是框架开销。
    let mut on_main = seq_body(&handle, payload_len, iters, deadline_ms).await;
    on_main.sort_unstable();

    println!("\n=== {label} · payload {payload_len} B · deadline {deadline_ms} ms · 1 条会话 ===");
    print_row("顺序(worker) ", &worker);
    print_row("顺序(主线程) ", &on_main);

    let (wall, done) = conc(&handle, payload_len, iters, conc_n, deadline_ms).await;
    println!(
        "并发     c={conc_n:<3} n={done:<6} wall={wall:>7.3}s  ≈{:>9.0} 次/秒",
        done as f64 / wall,
    );

    if with_raw {
        let mut raw = tokio::spawn(async move { raw_baseline(payload_len, iters).await })
            .await
            .expect("下限采样任务 panic");
        raw.sort_unstable();
        print_row("裸 mpsc 下限", &raw);

        let mut spawn_cost = tokio::spawn(async move { spawn_baseline(iters).await })
            .await
            .expect("spawn 采样任务 panic");
        spawn_cost.sort_unstable();
        print_row("空 spawn+join", &spawn_cost);
    }

    // 会话要活到最后一行打印完：`Session::drop` 会宣告关闭并取消两个 task。
    drop(handle);
    server.close().await.ok();
    black_box(&server);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let target = args.next().unwrap_or_else(|| "all".into());
    let payload_len: usize = args
        .next()
        .map_or(64, |v| v.parse().expect("payload 字节数"));
    let iters: usize = args.next().map_or(5_000, |v| v.parse().expect("次数"));
    let conc_n: usize = args.next().map_or(64, |v| v.parse().expect("并发数"));
    let deadline_ms: u32 = args.next().map_or(250, |v| v.parse().expect("期限毫秒"));
    let with_raw = std::env::var("BENCH_RAW").is_ok_and(|v| v != "0");

    println!(
        "spile 往返基准：transport={target} payload={payload_len}B iters={iters} \
         conc={conc_n} deadline={deadline_ms}ms raw_baseline={with_raw} \
         （release；数字只用于同机横比）"
    );

    if target == "memory" || target == "all" {
        let ((c_src, c_sink), (s_src, s_sink)) = Memory::pair();
        let (server, _sh) = Session::new(s_src, s_sink, registry(), SessionOptions::default())?;
        let (client, handle) = Session::new(
            c_src,
            c_sink,
            Arc::new(ServiceRegistry::empty()),
            SessionOptions::default(),
        )?;
        client.wait_ready().await?;
        measure(
            "memory（同进程、两条 mpsc）",
            server,
            handle,
            payload_len,
            iters,
            conc_n,
            deadline_ms,
            with_raw,
        )
        .await;
        client.close().await.ok();
    }

    if target == "tcp" || target == "all" {
        let topts = TransportOptions::default();
        let listener = TcpTransport::bind(
            &Endpoint::Tcp("127.0.0.1:0".parse().expect("字面地址合法")),
            &topts,
        )
        .await?;
        let addr = listener.local_addr()?;
        let accept_opts = topts.clone();
        let accept = tokio::spawn(async move { listener.accept(&accept_opts).await });

        let (c_src, c_sink) = TcpTransport::connect(&Endpoint::Tcp(addr), &topts).await?;
        let (s_src, s_sink) = accept.await??;

        let (server, _sh) = Session::new(s_src, s_sink, registry(), SessionOptions::default())?;
        let (client, handle) = Session::new(
            c_src,
            c_sink,
            Arc::new(ServiceRegistry::empty()),
            SessionOptions::default(),
        )?;
        client.wait_ready().await?;
        measure(
            &format!("tcp {addr}（回环）"),
            server,
            handle,
            payload_len,
            iters,
            conc_n,
            deadline_ms,
            false,
        )
        .await;
        client.close().await.ok();
    }

    // 让 `close()` 的收尾 task 有机会跑完，否则最后几行日志可能来不及打。
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
