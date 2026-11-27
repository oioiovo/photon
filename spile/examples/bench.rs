//! M4 的第一件东西是**尺子**，不是优化：在同一条会话上量「一次往返要多久」。
//!
//! 不引基准框架（`criterion` 会带进一整棵 dev 依赖树，而这里要的只是
//! p50 / p90 / p99 与 QPS）：用 `Instant` 采样 + 自己排序算分位数。
//!
//! 用法：
//! ```text
//! cargo run --release --example bench                 # memory + tcp，64 B，5000 次
//! cargo run --release --example bench -- tcp 1024 20000 128
//! cargo run --release --example bench -- memory 0 50000 32
//! ```
//!
//! 三个位置参数分别是：传输（`memory` / `tcp` / `all`）、payload 字节数、单连接次数、
//! 并发数。**数字只在同一台机器上横比有意义**，别和文档里的绝对数对线。

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use spile::transport::memory::Memory;
use spile::transport::tcp::TcpTransport;
use spile::transport::{Endpoint, MessageSink, MessageSource, Transport, TransportOptions};
use spile::{
    BoxFuture, HandlerError, Service, ServiceRegistry, Session, SessionHandle, SessionOptions,
};

/// 示例服务：原样回显。只在 `service_id = 7`、`method = 1` 上注册。
const SERVICE_ID: u16 = 7;
const METHOD: u64 = 1;
/// 期限给足：这里量的是往返，不是超时；250 ms 足够跑完任何一次单跳。
const DEADLINE_MS: u32 = 250;
/// 预热次数。第一次调用要等握手 `Ready`、第一次 `dispatch` 要建表，混进样本会污染 p99。
const WARMUP: usize = 200;

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

/// 顺序往返：量单次延迟。每次调用都 `await` 完再发下一次，所以这是**一跳的响应时间**，
/// 不含排队；批次总时长除以次数就是单连接的吞吐上限。
/// 顺序往返的**采样本体**：每次 `await` 完再发下一次。
///
/// 注意调用它所在的线程：在 `#[tokio::main]` 的阻塞主线程上跑，每次 `await`
/// 之后线程会真的 park，被唤醒要走一次 futex（本机约 +10 µs）；在 worker 线程
/// 里跑则只在本地队列里换任务。两者都要量，因为用户可能从任意上下文发起调用。
async fn seq_body<S, K>(handle: &SessionHandle<S, K>, payload_len: usize, iters: usize) -> Vec<u128>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    let payload = payload_of(payload_len);
    for _ in 0..WARMUP {
        handle
            .call(SERVICE_ID, METHOD, payload.clone(), DEADLINE_MS)
            .await
            .expect("预热调用失败");
    }

    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let reply = handle
            .call(SERVICE_ID, METHOD, payload.clone(), DEADLINE_MS)
            .await
            .expect("调用失败");
        let dt = t0.elapsed().as_nanos();
        assert_eq!(reply, payload, "回显内容必须一致，否则量的是别的东西");
        samples.push(dt);
    }
    black_box(&samples);
    samples
}

/// 并发吞吐：`conc` 条独立请求流共用一条会话，量墙钟时间。
/// 每次调用仍然按 id 配对，因此这里也顺带证明「高并发下 pending 表不错配」。
async fn conc<S, K>(
    handle: &SessionHandle<S, K>,
    payload_len: usize,
    iters: usize,
    conc: usize,
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
                    .call(SERVICE_ID, METHOD, payload.clone(), DEADLINE_MS)
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

async fn measure<S, K>(
    label: &str,
    server: Arc<Session<S, K>>,
    handle: SessionHandle<S, K>,
    payload_len: usize,
    iters: usize,
    conc_n: usize,
) where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    // ① worker 线程内：这是「框架自身的往返成本」。
    let mut worker = {
        let handle = handle.clone();
        tokio::spawn(async move { seq_body(&handle, payload_len, iters).await })
            .await
            .expect("采样任务 panic")
    };
    worker.sort_unstable();
    let mean_w = worker.iter().sum::<u128>() as f64 / worker.len() as f64;

    // ② 阻塞主线程上：多付的是一次线程 park/unpark，不是框架开销。
    let mut on_main = seq_body(&handle, payload_len, iters).await;
    on_main.sort_unstable();
    let mean_m = on_main.iter().sum::<u128>() as f64 / on_main.len() as f64;

    println!("\n=== {label} · payload {payload_len} B · 1 条会话 ===");
    println!(
        "顺序(worker) p50={:>7.2}µs p90={:>7.2}µs p99={:>7.2}µs mean={:>7.2}µs  ≈{:>9.0} 次/秒",
        us(percentile(&worker, 0.50)),
        us(percentile(&worker, 0.90)),
        us(percentile(&worker, 0.99)),
        us_f(mean_w),
        1.0e9 / mean_w,
    );
    println!(
        "顺序(主线程) p50={:>7.2}µs p90={:>7.2}µs p99={:>7.2}µs mean={:>7.2}µs  ≈{:>9.0} 次/秒 ← 含 park/unpark",
        us(percentile(&on_main, 0.50)),
        us(percentile(&on_main, 0.90)),
        us(percentile(&on_main, 0.99)),
        us_f(mean_m),
        1.0e9 / mean_m,
    );

    let (wall, done) = conc(&handle, payload_len, iters, conc_n).await;
    println!(
        "并发  c={conc_n:<3} n={done:<6} wall={wall:>7.3}s  ≈{:>9.0} 次/秒",
        done as f64 / wall,
    );

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

    println!(
        "spile 往返基准：transport={target} payload={payload_len}B iters={iters} conc={conc_n} \
         （release 构建；数字只用于同机横比）"
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
        )
        .await;
        client.close().await.ok();
    }

    // 让 `close()` 的收尾 task 有机会跑完，否则最后几行日志可能来不及打。
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
