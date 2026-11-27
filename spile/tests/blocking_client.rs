//! M3 同步客户端验收：`<Svc>BlockingClient` + `BlockingSession`。
//!
//! 这里全部是普通 `#[test]`（除了最后一个故意反着来）：同步客户端最大的价值就是
//! 「调用方完全不需要 runtime」，用 `#[tokio::test]` 测它等于把最容易出问题的
//! 场景绕过去了。
//!
//! 服务端一律跑在一条独立的 std 线程 + 自己的 runtime 上：同步客户端自带 runtime，
//! 两边都不能借测试线程的 executor。

#![cfg(feature = "memory")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use spile::blocking::BlockingSession;
use spile::session::{ServiceRegistry, Session, SessionOptions};
use spile::transport::memory::{Memory, MemorySink, MemorySource};
use spile::{CallError, RpcError};

/// 外层期限给足余量：用例失败时要变红，不能挂住。
const SHORT: Duration = Duration::from_millis(30);

#[spile::service(id = 21, blocking)]
trait SyncCalc {
    async fn add(&self, a: i64, b: i64) -> i64;
    async fn sleep_ms(&self, ms: u64) -> u64;
    /// 永不返回：只可能因为对端取消而结束，用来观察取消是否真的到了服务端。
    async fn hang(&self) -> i64;
    /// 通知目标：没有回包，只数次数。
    async fn note(&self) -> u64;
}

struct SyncCalcImpl {
    dropped: Arc<AtomicBool>,
    notes: Arc<AtomicUsize>,
}

impl SyncCalc for SyncCalcImpl {
    async fn add(&self, a: i64, b: i64) -> i64 {
        a + b
    }

    async fn sleep_ms(&self, ms: u64) -> u64 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        ms
    }

    async fn hang(&self) -> i64 {
        /// future 被 drop（任务被取消）时置位。
        struct Mark(Arc<AtomicBool>);
        impl Drop for Mark {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let _mark = Mark(Arc::clone(&self.dropped));
        std::future::pending::<()>().await;
        unreachable!("pending 不会完成")
    }

    async fn note(&self) -> u64 {
        self.notes.fetch_add(1, Ordering::SeqCst) as u64 + 1
    }
}

/// 业务错误：同步 stub 的 `Rejected` 一臂必须把它原样交回调用方。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum SyncBiz {
    InsufficientFunds,
}

#[spile::service(id = 22, blocking)]
trait SyncLedger {
    async fn withdraw(&self, amount: i64) -> Result<i64, SyncBiz>;
}

struct SyncLedgerImpl;

impl SyncLedger for SyncLedgerImpl {
    async fn withdraw(&self, amount: i64) -> Result<i64, SyncBiz> {
        if amount > 100 {
            return Err(SyncBiz::InsufficientFunds);
        }
        Ok(amount)
    }
}

/// 服务端：一条独立线程 + 自己的 runtime，跑会话直到被叫停。
struct Server {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// 建一对 memory 半身，服务端进线程，客户端交给同步 `BlockingSession`。
fn client_over(
    configure: impl FnOnce(&mut ServiceRegistry) + Send + 'static,
) -> (BlockingSession<MemorySource, MemorySink>, Server) {
    let ((server_src, server_sink), (client_src, client_sink)) = Memory::pair();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("服务端 runtime");
        let mut registry = ServiceRegistry::empty();
        configure(&mut registry);

        rt.block_on(async move {
            let (session, _handle) = Session::new(
                server_src,
                server_sink,
                Arc::new(registry),
                SessionOptions::default(),
            )
            .expect("服务端会话");
            // 先告诉主线程「会话已经在了」再等握手：两边都在等对方先发第一帧的话
            // 会互锁，而握手本身是并发进行的。
            let _ = started_tx.send(());
            session.wait_ready().await.expect("服务端握手");
            let _ = stop_rx.await;
            let _ = session.close().await;
        });
    });

    started_rx.recv().expect("服务端没起来");

    let session = BlockingSession::from_parts(client_src, client_sink, SessionOptions::default())
        .expect("同步客户端");
    (
        session,
        Server {
            stop: Some(stop_tx),
            thread: Some(thread),
        },
    )
}

/// 建客户端，并把服务端句柄一起交出来：用例结束时 `Server` 的 `Drop` 负责叫停。
fn calc_client(
    dropped: &Arc<AtomicBool>,
    notes: &Arc<AtomicUsize>,
) -> (SyncCalcBlockingClient<MemorySource, MemorySink>, Server) {
    let impl_ = Arc::new(SyncCalcImpl {
        dropped: Arc::clone(dropped),
        notes: Arc::clone(notes),
    });
    let (session, server) =
        client_over(move |registry| register_sync_calc(registry, impl_).expect("注册"));
    (SyncCalcBlockingClient::new(session), server)
}

/// 本用例的主体价值：没有任何 runtime 参与，调用照样完成。
#[test]
fn roundtrip_without_runtime() {
    let dropped = Arc::new(AtomicBool::new(false));
    let notes = Arc::new(AtomicUsize::new(0));
    let (client, _server) = calc_client(&dropped, &notes);

    assert_eq!(client.add(100, 200).expect("add"), 300);
    assert_eq!(client.add(-1, 1).expect("add"), 0);
}

/// 外层期限到点：调用方拿到 `Timeout`，且那条请求在服务端被真的取消掉
/// （handler 的 future 被 drop），不是留在后台继续跑完。
#[test]
fn timeout_cancels_handler() {
    let dropped = Arc::new(AtomicBool::new(false));
    let notes = Arc::new(AtomicUsize::new(0));
    let (client, _server) = calc_client(&dropped, &notes);
    let client = client.with_timeout(SHORT);

    let err = client.hang().expect_err("必须超时");
    assert!(
        matches!(err, RpcError::Timeout),
        "期望 Timeout，实际 {err:?}"
    );

    let until = Instant::now() + Duration::from_secs(2);
    while !dropped.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < until,
            "外层超时后 handler 没有被取消：请求大概还挂在 pending 表上"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// 一次超时不能把会话弄坏：pending 表项已被摘掉，后面的调用照常。
#[test]
fn timeout_keeps_session_usable() {
    let dropped = Arc::new(AtomicBool::new(false));
    let notes = Arc::new(AtomicUsize::new(0));
    let (client, _server) = calc_client(&dropped, &notes);
    let client = client.with_timeout(SHORT);

    assert!(matches!(client.hang(), Err(RpcError::Timeout)));
    assert_eq!(client.add(7, 8).expect("超时后仍可调用"), 15);
}

/// 内层期限（协议头 `deadline_ms`）与外层是两条独立的路：只设 deadline 时，
/// 计时由会话按 header 里的数字完成，调用方不需要自己兜。
#[test]
fn deadline_alone_bounds_the_call() {
    let dropped = Arc::new(AtomicBool::new(false));
    let notes = Arc::new(AtomicUsize::new(0));
    let (client, _server) = calc_client(&dropped, &notes);
    let client = client.with_deadline(30);
    assert_eq!(client.timeout(), None, "外层期限应当仍是无限等");

    let start = Instant::now();
    let err = client.sleep_ms(200).expect_err("必须超时");
    // 具体变体取决于「服务端 deadline 与发起侧计时器谁先到点」，两者都在 30 ms 量级，
    // 这是会话层固有的竞争，不在这里钉死：
    // - `PeerRejected{6}` 说明 header 里的期限被对端读到并执行了；
    // - `Timeout` 说明发起侧先响，同样来自 header 里那个数字（本用例没设外层期限）。
    let expired =
        matches!(err, RpcError::Timeout) || matches!(&err, RpcError::PeerRejected { code: 6, .. });
    assert!(expired, "期望 Timeout 或 Remote{{6}}，实际 {err:?}");
    assert!(
        start.elapsed() < Duration::from_millis(150),
        "期限来自 header，150 ms 内必须返回，实际 {:?}",
        start.elapsed()
    );
    assert_eq!(client.deadline_ms(), 30);
}

#[test]
fn notify_is_fire_and_forget() {
    let dropped = Arc::new(AtomicBool::new(false));
    let notes = Arc::new(AtomicUsize::new(0));
    let (client, _server) = calc_client(&dropped, &notes);

    client.note().expect("通知应当被受理");
    let until = Instant::now() + Duration::from_secs(2);
    while notes.load(Ordering::SeqCst) < 1 {
        assert!(Instant::now() < until, "通知没有到达 handler");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn close_then_call_is_closed() {
    let dropped = Arc::new(AtomicBool::new(false));
    let notes = Arc::new(AtomicUsize::new(0));
    let (client, _server) = calc_client(&dropped, &notes);

    // 与异步侧一致：关闭是会话的动作（`handle().close()`），不挂在按服务生成的 stub 上。
    client.session().close().expect("关闭");
    let err = client.add(1, 2).expect_err("关闭后不该还能调用");
    assert!(matches!(err, RpcError::Closed), "期望 Closed，实际 {err:?}");
}

/// 业务错误穿过同步 stub 后仍然是业务错误，而不是被抹成框架错误。
#[test]
fn business_error_survives_sync_stub() {
    let (session, server) = client_over(|registry| {
        register_sync_ledger(registry, Arc::new(SyncLedgerImpl)).expect("注册");
    });
    let client = SyncLedgerBlockingClient::new(session);

    assert_eq!(client.withdraw(50).expect("正常取款"), 50);
    let err = client.withdraw(200).expect_err("超额");
    assert!(
        matches!(err, CallError::Rejected(SyncBiz::InsufficientFunds)),
        "期望 Rejected(InsufficientFunds)，实际 {err:?}"
    );
    drop(server);
}

/// 反着来的一条：同步客户端在 async 上下文里调用**不能 panic**。
///
/// 这是「自建 runtime + 专属线程」相对「`rt.block_on`」的分水岭——后者在这里会
/// 直接 panic `Cannot start a runtime from within a runtime`，而线上往往正是
/// 「同步库被 async 代码调用」才暴露。用 `current_thread` 是为了把最坏情况摆出来：
/// 调用方的 runtime 只有一条线程，被同步调用占住时也没人替它跑。
#[tokio::test(flavor = "current_thread")]
async fn usable_inside_current_thread_runtime() {
    let dropped = Arc::new(AtomicBool::new(false));
    let notes = Arc::new(AtomicUsize::new(0));
    let (client, _server) = calc_client(&dropped, &notes);
    assert_eq!(client.add(21, 21).expect("async 上下文里同步调用"), 42);
}
