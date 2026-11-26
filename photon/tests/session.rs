//! L3 会话验收：同一套用例在多个 L1 实现上跑，**用例体只写一份**。
//!
//! 各实现真正不同的只有「怎么把一对半身接起来」（`Harness::connected()`）。
//! 这本身就是一条验收：把 transport 从 `memory` 换成 `local`，`Session` 的代码
//! 一行没动，用例照样全绿。

#![cfg(any(feature = "memory", feature = "local"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use photon::session::{ServiceRegistry, Session, SessionHandle, SessionOptions};
use photon::transport::{Endpoint, LocalAddr, MessageSink, MessageSource, TransportOptions};
use photon::{BoxFuture, RpcError, Schedule, Service};

#[cfg(feature = "local")]
use photon::transport::Transport;
#[cfg(feature = "local")]
use photon::transport::local::{LocalSink, LocalSource, LocalTransport};
#[cfg(feature = "memory")]
use photon::transport::memory::{Memory, MemorySink, MemorySource};

/// 每个用例都带期限：一旦路由错了，应当变红而不是永远挂着。
const CALL_DEADLINE: u32 = 5_000;

/// `Session` 只认 `MessageSource` / `MessageSink`，不认具体传输。
trait Harness {
    type Src: MessageSource + 'static;
    type Snk: MessageSink + 'static;

    /// 返回 `(服务端半身, 客户端半身)`。
    async fn connected() -> ((Self::Src, Self::Snk), (Self::Src, Self::Snk));
}

#[cfg(feature = "memory")]
struct MemoryHarness;

#[cfg(feature = "memory")]
impl Harness for MemoryHarness {
    type Src = MemorySource;
    type Snk = MemorySink;

    async fn connected() -> ((MemorySource, MemorySink), (MemorySource, MemorySink)) {
        Memory::pair()
    }
}

#[cfg(feature = "local")]
struct LocalHarness;

#[cfg(feature = "local")]
impl Harness for LocalHarness {
    type Src = LocalSource;
    type Snk = LocalSink;

    async fn connected() -> ((LocalSource, LocalSink), (LocalSource, LocalSink)) {
        use std::path::PathBuf;
        use std::sync::atomic::{AtomicU16, Ordering};

        // 名字必须唯一：同进程多用例并发跑，撞名会 bind 到别人的 socket。
        static SEQ: AtomicU16 = AtomicU16::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from(format!("/tmp/photon-s{}-{seq}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let ep = Endpoint::Local(LocalAddr::new(&path));
        let listener = LocalTransport::bind(&ep, &TransportOptions::default())
            .await
            .expect("bind");
        let addr = ep.clone();
        let accept = tokio::spawn(async move {
            listener
                .accept(&TransportOptions::default())
                .await
                .expect("accept")
        });

        let client = LocalTransport::connect(&addr, &TransportOptions::default())
            .await
            .expect("connect");
        let server = accept.await.expect("accept task");

        // 已经连上的 fd 不受 unlink 影响，路径现在就清掉，别留垃圾。
        let _ = std::fs::remove_file(&path);
        (server, client)
    }
}

/// 原样回显。`slow` 打开时按 payload 首字节睡 0–18 ms，用来制造乱序。
struct Echo {
    slow: bool,
}

impl Service for Echo {
    fn call(
        self: Arc<Self>,
        _method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, RpcError>> {
        let delay = if self.slow && !payload.is_empty() {
            u64::from(payload[0]) % 7
        } else {
            0
        };
        Box::pin(async move {
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay * 3)).await;
            }
            Ok(payload)
        })
    }
}

/// 只回一个固定的小 ack：用来单独验证请求方向的上限，不被「回显同样大」干扰。
struct Ack;

impl Service for Ack {
    fn call(
        self: Arc<Self>,
        _method: u64,
        _payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, RpcError>> {
        Box::pin(async { Ok(Bytes::from_static(b"ok")) })
    }
}

/// 声明成 `#[photon(blocking)]` 的方法在会话层的形状：占 blocking 额度，
/// 真的在另一条线程上睡。
struct BlockingEcho;

impl Service for BlockingEcho {
    fn call(
        self: Arc<Self>,
        _method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, RpcError>> {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                std::thread::sleep(Duration::from_millis(200));
                Ok(payload)
            })
            .await
            .map_err(|_| RpcError::RuntimeDropped)?
        })
    }

    fn schedule(&self, _method: u64) -> Schedule {
        Schedule::Blocking
    }
}

/// handler 永不返回，并在被丢弃（= 取消）时记一笔。
///
/// 用 `Drop` 而不是「读到某个标志」：future 被 `select!` 抢先返回时就是 drop，
/// 这是唯一能观测「取消真的发生了」的时点。
struct WatchCancel {
    started: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

struct DropMark(Arc<AtomicUsize>);

impl Drop for DropMark {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

impl Service for WatchCancel {
    fn call(
        self: Arc<Self>,
        _method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, RpcError>> {
        let started = Arc::clone(&self.started);
        let dropped = Arc::clone(&self.dropped);
        Box::pin(async move {
            started.fetch_add(1, Ordering::AcqRel);
            let _mark = DropMark(dropped);
            std::future::pending::<()>().await;
            Ok(payload)
        })
    }
}

/// 只计数：验证通知也会进 handler（只是不回包）。
struct Counter(Arc<AtomicUsize>);

impl Service for Counter {
    fn call(
        self: Arc<Self>,
        _method: u64,
        _payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, RpcError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::AcqRel);
            Ok(Bytes::new())
        })
    }
}

/// 调用就 panic：验证「服务实现炸了」也要有回包。
struct Boom;

impl Service for Boom {
    fn call(
        self: Arc<Self>,
        _method: u64,
        _payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, RpcError>> {
        panic!("boom: 服务实现炸了")
    }
}

fn registry_with(service_id: u16, svc: Arc<dyn Service>) -> Arc<ServiceRegistry> {
    let mut reg = ServiceRegistry::empty();
    reg.insert(service_id, svc).expect("注册不应撞号");
    Arc::new(reg)
}

async fn boot<H: Harness>(
    server_reg: Arc<ServiceRegistry>,
    server_opts: SessionOptions,
    client_opts: SessionOptions,
) -> (
    Arc<Session<H::Src, H::Snk>>,
    SessionHandle<H::Src, H::Snk>,
    Arc<Session<H::Src, H::Snk>>,
    SessionHandle<H::Src, H::Snk>,
) {
    let ((s_src, s_sink), (c_src, c_sink)) = H::connected().await;
    let (server, sh) = Session::new(s_src, s_sink, server_reg, server_opts).expect("服务端会话");
    let (client, ch) = Session::new(
        c_src,
        c_sink,
        Arc::new(ServiceRegistry::empty()),
        client_opts,
    )
    .expect("客户端会话");
    (server, sh, client, ch)
}

async fn single_call_roundtrip<H: Harness>() {
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, Arc::new(Echo { slow: false })),
        SessionOptions::default(),
        SessionOptions::default(),
    )
    .await;

    let payload = Bytes::from_static(b"hello");
    let reply = ch
        .call(7, 1, payload.clone(), CALL_DEADLINE)
        .await
        .expect("调用应当成功");
    assert_eq!(reply, payload);

    server.close().await.expect("关闭服务端");
}

async fn hundred_calls_out_of_order<H: Harness>() {
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, Arc::new(Echo { slow: true })),
        SessionOptions::default(),
        SessionOptions::default(),
    )
    .await;

    // 每个请求带不同的首字节，服务端据此延迟不同时长，回包顺序必然打乱。
    let mut calls = Vec::new();
    for i in 0..100u8 {
        let ch = ch.clone();
        calls.push(tokio::spawn(async move {
            let marker = i;
            let reply = ch
                .call(7, 1, Bytes::from(vec![marker, b'x']), CALL_DEADLINE)
                .await
                .expect("调用应当成功");
            assert_eq!(reply[0], marker, "第 {marker} 条回应串包了");
        }));
    }

    for call in calls {
        call.await.expect("调用任务不应 panic");
    }

    server.close().await.expect("关闭服务端");
}

async fn first_frame_must_be_handshake<H: Harness>() {
    let ((a_src, a_sink), (b_src, b_sink)) = H::connected().await;

    let (server, _sh) = Session::new(
        a_src,
        a_sink,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )
    .expect("会话");

    // 绕过会话层，直接塞一条 Request 当首帧。
    let header = photon::proto::Header {
        kind: photon::proto::Kind::Request,
        flags: 0,
        service_id: 7,
        request_id: 1,
        method: 1,
        deadline_ms: 0,
    };
    let mut frame = header.encode();
    frame.extend_from_slice(&[0u8; 4]);
    let mut raw_sink = b_sink;
    raw_sink.send(frame.freeze()).await.expect("raw 发送");

    // 首帧违规必然断连，而且要在很短时间内看出来。
    let closed = tokio::time::timeout(Duration::from_millis(200), async {
        while !server.is_closed() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(closed.is_ok(), "首帧不是 Handshake，会话却没有断开");

    drop(b_src);
    server.close().await.expect("关闭");
}

async fn unknown_service_returns_remote_2<H: Harness>() {
    let (server, _sh, _client, ch) = boot::<H>(
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
        SessionOptions::default(),
    )
    .await;

    let err = ch
        .call(99, 1, Bytes::from_static(b"x"), CALL_DEADLINE)
        .await
        .expect_err("未注册的服务应当报错");
    match err {
        RpcError::Remote(e) => {
            assert_eq!(e.code, 2, "UnknownService");
            assert!(!e.retryable);
        }
        other => panic!("应当是 Remote{{2}}，实际 {other:?}"),
    }

    server.close().await.expect("关闭");
}

async fn handler_panic_returns_internal<H: Harness>() {
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, Arc::new(Boom)),
        SessionOptions::default(),
        SessionOptions::default(),
    )
    .await;

    let err = ch
        .call(7, 1, Bytes::from_static(b"x"), CALL_DEADLINE)
        .await
        .expect_err("服务实现 panic，调用方不该干等到期限");
    match err {
        RpcError::Remote(e) => {
            assert_eq!(e.code, 0, "Internal");
            assert!(!e.retryable);
            assert!(
                e.message.contains("boom"),
                "panic 文本应当带回给调用方：{}",
                e.message
            );
        }
        other => panic!("应当是 Remote{{0}}，实际 {other:?}"),
    }

    // 一条请求 panic 不该把整条连接打死。
    assert!(!server.is_closed(), "panic 之后会话不应断开");

    server.close().await.expect("关闭");
}

async fn close_makes_subsequent_calls_closed<H: Harness>() {
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, Arc::new(Echo { slow: false })),
        SessionOptions::default(),
        SessionOptions::default(),
    )
    .await;

    ch.call(7, 1, Bytes::from_static(b"a"), CALL_DEADLINE)
        .await
        .expect("先能通");
    ch.close().await.expect("关闭客户端");

    let err = ch
        .call(7, 1, Bytes::from_static(b"b"), CALL_DEADLINE)
        .await
        .expect_err("关闭后不应再能调用");
    assert!(matches!(err, RpcError::Closed), "实际 {err:?}");

    server.close().await.expect("关闭服务端");
}

async fn peer_close_fails_new_calls_closed<H: Harness>() {
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, Arc::new(Echo { slow: false })),
        SessionOptions::default(),
        SessionOptions::default(),
    )
    .await;

    ch.call(7, 1, Bytes::from_static(b"a"), CALL_DEADLINE)
        .await
        .expect("先能通");
    server.close().await.expect("关闭服务端");

    // GoAway（以及紧随其后的 EOF）要在一个很短的窗口内让新调用失败，
    // 而不是让调用方一直挂在 pending 表上等一个永远不来的回包。
    let until = tokio::time::Instant::now() + Duration::from_millis(500);
    loop {
        let err = ch
            .call(7, 1, Bytes::from_static(b"after"), 1_000)
            .await
            .expect_err("对端已经关了，不该还能成功");
        if matches!(&err, RpcError::Closed) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "对端关闭 500 ms 后新调用仍是 {err:?}，GoAway 没有传导"
        );
    }
}

async fn peer_declared_limit_governs_sending<H: Harness>() {
    // 服务端声明 1 MiB，客户端仍用默认 64 KiB：客户端发 200 KiB 应当成功，
    // 因为写入上限取的是**对端**声明的值，不是自己的。
    let server_opts = SessionOptions {
        max_message_len: 1024 * 1024,
        ..SessionOptions::default()
    };
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, Arc::new(Ack)),
        server_opts,
        SessionOptions::default(),
    )
    .await;

    // 服务端只回 ack，所以这条用例只压「请求方向」的上限。
    let payload = Bytes::from(vec![0xABu8; 200 * 1024]);
    let reply = ch
        .call(7, 1, payload, CALL_DEADLINE)
        .await
        .expect("应当发得出去");
    assert_eq!(&reply[..], b"ok");

    server.close().await.expect("关闭");
}

/// 两类入站额度互不挤占：阻塞池打满时，异步请求必须照样跑得动。
async fn blocking_quota_is_separate<H: Harness>() {
    let mut registry = ServiceRegistry::empty();
    registry
        .insert(7, Arc::new(BlockingEcho) as Arc<dyn Service>)
        .expect("注册阻塞服务");
    registry
        .insert(8, Arc::new(Ack) as Arc<dyn Service>)
        .expect("注册 ack 服务");

    // 只给 1 个阻塞额度：三次并发阻塞请求里必然有两个当场被拒。
    let server_opts = SessionOptions {
        max_blocking_in_flight: 1,
        ..SessionOptions::default()
    };
    let (server, _sh, _client, ch) =
        boot::<H>(Arc::new(registry), server_opts, SessionOptions::default()).await;

    // 配额检查在 reader 的 dispatch 里同步做，三次请求不可能同时通过。
    let (b1, b2, b3, a) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            ch.call(7, 1, Bytes::new(), CALL_DEADLINE),
            ch.call(7, 1, Bytes::new(), CALL_DEADLINE),
            ch.call(7, 1, Bytes::new(), CALL_DEADLINE),
            ch.call(8, 1, Bytes::new(), CALL_DEADLINE),
        )
    })
    .await
    .expect("额度判断不该让请求挂住");

    let mut served = 0;
    let mut overloaded = 0;
    for reply in [b1, b2, b3] {
        match reply {
            Ok(_) => served += 1,
            Err(RpcError::Remote(remote)) if remote.code == 1 && remote.retryable => {
                overloaded += 1
            }
            other => panic!("阻塞请求只该有「成功」和「过载」两种结果：{other:?}"),
        }
    }
    assert_eq!((served, overloaded), (1, 2), "阻塞额度只有 1 个");

    a.expect("阻塞池打满不该连带拒绝异步请求");
    server.close().await.expect("关闭");
}

/// 超时要在客户端准时暴露，且会话不能因此坏掉。
///
/// 这条**测不出「guard 有没有摘表」**：服务端带着同一个 `deadline_ms`，会在几乎同一
/// 时刻回一个 `Remote{6}`，reader 收到后照样把 entry 摘掉——一个漏掉的泄漏被对端的
/// 回包顺手掩盖。真正能隔离 guard 的用例是下面的 `client_abandon_cancels_handler`
/// （期限为 0，对端永远不会自己结束）。这条留在这里是因为「超时后会话还能用」本身
/// 就值得锁住。
async fn client_timeout_surfaces_and_session_usable<H: Harness>() {
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let service: Arc<dyn Service> = Arc::new(WatchCancel {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    });

    // 只给 1 个异步额度：泄漏一次，下一次就必然 Overloaded。
    let server_opts = SessionOptions {
        max_async_in_flight: 1,
        ..SessionOptions::default()
    };
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, service),
        server_opts,
        SessionOptions::default(),
    )
    .await;

    let err = ch
        .call(7, 1, Bytes::new(), 50)
        .await
        .expect_err("50ms 后必须放弃");
    assert!(matches!(err, RpcError::Timeout), "实际 {err:?}");

    // 第二次仍然是 Timeout 而不是 Overloaded：会话没被上一次超时带坏。
    let again = ch
        .call(7, 1, Bytes::new(), 50)
        .await
        .expect_err("第二次同样会超时");
    assert!(matches!(again, RpcError::Timeout), "实际 {again:?}");

    server.close().await.expect("关闭");
}

/// 调用方直接丢掉 future（不是超时）时，向对端发 `Cancel` 让 handler 停下。
///
/// 这条用例**必须用 `deadline_ms = 0`**：头里带期限的话，服务端自己会在同一时刻
/// 超时把 handler 丢掉，`Cancel` 到底有没有发就测不出来——那正是「看起来在测，
/// 其实什么都没测」的假用例。
async fn client_abandon_cancels_handler<H: Harness>() {
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let service: Arc<dyn Service> = Arc::new(WatchCancel {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    });

    let server_opts = SessionOptions {
        max_async_in_flight: 1,
        ..SessionOptions::default()
    };
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, service),
        server_opts,
        SessionOptions::default(),
    )
    .await;

    let call = tokio::spawn({
        let ch = ch.clone();
        async move { ch.call(7, 1, Bytes::new(), 0).await }
    });

    // 等 handler 真的开始跑，再丢掉调用方。
    let started_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while started.load(Ordering::Acquire) == 0 && tokio::time::Instant::now() < started_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(started.load(Ordering::Acquire), 1, "handler 应当已经开始");

    call.abort();
    let _ = call.await;

    // 没有 `Cancel` 的话，这条 handler 会一直挂在 `pending()` 上（期限为 0）。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while dropped.load(Ordering::Acquire) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        dropped.load(Ordering::Acquire),
        1,
        "丢掉 future 必须发 Cancel，让对端 handler 停下来"
    );

    // pending 与额度也一起收回来了：再发一次会超时，而不是撞 Overloaded。
    let err = ch
        .call(7, 1, Bytes::new(), 50)
        .await
        .expect_err("仍然不会返回");
    assert!(matches!(err, RpcError::Timeout), "实际 {err:?}");

    server.close().await.expect("关闭");
}

/// 通知走完整 dispatch（查服务 → 扣额度 → handler），只是没有回包。
async fn notification_reaches_handler<H: Harness>() {
    let hits = Arc::new(AtomicUsize::new(0));
    let service: Arc<dyn Service> = Arc::new(Counter(Arc::clone(&hits)));
    let (server, _sh, _client, ch) = boot::<H>(
        registry_with(7, service),
        SessionOptions::default(),
        SessionOptions::default(),
    )
    .await;

    ch.notify(7, 1, Bytes::from_static(b"ping"))
        .await
        .expect("通知应当发得出去");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while hits.load(Ordering::Acquire) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(hits.load(Ordering::Acquire), 1, "通知必须进 handler");

    // 通知连发三条：不占 pending、不产生回包，连接照样可用。
    for _ in 0..3 {
        ch.notify(7, 1, Bytes::new()).await.expect("通知");
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(hits.load(Ordering::Acquire), 4, "三条通知都要到");

    // 通知指向未注册服务：只丢，不断连。
    ch.notify(99, 1, Bytes::new()).await.expect("发出即可");
    let reply = ch
        .call(7, 1, Bytes::new(), CALL_DEADLINE)
        .await
        .expect("连接仍可用");
    assert!(reply.is_empty());

    server.close().await.expect("关闭");
}

/// 把同一组用例挂到每个实现上。这里只有签名，用例体一行不改。
macro_rules! suite {
    ($name:ident, $H:ty) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn single_call_roundtrip() {
                super::single_call_roundtrip::<$H>().await
            }

            #[tokio::test]
            async fn hundred_calls_out_of_order() {
                super::hundred_calls_out_of_order::<$H>().await
            }

            #[tokio::test]
            async fn first_frame_must_be_handshake() {
                super::first_frame_must_be_handshake::<$H>().await
            }

            #[tokio::test]
            async fn unknown_service_returns_remote_2() {
                super::unknown_service_returns_remote_2::<$H>().await
            }

            #[tokio::test]
            async fn handler_panic_returns_internal() {
                super::handler_panic_returns_internal::<$H>().await
            }

            #[tokio::test]
            async fn close_makes_subsequent_calls_closed() {
                super::close_makes_subsequent_calls_closed::<$H>().await
            }

            #[tokio::test]
            async fn peer_close_fails_new_calls_closed() {
                super::peer_close_fails_new_calls_closed::<$H>().await
            }

            #[tokio::test]
            async fn peer_declared_limit_governs_sending() {
                super::peer_declared_limit_governs_sending::<$H>().await
            }

            #[tokio::test]
            async fn blocking_quota_is_separate() {
                super::blocking_quota_is_separate::<$H>().await
            }

            #[tokio::test]
            async fn client_timeout_surfaces_and_session_usable() {
                super::client_timeout_surfaces_and_session_usable::<$H>().await
            }

            #[tokio::test]
            async fn client_abandon_cancels_handler() {
                super::client_abandon_cancels_handler::<$H>().await
            }

            #[tokio::test]
            async fn notification_reaches_handler() {
                super::notification_reaches_handler::<$H>().await
            }
        }
    };
}

#[cfg(feature = "memory")]
suite!(memory, MemoryHarness);

#[cfg(feature = "local")]
suite!(local, LocalHarness);
