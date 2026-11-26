//! L1 验收：同一套用例在多个实现上跑，**用例体只写一份**。
//!
//! 各实现真正不同的只有两件事：怎么把一对半身接起来（`connected()`），以及
//! drop 写半之后对端能不能看到 EOF（`PEER_SEES_EOF`）。其余用例体全部与实现无关。
#![cfg(any(feature = "tcp", feature = "memory", feature = "local"))]

use std::future::Future;
use std::sync::atomic::Ordering;

use bytes::Bytes;
#[cfg(feature = "local")]
use photon::transport::local::{LocalSink, LocalSource, LocalTransport};
#[cfg(feature = "memory")]
use photon::transport::memory::{Memory, MemorySink, MemorySource};
#[cfg(feature = "tcp")]
use photon::transport::tcp::{TcpSink, TcpSource, TcpTransport};
use photon::transport::{Endpoint, MessageSink, MessageSource, Transport, TransportOptions};

/// 协议头长度。L1 不解释它的内容，但一条消息至少这么大。
const HEADER_LEN: usize = 24;

/// 拼一条「头 + 填充」的消息。头首字节当标记用，其余填 0；L1 不看这些字段。
fn msg(marker: u8, payload: usize) -> Bytes {
    let mut v = Vec::with_capacity(HEADER_LEN + payload);
    v.push(marker);
    v.resize(HEADER_LEN, 0);
    v.extend(std::iter::repeat_n(0xEEu8, payload));
    Bytes::from(v)
}

/// 两侧各一对半身。抽成别名是为了不让 `connected` 的返回类型长到读不懂。
type Pair<S, K> = ((S, K), (S, K));

/// 只描述「怎么接上一对半身」，其余能力全部来自 L1 自身的 trait。
trait Harness: Sized {
    // `'static` 是为了能把半身 move 进 `tokio::spawn`（1 MiB 用例必须并发读写）。
    type Src: MessageSource + 'static;
    type Sink: MessageSink + 'static;

    /// drop 写半之后，对端是否应当观测到 EOF。
    ///
    /// 这不是契约，是实现能力的**声明**：L1 只承诺「drop 后本端不再写」，
    /// 对端能不能看见收尾由底层决定（UDS/TCP 能，共享内存能，named pipe 不能）。
    /// 用例据此断言——声明 `true` 却没收到 EOF 是实现撒谎，声明 `false` 却收到
    /// 结束信号同样说明实现与文档不符。上层不得依赖这个值。
    const PEER_SEES_EOF: bool;

    /// 返回 (发起侧, 接受侧)，每侧都是完整的 (读半, 写半)。
    fn connected() -> impl Future<Output = Pair<Self::Src, Self::Sink>>;
}

#[cfg(feature = "tcp")]
impl Harness for TcpTransport {
    type Src = TcpSource;
    type Sink = TcpSink;
    const PEER_SEES_EOF: bool = true;

    async fn connected() -> Pair<TcpSource, TcpSink> {
        let opts = TransportOptions::default();
        let server = TcpTransport::bind(
            &Endpoint::Tcp("127.0.0.1:0".parse().expect("字面地址合法")),
            &opts,
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("取回端口");

        let accept_opts = opts.clone();
        let accepting =
            tokio::spawn(async move { server.accept(&accept_opts).await.expect("accept") });

        let client = TcpTransport::connect(&Endpoint::Tcp(addr), &opts)
            .await
            .expect("connect");
        let server_pair = accepting.await.expect("accept 任务");
        (client, server_pair)
    }
}

#[cfg(feature = "memory")]
impl Harness for Memory {
    type Src = MemorySource;
    type Sink = MemorySink;
    // 通道语义：写端全 drop 后读端必然拿到 `None`。
    const PEER_SEES_EOF: bool = true;

    async fn connected() -> Pair<MemorySource, MemorySink> {
        // 进程内配对不需要地址，也不需要监听。
        Memory::pair()
    }
}

#[cfg(feature = "local")]
impl Harness for LocalTransport {
    type Src = LocalSource;
    type Sink = LocalSink;

    // UDS 的 `OwnedWriteHalf` 在 drop 时 `shutdown(SHUT_WR)`，与 tcp 一致。
    const PEER_SEES_EOF: bool = true;

    async fn connected() -> Pair<LocalSource, LocalSink> {
        // 用例并行跑，同一条路径会互相抢；用进程内自增序号分开。
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path =
            std::path::PathBuf::from(format!("/tmp/photon-t{}-{seq}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let ep = Endpoint::Local(photon::transport::LocalAddr::new(&path));
        let opts = TransportOptions::default();

        let server = LocalTransport::bind(&ep, &opts).await.expect("bind");
        let ep2 = ep.clone();
        let accept_opts = opts.clone();
        let accepting = tokio::spawn(async move {
            let pair = server.accept(&accept_opts).await.expect("accept");
            (pair, server)
        });

        let client = LocalTransport::connect(&ep2, &opts).await.expect("connect");
        let (server_pair, server) = accepting.await.expect("accept 任务");
        // 连接已建立，监听端可以立刻退场；socket 文件得手动删，drop 不会清。
        drop(server);
        let _ = std::fs::remove_file(&path);
        (client, server_pair)
    }
}

/// 一问一答，双向都要通。
async fn single_frame<H: Harness>() {
    let ((mut c_src, mut c_sink), (mut s_src, mut s_sink)) = H::connected().await;

    let req = msg(0xA1, 8);
    c_sink.send(req.clone()).await.expect("发起侧发送");
    assert_eq!(s_src.recv().await.expect("接受侧接收"), req);

    let resp = msg(0xB2, 0);
    s_sink.send(resp.clone()).await.expect("接受侧回包");
    assert_eq!(c_src.recv().await.expect("发起侧收响应"), resp);
}

/// 连续 100 帧，顺序不能乱、也不能丢。
async fn hundred_frames<H: Harness>() {
    let ((_c_src, mut c_sink), (mut s_src, _s_sink)) = H::connected().await;

    let sent: Vec<Bytes> = (0..100u8).map(|i| msg(i, 4)).collect();
    for m in &sent {
        c_sink.send(m.clone()).await.expect("发送");
    }
    for (i, expected) in sent.iter().enumerate() {
        let got = s_src.recv().await.expect("接收");
        assert_eq!(&got[..], &expected[..], "第 {i} 帧内容或顺序错");
    }
}

/// 上限之内的 1 MiB 消息必须能过。
///
/// 两侧上限都要先提起来：默认 64 KiB 下这条用例本来就应该被拒。
async fn one_mib<H: Harness>() {
    let ((_c_src, mut c_sink), (mut s_src, _s_sink)) = H::connected().await;

    let raised = 2 * 1024 * 1024;
    c_sink.send_limit().store(raised, Ordering::Relaxed);
    s_src.recv_limit().store(raised, Ordering::Relaxed);

    let m = msg(0x77, 1024 * 1024 - HEADER_LEN);
    let expected = m.clone();

    // 必须在另一处并发读：1 MiB 远超内核 socket 缓冲，先写后读会自己把自己堵住。
    let sender = tokio::spawn(async move { c_sink.send(m).await.expect("发送 1 MiB") });

    let got = s_src.recv().await.expect("接收 1 MiB");
    assert_eq!(got.len(), 1024 * 1024);
    assert_eq!(&got[..], &expected[..]);
    sender.await.expect("发送任务结束");
}

/// 超过本端接收上限：读侧 `InvalidData`（该连接此后不可信）。
async fn over_local_limit<H: Harness>() {
    let ((_c_src, mut c_sink), (mut s_src, _s_sink)) = H::connected().await;

    s_src.recv_limit().store(1024, Ordering::Relaxed);
    c_sink
        .send(msg(0xC3, 2048 - HEADER_LEN))
        .await
        .expect("发起侧上限仍是 64 KiB，发得出去");

    let err = s_src.recv().await.expect_err("超接收上限应当拒绝");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

/// 超过对端上限：写侧 `InvalidInput`，且**连接保持可用**。
async fn over_peer_limit<H: Harness>() {
    let ((_c_src, mut c_sink), (mut s_src, _s_sink)) = H::connected().await;

    c_sink.send_limit().store(1024, Ordering::Relaxed);
    let err = c_sink
        .send(msg(0xD4, 2048 - HEADER_LEN))
        .await
        .expect_err("超对端上限应当拒发");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    // 拒发发生在写第一个字节之前，队列与流都干净，小消息照常能过。
    c_sink.send_limit().store(64 * 1024, Ordering::Relaxed);
    let small = msg(0xD5, 0);
    c_sink.send(small.clone()).await.expect("连接仍可用");
    assert_eq!(s_src.recv().await.expect("接收"), small);
}

/// drop 写半之后：按 [`Harness::PEER_SEES_EOF`] 的声明断言对端看到什么。
///
/// 声明 `true` 时必须读到 `UnexpectedEof`；声明 `false` 时必须读不到任何结束信号
/// （既不报错也不给半条消息）。两条分支都只断言「实现与自己的声明一致」，
/// 不断言「必须有 EOF」——后者会逼着所有实现去追一个 platform 给不了的能力。
async fn eof_after_sink_drop<H: Harness>() {
    let ((_c_src, c_sink), (mut s_src, _s_sink)) = H::connected().await;
    drop(c_sink);

    // 上限 200 ms：有半关闭的实现在这里立刻返回，无半关闭的实现必须挂住。
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(200), s_src.recv()).await;

    if H::PEER_SEES_EOF {
        let err = outcome
            .expect("声明有 EOF，却一直没有结束信号")
            .expect_err("drop 后不该再收到消息");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    } else {
        assert!(outcome.is_err(), "声明无 EOF，却收到了结束信号");
    }
}

/// 同一个旋钮：先放宽到 1 MiB 发 500 KiB（通过），再调回 64 KiB 发 500 KiB（拒发）。
async fn limit_knob<H: Harness>() {
    let ((_c_src, mut c_sink), (mut s_src, _s_sink)) = H::connected().await;

    let big = msg(0xE6, 500 * 1024 - HEADER_LEN);

    c_sink.send_limit().store(1024 * 1024, Ordering::Relaxed);
    s_src.recv_limit().store(1024 * 1024, Ordering::Relaxed);
    // 收发必须并发推进：500 KiB 远超 UDS 缓冲区，先写完再读会自己把自己堵住。
    // 用 `join!` 而不是 `spawn`，是为了把 `c_sink` 留着给下一段断言。
    let (send_res, recv_res) = tokio::join!(c_sink.send(big.clone()), s_src.recv());
    send_res.expect("放宽后应当发得出去");
    assert_eq!(recv_res.expect("接收 500 KiB").len(), big.len());

    c_sink.send_limit().store(64 * 1024, Ordering::Relaxed);
    let err = c_sink.send(big).await.expect_err("调回默认后应当拒发");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

/// 把同一组用例挂到每个实现上。这里只有签名，用例体一行不改。
macro_rules! suite {
    ($name:ident, $T:ty) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn single_frame() {
                super::single_frame::<$T>().await
            }

            #[tokio::test]
            async fn hundred_frames() {
                super::hundred_frames::<$T>().await
            }

            #[tokio::test]
            async fn one_mib() {
                super::one_mib::<$T>().await
            }

            #[tokio::test]
            async fn over_local_limit() {
                super::over_local_limit::<$T>().await
            }

            #[tokio::test]
            async fn over_peer_limit() {
                super::over_peer_limit::<$T>().await
            }

            #[tokio::test]
            async fn limit_knob() {
                super::limit_knob::<$T>().await
            }

            #[tokio::test]
            async fn eof_after_sink_drop() {
                super::eof_after_sink_drop::<$T>().await
            }
        }
    };
}

#[cfg(feature = "tcp")]
suite!(tcp, TcpTransport);
#[cfg(feature = "memory")]
suite!(memory, Memory);

#[cfg(feature = "local")]
suite!(local, LocalTransport);
