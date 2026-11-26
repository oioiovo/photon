//! L3 会话验收：握手、配对、并发乱序、关闭。
#![cfg(feature = "memory")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use photon::proto::{Header, Kind};
use photon::session::{ServiceRegistry, Session, SessionHandle, SessionOptions};
use photon::transport::MessageSink;
use photon::transport::memory::{Memory, MemorySink, MemorySource};
use photon::{BoxFuture, RpcError, Service};

/// 所有用例都带期限：一旦路由错了，应当变红而不是永远挂着。
const CALL_DEADLINE: u32 = 5_000;

type Sess = Session<MemorySource, MemorySink>;
type Handle = SessionHandle<MemorySource, MemorySink>;

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

fn registry_with(service_id: u16, svc: Arc<dyn Service>) -> Arc<ServiceRegistry> {
    let mut reg = ServiceRegistry::empty();
    reg.insert(service_id, svc).expect("注册不应撞号");
    Arc::new(reg)
}

fn boot(
    server_reg: Arc<ServiceRegistry>,
    server_opts: SessionOptions,
    client_opts: SessionOptions,
) -> (Arc<Sess>, Handle, Arc<Sess>, Handle) {
    let ((s_src, s_sink), (c_src, c_sink)) = Memory::pair();
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

#[tokio::test]
async fn single_call_roundtrip() {
    let (server, _sh, _client, ch) = boot(
        registry_with(7, Arc::new(Echo { slow: false })),
        SessionOptions::default(),
        SessionOptions::default(),
    );

    let payload = Bytes::from_static(b"hello");
    let reply = ch
        .call(7, 1, payload.clone(), CALL_DEADLINE)
        .await
        .expect("调用应当成功");
    assert_eq!(reply, payload);

    server.close().await.expect("关闭服务端");
}

#[tokio::test]
async fn hundred_calls_out_of_order() {
    let (server, _sh, _client, ch) = boot(
        registry_with(7, Arc::new(Echo { slow: true })),
        SessionOptions::default(),
        SessionOptions::default(),
    );

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

#[tokio::test]
async fn first_frame_must_be_handshake() {
    let (a_src, a_sink, b_src, b_sink) = {
        let ((a_src, a_sink), (b_src, b_sink)) = Memory::pair();
        (a_src, a_sink, b_src, b_sink)
    };

    let (server, _sh) = Session::new(
        a_src,
        a_sink,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )
    .expect("会话");

    // 绕过会话层，直接塞一条 Request 当首帧。
    let header = Header {
        kind: Kind::Request,
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

#[tokio::test]
async fn unknown_service_returns_remote_2() {
    let (server, _sh, _client, ch) = boot(
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
        SessionOptions::default(),
    );

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

#[tokio::test]
async fn close_makes_subsequent_calls_closed() {
    let (server, _sh, _client, ch) = boot(
        registry_with(7, Arc::new(Echo { slow: false })),
        SessionOptions::default(),
        SessionOptions::default(),
    );

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

#[tokio::test]
async fn peer_declared_limit_governs_sending() {
    // 服务端声明 1 MiB，客户端仍用默认 64 KiB：客户端发 200 KiB 应当成功，
    // 因为写入上限取的是**对端**声明的值，不是自己的。
    let server_opts = SessionOptions {
        max_message_len: 1024 * 1024,
        ..SessionOptions::default()
    };
    let (server, _sh, _client, ch) = boot(
        registry_with(7, Arc::new(Ack)),
        server_opts,
        SessionOptions::default(),
    );

    // 服务端只回 ack，所以这条用例只压「请求方向」的上限。
    let payload = Bytes::from(vec![0xABu8; 200 * 1024]);
    let reply = ch
        .call(7, 1, payload, CALL_DEADLINE)
        .await
        .expect("应当发得出去");
    assert_eq!(&reply[..], b"ok");

    server.close().await.expect("关闭");
}
