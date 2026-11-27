//! 拦截器验收：两条链的位置、顺序、短路与重试。
//!
//! 全部跑在 `memory` 上：拦截器是 L3/L4 的行为，与具体传输无关，换 `local`
//! 只是多一层连接建立，对这里要验的东西没有增益。

#![cfg(feature = "memory")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use spile::interceptor::{ClientInterceptor, ClientNext, ServerInterceptor, ServerNext};
use spile::session::{ServiceRegistry, Session, SessionHandle, SessionOptions};
use spile::transport::memory::Memory;
use spile::{BoxFuture, CallFailure, ClientCall, HandlerError, RpcError, ServerCall, Service};

const SERVICE_ID: u16 = 7;
const CALL_DEADLINE: u32 = 5_000;

/// 回显 payload。`calls` 记进过 handler 的次数，用来验「短路时 handler 没跑」。
struct Echo {
    calls: Arc<AtomicUsize>,
}

impl Service for Echo {
    fn call(
        self: Arc<Self>,
        _method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, HandlerError>> {
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(payload)
        })
    }

    fn method_name(&self, _method: u64) -> Option<&'static str> {
        // 真实场景里这张表由 `#[spile::service]` 展开生成；手写 `Service`
        // 也可以像这样交出来，否则服务端拦截器只会看到 `<unknown>`。
        Some("add")
    }
}

/// 业务拒绝：payload 是 `b"e"` 时回 `Err(HandlerError::Business(...))`。
struct Rejecting;

impl Service for Rejecting {
    fn call(
        self: Arc<Self>,
        _method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, HandlerError>> {
        Box::pin(async move {
            if payload.as_ref() == b"e" {
                Err(HandlerError::Business(Bytes::from_static(b"nope")))
            } else {
                Ok(payload)
            }
        })
    }
}

fn registry(svc: Arc<dyn Service>) -> Arc<ServiceRegistry> {
    let mut reg = ServiceRegistry::empty();
    reg.insert(SERVICE_ID, svc).expect("注册不应撞号");
    Arc::new(reg)
}

/// 记录进入/退出顺序的客户端拦截器。
struct TraceClient {
    label: &'static str,
    log: Arc<std::sync::Mutex<Vec<String>>>,
}

impl ClientInterceptor for TraceClient {
    fn intercept<'a>(
        &'a self,
        call: ClientCall,
        next: ClientNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
        Box::pin(async move {
            self.log.lock().unwrap().push(format!("{}:in", self.label));
            let result = next.run(call).await;
            self.log.lock().unwrap().push(format!("{}:out", self.label));
            result
        })
    }
}

/// 记录进入/退出的服务端拦截器。
struct TraceServer {
    label: &'static str,
    log: Arc<std::sync::Mutex<Vec<String>>>,
}

impl ServerInterceptor for TraceServer {
    fn intercept<'a>(
        &'a self,
        call: ServerCall,
        next: ServerNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
        Box::pin(async move {
            self.log.lock().unwrap().push(format!("{}:in", self.label));
            let result = next.run(call).await;
            self.log.lock().unwrap().push(format!("{}:out", self.label));
            result
        })
    }
}

/// 客户端重试：第一次失败（任意 `Failed`）就再放一次，最多 `max` 次。
struct Retry {
    max: usize,
    attempts: Arc<AtomicUsize>,
    seen: Arc<std::sync::Mutex<Vec<Option<&'static str>>>>,
}

impl ClientInterceptor for Retry {
    fn intercept<'a>(
        &'a self,
        call: ClientCall,
        next: ClientNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(call.method_name());
            for attempt in 0..=self.max {
                self.attempts.fetch_add(1, Ordering::AcqRel);
                let result = next.clone().run(call.clone()).await;
                match &result {
                    Ok(_) => return result,
                    Err(CallFailure::Failed(RpcError::PeerRejected { code: 1, .. }))
                        if attempt < self.max =>
                    {
                        // 真实的重试还要退避；这里只验机制，睡 0 就够。
                        tokio::task::yield_now().await;
                    }
                    _ => return result,
                }
            }
            unreachable!("循环最后一次一定 return")
        })
    }
}

/// 客户端短路：根本不往下走。
struct ShortCircuit;

impl ClientInterceptor for ShortCircuit {
    fn intercept<'a>(
        &'a self,
        _call: ClientCall,
        _next: ClientNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
        Box::pin(async { Err(CallFailure::Failed(RpcError::Overloaded)) })
    }
}

/// 服务端短路：直接回框架码 7。
struct ServerShort;

impl ServerInterceptor for ServerShort {
    fn intercept<'a>(
        &'a self,
        _call: ServerCall,
        _next: ServerNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
        Box::pin(async {
            Err(HandlerError::Framework {
                code: 7,
                message: "rejected by interceptor".into(),
            })
        })
    }
}

/// 客户端软过载：第一次一律回 `PeerRejected{1}`，之后才放行。
struct BounceOnce {
    bounced: Arc<AtomicUsize>,
}

impl ClientInterceptor for BounceOnce {
    fn intercept<'a>(
        &'a self,
        call: ClientCall,
        next: ClientNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
        Box::pin(async move {
            if self.bounced.fetch_add(1, Ordering::AcqRel) == 0 {
                return Err(CallFailure::Failed(RpcError::PeerRejected {
                    code: 1,
                    message: "busy".into(),
                }));
            }
            next.run(call).await
        })
    }
}

type Src = spile::transport::memory::MemorySource;
type Snk = spile::transport::memory::MemorySink;

struct Fixture {
    handle: SessionHandle<Src, Snk>,
    calls: Arc<AtomicUsize>,
    /// 两端会话必须活到用例结束：`Session::drop` 会 `shutdown.cancel()`，
    /// 提前丢掉 `Arc` 等于把连接关掉，之后所有调用都只会拿到 `Closed`。
    _server: Arc<Session<Src, Snk>>,
    _client: Arc<Session<Src, Snk>>,
}

fn boot_pair(
    svc_calls: Arc<AtomicUsize>,
    server_opts: SessionOptions,
    client_opts: SessionOptions,
) -> Fixture {
    let svc = Arc::new(Echo {
        calls: Arc::clone(&svc_calls),
    }) as Arc<dyn Service>;
    boot_pair_with(svc, svc_calls, server_opts, client_opts)
}

fn boot_pair_with(
    svc: Arc<dyn Service>,
    calls: Arc<AtomicUsize>,
    server_opts: SessionOptions,
    client_opts: SessionOptions,
) -> Fixture {
    let ((s_src, s_sink), (c_src, c_sink)) = Memory::pair();
    let (server, _sh) =
        Session::new(s_src, s_sink, registry(svc), server_opts).expect("服务端会话");
    let (client, handle) = Session::new(
        c_src,
        c_sink,
        Arc::new(ServiceRegistry::empty()),
        client_opts,
    )
    .expect("客户端会话");
    Fixture {
        handle,
        calls,
        _server: server,
        _client: client,
    }
}

async fn call(
    handle: &SessionHandle<impl spile::MessageSource + 'static, impl spile::MessageSink + 'static>,
) -> Result<Bytes, CallFailure> {
    handle
        .call_named(
            SERVICE_ID,
            1,
            Some("add"),
            Bytes::from_static(b"hi"),
            CALL_DEADLINE,
        )
        .await
}

#[tokio::test]
async fn client_chain_runs_in_registration_order_outermost_first() {
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let client_opts = SessionOptions::default()
        .client_interceptor(TraceClient {
            label: "a",
            log: Arc::clone(&log),
        })
        .client_interceptor(TraceClient {
            label: "b",
            log: Arc::clone(&log),
        });
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default(),
        client_opts,
    );

    let reply = call(&fixture.handle).await.expect("应当成功");
    assert_eq!(reply, Bytes::from_static(b"hi"));
    assert_eq!(
        *log.lock().unwrap(),
        ["a:in", "b:in", "b:out", "a:out"],
        "先注册的必须在最外层：进入按注册顺序，退出逆序"
    );
}

#[tokio::test]
async fn server_chain_runs_in_registration_order_outermost_first() {
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let server_opts = SessionOptions::default()
        .server_interceptor(TraceServer {
            label: "s1",
            log: Arc::clone(&log),
        })
        .server_interceptor(TraceServer {
            label: "s2",
            log: Arc::clone(&log),
        });
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        server_opts,
        SessionOptions::default(),
    );

    call(&fixture.handle).await.expect("应当成功");
    assert_eq!(*log.lock().unwrap(), ["s1:in", "s2:in", "s2:out", "s1:out"]);
}

#[tokio::test]
async fn empty_chain_is_not_observed_at_all() {
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default(),
        SessionOptions::default(),
    );
    assert!(fixture.handle.session().options().interceptors.is_empty());
    call(&fixture.handle).await.expect("无拦截器也要能调");
    assert_eq!(fixture.calls.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn client_short_circuit_skips_the_network() {
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default(),
        SessionOptions::default().client_interceptor(ShortCircuit),
    );

    let err = call(&fixture.handle).await.expect_err("短路必须失败");
    assert!(matches!(err, CallFailure::Failed(RpcError::Overloaded)));
    assert_eq!(
        fixture.calls.load(Ordering::Acquire),
        0,
        "短路后 handler 一次都不该跑"
    );
}

#[tokio::test]
async fn server_short_circuit_becomes_peer_rejected_7() {
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default().server_interceptor(ServerShort),
        SessionOptions::default(),
    );

    let err = call(&fixture.handle).await.expect_err("服务端拒绝必须失败");
    match err {
        CallFailure::Failed(RpcError::PeerRejected { code, message }) => {
            assert_eq!(code, 7);
            assert!(message.contains("rejected by interceptor"));
        }
        other => panic!("期望 PeerRejected，得到 {other:?}"),
    }
    assert_eq!(fixture.calls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn business_rejection_passes_through_the_chain_untouched() {
    let server_opts = SessionOptions::default()
        .server_interceptor(TraceServer {
            label: "s",
            log: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
        .server_interceptor(PassthroughServer);
    let mut reg = ServiceRegistry::empty();
    reg.insert(SERVICE_ID, Arc::new(Rejecting) as Arc<dyn Service>)
        .expect("注册");
    let ((s_src, s_sink), (c_src, c_sink)) = Memory::pair();
    let (_server, _sh) =
        Session::new(s_src, s_sink, Arc::new(reg), server_opts).expect("服务端会话");
    let (_client, handle) = Session::new(
        c_src,
        c_sink,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )
    .expect("客户端会话");
    std::mem::forget(_server);
    std::mem::forget(_client);

    let err = handle
        .call_named(
            SERVICE_ID,
            1,
            Some("div"),
            Bytes::from_static(b"e"),
            CALL_DEADLINE,
        )
        .await
        .expect_err("业务拒绝必须原样到达客户端");
    match err {
        CallFailure::Rejected(tail) => assert_eq!(tail, Bytes::from_static(b"nope")),
        other => panic!("业务拒绝不许被翻译成框架错误，得到 {other:?}"),
    }
}

/// 透传但不做任何事的服务端拦截器：验「多层链也不改变语义」。
struct PassthroughServer;

impl ServerInterceptor for PassthroughServer {
    fn intercept<'a>(
        &'a self,
        call: ServerCall,
        next: ServerNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
        Box::pin(async move { next.run(call).await })
    }
}

#[tokio::test]
async fn replay_gets_a_fresh_request_id() {
    // 服务端只在第一轮拒绝：这样失败发生在「真的发出去之后」，客户端重放
    // 必须拿一个新的 request_id，否则上一轮残留的 pending 表项会被误命中。
    struct RejectFirst {
        ids: Arc<std::sync::Mutex<Vec<u64>>>,
    }
    impl ServerInterceptor for RejectFirst {
        fn intercept<'a>(
            &'a self,
            call: ServerCall,
            next: ServerNext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
            let ids = Arc::clone(&self.ids);
            let id = call.request_id();
            Box::pin(async move {
                // 锁必须在 await 之前放掉：`MutexGuard` 不是 `Send`。
                let first = {
                    let mut guard = ids.lock().unwrap();
                    guard.push(id);
                    guard.len() == 1
                };
                if first {
                    return Err(HandlerError::Framework {
                        code: 1,
                        message: "first attempt rejected".into(),
                    });
                }
                next.run(call).await
            })
        }
    }

    let ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let fixture = boot_pair_with(
        Arc::new(Echo {
            calls: Arc::new(AtomicUsize::new(0)),
        }) as Arc<dyn Service>,
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default().server_interceptor(RejectFirst {
            ids: Arc::clone(&ids),
        }),
        SessionOptions::default().client_interceptor(Retry {
            max: 1,
            attempts: Arc::clone(&attempts),
            seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        }),
    );

    let reply = call(&fixture.handle).await.expect("重放后应当成功");
    assert_eq!(reply, Bytes::from_static(b"hi"));
    assert_eq!(attempts.load(Ordering::Acquire), 2);
    let ids = ids.lock().unwrap();
    assert_eq!(ids.len(), 2, "两轮都该到服务端");
    assert_ne!(ids[0], ids[1], "重放必须是新的 request_id");
}

#[tokio::test]
async fn retry_after_peer_overloaded_succeeds_on_second_attempt() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let client_opts = SessionOptions::default()
        .client_interceptor(Retry {
            max: 2,
            attempts: Arc::clone(&attempts),
            seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
        // 内层先挡一次：`BounceOnce` 在第一轮回可重试的失败。
        .client_interceptor(BounceOnce {
            bounced: Arc::new(AtomicUsize::new(0)),
        });
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default(),
        client_opts,
    );

    let reply = call(&fixture.handle).await.expect("重试后应当成功");
    assert_eq!(reply, Bytes::from_static(b"hi"));
    assert_eq!(attempts.load(Ordering::Acquire), 2, "应当正好重试一次");
    assert_eq!(
        fixture.calls.load(Ordering::Acquire),
        1,
        "被拦下的那一次没有真的发出去"
    );
}

#[tokio::test]
async fn context_carries_identity_and_deadline() {
    type ClientSnapshot = (u16, u64, Option<&'static str>, u32);

    struct Snapshot {
        seen: Arc<std::sync::Mutex<Option<ClientSnapshot>>>,
    }
    impl ClientInterceptor for Snapshot {
        fn intercept<'a>(
            &'a self,
            call: ClientCall,
            next: ClientNext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
            Box::pin(async move {
                *self.seen.lock().unwrap() = Some((
                    call.service_id(),
                    call.method(),
                    call.method_name(),
                    call.deadline_ms(),
                ));
                next.run(call).await
            })
        }
    }

    let seen = Arc::new(std::sync::Mutex::new(None));
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default(),
        SessionOptions::default().client_interceptor(Snapshot {
            seen: Arc::clone(&seen),
        }),
    );

    call(&fixture.handle).await.expect("应当成功");
    assert_eq!(
        seen.lock().unwrap().expect("拦截器必须被调用"),
        (SERVICE_ID, 1, Some("add"), CALL_DEADLINE)
    );
}

#[tokio::test]
async fn server_context_sees_request_id_and_name() {
    type ServerSnapshot = (u16, u64, &'static str, u64);

    struct Snapshot {
        seen: Arc<std::sync::Mutex<Option<ServerSnapshot>>>,
    }
    impl ServerInterceptor for Snapshot {
        fn intercept<'a>(
            &'a self,
            call: ServerCall,
            next: ServerNext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
            Box::pin(async move {
                *self.seen.lock().unwrap() = Some((
                    call.service_id(),
                    call.method(),
                    call.method_name(),
                    call.request_id(),
                ));
                next.run(call).await
            })
        }
    }

    let seen = Arc::new(std::sync::Mutex::new(None));
    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default().server_interceptor(Snapshot {
            seen: Arc::clone(&seen),
        }),
        SessionOptions::default(),
    );

    call(&fixture.handle).await.expect("应当成功");
    let (service_id, method, name, request_id) = seen.lock().unwrap().expect("拦截器必须被调用");
    assert_eq!((service_id, method, name), (SERVICE_ID, 1, "add"));
    assert!(request_id > 0, "request_id 由客户端分配，必须非 0");
}

#[tokio::test]
async fn interceptor_can_shrink_the_deadline() {
    struct Shrink;
    impl ClientInterceptor for Shrink {
        fn intercept<'a>(
            &'a self,
            call: ClientCall,
            next: ClientNext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
            // 把 5 s 缩到 0：服务端不再自己计时，超时只能由本端未来触发。
            Box::pin(async move { next.run(call.with_deadline(0)).await })
        }
    }

    let fixture = boot_pair(
        Arc::new(AtomicUsize::new(0)),
        SessionOptions::default(),
        SessionOptions::default().client_interceptor(Shrink),
    );

    let _ = tokio::time::timeout(Duration::from_secs(2), call(&fixture.handle))
        .await
        .expect("缩期限后仍应很快返回")
        .expect("应当成功");
}
