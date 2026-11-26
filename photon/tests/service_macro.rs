//! `#[photon::service]` 的展开产物验收：dispatch、method id、注册、错误码。
//!
//! 这里不走线上会话——展开出来的东西是不是对的，与传输无关。等到 `Server`
//! 装配落地，同一套断言会在 memory / local 上各跑一遍。

use std::sync::Arc;

use photon::{Bytes, Codec, RemoteError, RpcError, Service, ServiceRegistry};

#[photon::service(id = 7)]
trait Calc {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
    async fn div(&self, a: i64, b: i64) -> Result<i64, RpcError>;
}

struct CalcImpl;

impl Calc for CalcImpl {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        Ok(a + b)
    }

    async fn div(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        if b == 0 {
            return Err(RpcError::Remote(RemoteError {
                code: 0x10001,
                retryable: false,
                message: "divide by zero".into(),
            }));
        }
        Ok(a / b)
    }
}

#[test]
fn generated_module_exposes_service_id() {
    // 一处声明、两处使用：`register_calc` 与 `CalcClient` 里的 id 都来自这里。
    assert_eq!(__photon_calc::SERVICE_ID, 7);
}

#[test]
fn method_name_table_is_generated() {
    assert_eq!(__photon_calc::METHOD_NAMES.len(), 2);
    assert_eq!(__photon_calc::METHOD_NAMES[0].1, "add");
    assert_eq!(__photon_calc::METHOD_NAMES[1].1, "div");
    assert_eq!(__photon_calc::METHOD_NAMES[0].0, __photon_calc::HASH_ADD);
    assert_eq!(__photon_calc::HASH_ADD, 0xffb3_52e1_3fd2_8e80);
}

#[tokio::test]
async fn dispatch_add_decodes_args_and_encodes_reply() {
    let service = Arc::new(CalcService::new(Arc::new(CalcImpl)));
    let payload = photon::codec::Default::encode(&(2i64, 3i64)).expect("编码参数");

    let reply = Service::call(service, __photon_calc::HASH_ADD, payload)
        .await
        .expect("dispatch 必须成功");

    let sum: i64 = photon::codec::Default::decode(&reply).expect("解码返回值");
    assert_eq!(sum, 5);
}

#[tokio::test]
async fn user_error_code_survives_dispatch() {
    let service = Arc::new(CalcService::new(Arc::new(CalcImpl)));
    let payload = photon::codec::Default::encode(&(1i64, 0i64)).expect("编码参数");

    let err = Service::call(service, __photon_calc::HASH_DIV, payload)
        .await
        .expect_err("除零必须报错");

    let RpcError::Remote(remote) = err else {
        panic!("handler 自己返回的 Remote 不该被改写：{err:?}");
    };
    assert_eq!(remote.code, 0x10001);
}

#[tokio::test]
async fn unknown_method_is_remote_3() {
    let service = Arc::new(CalcService::new(Arc::new(CalcImpl)));

    let err = Service::call(service, 0xdead_beef, Bytes::new())
        .await
        .expect_err("未注册的方法必须报错");

    // 帧格式是对的，只是没有这个方法：必须是 Remote{3}，不是 Protocol——后者会让
    // 会话以为对端乱发帧而断连。
    let RpcError::Remote(remote) = err else {
        panic!("未知方法必须是 Remote{{3}}，实际 {err:?}");
    };
    assert_eq!(remote.code, 3);
}

#[tokio::test]
async fn bad_payload_is_invalid_input() {
    let service = Arc::new(CalcService::new(Arc::new(CalcImpl)));

    // 只给 1 字节，两个 i64 解不出来：参数错，不是协议错。
    let err = Service::call(
        service,
        __photon_calc::HASH_ADD,
        Bytes::from_static(&[0x01]),
    )
    .await
    .expect_err("截断参数必须报错");

    assert!(matches!(err, RpcError::InvalidInput(_)), "实际 {err:?}");
}

#[tokio::test]
async fn register_uses_declared_service_id() {
    let mut registry = ServiceRegistry::empty();
    register_calc(&mut registry, Arc::new(CalcImpl)).expect("首次注册");

    // 按宏里声明的 id 取回，并真的跑一次 dispatch：注册表接上的是对的实现。
    let stored = registry.get(__photon_calc::SERVICE_ID).expect("按 id 取回");
    let payload = photon::codec::Default::encode(&(4i64, 5i64)).expect("编码参数");
    let reply = stored
        .call(__photon_calc::HASH_ADD, payload)
        .await
        .expect("dispatch");

    let sum: i64 = photon::codec::Default::decode(&reply).expect("解码返回值");
    assert_eq!(sum, 9);
}

#[test]
fn register_twice_is_rejected() {
    let mut registry = ServiceRegistry::empty();

    register_calc(&mut registry, Arc::new(CalcImpl)).expect("首次注册");
    let err = register_calc(&mut registry, Arc::new(CalcImpl)).expect_err("二次注册");

    // 覆盖会静默改掉路由目标，所以在注册期就拒绝。
    assert!(matches!(err, RpcError::InvalidInput(_)), "实际 {err:?}");
}

/// 三档声明混在同一个 trait 里：调度策略逐方法决定，互不影响。
#[photon::service(id = 8)]
trait Mixed {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
    /// 同步、纯计算：就地跑完，不换线程。
    fn double(&self, x: i64) -> Result<i64, RpcError>;
    /// 同步但会阻塞：唯一换线程的分支。
    #[photon(blocking)]
    fn slow_double(&self, x: i64) -> Result<i64, RpcError>;
    /// 在 blocking 线程里 panic：JoinError 必须变成错误体，而不是挂住请求。
    #[photon(blocking)]
    fn boom(&self) -> Result<(), RpcError>;
}

struct MixedImpl;

impl Mixed for MixedImpl {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        Ok(a + b)
    }

    fn double(&self, x: i64) -> Result<i64, RpcError> {
        Ok(x * 2)
    }

    fn slow_double(&self, x: i64) -> Result<i64, RpcError> {
        // 故意睡一下：调用方能拿到结果，说明它确实落在另一个线程上跑完了。
        std::thread::sleep(std::time::Duration::from_millis(20));
        Ok(x * 2)
    }

    fn boom(&self) -> Result<(), RpcError> {
        panic!("blocking handler 炸了")
    }
}

#[tokio::test]
async fn sync_method_runs_inline() {
    let service = Arc::new(MixedService::new(Arc::new(MixedImpl)));
    let payload = photon::codec::Default::encode(&(21i64,)).expect("编码参数");

    let reply = Service::call(service, __photon_mixed::HASH_DOUBLE, payload)
        .await
        .expect("同步 handler 也要走同一个 dispatch");

    let doubled: i64 = photon::codec::Default::decode(&reply).expect("解码返回值");
    assert_eq!(doubled, 42);
}

#[tokio::test]
async fn blocking_method_runs_on_blocking_pool() {
    let service = Arc::new(MixedService::new(Arc::new(MixedImpl)));
    let payload = photon::codec::Default::encode(&(4i64,)).expect("编码参数");

    let reply = Service::call(service, __photon_mixed::HASH_SLOW_DOUBLE, payload)
        .await
        .expect("blocking handler 应当正常返回");

    let doubled: i64 = photon::codec::Default::decode(&reply).expect("解码返回值");
    assert_eq!(doubled, 8);
}

#[tokio::test]
async fn blocking_panic_becomes_error_not_hang() {
    let service = Arc::new(MixedService::new(Arc::new(MixedImpl)));

    // 10s 兜底：真挂住必须变红，而不是把整个测试二进制拖死。
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        Service::call(service, __photon_mixed::HASH_BOOM, Bytes::new()),
    )
    .await
    .expect("blocking handler panic 不该让请求永久挂起")
    .expect_err("panic 必须变成错误");

    assert!(matches!(outcome, RpcError::Internal(_)), "实际 {outcome:?}");
}

#[tokio::test]
async fn schedule_reflects_method_annotation() {
    let service = Arc::new(MixedService::new(Arc::new(MixedImpl)));

    // 额度在「进 handler」之前就要扣，所以这张表必须由生成代码给出。
    assert_eq!(
        Service::schedule(&*service, __photon_mixed::HASH_ADD),
        photon::Schedule::Async
    );
    assert_eq!(
        Service::schedule(&*service, __photon_mixed::HASH_DOUBLE),
        photon::Schedule::Async
    );
    assert_eq!(
        Service::schedule(&*service, __photon_mixed::HASH_SLOW_DOUBLE),
        photon::Schedule::Blocking
    );
}

// ---- stub 的期限：`with_deadline` 必须真的进头 ----

#[photon::service(id = 9)]
trait Sleepy {
    async fn nap(&self, ms: u64) -> Result<u64, RpcError>;
}

struct SleepyImpl;

impl Sleepy for SleepyImpl {
    async fn nap(&self, ms: u64) -> Result<u64, RpcError> {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        Ok(ms)
    }
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn client_deadline_reaches_header() {
    use photon::{Session, SessionOptions, transport::memory::Memory};

    let (client_halves, server_halves) = Memory::pair();
    let mut registry = ServiceRegistry::empty();
    register_sleepy(&mut registry, Arc::new(SleepyImpl)).expect("注册");

    let (_server, _) = Session::new(
        server_halves.0,
        server_halves.1,
        Arc::new(registry),
        SessionOptions::default(),
    )
    .expect("建服务端会话");
    let (client_session, handle) = Session::new(
        client_halves.0,
        client_halves.1,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )
    .expect("建客户端会话");
    client_session.wait_ready().await.expect("握手");

    // 默认期限 0：300 ms 的活照常跑完。
    let lenient = SleepyClient::new(handle.clone());
    assert_eq!(lenient.deadline_ms(), 0);
    assert_eq!(lenient.nap(300).await.expect("无期限调用"), 300);

    // 期限 30 ms：同一个方法必须在本端被放弃，而不是等服务端 300 ms 跑完。
    // 变异验证：把生成代码里的 `self.deadline_ms` 换回 `0`，这条会红（拿到 Ok(300)）。
    let strict = SleepyClient::new(handle.clone()).with_deadline(30);
    assert_eq!(strict.deadline_ms(), 30);
    let started = std::time::Instant::now();
    let err = strict.nap(300).await.expect_err("必须超时");
    assert!(
        matches!(err, RpcError::Timeout),
        "期望 Timeout，实际 {err:?}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_millis(300),
        "超时判定没有生效，等了 {:?}",
        started.elapsed()
    );
}

// ---- 业务错误映射：`#[photon(service(id = N, error = E))]` ----

#[derive(Debug)]
enum BizError {
    /// 用户码段的业务错误（`code >= 0x1_0000`）。
    Code(u32, String),
    /// 框架本地/远端错误，原样兜住，不当业务错误解释。
    Transport(RpcError),
}

impl photon::ServiceError for BizError {
    fn into_error(self) -> RpcError {
        match self {
            BizError::Code(code, message) => RpcError::Remote(RemoteError {
                code,
                retryable: false,
                message,
            }),
            BizError::Transport(err) => err,
        }
    }

    fn from_error(err: RpcError) -> Self {
        match err {
            RpcError::Remote(remote) if remote.code >= photon::BUSINESS_CODE_MIN => {
                BizError::Code(remote.code, remote.message)
            }
            other => BizError::Transport(other),
        }
    }
}

#[photon::service(id = 11, error = BizError)]
trait Ledger {
    async fn withdraw(&self, amount: i64) -> Result<i64, BizError>;
}

struct LedgerImpl;

impl Ledger for LedgerImpl {
    async fn withdraw(&self, amount: i64) -> Result<i64, BizError> {
        if amount > 100 {
            return Err(BizError::Code(0x2_0001, "insufficient funds".into()));
        }
        Ok(amount)
    }
}

#[tokio::test]
async fn business_error_is_remote_user_code() {
    let service = Arc::new(LedgerService::new(Arc::new(LedgerImpl)));
    let payload = photon::codec::Default::encode(&(200i64,)).expect("编码参数");

    // 回包侧：业务错误必须已经是 `Remote{ code >= 0x1_0000 }`，否则 debug 会炸。
    let err = Service::call(service, __photon_ledger::HASH_WITHDRAW, payload)
        .await
        .expect_err("必须失败");
    match err {
        RpcError::Remote(remote) => {
            assert_eq!(remote.code, 0x2_0001);
            assert_eq!(remote.message, "insufficient funds");
        }
        other => panic!("期望 Remote，实际 {other:?}"),
    }
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn business_error_roundtrips_through_session() {
    use photon::{Session, SessionOptions, transport::memory::Memory};

    let (client_halves, server_halves) = Memory::pair();
    let mut registry = ServiceRegistry::empty();
    register_ledger(&mut registry, Arc::new(LedgerImpl)).expect("注册");

    let (_server, _) = Session::new(
        server_halves.0,
        server_halves.1,
        Arc::new(registry),
        SessionOptions::default(),
    )
    .expect("建服务端会话");
    let (client_session, handle) = Session::new(
        client_halves.0,
        client_halves.1,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )
    .expect("建客户端会话");
    client_session.wait_ready().await.expect("握手");

    let client = LedgerClient::new(handle.clone());
    assert!(matches!(client.withdraw(50).await, Ok(50)));
    match client.withdraw(200).await {
        Err(BizError::Code(0x2_0001, message)) => assert_eq!(message, "insufficient funds"),
        other => panic!("期望业务错误码 0x2_0001，实际 {other:?}"),
    }

    // 本端错误也要经 `from_error` 收敛，而不是把 `RpcError` 泄给调用方。
    client.handle().close().await.expect("关闭");
    match client.withdraw(50).await {
        Err(BizError::Transport(RpcError::Closed)) => {}
        other => panic!("期望 Closed 被收敛成 Transport(Closed)，实际 {other:?}"),
    }
}

/// 把业务错误写成框架保留段（`3` = UnknownMethod）必须立刻炸，而不是让客户端
/// 把「余额不足」当成「方法不存在」。
#[tokio::test]
async fn business_error_below_user_segment_panics_in_debug() {
    #[photon::service(id = 12, error = BizError)]
    trait Broken {
        async fn bad(&self) -> Result<(), BizError>;
    }

    struct BrokenImpl;
    impl Broken for BrokenImpl {
        async fn bad(&self) -> Result<(), BizError> {
            // 故意用框架码段。
            Err(BizError::Code(3, "should be 0x1_0000+".into()))
        }
    }

    let service = Arc::new(BrokenService::new(Arc::new(BrokenImpl)));
    let payload = photon::codec::Default::encode(&()).expect("编码参数");
    // 断言在 poll 里炸，抓 `JoinError` 即可——不用把整个 runtime 拆出来。
    let handle = tokio::spawn(Service::call(service, __photon_broken::HASH_BAD, payload));
    let joined = handle.await;
    assert!(
        joined.expect_err("必须 panic").is_panic(),
        "code 3 必须触发 debug 断言"
    );
}
