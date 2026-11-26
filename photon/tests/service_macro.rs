//! `#[photon::service]` 的展开产物验收：dispatch、method id、注册、错误通道。
//!
//! 这里不走线上会话——展开出来的东西是不是对的，与传输无关。等 `Server` 装配
//! 落地，同一套断言会在 memory / local 上各跑一遍。

use std::sync::Arc;

use photon::{Bytes, CallError, Codec, HandlerError, RpcError, Service, ServiceRegistry};

/// 契约自己的错误类型：框架不认识它，只负责把字节搬回来。
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum CalcError {
    DivideByZero,
}

#[photon::service(id = 7)]
trait Calc {
    /// 不可失败：签名里连错误类型都不用出现。
    async fn add(&self, a: i64, b: i64) -> i64;
    async fn div(&self, a: i64, b: i64) -> Result<i64, CalcError>;
}

struct CalcImpl;

impl Calc for CalcImpl {
    async fn add(&self, a: i64, b: i64) -> i64 {
        a + b
    }

    async fn div(&self, a: i64, b: i64) -> Result<i64, CalcError> {
        if b == 0 {
            return Err(CalcError::DivideByZero);
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
async fn business_error_keeps_tail_bytes() {
    let service = Arc::new(CalcService::new(Arc::new(CalcImpl)));
    let payload = photon::codec::Default::encode(&(1i64, 0i64)).expect("编码参数");

    let HandlerError::Business(tail) = Service::call(service, __photon_calc::HASH_DIV, payload)
        .await
        .expect_err("除零必须报错")
    else {
        panic!("业务失败必须走 Business 通道");
    };

    // 框架一个字节都不解释：tail 就是契约里 `CalcError` 的 codec 输出。
    let decoded: CalcError = photon::codec::Default::decode(&tail).expect("业务错误解码");
    assert_eq!(decoded, CalcError::DivideByZero);
}

#[tokio::test]
async fn unknown_method_is_framework_3() {
    let service = Arc::new(CalcService::new(Arc::new(CalcImpl)));

    let err = Service::call(service, 0xdead_beef, Bytes::new())
        .await
        .expect_err("未注册的方法必须报错");

    // 帧格式是对的，只是没有这个方法：必须是框架码 3，不是 Protocol——后者会让
    // 会话以为对端乱发帧而断连。
    match err {
        HandlerError::Framework { code, .. } => assert_eq!(code, 3),
        other => panic!("未知方法必须是框架码 3，实际 {other:?}"),
    }
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

    match err {
        HandlerError::Framework { code, message } => {
            assert_eq!(code, 4, "参数错必须是框架码 4，实际文案 {message}");
        }
        other => panic!("参数错必须是框架码，实际 {other:?}"),
    }
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
    async fn add(&self, a: i64, b: i64) -> i64;
    /// 同步、纯计算：就地跑完，不换线程。
    fn double(&self, x: i64) -> i64;
    /// 同步但会阻塞：唯一换线程的分支。
    #[photon(blocking)]
    fn slow_double(&self, x: i64) -> i64;
    /// 在 blocking 线程里 panic：JoinError 必须变成错误体，而不是挂住请求。
    #[photon(blocking)]
    fn boom(&self) -> ();
    /// 显式声明零拷贝：只有同步 `fn` 能这么写（async / blocking 都会在展开期被拒）。
    #[photon(zero_copy)]
    fn digest(&self, data: Vec<u8>) -> u64;
}

struct MixedImpl;

impl Mixed for MixedImpl {
    async fn add(&self, a: i64, b: i64) -> i64 {
        a + b
    }

    fn double(&self, x: i64) -> i64 {
        x * 2
    }

    fn slow_double(&self, x: i64) -> i64 {
        // 故意睡一下：调用方能拿到结果，说明它确实落在另一个线程上跑完了。
        std::thread::sleep(std::time::Duration::from_millis(20));
        x * 2
    }

    fn boom(&self) -> () {
        panic!("blocking handler 炸了")
    }

    fn digest(&self, data: Vec<u8>) -> u64 {
        u64::try_from(data.len()).unwrap_or(0)
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

    match outcome {
        HandlerError::Framework { code, message } => {
            assert_eq!(code, 0, "panic 归为框架 Internal（0），实际文案 {message}");
        }
        other => panic!("panic 必须走框架通道，实际 {other:?}"),
    }
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

#[test]
fn payload_mode_reflects_method_annotation() {
    let service = Arc::new(MixedService::new(Arc::new(MixedImpl)));

    // 默认 Copy：没标的方法一律走安全路径，框架不替用户做决定。
    assert_eq!(
        Service::payload_mode(&*service, __photon_mixed::HASH_DOUBLE),
        photon::PayloadMode::Copy
    );
    // 只有显式 opt-in 的同步 fn 才是 ZeroCopy。
    assert_eq!(
        Service::payload_mode(&*service, __photon_mixed::HASH_DIGEST),
        photon::PayloadMode::ZeroCopy
    );
    // 未知 method 也必须是安全默认值，而不是「猜一个」。
    assert_eq!(
        Service::payload_mode(&*service, 0xdead_beef),
        photon::PayloadMode::Copy
    );
}

// ---- stub 的期限：`with_deadline` 必须真的进头 ----

#[photon::service(id = 9)]
trait Sleepy {
    async fn nap(&self, ms: u64) -> u64;
}

struct SleepyImpl;

impl Sleepy for SleepyImpl {
    async fn nap(&self, ms: u64) -> u64 {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        ms
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

// ---- 业务拒绝 vs 本端失败：`CallError` 的两臂 ----

#[photon::service(id = 11)]
trait Ledger {
    async fn withdraw(&self, amount: i64) -> Result<i64, CalcError>;
}

struct LedgerImpl;

impl Ledger for LedgerImpl {
    async fn withdraw(&self, amount: i64) -> Result<i64, CalcError> {
        if amount > 100 {
            return Err(CalcError::DivideByZero);
        }
        Ok(amount)
    }
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn business_rejection_and_local_failure_are_distinguishable() {
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

    // 对端的业务拒绝：`E` 原样回来。
    match client.withdraw(200).await {
        Err(CallError::Rejected(CalcError::DivideByZero)) => {}
        other => panic!("期望 Rejected(DivideByZero)，实际 {other:?}"),
    }

    // 本端失败：`RpcError` 原样回来，不与业务错误混淆。
    client.handle().close().await.expect("关闭");
    match client.withdraw(50).await {
        Err(CallError::Failed(RpcError::Closed)) => {}
        other => panic!("期望 Failed(Closed)，实际 {other:?}"),
    }

    // 可重试性由 `RpcError` 自己给，业务错误的重试性写在 `E` 里。
    let overloaded: CallError<CalcError> = CallError::Failed(RpcError::Overloaded);
    assert!(overloaded.retryable());
}
