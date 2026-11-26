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
