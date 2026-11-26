//! 示例的**接口半**：只有 `Calc` trait 与几个与实现无关的小工具。
//!
//! 这个文件被四个 example 各自 `#[path]` 引入一次，**客户端 example 只引它**——
//! 服务端的具体实现（`imp.rs`）不进客户端二进制。这条边界不是洁癖：
//! `#[photon::service]` 展开产出的东西里，只有 `Service::call` 的实现体（dispatch
//! 里调用的业务逻辑）来自 `impl Calc for ...`，把它一起编进客户端等于把业务逻辑
//! 分发出去，也让客户端多背一份它永远不会执行的代码。
#![allow(dead_code)]

use photon::{RemoteError, RpcError};

#[photon::service(id = 7, blocking)]
pub trait Calc {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
    async fn div(&self, a: i64, b: i64) -> Result<i64, RpcError>;

    // ---- 以下三个方法只为 M3 的形态演示而存在 ----
    /// 通知的目标：没有回包，调用方只能确认「发出去了」。
    async fn ping(&self) -> Result<(), RpcError>;

    /// 故意慢：用来演示 per-request timeout（客户端 50 ms 放弃，handler 还在睡）。
    async fn slow_op(&self, ms: u64) -> Result<u64, RpcError>;

    /// 同步 handler：`fn` 声明，就地跑完，不 `await` 也不换线程。
    fn fast_hash(&self, data: Vec<u8>) -> Result<u64, RpcError>;
}

/// 服务 id 由宏生成、只在服务模块内可见；通知这类不经过 stub 的调用需要它。
pub fn service_id() -> u16 {
    __photon_calc::SERVICE_ID
}

/// `ping` 的 method id，同上。
pub fn ping_method() -> u64 {
    __photon_calc::HASH_PING
}

/// 把错误打成与文档一致的一行，示例输出因此可以逐字比对。
///
/// 放在接口半而不是实现半：客户端也要打印同一行格式。
pub fn describe(err: &RpcError) -> String {
    match err {
        RpcError::Remote(remote) => {
            format!(
                "Err(Remote(code=0x{:X}, {:?}))",
                remote.code, remote.message
            )
        }
        other => format!("Err({other:?})"),
    }
}

/// 让示例能构造一个「用户码段」的错误，而不必各自写一遍。
pub fn business_error(code: u32, message: &str) -> RpcError {
    RpcError::Remote(RemoteError {
        code,
        retryable: false,
        message: message.into(),
    })
}
