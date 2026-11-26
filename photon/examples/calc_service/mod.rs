//! 示例共用：一个 `service_id = 7` 的 `Calc` 服务。
//!
//! 这里就是用户视角的全部——写一个 trait、实现它，剩下的 client stub、
//! dispatch、注册函数都由宏产出。
//!
//! 本文件被两个 example 各自 `#[path]` 引入一次，每个 example 只用其中一部分，
//! 因此对「本 example 没用到的项」统一放行 dead_code。
#![allow(dead_code)]

use photon::{RemoteError, RpcError};

#[photon::service(id = 7)]
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

pub struct Calculator;

impl Calc for Calculator {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        Ok(a + b)
    }

    async fn div(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        if b == 0 {
            // 用户码段（≥ 0x1_0000）：框架原样透传，不解释、不改写。
            return Err(RpcError::Remote(RemoteError {
                code: 0x10001,
                retryable: false,
                message: "divide by zero".into(),
            }));
        }
        Ok(a / b)
    }

    async fn ping(&self) -> Result<(), RpcError> {
        Ok(())
    }

    async fn slow_op(&self, ms: u64) -> Result<u64, RpcError> {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        Ok(ms)
    }

    fn fast_hash(&self, data: Vec<u8>) -> Result<u64, RpcError> {
        // FNV-1a：够简单，值也稳定，示例输出才能逐字比对。
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        Ok(hash)
    }
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
