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
