//! 示例的**实现半**：`Calculator` 与业务逻辑。
//!
//! 只有服务端 example 引它（`calc_server` / `calc_local`），以及单进程的
//! `local_calc`（它同时扮演两端，本来就是两者都在）。客户端 example 不引，
//! 否则业务逻辑会被编进客户端二进制——见 `api.rs` 顶部的说明。

use std::time::Duration;

use photon::RpcError;

use crate::calc::{Calc, business_error};

pub struct Calculator;

impl Calc for Calculator {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        Ok(a + b)
    }

    async fn div(&self, a: i64, b: i64) -> Result<i64, RpcError> {
        if b == 0 {
            // 用户码段（≥ 0x1_0000）：框架原样透传，不解释、不改写。
            return Err(business_error(0x10001, "divide by zero"));
        }
        Ok(a / b)
    }

    async fn ping(&self) -> Result<(), RpcError> {
        Ok(())
    }

    async fn slow_op(&self, ms: u64) -> Result<u64, RpcError> {
        tokio::time::sleep(Duration::from_millis(ms)).await;
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
