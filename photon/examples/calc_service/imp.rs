//! 示例的**实现半**：`Calculator` 与业务逻辑。
//!
//! 现在只有两个示例引它：`calc_local`（一个二进制两个角色）与单进程的 `local_calc`
//! （同时扮演两端）。v0.30 删掉 `calc_server` / `calc_client` 后，`photon/examples/`
//! 里已经没有「纯客户端」的 example 了——那条边界改由 `examples/calc-split/` 的
//! `calc-client` 守住（它是独立 crate，依赖树里没有服务端）。

use std::time::Duration;

use crate::calc::{Calc, CalcError};

pub struct Calculator;

impl Calc for Calculator {
    async fn add(&self, a: i64, b: i64) -> i64 {
        a + b
    }

    async fn div(&self, a: i64, b: i64) -> Result<i64, CalcError> {
        if b == 0 {
            // 业务错误原样走线路：框架只加一个「这是业务拒绝」的标志位。
            return Err(CalcError::DivideByZero);
        }
        Ok(a / b)
    }

    async fn ping(&self) {}

    async fn slow_op(&self, ms: u64) -> u64 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        ms
    }

    fn fast_hash(&self, data: Vec<u8>) -> u64 {
        // FNV-1a：够简单，值也稳定，示例输出才能逐字比对。
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash
    }
}
