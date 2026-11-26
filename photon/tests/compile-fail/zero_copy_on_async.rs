#![allow(unused_imports)]

use photon::RpcError;

// `zero_copy` 只对同步 `fn` 开放：async handler 跨 await 拿着 payload，
// 会把 shm 槽位攥上不可预期的时间。
#[photon::service(id = 8)]
trait Calc {
    #[photon(zero_copy)]
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
}

fn main() {}
