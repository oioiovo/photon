#![allow(unused_imports)]

use photon::RpcError;

// blocking 方法在池线程上跑，占用时长由用户计算量决定，同样不可预期。
#[photon::service(id = 9)]
trait Calc {
    #[photon(blocking, zero_copy)]
    fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
}

fn main() {}
