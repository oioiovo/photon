#![allow(unused_imports)]

use photon::RpcError;

// 服务声明了业务错误类型，方法却仍返回 `RpcError`：必须在展开期报错，
// 而不是让用户去读「From<Biz> for RpcError 未实现」。
#[photon::service(id = 13, error = Biz)]
trait Calc {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
}

fn main() {}
