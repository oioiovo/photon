#![allow(unused_imports)]

use photon::RpcError;

#[photon::service]
trait Calc {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
}

fn main() {}
