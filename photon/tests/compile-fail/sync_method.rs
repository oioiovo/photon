#![allow(unused_imports)]

use photon::RpcError;

#[photon::service(id = 7)]
trait Calc {
    fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;
}

fn main() {}
