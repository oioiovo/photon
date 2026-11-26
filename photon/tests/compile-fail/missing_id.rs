#![allow(unused_imports)]

#[photon::service]
trait Calc {
    async fn add(&self, a: i64, b: i64) -> i64;
}

fn main() {}
