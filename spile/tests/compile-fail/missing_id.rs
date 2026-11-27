#![allow(unused_imports)]

#[spile::service]
trait Calc {
    async fn add(&self, a: i64, b: i64) -> i64;
}

fn main() {}
