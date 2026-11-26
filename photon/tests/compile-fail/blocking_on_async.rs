#![allow(unused_imports)]

#[photon::service(id = 7)]
trait Calc {
    #[photon(blocking)]
    async fn add(&self, a: i64, b: i64) -> i64;
}

fn main() {}
