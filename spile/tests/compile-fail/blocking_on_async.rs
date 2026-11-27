#![allow(unused_imports)]

#[spile::service(id = 7)]
trait Calc {
    #[spile(blocking)]
    async fn add(&self, a: i64, b: i64) -> i64;
}

fn main() {}
