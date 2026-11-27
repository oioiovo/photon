#![allow(unused_imports)]

// blocking 方法在池线程上跑，占用时长由用户计算量决定，同样不可预期。
#[spile::service(id = 9)]
trait Calc {
    #[spile(blocking, zero_copy)]
    fn add(&self, a: i64, b: i64) -> i64;
}

fn main() {}
