#![allow(unused_imports)]

// 方法级属性只认 `blocking` 与 `zero_copy`：拼错或写了未支持的属性必须报错，
// 静默忽略等于让用户以为它生效了。
#[photon::service(id = 7)]
trait Calc {
    #[photon(cache)]
    fn add(&self, a: i64, b: i64) -> i64;
}

fn main() {}
