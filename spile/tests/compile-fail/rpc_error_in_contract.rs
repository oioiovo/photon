// 契约里出现框架错误类型必须当场拒绝：`RpcError` 描述的是「本端这次调用为什么失败」，
// handler 既无从产生也无权声明它——能声明就会逼出一个假的业务错误映射层。
#[spile::service(id = 13)]
trait Calc {
    async fn add(&self, a: i64, b: i64) -> Result<i64, spile::RpcError>;
}

fn main() {}
