//! 契约 crate：**只放接口**，不放实现。
//!
//! 这里能出现的只有：trait 形状、DTO、业务错误码段、`service_id`（由宏在 `#[photon::service]`
//! 上展开）。出现任何 `impl Calc for ...` 都说明拆错了——那等于把业务逻辑分发到客户端。
//!
//! `#[photon::service]` 会同时展开出客户端 stub（`CalcClient`）、服务端包装（`CalcServer`）
//! 与注册函数（`register_calc`）。后两者只是泛型占位、不含业务代码，客户端编进来也不会执行；
//! 要彻底剔除得把宏拆成两半，收益只是少个空壳，代价是「契约只写一份」没了（见 README）。

use photon::RpcError;

#[photon::service(id = 9)]
pub trait Calc {
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;

    async fn div(&self, a: i64, b: i64) -> Result<i64, RpcError>;
}

/// 业务错误码段：`0x10001` 起是用户码（框架保留 `0..7`，见 `docs/PROTOCOL.md` 3.8）。
pub const CODE_DIVIDE_BY_ZERO: u32 = 0x10001;

/// 把错误打成与文档一致的一行。两端共用同一份格式，输出才能逐字比对。
pub fn describe(err: &RpcError) -> String {
    match err {
        RpcError::Remote(remote) => {
            format!(
                "Err(Remote(code=0x{:X}, {:?}))",
                remote.code, remote.message
            )
        }
        other => format!("Err({other:?})"),
    }
}
