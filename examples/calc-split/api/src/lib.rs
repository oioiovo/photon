//! 契约 crate：**只放接口**，不放实现。
//!
//! 这里能出现的只有：trait 形状、DTO、**业务错误类型**、`service_id`（由宏在
//! `#[photon::service]` 上展开）。出现任何 `impl Calc for ...` 都说明拆错了——那等于把
//! 业务逻辑分发到客户端。
//!
//! `#[photon::service]` 会同时展开出客户端 stub（`CalcClient`）、服务端包装（`CalcServer`）
//! 与注册函数（`register_calc`）。后两者只是泛型占位、不含业务代码，客户端编进来也不会执行；
//! 要彻底剔除得把宏拆成两半，收益只是少个空壳，代价是「契约只写一份」没了（见 README）。
//!
//! 契约里**不出现框架类型**：`add` 不可能失败，直接回 `i64`；`div` 的失败是
//! `DivideByZero`，那是业务语义，只有本 crate 知道它怎么编码。

use photon::CallError;

/// 业务错误：契约自己定义、自己编码。框架只负责把它的字节搬到对端。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CalcError {
    DivideByZero,
}

#[photon::service(id = 9)]
pub trait Calc {
    async fn add(&self, a: i64, b: i64) -> i64;

    async fn div(&self, a: i64, b: i64) -> Result<i64, CalcError>;
}

/// 把可失败方法的失败打成与文档一致的一行。两端共用同一份格式，输出才能逐字比对。
pub fn describe(err: &CallError<CalcError>) -> String {
    match err {
        // 业务拒绝：`E` 原样回来。
        CallError::Rejected(e) => format!("Err(Rejected({e:?}))"),
        // 本端/框架判定：超时、断连、对端框架码……
        CallError::Failed(err) => format!("Err({err:?})"),
        other => format!("Err({other:?})"),
    }
}
