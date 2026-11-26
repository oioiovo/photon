//! 示例的**接口半**：只有 `Calc` trait、契约自己的错误类型与几个小工具。
//!
//! 这个文件被 `local_calc` 与 `calc_local` 各自 `#[path]` 引入一次。分成独立程序时，
//! **客户端只引它**——
//! 服务端的具体实现（`imp.rs`）不进客户端二进制。这条边界不是洁癖：
//! `#[photon::service]` 展开产出的东西里，只有 `Service::call` 的实现体（dispatch
//! 里调用的业务逻辑）来自 `impl Calc for ...`，把它一起编进客户端等于把业务逻辑
//! 分发出去，也让客户端多背一份它永远不会执行的代码。
//!
//! 注意这个文件里**没有任何框架错误类型**：`add` 直接回 `i64`，`div` 失败时回
//! `CalcError`。框架对本端的判定（超时/断连）由框架在客户端那一侧补上。
#![allow(dead_code)]

use photon::CallError;

/// 业务错误：契约自己定义、自己编码，框架不解释它的任何一个字节。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CalcError {
    DivideByZero,
}

#[photon::service(id = 7, blocking)]
pub trait Calc {
    /// 不可失败：签名里连错误类型都不出现。
    async fn add(&self, a: i64, b: i64) -> i64;
    /// 可失败：失败类型是契约自己的 `CalcError`。
    async fn div(&self, a: i64, b: i64) -> Result<i64, CalcError>;

    // ---- 以下三个方法只为 M3 的形态演示而存在 ----
    /// 通知的目标：没有回包，调用方只能确认「发出去了」。
    async fn ping(&self) -> ();

    /// 故意慢：用来演示 per-request timeout（客户端 50 ms 放弃，handler 还在睡）。
    async fn slow_op(&self, ms: u64) -> u64;

    /// 同步 handler：`fn` 声明，就地跑完，不 `await` 也不换线程。
    fn fast_hash(&self, data: Vec<u8>) -> u64;
}

/// 服务 id 由宏生成、只在服务模块内可见；通知这类不经过 stub 的调用需要它。
pub fn service_id() -> u16 {
    __photon_calc::SERVICE_ID
}

/// `ping` 的 method id，同上。
pub fn ping_method() -> u64 {
    __photon_calc::HASH_PING
}

/// 把**框架**错误打成一行：`Err(Timeout)`、`Err(Closed)`……
///
/// 放在接口半而不是实现半：客户端也要打印同一行格式。
pub fn describe(err: &photon::RpcError) -> String {
    format!("Err({err:?})")
}

/// 把**可失败方法**的失败打成一行：`Rejected` 是业务拒绝，`Failed` 是本端判定。
pub fn describe_call(err: &CallError<CalcError>) -> String {
    match err {
        CallError::Rejected(e) => format!("Err(Rejected({e:?}))"),
        CallError::Failed(err) => describe(err),
        // 契约与框架的边界在将来可能多出别的臂（例如流式的中断），示例不假设封闭。
        other => format!("Err({other:?})"),
    }
}
