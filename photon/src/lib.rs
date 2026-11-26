//! photon —— 轻量 RPC/IPC 框架。
//!
//! 分层自下而上：L1 传输 → L2 协议（24 字节头）→ L3 会话 → L4 API。
//! 每层只依赖下一层：L2 不认识 `Transport`，L3 不认识字节序。
//!
//! 当前 L1/L2/L3 可用（M1 完成），L4 的 `#[photon::service]` 已落地（M2 完成）。
//! M3 进行中：三档声明形态（`async fn` / `fn` / `#[photon(blocking)] fn`）已支持，
//! 同步客户端（`BlockingClient`）待做。

pub mod codec;
pub mod error;
pub mod proto;
pub mod session;
pub mod transport;

/// 宏展开里要用 `::photon::Bytes`，所以这里必须公开重导出。
pub use bytes::Bytes;
pub use codec::Codec;
pub use error::{RemoteError, RpcError};
pub use photon_macros::service;
pub use session::{
    ConnectOptions, Server, ServiceRegistry, Session, SessionHandle, SessionOptions,
    client::connect,
};
pub use transport::{Endpoint, MessageSink, MessageSource, Transport, TransportOptions};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// 装箱后的 `Send` future。
///
/// 必须是 `dyn Future` 而不是 `impl Future`（RPITIT）：`Service::call` 要能被
/// `tokio::spawn` 承载，而每个 RPITIT 实现都会生成匿名类型，无法放进同一张
/// 待回包表。公开而非私有：它出现在 `Service` 的签名里。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 一次调用该占哪一类额度。
///
/// 调度策略是编译期属性（宏逐方法写死），这个枚举只是把它交到 L3 手上——
/// 配额检查发生在「进入 handler 之前」，那时还看不到 service 内部的分支。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Schedule {
    /// 占一个 task 额度，在 runtime worker 上执行。
    Async,
    /// 占一个 blocking-pool 额度，在 `spawn_blocking` 线程上执行。
    Blocking,
}

/// 被暴露为 RPC 服务的对象。
///
/// `self: Arc<Self>` 而不是 `&self`：处理一次调用期间对象必须保持存活，且同一
/// 服务会被多个连接、多个在飞请求共享；是否要内部可变性由实现方自己决定。
pub trait Service: Send + Sync + 'static {
    /// 处理一次请求并返回应答字节。
    ///
    /// 返回的 future 是 `'static`，实现里不能借用 `self` 之外的局部变量；
    /// `payload` 是已拆掉 24 字节头的原始字节，codec 的还原在上层。
    fn call(
        self: Arc<Self>,
        method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, RpcError>>;

    /// 这个方法占哪一类入站额度。
    ///
    /// 默认 `Async`；宏只给 `#[photon(blocking)]` 的方法生成 `Blocking`。放在
    /// `Service` 上而不是让 L3 猜，是因为额度必须在「进 handler」之前就扣——
    /// 那时只有 `service_id + method`，还没机会看到实现的分支。
    fn schedule(&self, _method: u64) -> Schedule {
        Schedule::Async
    }
}
