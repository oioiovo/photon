//! photon —— 轻量 RPC/IPC 框架。
//!
//! 分层自下而上：L1 传输 → L2 协议（24 字节头）→ L3 会话 → L4 API。
//! 每层只依赖下一层：L2 不认识 `Transport`，L3 不认识字节序。当前处于 M0，
//! 只有 L2 可用。

pub mod error;
pub mod proto;

pub use error::{RemoteError, RpcError};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

/// 装箱后的 `Send` future。
///
/// 必须是 `dyn Future` 而不是 `impl Future`（RPITIT）：`Service::call` 要能被
/// `tokio::spawn` 承载，而每个 RPITIT 实现都会生成匿名类型，无法放进同一张
/// 待回包表。公开而非私有：它出现在 `Service` 的签名里。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
}
