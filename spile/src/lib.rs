//! spile —— 轻量 RPC/IPC 框架。
//!
//! 分层自下而上：L1 传输 → L2 协议（24 字节头）→ L3 会话 → L4 API。
//! 每层只依赖下一层：L2 不认识 `Transport`，L3 不认识字节序。
//!
//! 当前 L1/L2/L3 可用（M1 完成），L4 的 `#[spile::service]` 已落地（M2 完成）。
//! M3 进行中：三档声明形态（`async fn` / `fn` / `#[spile(blocking)] fn`）与同步
//! 客户端（`<Svc>BlockingClient`）都已支持。

pub mod blocking;
pub mod codec;
pub mod error;
pub mod interceptor;
pub mod proto;
pub mod session;
pub mod transport;

/// 宏展开里要用 `::spile::Bytes`，所以这里必须公开重导出。
pub use blocking::BlockingSession;
pub use bytes::Bytes;
pub use codec::Codec;
pub use error::{
    BUSINESS_CODE_MARK, CallError, CallFailure, FRAMEWORK_CODE_MAX, HandlerError, RpcError,
};
pub use interceptor::{
    ClientCall, ClientInterceptor, ClientNext, Interceptors, ServerCall, ServerInterceptor,
    ServerNext,
};
pub use session::{
    ConnectOptions, Server, ServiceRegistry, Session, SessionHandle, SessionOptions,
    client::connect,
};
pub use spile_macros::service;
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

/// `method_name` 未知时的占位：用户自己实现 `Service` 却没覆盖 `method_name`
/// （异步与同步两条路径共用一个值，日志里一眼看得出「没接名字表」）。
pub(crate) const UNKNOWN_METHOD_NAME: &str = "<unknown>";

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

/// payload 交给 handler 的方式。
///
/// **v1 的两种取值都是 no-op**：tcp / local / memory 交出来的 `Bytes` 本身就是
/// owned + `'static`，拷一次没有收益，`Copy` 只是「默认安全」的语义。真正为它付钱
/// 的是 M5 的 shm 借用槽位——那时 `Copy` 才等于「先拷到堆缓冲再交给 handler」。
///
/// 之所以现在就定下来：语法一旦晚于用户代码出现，就得逼所有人回来改 trait。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadMode {
    /// 默认。借用型 payload 先拷出再进 handler；owned payload 上是 no-op。
    #[default]
    Copy,
    /// 高风险：payload 的生命周期会与 handler 执行时长绑定（shm 槽位迟迟不归还）。
    ///
    /// 只对同步 `fn` 开放，且必须逐方法显式写 `#[spile(zero_copy)]`。
    ZeroCopy,
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
    ) -> BoxFuture<'static, Result<Bytes, HandlerError>>;

    /// 这个方法占哪一类入站额度。
    ///
    /// 默认 `Async`；宏只给 `#[spile(blocking)]` 的方法生成 `Blocking`。放在
    /// `Service` 上而不是让 L3 猜，是因为额度必须在「进 handler」之前就扣——
    /// 那时只有 `service_id + method`，还没机会看到实现的分支。
    fn schedule(&self, _method: u64) -> Schedule {
        Schedule::Async
    }

    /// 这个方法的 payload 怎么交给 handler。
    ///
    /// 默认 `Copy`；宏只给显式标了 `#[spile(zero_copy)]` 的同步 `fn` 生成
    /// `ZeroCopy`。和 `schedule` 放在一起，是因为两者都是「进入 handler 之前
    /// 就得知道、且只有宏知道」的编译期属性，M5 的 shm 路径要读这个值决定
    /// 是否把槽位直接借用出去。
    fn payload_mode(&self, _method: u64) -> PayloadMode {
        PayloadMode::Copy
    }

    /// 这个方法的名字，供观测用（请求 span 的 `method_name` 字段）。
    ///
    /// 默认 `None`，宏用展开期写死的名字表覆盖。为什么放在 trait 上而不是让
    /// L3 自己维护一张表：method id 是 `xxh3` 哈希，**单靠哈希逆向不出名字**，
    /// 而只有 id 的 span（`service_id=7 method=0xffb35...`）在排障时基本没用。
    /// 名字只在宏产物的那个模块里，L3 拿不到，只能由实现方交出来。
    fn method_name(&self, _method: u64) -> Option<&'static str> {
        None
    }
}

/// 宏产物专用的小工具，**不是公开 API 的一部分**（将来可能随宏一起改）。
///
/// 之所以要有：生成的 stub 需要给调用包一个 span，而 `tracing` 是 `spile`
/// 的依赖、不是用户的依赖——宏里写 `tracing::` 会让没用它的下游 crate 编不过。
#[doc(hidden)]
pub mod __private {
    pub use tracing;

    /// 把 future 装进给定 span，返回可 `await` 的包装。
    ///
    /// 用户代码里不需要 `use tracing::Instrument`，宏直接调这个函数就够了；
    /// 返回值是 `Instrumented<F>`，`await` 之后拿到 `F` 的输出。
    pub fn instrument<F>(span: tracing::Span, future: F) -> tracing::instrument::Instrumented<F>
    where
        F: std::future::Future,
    {
        use tracing::Instrument as _;
        future.instrument(span)
    }
}
