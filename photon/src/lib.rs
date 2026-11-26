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
pub use error::{BUSINESS_CODE_MIN, RemoteError, RpcError};
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

/// 业务错误与线上错误体之间的双向映射。
///
/// `#[photon::service(id = N, error = MyError)]` 之后，方法返回 `Result<T, MyError>`，
/// 生成代码在每个 handler 上调用 [`ServiceError::into_error`]、在每个客户端方法上
/// 调用 [`ServiceError::from_error`]。只要求一个 impl、两个方向，而不是两个 `From`
/// 实现——`From` 的方向依赖孤儿规则，用户一旦要把 `RpcError` 往自己的错误类型里塞，
/// 靠 `impl From<RpcError> for MyError` 才行，而反方向又得 `impl From<MyError> for
/// RpcError`，两处分散、容易只写一半。
///
/// 契约只有一句：**`into_error` 必须返回 `RpcError::Remote`，`code ≥ 0x1_0000`**。
/// 低于这段是框架保留段（`2` UnknownService、`3` UnknownMethod、`7` ShuttingDown
/// 等），业务错误占了它，客户端看到的就是「方法不存在」这种假象。debug 构建里
/// 框架会断言这一点（release 下 `debug_assert` 编译掉，不留分支）。
pub trait ServiceError: Sized {
    /// 回包侧：把业务错误变成要上线路的 `RpcError`。
    fn into_error(self) -> RpcError;

    /// 发起侧：把本地或远端错误还原成业务错误类型。
    ///
    /// 收到的可能是 `Remote`（业务错误或框架码），也可能是本端的 `Timeout` /
    /// `Closed` / `Io` 等——实现方通常把后者收敛成一个「未分类失败」变体。
    fn from_error(err: RpcError) -> Self;
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
    /// 只对同步 `fn` 开放，且必须逐方法显式写 `#[photon(zero_copy)]`。
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
    ) -> BoxFuture<'static, Result<Bytes, RpcError>>;

    /// 这个方法占哪一类入站额度。
    ///
    /// 默认 `Async`；宏只给 `#[photon(blocking)]` 的方法生成 `Blocking`。放在
    /// `Service` 上而不是让 L3 猜，是因为额度必须在「进 handler」之前就扣——
    /// 那时只有 `service_id + method`，还没机会看到实现的分支。
    fn schedule(&self, _method: u64) -> Schedule {
        Schedule::Async
    }

    /// 这个方法的 payload 怎么交给 handler。
    ///
    /// 默认 `Copy`；宏只给显式标了 `#[photon(zero_copy)]` 的同步 `fn` 生成
    /// `ZeroCopy`。和 `schedule` 放在一起，是因为两者都是「进入 handler 之前
    /// 就得知道、且只有宏知道」的编译期属性，M5 的 shm 路径要读这个值决定
    /// 是否把槽位直接借用出去。
    fn payload_mode(&self, _method: u64) -> PayloadMode {
        PayloadMode::Copy
    }
}
