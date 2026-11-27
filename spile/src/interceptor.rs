//! 拦截器：客户端 / 服务端各一条链，包住「字节已成型、还没落到协议」的那一段。
//!
//! 为什么切在字节级：看得见类型化参数就意味着要为每个方法生成一层泛型包装，
//! 宏展开量与拦截器作者的心智负担都会翻倍。字节级只能看到 `service_id` /
//! `method` / `method_name` / `deadline_ms` / `payload`，而 metrics、重试、
//! 鉴权注入、审计恰好只需要这些。
//!
//! 三条硬约束（结构上尽量做成「做不到」而不是「别这么做」）：
//!
//! 1. **不许改路由**：`ClientCall` 的 `service_id` / `method` 只有 getter，没有
//!    setter。要改只能换一个 `Call`，那等于换一次调用，得自己负责。
//! 2. **不许翻译错误通道**：`CallFailure::Rejected(E)` 与 `Failed(RpcError)`、
//!    `HandlerError::Business` 与 `Framework` 表达的是「谁的错」，拦截器只允许
//!    透传或短路（返回自己的 `Err`），不允许把业务拒绝改写成框架错误。
//! 3. **顺序是洋葱**：先注册的在最外层，进入顺序 = 注册顺序，返回顺序相反。
//!    与 `tower` 的 `Layer` 一致，避免两套直觉。
//!
//! 拦截器是纯本端行为：不进握手、不动字节布局、不影响 `protocol_version`。

use std::fmt;
use std::slice::Iter;
use std::sync::Arc;

use bytes::Bytes;

use crate::{BoxFuture, error::CallFailure, error::HandlerError};

/// 客户端一次调用的上下文。
///
/// 字段私有 + 只读 getter，是为了让「改路由」在类型层面就做不到；需要派生副本
/// 时只有 [`ClientCall::with_deadline`] 与 [`ClientCall::with_payload`] 两个口子。
#[derive(Clone, Debug)]
pub struct ClientCall {
    service_id: u16,
    method: u64,
    method_name: Option<&'static str>,
    payload: Bytes,
    deadline_ms: u32,
}

impl ClientCall {
    pub(crate) fn new(
        service_id: u16,
        method: u64,
        method_name: Option<&'static str>,
        payload: Bytes,
        deadline_ms: u32,
    ) -> Self {
        Self {
            service_id,
            method,
            method_name,
            payload,
            deadline_ms,
        }
    }

    pub fn service_id(&self) -> u16 {
        self.service_id
    }

    pub fn method(&self) -> u64 {
        self.method
    }

    /// 方法名。宏生成的 stub 一定会带上；手写调用方走 `call` 时为 `None`。
    pub fn method_name(&self) -> Option<&'static str> {
        self.method_name
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// 期限（毫秒，0 = 不设）。它是**每一次尝试**的期限，不是整条调用的总预算。
    pub fn deadline_ms(&self) -> u32 {
        self.deadline_ms
    }

    /// 派生副本，只换期限。重试时的常规做法：把剩余总预算折算成单次期限。
    pub fn with_deadline(self, deadline_ms: u32) -> Self {
        Self {
            deadline_ms,
            ..self
        }
    }

    /// 派生副本，只换 payload。注入 / 脱敏 / 加密这类场景用。
    pub fn with_payload(self, payload: Bytes) -> Self {
        Self { payload, ..self }
    }

    /// 交给 `Session::send_once` 的四个字段。`method_name` 只服务观测，不进协议。
    pub(crate) fn into_request(self) -> (u16, u64, Bytes, u32) {
        (self.service_id, self.method, self.payload, self.deadline_ms)
    }
}

/// 服务端一次请求的上下文（只读）。
#[derive(Clone, Debug)]
pub struct ServerCall {
    service_id: u16,
    method: u64,
    method_name: &'static str,
    request_id: u64,
    deadline_ms: u32,
    payload: Bytes,
}

impl ServerCall {
    pub(crate) fn new(
        service_id: u16,
        method: u64,
        method_name: &'static str,
        request_id: u64,
        deadline_ms: u32,
        payload: Bytes,
    ) -> Self {
        Self {
            service_id,
            method,
            method_name,
            request_id,
            deadline_ms,
            payload,
        }
    }

    pub fn service_id(&self) -> u16 {
        self.service_id
    }

    pub fn method(&self) -> u64 {
        self.method
    }

    pub fn method_name(&self) -> &'static str {
        self.method_name
    }

    /// 对端分配的请求 id。同一连接内唯一，日志里靠它把两侧对上。
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    /// 对端声明的期限（毫秒，0 = 没设）。
    pub fn deadline_ms(&self) -> u32 {
        self.deadline_ms
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub(crate) fn into_handler_args(self) -> (u64, Bytes) {
        (self.method, self.payload)
    }
}

/// 客户端链的下一环。链尾才真正发一次请求。
pub struct ClientNext<'a> {
    rest: Iter<'a, Arc<dyn ClientInterceptor>>,
    leaf: &'a dyn ClientLeaf,
}

impl<'a> Clone for ClientNext<'a> {
    /// 链位置可以复制——这正是「重试」在结构上成立的前提：复制出来的
    /// `Next` 指向同一个位置，两次 `run` 互不影响。
    fn clone(&self) -> Self {
        Self {
            rest: self.rest.clone(),
            leaf: self.leaf,
        }
    }
}

impl<'a> ClientNext<'a> {
    pub(crate) fn new(
        rest: Iter<'a, Arc<dyn ClientInterceptor>>,
        leaf: &'a dyn ClientLeaf,
    ) -> Self {
        Self { rest, leaf }
    }

    /// 交给下一个拦截器；链走完就发请求。
    ///
    /// 重试的写法是 `next.clone().run(call.clone())`：`Next` 自己会被消费，
    /// 复制一份就能从同一位置再走一遍。每次重放都是一次新的 `request_id`，
    /// 不会和上一轮还挂在 pending 表里的条目撞车。
    pub fn run(mut self, call: ClientCall) -> BoxFuture<'a, Result<Bytes, CallFailure>> {
        match self.rest.next() {
            Some(interceptor) => interceptor.intercept(
                call,
                ClientNext {
                    rest: self.rest,
                    leaf: self.leaf,
                },
            ),
            None => self.leaf.run_once(call),
        }
    }
}

/// 服务端链的下一环。链尾才真正进 handler。
pub struct ServerNext<'a> {
    rest: Iter<'a, Arc<dyn ServerInterceptor>>,
    leaf: &'a dyn ServiceLeaf,
}

impl<'a> ServerNext<'a> {
    pub(crate) fn new(
        rest: Iter<'a, Arc<dyn ServerInterceptor>>,
        leaf: &'a dyn ServiceLeaf,
    ) -> Self {
        Self { rest, leaf }
    }

    /// 交给下一个拦截器；链走完就执行 handler。
    ///
    /// 服务端**不建议**重放：handler 可能有副作用（写库、发通知），而框架此刻
    /// 既没有幂等键也没有去重表。要重试请放在客户端那一侧。
    pub fn run(mut self, call: ServerCall) -> BoxFuture<'a, Result<Bytes, HandlerError>> {
        match self.rest.next() {
            Some(interceptor) => interceptor.intercept(
                call,
                ServerNext {
                    rest: self.rest,
                    leaf: self.leaf,
                },
            ),
            None => {
                let (method, payload) = call.into_handler_args();
                self.leaf.invoke(method, payload)
            }
        }
    }
}

/// 客户端拦截器。实现方通常是「包住 `next.run(call.clone())` 再做点事」。
///
/// ```ignore
/// struct Metrics { calls: AtomicU64 }
/// impl ClientInterceptor for Metrics {
///     fn intercept<'a>(&'a self, call: ClientCall, next: ClientNext<'a>)
///         -> BoxFuture<'a, Result<Bytes, CallFailure>>
///     {
///         Box::pin(async move {
///             let started = std::time::Instant::now();
///             let result = next.clone().run(call.clone()).await;
///             self.calls.fetch_add(1, Ordering::Relaxed);
///             tracing::info!(?result, elapsed = ?started.elapsed(), method = call.method(), "rpc");
///             result
///         })
///     }
/// }
/// ```
pub trait ClientInterceptor: Send + Sync + 'static {
    fn intercept<'a>(
        &'a self,
        call: ClientCall,
        next: ClientNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, CallFailure>>;
}

/// 服务端拦截器。它包住的是「handler 执行」这一段，取消 / 期限 / 关闭的
/// `select!` 在它外面——所以拦截器吞不掉取消语义。
pub trait ServerInterceptor: Send + Sync + 'static {
    fn intercept<'a>(
        &'a self,
        call: ServerCall,
        next: ServerNext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, HandlerError>>;
}

/// 链尾：真正发一次请求。由 `Session` 实现，用户拿不到。
pub(crate) trait ClientLeaf: Send + Sync {
    fn run_once(&self, call: ClientCall) -> BoxFuture<'_, Result<Bytes, CallFailure>>;
}

/// 链尾：真正进 handler。由 `Arc<dyn Service>` 实现，用户拿不到。
pub(crate) trait ServiceLeaf: Send + Sync {
    fn invoke(
        &self,
        method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, HandlerError>>;
}

impl ServiceLeaf for Arc<dyn crate::Service> {
    fn invoke(
        &self,
        method: u64,
        payload: Bytes,
    ) -> BoxFuture<'static, Result<Bytes, HandlerError>> {
        crate::Service::call(Arc::clone(self), method, payload)
    }
}

/// 两条链。放在 [`crate::SessionOptions`] 里，随会话一起建出来。
///
/// 默认是空的，且空链走的是**没有额外分配**的快路径：没有拦截器时 `call`
/// 直通原来的实现，等于给所有存量调用零加价。
#[derive(Clone, Default)]
pub struct Interceptors {
    client: Arc<[Arc<dyn ClientInterceptor>]>,
    server: Arc<[Arc<dyn ServerInterceptor>]>,
}

impl Interceptors {
    pub fn new() -> Self {
        Self::default()
    }

    /// 追加一个客户端拦截器（**最先追加的在最外层**）。
    pub fn with_client(mut self, interceptor: impl ClientInterceptor) -> Self {
        self.push_client(Arc::new(interceptor));
        self
    }

    /// 追加一个服务端拦截器（**最先追加的在最外层**）。
    pub fn with_server(mut self, interceptor: impl ServerInterceptor) -> Self {
        self.push_server(Arc::new(interceptor));
        self
    }

    /// 追加已装箱的客户端拦截器。
    pub fn push_client(&mut self, interceptor: Arc<dyn ClientInterceptor>) {
        let mut next: Vec<_> = self.client.iter().cloned().collect();
        next.push(interceptor);
        self.client = next.into();
    }

    /// 追加已装箱的服务端拦截器。
    pub fn push_server(&mut self, interceptor: Arc<dyn ServerInterceptor>) {
        let mut next: Vec<_> = self.server.iter().cloned().collect();
        next.push(interceptor);
        self.server = next.into();
    }

    pub fn client(&self) -> &[Arc<dyn ClientInterceptor>] {
        &self.client
    }

    pub fn server(&self) -> &[Arc<dyn ServerInterceptor>] {
        &self.server
    }

    /// 供 `Session` 在 spawn 之前把链搬进 task：`Arc` 计数 +1，不复制链本身。
    pub(crate) fn server_arc(&self) -> Arc<[Arc<dyn ServerInterceptor>]> {
        Arc::clone(&self.server)
    }

    /// 两条链都是空的。空链有专门的快路径，所以这个判断值得公开。
    pub fn is_empty(&self) -> bool {
        self.client.is_empty() && self.server.is_empty()
    }
}

impl fmt::Debug for Interceptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `dyn` 拦截器没有 `Debug`，也不该为了日志要求它：这里只报条数。
        f.debug_struct("Interceptors")
            .field("client", &self.client.len())
            .field("server", &self.server.len())
            .finish()
    }
}

impl fmt::Debug for dyn ClientInterceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientInterceptor")
    }
}

impl fmt::Debug for dyn ServerInterceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ServerInterceptor")
    }
}
