//! 同步客户端底座：把一条会话连同它的 runtime 搬到独立的线程上。
//!
//! 同步调用方唯一的诉求是「调一次、拿到结果、中间不写 `await`」，但这条链路上
//! 有三个它解决不了的问题：
//!
//! 1. 会话必须先握手才能发请求，而握手是异步的（`Session::new` 一进来就要
//!    `tokio::spawn` 两个 task）。
//! 2. 直接在本线程 `Runtime::block_on` 会绑定调用方的上下文：调用方若本身就在
//!    runtime 里（哪怕只是 `#[tokio::test]`），会直接 panic `Cannot start a runtime
//!    from within a runtime`——这个错误只有到线上第一次同步调用时才出现。
//! 3. 同步客户端最常见的用法是「在 `main` 里、在数据库回调里、在 `Drop` 里」，
//!    它不能假设调用方有 runtime，更不能假设调用方愿意被它骗走一个 worker。
//!
//! 所以这里不共享调用方的 runtime，而是**自建**：一条专属线程负责跑 runtime，
//! 会话的 reader / writer / dispatch 全在这条线程的 worker 上，调用方只通过一条
//! 命令通道把请求丢过去、再阻塞等回包。代价是每个同步客户端多 3 条线程（2 个
//! worker + 1 个驱动），换来的是「在任何上下文里调用都不会 panic、也不会把调用方
//! 的 runtime 卡死」。
//!
//! 两层超时的分工（两者都不是对方的替代品）：
//!
//! - **内层**是协议头里的 `deadline_ms`：它跨进程可见，到点由**服务端**放弃执行
//!   并回 `DeadlineExceeded`。
//! - **外层**是 [`BlockingSession::with_timeout`]：它只保护调用方这一侧，用来兜
//!   住「对端根本不回」——进程僵死、半开连接、handler 卡在不可中断的 syscall 上。
//!   内层对这种情形完全无效，因为它依赖对端愿意读帧、愿意回包。
//!
//! 外层默认 `None`：没有期限的调用就是「等到有结果为止」，和异步客户端的语义
//! 一致。想让同步调用有本地上限，必须自己写 `with_timeout`——默认给一个数字会
//! 变成「框架替用户决定他最多能等多久」，那是最难排查的一类超时。

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::time::Duration;

use bytes::Bytes;
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument as _;

use crate::error::{CallFailure, RpcError};
use crate::session::{ConnectOptions, ServiceRegistry, Session, SessionHandle, SessionOptions};
use crate::transport::{Endpoint, MessageSink, MessageSource, Transport};

/// 专属 runtime 的 worker 数。
///
/// 不设成 1：reader 在等 socket 的间隙里，writer 与 dispatch 也得跑起来，单 worker
/// 会把「等 IO」直接摊到「等 CPU」上。也不设成 `num_cpus`：同步客户端同时只有
/// 个位数在飞请求，多开线程只是白白占内存。
const WORKER_THREADS: usize = 2;

/// 一条同步客户端会话。
///
/// `Clone` 的语义是「共享同一条连接」——克隆出来的副本共享底层会话与命令通道，
/// 各自可以带不同的 `timeout`。最后一个副本被丢弃时才关闭连接。
pub struct BlockingSession<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    inner: Arc<Inner<S, K>>,
    /// 外层期限；`None` 表示无限等。
    timeout: Option<Duration>,
}

struct Inner<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    /// 最后一个 `BlockingSession` 被丢弃时这条通道随之关闭，dispatcher 收到 `None`
    /// 后自己优雅收尾（`close()` 有 `close_timeout` 兜底），随后 runtime 线程退出。
    /// `Drop` 不等待：它跑在调用方线程上，而关一次连接最长要 5 s。
    jobs: mpsc::UnboundedSender<Command>,
    /// 只用来在类型上绑定 `S` / `K`；`fn() -> (S, K)` 这种形式让 `Inner` 始终
    /// `Send + Sync`，不会莫名其妙地把同步客户端变成不可跨线程共享。
    _marker: PhantomData<fn() -> (S, K)>,
}

/// 一条发给会话线程的命令。
///
/// 回包走 `std::sync::mpsc::SyncSender`：调用方要的正是 `recv_timeout` 这种
/// 「带期限的阻塞收包」，`tokio` 的通道反而还要再包一层 `block_on`。
enum Command {
    Call {
        service_id: u16,
        method: u64,
        /// 方法名，只用于 span（`method` 是哈希，日志里认不出来）。
        method_name: Option<&'static str>,
        payload: Bytes,
        deadline_ms: u32,
        /// 外层期限，与调用方 `recv_timeout` 用的是同一个值。
        timeout: Option<Duration>,
        reply: SyncSender<Result<Bytes, CallFailure>>,
    },
    Notify {
        service_id: u16,
        method: u64,
        payload: Bytes,
        reply: SyncSender<Result<(), RpcError>>,
    },
    Close {
        reply: SyncSender<Result<(), RpcError>>,
    },
}

impl<S, K> BlockingSession<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    /// 用已经连上的两端半身建会话，跳过 transport。
    ///
    /// 给两类场景：进程内配对（`Memory::pair`，它刻意不实现 `Transport`），以及
    /// 「连接已经由别处建好、只想把它搬进同步世界」的调用方。注册表恒为空——
    /// 同步客户端只发请求，不响应回调。
    pub fn from_parts(source: S, sink: K, opts: SessionOptions) -> Result<Self, RpcError> {
        Self::spawn(async move {
            let (session, handle) =
                Session::new(source, sink, Arc::new(ServiceRegistry::empty()), opts)?;
            session.wait_ready().await?;
            Ok(handle)
        })
    }

    fn spawn<F>(open: F) -> Result<Self, RpcError>
    where
        F: Future<Output = Result<SessionHandle<S, K>, RpcError>> + Send + 'static,
    {
        let (jobs, job_rx) = mpsc::unbounded_channel();
        // 构造必须等到握手结果：`sync_channel(1)` 让它只能被填一次，
        // 线程在握手前死掉时调用方会拿到 `Disconnected` 而不是一个假客户端。
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);

        std::thread::Builder::new()
            .name("photon-blocking-session".into())
            .spawn(move || worker(open, job_rx, ready_tx))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                inner: Arc::new(Inner {
                    jobs,
                    _marker: PhantomData,
                }),
                timeout: None,
            }),
            Ok(Err(err)) => Err(err),
            // 线程连一句结果都没留下就没了（panic）：连接不可用。
            Err(_) => Err(RpcError::Closed),
        }
    }

    /// 给这个副本上的所有调用设一个外层期限，返回新副本（原值不变）。
    ///
    /// 与异步客户端 `with_deadline` 的取向一致：期限是每个调用点的选择，
    /// 让 `&self` 上挂着可变期限会让并发调用互相影响。
    pub fn with_timeout(self, timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            ..self
        }
    }

    /// 当前外层期限（`None` = 无限等）。
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// 发一次请求并阻塞等回包。
    ///
    /// `deadline_ms` 进协议头交给服务端；本地的等待上限来自 [`Self::with_timeout`]。
    pub fn call(
        &self,
        service_id: u16,
        method: u64,
        payload: Bytes,
        deadline_ms: u32,
    ) -> Result<Bytes, CallFailure> {
        self.call_named(service_id, method, None, payload, deadline_ms)
    }

    /// 同 [`Self::call`]，但把方法名一并带进会话线程的 span。
    ///
    /// 宏生成的 stub 走这一条；手写调用方拿不到名字就传 `None`，span 里会写
    /// `<unknown>`。名字只影响观测，不影响线格式与语义。
    pub fn call_named(
        &self,
        service_id: u16,
        method: u64,
        method_name: Option<&'static str>,
        payload: Bytes,
        deadline_ms: u32,
    ) -> Result<Bytes, CallFailure> {
        let (reply, wait) = std::sync::mpsc::sync_channel(1);
        self.inner
            .jobs
            .send(Command::Call {
                service_id,
                method,
                method_name,
                payload,
                deadline_ms,
                timeout: self.timeout,
                reply,
            })
            .map_err(|_| RpcError::Closed)?;
        self.recv(wait)?
    }

    /// 发通知：不等结果，只等「已经交给会话」。
    pub fn notify(&self, service_id: u16, method: u64, payload: Bytes) -> Result<(), RpcError> {
        let (reply, wait) = std::sync::mpsc::sync_channel(1);
        self.inner
            .jobs
            .send(Command::Notify {
                service_id,
                method,
                payload,
                reply,
            })
            .map_err(|_| RpcError::Closed)?;
        self.recv(wait)?
    }

    /// 优雅关闭：等在飞请求收尾（上限是会话的 `close_timeout`）。
    pub fn close(&self) -> Result<(), RpcError> {
        let (reply, wait) = std::sync::mpsc::sync_channel(1);
        self.inner
            .jobs
            .send(Command::Close { reply })
            .map_err(|_| RpcError::Closed)?;
        self.recv(wait)?
    }

    /// 阻塞收包，期限取本副本的 `timeout`。
    ///
    /// 期限到点时**不做额外善后**：会话线程那一侧的 `tokio::time::timeout` 用的是
    /// 同一个值，到点 drop 掉 future，`CallGuard` 随之摘表并送出 `Cancel`。
    /// 这边提前几微秒返回，不影响那条请求被正确收尾。
    fn recv<T>(&self, rx: Receiver<T>) -> Result<T, RpcError> {
        match self.timeout {
            Some(timeout) => rx.recv_timeout(timeout).map_err(|err| match err {
                RecvTimeoutError::Timeout => RpcError::Timeout,
                RecvTimeoutError::Disconnected => RpcError::Closed,
            }),
            None => rx.recv().map_err(|_| RpcError::Closed),
        }
    }
}

/// 按 transport 建连，握手完成后返回同步客户端。
///
/// 做成自由函数而不是 `BlockingSession::connect`：半身类型来自 `T` 的关联类型，
/// 塞进 `impl<S, K>` 里 `T` 就成了不出现于 `Self` 的自由参数（E0207）。命名与
/// 异步侧的 [`crate::session::client::connect`] 对齐，一眼能看出是同一件事的
/// 同步版本。
///
/// 握手在这条专属线程上等到 `Ready`：连不上 / 版本不符这类问题应该在构造时就
/// 变成 `Err`，而不是让第一次业务调用莫名其妙地超时。
pub fn connect<T>(
    ep: &Endpoint,
    opts: ConnectOptions,
) -> Result<BlockingSession<T::Source, T::Sink>, RpcError>
where
    T: Transport + 'static,
    T::Source: 'static,
    T::Sink: 'static,
{
    // 端点要 clone 进闭包：会话线程活得比这次调用久。
    let ep = ep.clone();
    BlockingSession::<T::Source, T::Sink>::spawn(async move {
        crate::session::client::connect::<T>(&ep, opts).await
    })
}

impl<S, K> Clone for BlockingSession<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            timeout: self.timeout,
        }
    }
}

/// 会话线程主体：建 runtime、握手、跑 dispatcher，最后等 dispatcher 收尾。
fn worker<S, K, F>(
    open: F,
    jobs: mpsc::UnboundedReceiver<Command>,
    ready: SyncSender<Result<(), RpcError>>,
) where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
    F: Future<Output = Result<SessionHandle<S, K>, RpcError>> + Send + 'static,
{
    let rt = match Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            let _ = ready.send(Err(RpcError::Io(err)));
            return;
        }
    };

    let handle = match rt.block_on(open) {
        Ok(handle) => handle,
        Err(err) => {
            let _ = ready.send(Err(err));
            return;
        }
    };

    let (done_tx, done_rx) = oneshot::channel();
    rt.spawn(dispatcher(jobs, handle, done_tx));
    let _ = ready.send(Ok(()));

    // 只等「收尾完成」这一个信号：会话的 task 都在 worker 线程上，这条线程停在
    // 这里等不影响它们。`done` 只会在命令通道关闭、会话关过之后才到。
    let _ = rt.block_on(done_rx);
    // runtime 必须在这条线程上丢掉，不能在 async 上下文里 drop（那会 panic）。
    drop(rt);
}

/// 命令循环：每条请求一个 task，`close` 与通道关闭则收尾。
async fn dispatcher<S, K>(
    mut jobs: mpsc::UnboundedReceiver<Command>,
    handle: SessionHandle<S, K>,
    done: oneshot::Sender<()>,
) where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    while let Some(command) = jobs.recv().await {
        match command {
            Command::Call {
                service_id,
                method,
                method_name,
                payload,
                deadline_ms,
                timeout,
                reply,
            } => {
                // 每条请求单独 spawn：同步调用方可能来自多个线程，串行化会把
                // 「并发调用」变成「排队等前一个超时」。
                let handle = handle.clone();
                // 与异步 stub 的 `photon.call` 同名同字段：抓日志时两条路径的
                // 输出能用一个 filter 捞全。`request_id` 由 `Session::call` 回填。
                let span = tracing::info_span!(
                    "photon.call",
                    service_id,
                    method,
                    method_name = method_name.unwrap_or(crate::UNKNOWN_METHOD_NAME),
                    deadline_ms,
                    request_id = tracing::field::Empty,
                );
                tokio::spawn(
                    async move {
                        let call = handle.call(service_id, method, payload, deadline_ms);
                        let result = match timeout {
                            Some(limit) => match tokio::time::timeout(limit, call).await {
                                Ok(result) => result,
                                // 期限到：future 被 drop，`CallGuard` 摘掉 pending 表项并
                                // 给对端送 `Cancel`——外层超时不会留下一张永远挂着
                                // 待回包的条目。
                                Err(_) => Err(CallFailure::Failed(RpcError::Timeout)),
                            },
                            None => call.await,
                        };
                        // 调用方可能已经超时走人，此时它那一端已经 drop：发失败无所谓。
                        let _ = reply.send(result);
                    }
                    .instrument(span),
                );
            }
            Command::Notify {
                service_id,
                method,
                payload,
                reply,
            } => {
                let handle = handle.clone();
                tokio::spawn(async move {
                    let _ = reply.send(handle.notify(service_id, method, payload).await);
                });
            }
            Command::Close { reply } => {
                let _ = reply.send(handle.close().await);
            }
        }
    }

    // 调用方那边的最后一个句柄被丢了：替它把这条连接优雅关掉，再把 runtime 放走。
    // `close()` 内部已有 `close_timeout` 兜底，这里不再套一层期限。
    let _ = handle.close().await;
    let _ = done.send(());
}
