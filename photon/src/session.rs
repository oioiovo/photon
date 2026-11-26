//! L3 会话：一条连接上的请求复用、配额、握手与关闭。
//!
//! 这里只做三件事：分配 `request_id` 并在 pending 表里配对、按配额调度 handler、
//! 按状态机收尾。字节序、帧边界、kind 校验都属于更下层，本文件不碰。
//!
//! 两个 task 的分工是硬的：**reader 只读不写，writer 只写不读**。`Source` / `Sink`
//! 都是 `&mut self` 且非 `Sync`，拆成两个 task 之后，「谁来保证不并发写」这个问题
//! 在结构上就不存在了。

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub mod client;
pub mod server;

pub use client::ConnectOptions;
pub use server::Server;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    Schedule, Service,
    error::{RemoteError, RpcError},
    proto::{
        Decoded, FLAG_ERR, FLAG_GOAWAY_FORCE, HEADER_LEN, Header, Kind, MAX_MESSAGE_LEN,
        error_body,
        handshake::{CODEC_POSTCARD, Handshake},
        parse_with_limit,
    },
    transport::{MessageSink, MessageSource},
};

/// `request_id` 从 1 起，0 是保留值。
const FIRST_REQUEST_ID: u64 = 1;

/// 写队列容量。它只承托「已决定要发、还没写出去」的帧；真正的背压在 socket 写阻塞上。
const WRITE_QUEUE: usize = 1024;

/// 会话阶段。只表达「整体是否可用」，方向的在飞状态另算。
const PHASE_NEGOTIATING: u8 = 0;
const PHASE_READY: u8 = 1;
const PHASE_CLOSED: u8 = 2;

/// 会话参数。
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// 本端愿意接收的单帧上限（含 24 字节头）。握手前它就是 L1 的默认值。
    pub max_message_len: u32,
    /// 优雅关闭的总预算，从本端调 `close()` 起算。
    pub close_timeout: Duration,
    /// 等对端握手帧的上限。
    pub handshake_timeout: Duration,
    /// 入站异步 handler 并发上限。
    pub max_async_in_flight: usize,
    /// 入站阻塞 handler 并发上限（M1 尚无 blocking 分支，先占位）。
    pub max_blocking_in_flight: usize,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            max_message_len: MAX_MESSAGE_LEN as u32,
            close_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(1),
            max_async_in_flight: 1024,
            max_blocking_in_flight: 256,
        }
    }
}

/// 服务注册表：`service_id` → 实现。
///
/// 客户端也持有一张（空表），而不是 `Option`：双向对等下客户端同样可能被回调，
/// 用 `Option` 会让「收到 Request」这条路径在类型上就分叉。
pub struct ServiceRegistry {
    services: HashMap<u16, Arc<dyn Service>>,
}

impl ServiceRegistry {
    pub fn empty() -> Self {
        Self {
            services: HashMap::new(),
        }
    }

    /// 撞号直接覆盖会静默改掉路由目标，所以在注册期就拒绝。
    pub fn insert(&mut self, service_id: u16, service: Arc<dyn Service>) -> Result<(), RpcError> {
        if self.services.contains_key(&service_id) {
            return Err(RpcError::InvalidInput(format!(
                "service_id {service_id} 已注册"
            )));
        }
        self.services.insert(service_id, service);
        Ok(())
    }

    pub fn get(&self, service_id: u16) -> Option<Arc<dyn Service>> {
        self.services.get(&service_id).cloned()
    }
}

/// 出站 pending 表。`pending` 与计数同锁，检查与插入才能是一次原子动作。
struct OutboundTable {
    pending: HashMap<u64, PendingEntry>,
    async_used: usize,
}

/// 等待中的一条请求。
///
/// `tx` 用 `Option` 是为了能在 `Drop` 里拿走：还没回包就被丢弃，说明这次调用
/// 已经失败（超时、断连、被回收），必须当场唤醒等待者，而不是让它等到自己的超时。
struct PendingEntry {
    tx: Option<oneshot::Sender<Result<Bytes, RpcError>>>,
    table: Arc<Mutex<OutboundTable>>,
    /// 还没拿到结果就被丢弃：调用方不等了，顺手告诉对端别再算。
    cancel: Option<(mpsc::Sender<Bytes>, u64)>,
}

impl PendingEntry {
    /// 正常完成：把结果交给等待者。之后 `Drop` 只做配额归还。
    fn finish(mut self, result: Result<Bytes, RpcError>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(result);
        }
    }
}

impl Drop for PendingEntry {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Err(RpcError::Closed));
            // 走到这里说明结果从未送达（超时、放弃、断连）：对端那条 handler 还在算，
            // 告诉它别算了。`try_send` 而不是 `send`——为了发一条「别算了」而卡在
            // 背压上比不发更糟；队列满时对端仍会按 deadline 自行收尾。
            if let Some((frames, request_id)) = self.cancel.take()
                && frames.try_send(cancel_frame(request_id)).is_err()
            {
                tracing::debug!(request_id, "cancel 帧未能入队，交由对端 deadline 收尾");
            }
        }
        let mut table = self.table.lock();
        table.async_used = table.async_used.saturating_sub(1);
    }
}

/// 调用方一侧的收尾钩子：`call` 的 future 被丢弃（超时、被 abort）时把 pending
/// 摘掉。摘掉这一步同时归还配额，并触发 `PendingEntry::drop` 里的 `Cancel`。
struct CallGuard {
    table: Arc<Mutex<OutboundTable>>,
    request_id: u64,
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        // 锁作用域必须在这里结束：entry 的 `Drop` 还会再锁同一把锁，而
        // `parking_lot::Mutex` 不可重入。
        let entry = { self.table.lock().pending.remove(&self.request_id) };
        drop(entry);
    }
}

/// 入站额度分两类：异步任务占 task，`#[photon(blocking)]` 方法占 blocking 线程。
///
/// 混用一个计数器会造出一种很难查的故障：一批慢的阻塞 handler 把额度吃满，
/// 此时异步请求明明跑得动，却同样收到 `Overloaded`——过载信号失去意义。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Slot {
    Async,
    Blocking,
}

/// 入站配额。与出站分开计数：双向会话里「我过载」和「对端过载」不是一回事。
struct InboundQuota {
    async_used: usize,
    blocking_used: usize,
}

impl InboundQuota {
    /// 检查与计数在同一次持锁内完成。分成两次（先查后加）会让两个并发请求
    /// 同时通过检查，然后再一起把计数推过上限。
    fn try_acquire(&mut self, slot: Slot, opts: &SessionOptions) -> bool {
        let (used, max) = match slot {
            Slot::Async => (&mut self.async_used, opts.max_async_in_flight),
            Slot::Blocking => (&mut self.blocking_used, opts.max_blocking_in_flight),
        };
        if *used >= max {
            return false;
        }
        *used += 1;
        true
    }

    fn release(&mut self, slot: Slot) {
        let used = match slot {
            Slot::Async => &mut self.async_used,
            Slot::Blocking => &mut self.blocking_used,
        };
        *used = used.saturating_sub(1);
    }
}

/// 持有一次入站 handler 的配额，drop 即归还，并同步减掉 handler 契约计数。
struct InboundGuard {
    quota: Arc<Mutex<InboundQuota>>,
    handlers: Arc<AtomicUsize>,
    slot: Slot,
}

impl Drop for InboundGuard {
    fn drop(&mut self) {
        self.quota.lock().release(self.slot);
        self.handlers.fetch_sub(1, Ordering::AcqRel);
    }
}

/// debug 构建下慢 handler 的告警阈值。
///
/// 宏没法知道 handler 里会不会读文件，只能退一步在运行期量：超过这个数就说明
/// 它不该待在 runtime worker 上，应当加 `#[photon(blocking)]`。
#[cfg(debug_assertions)]
const SLOW_HANDLER_WARN: Duration = Duration::from_micros(100);

/// 一条会话。
pub struct Session<S, K>
where
    S: MessageSource,
    K: MessageSink,
{
    next_id: AtomicU64,
    outbound: Arc<Mutex<OutboundTable>>,
    inbound: Arc<Mutex<InboundQuota>>,
    /// 已 dispatch、还没回包的 handler 数。关闭时等的是它，不是配额数字。
    handlers: Arc<AtomicUsize>,
    cancels: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    registry: Arc<ServiceRegistry>,
    shutdown: CancellationToken,
    tx: mpsc::Sender<Bytes>,
    phase: AtomicU8,
    /// 出站是否允许发起新请求。`close()` 与收到 `GoAway` 都会关掉它。
    egress_open: AtomicBool,
    peer_goaway: AtomicBool,
    closing: AtomicBool,
    /// 进 `Closed` 的唯一通知点：`serve` 循环靠它知道这条连接该收尾了。
    closed: Arc<Notify>,
    limits: (Arc<AtomicU32>, Arc<AtomicU32>),
    reader: Mutex<Option<JoinHandle<()>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
    opts: SessionOptions,
    _marker: PhantomData<fn() -> (S, K)>,
}

impl<S, K> Session<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    /// 起一条会话：取旋钮 → 入队首帧 `Handshake` → spawn 两个 task。
    ///
    /// 必须在 runtime 上下文里调用（内部要 `tokio::spawn`）。
    pub fn new(
        source: S,
        sink: K,
        registry: Arc<ServiceRegistry>,
        opts: SessionOptions,
    ) -> Result<(Arc<Self>, SessionHandle<S, K>), RpcError> {
        if opts.max_message_len < HEADER_LEN as u32 {
            return Err(RpcError::InvalidInput(format!(
                "max_message_len={} < {HEADER_LEN}",
                opts.max_message_len
            )));
        }

        // 旋钮必须在把半身 move 进 task 之前取出来：之后 &self 再也拿不到它们。
        let limits = (source.recv_limit(), sink.send_limit());
        let (tx, rx) = mpsc::channel(WRITE_QUEUE);
        let shutdown = CancellationToken::new();

        // 握手帧先入队，writer 按 FIFO 写出，所以它必然是首帧。
        let local = Handshake::new(opts.max_message_len, CODEC_POSTCARD);
        let _ = tx.try_send(local.encode_frame());

        let session = Arc::new(Self {
            next_id: AtomicU64::new(FIRST_REQUEST_ID),
            outbound: Arc::new(Mutex::new(OutboundTable {
                pending: HashMap::new(),
                async_used: 0,
            })),
            inbound: Arc::new(Mutex::new(InboundQuota {
                async_used: 0,
                blocking_used: 0,
            })),
            handlers: Arc::new(AtomicUsize::new(0)),
            cancels: Arc::new(Mutex::new(HashMap::new())),
            registry,
            shutdown: shutdown.clone(),
            tx,
            phase: AtomicU8::new(PHASE_NEGOTIATING),
            egress_open: AtomicBool::new(true),
            peer_goaway: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            closed: Arc::new(Notify::new()),
            limits,
            reader: Mutex::new(None),
            writer: Mutex::new(None),
            opts,
            _marker: PhantomData,
        });

        let writer = {
            let shutdown = shutdown.clone();
            tokio::spawn(async move { writer_loop(sink, rx, shutdown).await })
        };

        let reader = {
            let this = Arc::clone(&session);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { this.reader_loop(source, shutdown).await })
        };

        *session.writer.lock() = Some(writer);
        *session.reader.lock() = Some(reader);

        let handle = SessionHandle {
            inner: Arc::clone(&session),
        };
        Ok((session, handle))
    }

    /// 等握手完成。用轮询而不是通知：一条会话只走一次，换来的是没有
    /// 「通知先到、等待后挂」这类时序坑。
    pub async fn wait_ready(&self) -> Result<(), RpcError> {
        let deadline = Instant::now() + self.opts.handshake_timeout;
        loop {
            match self.phase.load(Ordering::Acquire) {
                PHASE_READY => return Ok(()),
                PHASE_CLOSED => return Err(RpcError::Closed),
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(RpcError::Timeout);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.phase.load(Ordering::Acquire) == PHASE_CLOSED
    }

    /// 等这条连接彻底结束（本端关闭或对端消失都算）。
    ///
    /// `serve` 循环需要它：会话的 reader / writer 是内部 spawn 的，外面没有别的
    /// 信号能说明「这条连接已经不用再持有」。
    pub async fn wait_closed(&self) {
        loop {
            // 先建 waiter 再看状态：反过来的话，检查完到 await 之间发生的
            // `notify_waiters` 会丢，留下一个永远等不到的通知。
            let notified = self.closed.notified();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }

    /// 进 `Closed` 的唯一写点，顺带唤醒所有 `wait_closed`。
    fn set_closed(&self) {
        self.phase.store(PHASE_CLOSED, Ordering::Release);
        self.closed.notify_waiters();
    }

    pub fn options(&self) -> &SessionOptions {
        &self.opts
    }

    /// 发一次请求，返回应答 payload。`deadline_ms = 0` 表示不设期限。
    ///
    /// 超时后必须把 pending 摘掉：只丢 receiver 会让这条 entry 永远挂在表里，
    /// 配额被一条永远不会回的请求吃掉。
    pub async fn call(
        &self,
        service_id: u16,
        method: u64,
        payload: Bytes,
        deadline_ms: u32,
    ) -> Result<Bytes, RpcError> {
        self.wait_ready().await?;
        if !self.egress_open.load(Ordering::Acquire) {
            return Err(RpcError::Closed);
        }

        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        {
            // 检查、计数、插入必须在同一次持锁内完成。
            let mut table = self.outbound.lock();
            if table.async_used >= self.opts.max_async_in_flight {
                return Err(RpcError::Overloaded);
            }
            table.async_used += 1;
            table.pending.insert(
                request_id,
                PendingEntry {
                    tx: Some(tx),
                    table: Arc::clone(&self.outbound),
                    cancel: Some((self.tx.clone(), request_id)),
                },
            );
        }

        // 后面任何提前返回 / 超时 / future 被丢弃，都靠它摘表 + 发 Cancel。
        let _guard = CallGuard {
            table: Arc::clone(&self.outbound),
            request_id,
        };

        let frame = request_frame(service_id, request_id, method, deadline_ms, payload);
        if self.tx.send(frame).await.is_err() {
            // writer 已经不在了：把刚插进去的那条收回来，不让调用方白等。
            drop(self.take_pending(request_id));
            return Err(RpcError::Closed);
        }

        if deadline_ms == 0 {
            match rx.await {
                Ok(result) => result,
                Err(_) => Err(RpcError::Closed),
            }
        } else {
            match timeout(Duration::from_millis(u64::from(deadline_ms)), rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(RpcError::Closed),
                Err(_) => Err(RpcError::Timeout),
            }
        }
    }

    /// 发一条通知：不占 pending 名额，也永远没有回包。
    ///
    /// 失败只能靠日志察觉——这正是「用通知换一次往返」的代价，调用方必须自愿接受。
    pub async fn notify(
        &self,
        service_id: u16,
        method: u64,
        payload: Bytes,
    ) -> Result<(), RpcError> {
        self.wait_ready().await?;
        if !self.egress_open.load(Ordering::Acquire) {
            return Err(RpcError::Closed);
        }
        // 协议要求 `request_id != 0`：通知也要能被日志和对端 handler 区分开。
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(notification_frame(service_id, request_id, method, payload))
            .await
            .map_err(|_| RpcError::Closed)
    }

    /// 优雅关闭：宣告 → 等在飞归零 → 停两个 task → 兜底。
    ///
    /// 重复调用安全。
    pub async fn close(&self) -> Result<(), RpcError> {
        if self.closing.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        self.egress_open.store(false, Ordering::Release);
        let _ = self.tx.send(goaway_frame(false)).await;

        // 等在飞归零：pending 是「我发出去的」，handlers 是「别人发给我、我还没回」。
        let drained = async {
            loop {
                let pending = self.outbound.lock().pending.len();
                if pending == 0 && self.handlers.load(Ordering::Acquire) == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };

        if timeout(self.opts.close_timeout, drained).await.is_err() {
            // 兜底：不等了，下面照样停 task、唤醒等待者。
        }

        self.shutdown.cancel();
        self.join_tasks().await;

        let leftovers: Vec<PendingEntry> = {
            let mut table = self.outbound.lock();
            table.pending.drain().map(|(_, e)| e).collect()
        };
        drop(leftovers);

        self.set_closed();
        Ok(())
    }

    async fn join_tasks(&self) {
        let reader = self.reader.lock().take();
        if let Some(h) = reader {
            let _ = h.await;
        }
        let writer = self.writer.lock().take();
        if let Some(h) = writer {
            let _ = h.await;
        }
    }

    /// 摘一条 pending。**返回值必须在锁作用域外 drop**：`PendingEntry::drop`
    /// 还会再锁同一把 `Mutex`，而 `parking_lot` 不可重入，锁内 drop 就是死锁。
    fn take_pending(&self, request_id: u64) -> Option<PendingEntry> {
        self.outbound.lock().pending.remove(&request_id)
    }

    /// 出错即断连：连接状态已经不可信，继续读只会放大问题。
    fn fail(&self) {
        // swap 而不是「先读后写」：并发 reader / 意外路径同时 fail 时，
        // 只有一方去 drain pending，否则同一条 entry 会被两个线程摘两次。
        if self.phase.swap(PHASE_CLOSED, Ordering::AcqRel) == PHASE_CLOSED {
            return;
        }
        self.closed.notify_waiters();
        self.egress_open.store(false, Ordering::Release);
        self.shutdown.cancel();

        let leftovers: Vec<PendingEntry> = {
            let mut table = self.outbound.lock();
            table.pending.drain().map(|(_, e)| e).collect()
        };
        drop(leftovers);
    }

    async fn reader_loop(self: Arc<Self>, mut source: S, shutdown: CancellationToken) {
        loop {
            let received = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                msg = source.recv() => msg,
            };

            let msg = match received {
                Ok(msg) => msg,
                // 对端正常收尾：不是错误。
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.fail();
                    return;
                }
                Err(e) => {
                    self.fail();
                    let _ = e;
                    return;
                }
            };

            let limit = self.limits.0.load(Ordering::Acquire) as usize;
            let decoded = match parse_with_limit(msg, limit) {
                Ok(d) => d,
                Err(_) => {
                    self.fail();
                    return;
                }
            };

            // 处理也可能卡在写队列上，所以同样要能被关闭唤醒。
            let handled = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                r = self.handle_frame(decoded) => r,
            };

            if handled.is_err() {
                self.fail();
                return;
            }
        }
    }

    async fn handle_frame(self: &Arc<Self>, decoded: Decoded) -> Result<(), RpcError> {
        let header = decoded.header;
        let payload = decoded.payload;

        // 首帧必须是 Handshake，且只允许一次；这之前任何业务帧都是协议违规。
        if self.phase.load(Ordering::Acquire) == PHASE_NEGOTIATING && header.kind != Kind::Handshake
        {
            return Err(RpcError::Protocol {
                detail: format!("首帧必须是 Handshake，实际 {:?}", header.kind),
            });
        }

        match header.kind {
            Kind::Handshake => {
                if self.phase.load(Ordering::Acquire) != PHASE_NEGOTIATING {
                    return Err(RpcError::Protocol {
                        detail: "Handshake 只允许出现一次".into(),
                    });
                }
                let local = Handshake::new(self.opts.max_message_len, CODEC_POSTCARD);
                let peer = Handshake::decode(&payload)?;
                local.check_against(&peer)?;
                // 两个方向各用各的声明值，不取小。
                self.limits
                    .0
                    .store(local.max_message_len, Ordering::Release);
                self.limits.1.store(peer.max_message_len, Ordering::Release);
                self.phase.store(PHASE_READY, Ordering::Release);
                Ok(())
            }

            Kind::Response => {
                let result = if header.flags & FLAG_ERR != 0 {
                    Err(RpcError::Remote(error_body::decode(&payload)?))
                } else {
                    Ok(payload)
                };

                // 摘出来再交给等待者：留在表里会让下一次同 id 的响应重复投递。
                if let Some(entry) = self.take_pending(header.request_id) {
                    entry.finish(result);
                }
                // 找不到 pending 是正常的：调用方超时后 entry 已经被摘掉。
                Ok(())
            }

            Kind::Request => self.dispatch(header, payload, true).await,

            // M1 不实现通知投递：没有回包通道，静默丢弃比假装处理更诚实。
            // 通知与请求共用「查服务 → 扣额度 → spawn」这一套，差别只有「不回包」。
            Kind::Notification => self.dispatch(header, payload, false).await,

            Kind::Cancel => {
                if let Some(token) = self.cancels.lock().get(&header.request_id) {
                    token.cancel();
                }
                Ok(())
            }

            Kind::GoAway => {
                self.peer_goaway.store(true, Ordering::Release);
                self.egress_open.store(false, Ordering::Release);
                if header.flags & FLAG_GOAWAY_FORCE != 0 {
                    // 急停：不等在飞，立刻让它们停下（回包仍尽力发出）。
                    for token in self.cancels.lock().values() {
                        token.cancel();
                    }
                }
                Ok(())
            }

            // 预留编号在 validate 里已被拒；走到这里说明校验漏了。
            Kind::StreamChunk => Err(RpcError::Protocol {
                detail: "StreamChunk 是预留编号，不应到达会话层".into(),
            }),
        }
    }

    async fn dispatch(
        self: &Arc<Self>,
        header: Header,
        payload: Bytes,
        want_reply: bool,
    ) -> Result<(), RpcError> {
        let Some(svc) = self.registry.get(header.service_id) else {
            // 通知没有回包通道：给它回一个 `Response` 反而是协议噪声（对端没有对应的
            // pending，只会被丢掉），所以这里只记日志。
            if want_reply {
                self.send_error(header.request_id, 2, false, "unknown service")
                    .await?;
            } else {
                tracing::debug!(
                    service_id = header.service_id,
                    request_id = header.request_id,
                    "notification 指向未注册的服务，已丢弃"
                );
            }
            return Ok(());
        };

        // 先查服务再谈配额：服务不存在时「你过载了」是假话。
        //
        // 占哪类额度由 `Service::schedule` 决定——调度策略是展开期写死的属性，
        // 但扣额度发生在「进 handler」之前，那时还看不到实现里的分支。
        let slot = match svc.schedule(header.method) {
            Schedule::Async => Slot::Async,
            Schedule::Blocking => Slot::Blocking,
        };
        let guard = {
            let mut quota = self.inbound.lock();
            if quota.try_acquire(slot, &self.opts) {
                self.handlers.fetch_add(1, Ordering::AcqRel);
                Some(InboundGuard {
                    quota: Arc::clone(&self.inbound),
                    handlers: Arc::clone(&self.handlers),
                    slot,
                })
            } else {
                None
            }
        };

        let Some(guard) = guard else {
            let message = match slot {
                Slot::Async => "overloaded",
                Slot::Blocking => "blocking pool overloaded",
            };
            if want_reply {
                self.send_error(header.request_id, 1, true, message).await?;
            } else {
                tracing::warn!(
                    service_id = header.service_id,
                    request_id = header.request_id,
                    message,
                    "notification 因额度不足被丢弃"
                );
            }
            return Ok(());
        };

        let token = CancellationToken::new();
        // 登记必须在 spawn 之前：紧跟 Request 到达的 Cancel 才能命中。
        self.cancels.lock().insert(header.request_id, token.clone());

        let tx = self.tx.clone();
        let cancels = Arc::clone(&self.cancels);
        let shutdown = self.shutdown.clone();
        let request_id = header.request_id;
        let service_id = header.service_id;
        let method = header.method;
        let deadline_ms = header.deadline_ms;

        // 外层 task 只负责一件事：里层 panic 时也要回一个错。
        // 服务是被第三方代码调用的，panic 属于真实输入；没有这一层，那个请求
        // 就只能等调用方的期限自己超时，而服务端看起来「什么都没发生」。
        // 代价是每个请求多一次 spawn；将来若压测显示这层成本显著，再换成
        // `catch_unwind`（要引入 futures，且 panic 语义得搬进 async 块里）。
        let outer_tx = tx.clone();
        let outer_cancels = Arc::clone(&cancels);
        tokio::spawn(async move {
            let inner = tokio::spawn(async move {
                let deadline = async move {
                    match deadline_ms {
                        0 => std::future::pending::<()>().await,
                        ms => tokio::time::sleep(Duration::from_millis(u64::from(ms))).await,
                    }
                };

                #[cfg(debug_assertions)]
                let started_at = std::time::Instant::now();

                let outcome = tokio::select! {
                    biased;
                    _ = token.cancelled() => Err(RpcError::Remote(RemoteError {
                        code: 5,
                        retryable: false,
                        message: "cancelled by peer".into(),
                    })),
                    _ = shutdown.cancelled() => Err(RpcError::Closed),
                    _ = deadline => Err(RpcError::Timeout),
                    res = svc.call(method, payload) => res,
                };

                #[cfg(debug_assertions)]
                {
                    let elapsed = started_at.elapsed();
                    if elapsed > SLOW_HANDLER_WARN {
                        tracing::warn!(
                            service_id,
                            method,
                            ?elapsed,
                            "handler 超过 100µs 仍占用 runtime worker，考虑加 #[photon(blocking)]"
                        );
                    }
                }

                if !want_reply {
                    // 通知没有回包通道，失败只能记日志：静默吞掉等于让调用方
                    // 以为「发到了」，而服务端什么都没发生。
                    if let Err(e) = &outcome {
                        tracing::warn!(service_id, method, request_id, %e, "notification handler 失败");
                    }
                } else {
                    let frame = match outcome {
                        Ok(body) => response_frame(request_id, body, false),
                        Err(e) => {
                            // 框架级故障（Io / Protocol / RuntimeDropped）没有合适的业务码，
                            // 按 `0` 兜底；但回包路径上绝不静默丢帧。
                            let (code, retryable, message) = error_body::to_wire(&e)
                                .unwrap_or_else(|| (0, false, format!("{e}")));
                            let body = error_body::encode(code, retryable, &message);
                            response_frame(request_id, body, true)
                        }
                    };
                    let _ = tx.send(frame).await;
                }

                cancels.lock().remove(&request_id);
                // guard 最后 drop：先回包再减计数，关闭流程等的就是「已回包」。
                drop(guard);
            });

            if let Err(join) = inner.await
                && join.is_panic()
            {
                let text = panic_text(join);
                // 只回包不记日志，就变成「客户端收到 code 0，服务端日志一片安静」，
                // 排障时根本看不出是 handler 崩了。
                tracing::error!(service_id, method, request_id, %text, "handler panic");
                // guard 已在 unwind 里 drop，这里只剩「回包 + 摘 token」。
                let body = error_body::encode(0, false, &text);
                let _ = outer_tx.send(response_frame(request_id, body, true)).await;
                outer_cancels.lock().remove(&request_id);
            }
        });

        Ok(())
    }

    /// 回一个错误体。它也走正常写队列，与业务回包共用同一套顺序保证。
    async fn send_error(
        &self,
        request_id: u64,
        code: u32,
        retryable: bool,
        message: &str,
    ) -> Result<(), RpcError> {
        let body = error_body::encode(code, retryable, message);
        self.tx
            .send(response_frame(request_id, body, true))
            .await
            .map_err(|_| RpcError::Closed)
    }
}

impl<S, K> Drop for Session<S, K>
where
    S: MessageSource,
    K: MessageSink,
{
    fn drop(&mut self) {
        // 会话被丢弃但没有 close()：至少把两个 task 叫停，别留成后台僵尸。
        self.shutdown.cancel();
    }
}

/// 可 clone 的发起端。`Arc<Session>` 留给 accept 侧驱动 `close()`。
pub struct SessionHandle<S, K>
where
    S: MessageSource,
    K: MessageSink,
{
    inner: Arc<Session<S, K>>,
}

impl<S, K> Clone for SessionHandle<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S, K> SessionHandle<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    pub fn session(&self) -> &Arc<Session<S, K>> {
        &self.inner
    }

    pub async fn call(
        &self,
        service_id: u16,
        method: u64,
        payload: Bytes,
        deadline_ms: u32,
    ) -> Result<Bytes, RpcError> {
        self.inner
            .call(service_id, method, payload, deadline_ms)
            .await
    }

    /// 发通知：只有「发出去」这一个结果，没有回包可等。
    pub async fn notify(
        &self,
        service_id: u16,
        method: u64,
        payload: Bytes,
    ) -> Result<(), RpcError> {
        self.inner.notify(service_id, method, payload).await
    }

    pub async fn close(&self) -> Result<(), RpcError> {
        self.inner.close().await
    }
}

async fn writer_loop<K>(mut sink: K, mut rx: mpsc::Receiver<Bytes>, shutdown: CancellationToken)
where
    K: MessageSink,
{
    loop {
        let frame = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                // 排空再退：队列里的帧都是上层已提交的，丢掉等于静默丢数据。
                while let Ok(frame) = rx.try_recv() {
                    if sink.send(frame).await.is_err() {
                        return;
                    }
                }
                return;
            }
            frame = rx.recv() => frame,
        };

        match frame {
            Some(frame) => {
                if sink.send(frame).await.is_err() {
                    // 写失败是连接层面的死，叫停另一侧别再等。
                    shutdown.cancel();
                    return;
                }
            }
            None => return,
        }
    }
}

/// 从 `JoinError` 里掏出 panic 的文本：`panic!("boom")` 与 `panic!("{}", x)` 的
/// payload 类型不同，两种都要认。
fn panic_text(err: JoinError) -> String {
    let payload = err.into_panic();
    if let Some(text) = payload.downcast_ref::<&'static str>() {
        format!("handler panicked: {text}")
    } else if let Some(text) = payload.downcast_ref::<String>() {
        format!("handler panicked: {text}")
    } else {
        "handler panicked".into()
    }
}

fn base_frame(
    kind: Kind,
    flags: u8,
    service_id: u16,
    request_id: u64,
    method: u64,
    deadline_ms: u32,
    payload: Bytes,
) -> Bytes {
    let header = Header {
        kind,
        flags,
        service_id,
        request_id,
        method,
        deadline_ms,
    };
    let mut buf = header.encode();
    buf.extend_from_slice(&payload);
    buf.freeze()
}

fn request_frame(
    service_id: u16,
    request_id: u64,
    method: u64,
    deadline_ms: u32,
    payload: Bytes,
) -> Bytes {
    base_frame(
        Kind::Request,
        0,
        service_id,
        request_id,
        method,
        deadline_ms,
        payload,
    )
}

fn notification_frame(service_id: u16, request_id: u64, method: u64, payload: Bytes) -> Bytes {
    base_frame(
        Kind::Notification,
        0,
        service_id,
        request_id,
        method,
        0,
        payload,
    )
}

fn cancel_frame(request_id: u64) -> Bytes {
    base_frame(Kind::Cancel, 0, 0, request_id, 0, 0, Bytes::new())
}

fn response_frame(request_id: u64, payload: Bytes, is_error: bool) -> Bytes {
    let flags = if is_error { FLAG_ERR } else { 0 };
    base_frame(Kind::Response, flags, 0, request_id, 0, 0, payload)
}

fn goaway_frame(force: bool) -> Bytes {
    let flags = if force { FLAG_GOAWAY_FORCE } else { 0 };
    base_frame(Kind::GoAway, flags, 0, 0, 0, 0, Bytes::new())
}
