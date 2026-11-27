# L3 会话层详细设计

> 上位文档：`docs/design.md` 4 节、6 节「机制 1 / 机制 2」。
> 本层是**唯一有 task、有状态机、有超时**的一层。它把「一条通道」变成「一张 pending 表 + 两个方向的状态」。
> 产出：`Service::call` 可被调用的运行环境，以及一条连接的可预测生命周期。

---

## 1. 职责边界

**做**：分配 `request_id`、维护 pending、把 `Request` 派给 `ServiceRegistry`、把 `Response` 交回等它的调用方、处理 `Cancel` / deadline、发起并校验 `Handshake`、执行 `GoAway` 与优雅关闭、执行两组配额。

**不做**：不认识 `TcpStream` / `Endpoint` 变体（只吃 `MessageSource` / `MessageSink` 泛型）；不解析 24 字节头（那是 L2）；不认识业务类型（那是 L4 的宏产物）。

红线：`session` 目录里不许出现 `Tcp` / `Local` / `Shm` 字样，也不许 `match Endpoint`。

---

## 2. 结构与泛型

```rust
pub struct Session<S, K>
where S: MessageSource, K: MessageSink
{
    next_id:     AtomicU64,                              // 从 1 起；0 保留
    outbound:    Arc<parking_lot::Mutex<OutboundTable>>,  // pending + async_used（出站配额）
    inbound:     Arc<parking_lot::Mutex<InboundQuota>>,   // async_used（入站配额）
    handlers:    Arc<AtomicUsize>,                        // 已 dispatch、未回包的 handler 数
    cancels:     Arc<parking_lot::Mutex<HashMap<u64, CancellationToken>>>,
    registry:    Arc<ServiceRegistry>,                    // 客户端传 empty()，不是 Option（v0.18）
    shutdown:    CancellationToken,                       // 覆盖 reader 与 writer 两个 task
    tx:          mpsc::Sender<Bytes>,                     // bounded(1024)，可 clone
    phase:       AtomicU8,                                // 协商 / Ready / Closed（见 7.1）
    egress_open: AtomicBool,                              // 还能不能发起新请求
    peer_goaway: AtomicBool,                              // 对端宣告过关闭
    closing:     AtomicBool,                               // `close()` 只走一次
    closed:      Arc<Notify>,                              // `wait_closed()` 的通知源（v0.18）
    limits:      (Arc<AtomicU32>, Arc<AtomicU32>),        // (recv_limit, send_limit)，握手后写
    reader:      parking_lot::Mutex<Option<JoinHandle<()>>>,
    writer:      parking_lot::Mutex<Option<JoinHandle<()>>>,
    opts:        SessionOptions,
}
```

| 字段 | 关键约束 | 违反后果 |
|---|---|---|
| `next_id` | `AtomicU64::new(1)` | 首个请求 `request_id = 0`，与 3.3.1 硬约束冲突 |
| `outbound` | `pending` 与 `async_used` **同一把锁** | 并发检查通过后一起超额（`INV-07`） |
| `inbound` | 与 `outbound` **分开** | 双向会话里「我方过载」与「对方过载」共用一个数字，语义糊掉（v0.10） |
| `cancels` | reader 在 spawn **之前**登记 | 紧跟 `Request` 到达的 `Cancel` 扑空，取消静默丢失 |
| `shutdown` | 必须同时被 reader 与 writer 的 `select!` 监听 | 只取消 reader 时，卡在 `sink.send().await` 的 writer 关不掉 |
| `tx` | 普通 `Sender`，**不是 `Option`** | 置 `None` 会让在途 `Response` 与框架码 `7` 都发不出去 |
| `phase` + `egress_open` + `peer_goaway` + `closing` | 用一个 `AtomicU8` 表达「整条会话走到哪一步」，三个 bool 各管一件正交的事 | 用单一枚举表达全部状态，必然出现「已 Draining 但仍需回包」无法表达 |
| `limits` | **在 move 半身进 task 之前**取好 | move 之后 `&self` 再也拿不到 `Arc` |
| `reader` / `writer` | `Mutex<Option<JoinHandle>>` | 普通字段无法从 `&self` 里 `take()` 出来 await |
| `closed` | 进 `Closed` 的唯一写点（`set_closed`）都 `notify_waiters` | 漏一处就打不开 `wait_closed`，`serve` 循环会永远按着那条连接 |

**`wait_closed()` 是「连接结束」的唯一观测点（v0.18）**。reader / writer 都是内部 spawn 的，外部拿不到它们的句柄；`serve` 循环又不能靠轮询 `is_closed()`（每条连接一个 1 ms 定时器，1024 条连接就是 1024 个空转）。所以加一个 `Arc<Notify>`，进 `Closed` 的两个写点（`close()` 正常收尾、`fail()` 断连）都通知一次。

实现细节：`wait_closed()` 里**先建 `notified()` 再看 `phase`**。反过来写（先看状态再 await）会丢掉「检查完到 await 之间」那次通知——`notify_waiters` 只唤醒当时已经存在的 waiter，于是这个等待永远醒不过来。

`Session` 用 `AnySource` / `AnySink` 时退化为枚举门面（代价见 L1 文档第 8 节；**基准测试禁用门面**，`INV-18`）。

> 与 `design.md` 4.1 的关系：结构骨架以 4.1 为准，本节补出实现必需的两处细化——`cancels`（取消 token 表，与出站 pending 分开是为了不让取消路径与响应路径抢同一把锁）与 `limits`（旋钮 `Arc`，`design.md` 2.1/2.3 要求 L3 持有它）。**codec 不在泛型参数里**（v0.10）：全项目只用 `codec::Default`，握手填 `Default::ID`，理由见 `design.md` 4.1 的取舍说明——带上 `C` 只会造成「宣告 A、线上 B」的静默错线格式。

### 2.1 构造入口

```rust
impl<S, K> Session<S, K> where S: MessageSource, K: MessageSink {
    pub fn new(source: S, sink: K, registry: Arc<ServiceRegistry>, opts: SessionOptions)
        -> (Arc<Session<S, K>>, SessionHandle);
}
```

- 前置条件：**必须在 runtime 上下文内调用**（内部 `tokio::spawn` 两个 task）。
- `registry`：服务端传真实注册表，客户端传 `Arc::new(ServiceRegistry::empty())`（不是 `None`——`None` 会让「客户端收到 `Request`」这条路径的行为在类型上就分叉，而 4.4 要求双向对等：客户端也可能被回调）。
- 返回 `SessionHandle`：可 clone 的请求发起端（内部持 `Arc<Session>`），给 client stub 用；`Arc<Session>` 本身留给 `accept` 侧驱动 `close()`。
- **构造内部第一步就是取旋钮**：`let (rl, sl) = (source.recv_limit(), sink.send_limit())`，然后才 spawn。
- `memory` / 测试矩阵走这条；`Transport::connect` 成功后由封装层再调一次 `new`。

### 2.2 启动序列

1. spawn writer task（`sink` 与 `send_limit` 的 `Arc` 一起 move 进去）；
2. spawn reader task（`source` 同理）；
3. **双方各自主动发一条 `Handshake`**（12 字节，首帧，`protocol_version = 2`、`codec::Default::ID`、`opts.max_message_len`）；
4. 状态 `Connected → Negotiating`，等对端 `Handshake`；
5. 收到并通过 §9 校验 → `Ready`。此刻把两个上限写进旋钮（L3 存着的 `Arc`）：`limits.0.store(opts.max_message_len)`（本端接收上限）、`limits.1.store(peer.max_message_len)`（对端接收上限，也就是本端发送上限）。握手前两者恒为 64 KiB。

   这两行必须各有一条验收，否则「两个方向各用各的」只被证明了半边：`peer_declared_limit_governs_sending`（对端声明 1 MiB → 本端能发 200 KiB，证明 `limits.1`）与 `own_declared_limit_governs_recv`（本端声明 1 MiB → 收得下对端 200 KiB 的回包，证明 `limits.0`）。变异验证就删对应那一行：前者删 `limits.1`、后者删 `limits.0`，各自只让本方向的用例变红。

> 双方都主动发，不做「客户端先发、服务端再回」的角色机——4.4 的双向对等要求这样做，否则服务端回调客户端时握手语义要分两套。
>
> 握手时还要做两条**建会话前**的校验：`opts.max_message_len >= 24`；shm transport 上 `opts.max_message_len <= slot_size`（`slot_size` 从段头读，见 L1 6.4）。两者不满足直接拒绝，不要拖到运行时才发现帧超限。
>
> 对端发来的坏值走另一条路：`Handshake::decode` 拒 payload 长度 / 保留位 / `< 24`，`check_against` 拒版本与 codec，失败即 `fail()`（建连期断连，`INV-02`）。**造这类反例不要用 `Handshake::encode`**——它对 `< 24` 有 `debug_assert`，拦的是「我方发错」这种编程错误，对端发错走的是网络，`encode` 根本造不出那个字节；用例里只能手写 payload（`raw_handshake_frame`）。

---

## 3. 两张表：出站 pending 与入站配额

**v0.10 拆表的原因**：v0.9 用一张 `PendingTable { pending, async_used, blocking_used }` 同时表达两件事，坏在两点——① 服务端收请求根本不往 `pending` 里插，于是入站侧**根本没有入口**能做原子配额检查，`try_reserve` 在入站调不起来；② 双向会话里本端出站与对端入站挤同一组计数，「谁该回 `Overloaded`」变得说不清。

### 3.1 出站：`OutboundTable`

```rust
struct OutboundTable {
    pending:    HashMap<u64, PendingEntry>,
    async_used: usize,        // 本端发出、等 Response 的条数
}

struct PendingEntry {
    tx:    oneshot::Sender<Response>,  // Response = Result<Bytes, RpcError>
    guard: PermitGuard,                // Drop 时 remove + 归还计数
}
```

`PendingEntry` 里**不能**只放裸 `oneshot::Sender`：调用方超时、取消、panic unwind 都可能在响应到达前断开，entry 必须由 `Drop` guard 显式回收，否则配额永久泄漏（`design.md` 4.2 第 ⑧ 步）。

出站只有一个计数：**出站不区分 async / blocking**——形态是 handler 的属性，与本端发出去的请求无关（客户端不跑 handler，`INV-08`）。

### 3.2 `try_reserve`（出站唯一的临界区）

```rust
impl OutboundTable {
    fn try_reserve(&mut self, id: u64, entry: PendingEntry, max_async: usize)
        -> Result<(), RpcError>;   // Err(RpcError::Overloaded)
}
```

语义：**检查 + 计数 + 插入，三件事在同一次持锁内完成**（`INV-07`）。

**不要**把计数改成 `AtomicUsize` + CAS 循环：两个并发调用会同时通过检查再一起插入；CAS 重试还会让「过载」这件事变得不可复现（压测时今天过载、明天不过载）。

### 3.3 入站：`InboundQuota`

```rust
struct InboundQuota {
    async_used:    usize,
    blocking_used: usize,
}

struct InboundGuard { /* 持 Arc<Mutex<InboundQuota>> + Slot */ }
```

| 入口 | 检查 | 归还 |
|---|---|---|
| `try_acquire(Slot::Async, max_async)` | 未超 `max_async_in_flight` → `async_used += 1`，返回 guard | `InboundGuard::drop` → `-= 1` |
| `try_acquire(Slot::Blocking, max_blocking)` | 未超 `max_blocking_in_flight` → `blocking_used += 1` | 同上 |

抢不到 → **当场回框架码 `1`**（`send_error`，见 6.2），不 spawn、不登记 cancel token。这一步放在「查 registry 之后、spawn 之前」：先查服务是因为 `UnknownService(2)` 比 `Overloaded(1)` 更准确（服务不存在时谈配额没有意义）。

`handlers: AtomicUsize` 是给关闭流程用的**契约计数**（7.3 第 2 步）：guard 建立时 +1、drop 时 -1，与 `InboundQuota` 分开是因为关闭要等的是「有没有活没回包」，而不是「配额数字」，后者在正常流量下总是非零。

### 3.4 两类在飞工作必须分开数

| 计数 | 含义 | 谁读它 |
|---|---|---|
| `outbound.pending` | 我发出去、等 `Response` | 关闭第 2 步（等它归零） |
| `handlers` | 对端发来、已进入 handler、还没回 `Response` | 关闭第 2 步（等它归零） |

两者都要等，方向不同。**「在飞」= 已进入 handler**，不含「已写进 `tx` 但 writer 还没写出去」的请求（那不可观测，承诺不了）。

> **guard 与锁的自锁陷阱（实现必踩）**：guard 被 drop 时要 `remove` + 归还计数，所以它必须持 `Arc<parking_lot::Mutex<..>>`。而 `parking_lot::Mutex` 不可重入——若「移除 entry」在持锁作用域内就把 entry drop 掉，guard 会在同一把锁上再 lock 一次，直接死锁。规定：**remove 只把 entry 移出 map，entry 的 drop 必须发生在锁作用域结束之后**（写法：`let entry = { let mut t = table.lock(); t.pending.remove(&id) };` 再让它自然 drop）。不写这条，M1 一定会在「响应到达」这条最热路径上死锁。

---

## 4. reader task

```
loop {
  select! {
    biased;
    _ = shutdown.cancelled() => break,
    r = source.recv() => match r {
      Err(e)  => { mark_ingress(Closed); fail_all_pending(e); break }
      Ok(msg) => dispatch_one(msg)   // 不 await 业务，不同步 decode
    }
  }
}
```

### 4.1 `dispatch_one` 的分流（顺序敏感）

1. **若尚未 `Ready`**：只允许 `Handshake`，其它 kind 一律 `Protocol` 断连；收到第二个 `Handshake` 也断连。
2. 解析头（固定偏移，零分配）→ 按 kind：
   - `Response`：`outbound.lock().pending.remove(id)` → `oneshot.send(...)`；未知 id **静默忽略**并记 `debug`（对端超时后回包是正常竞态）。
   - `Cancel`：`cancels.lock().get(id)` → `token.cancel()`；未知 id 静默忽略。
   - `GoAway`：`state.egress = Draining`（本端不再发新请求）＋ **`state.peer_goaway = true`**；若 `state.self_goaway` 也已为真（双方都宣告过），则 `state.ingress = Draining`（此后到达的 `Request` 回 框架码 `7`）。bit2 急停则直接走急停分支（见 6.4）。**注意 `GoAway` 本身不改 `ingress`**，除非满足「双方都已宣告」。
   - `Request` / `Notification`：先看 `state.ingress`：
     - `Draining` → **不 dispatch**，直接回框架码 `7`（`Request` 才回，`Notification` 静默丢弃）；
     - `Open` → 查 `registry[service_id]`：查不到 → 回框架码 `2`（回包侧陈述：`send_error(req_id, 2, false, "unknown service")`，见 `02` 7.3）；查得到再走 6.2。
3. `Request` 的准入顺序（一步都不能挪）：
   - **先**入站配额 `inbound.try_acquire(svc.schedule(method))`（`Slot::Async` / `Slot::Blocking` 两张表分别计，v0.19）；抢不到 → 合成 `code = 1` 回包，结束（不 spawn、不登记 token）；
   - **再** `cancels.lock().insert(id, token)`；
   - **再** `tokio::spawn(...)`，把 `Bytes` move 进去；`handlers += 1`；
   - handler 结束时由 guard 移除 token、`handlers -= 1`、归还入站配额。
4. `Notification` 与 `Request` 共用同一个 `dispatch`（查服务 → 扣额度 → spawn），差别只有 `want_reply = false`：**未注册服务、额度不足都只记日志，绝不回 `Response`**——给通知回包是协议噪声，对端没有对应的 pending，收到只会丢掉。

### 4.2 为什么 decode 不在 reader 里

reader 只做「头解析 + 路由 + 登记 + spawn」。`codec.decode` 在 spawn 出来的 task 里做：

- 大 payload 解码可能几十 µs，留在 reader 里会卡住整条通道的多路复用（`design.md` 8 节第 1 条）；
- `#[spile(blocking)]` 方法在 `spawn_blocking` 的闭包里 decode + 执行，一次线程切换干两件事。

### 4.3 reader 出错的处理

| 错误 | 处理 |
|---|---|
| `UnexpectedEof`（对端正常关） | `ingress = Closed`；所有 pending 置 `Closed`；`shutdown.cancel()` 让 writer 也退 |
| `Protocol` / `InvalidData` | 同上，但 pending 置 `Protocol`；记 `error` 日志（这是真 bug） |
| `InvalidInput`（不该出现在读侧） | 视为实现 bug，按 `Protocol` 处理 |

---

## 5. writer task

```
loop {
  select! {
    biased;
    _ = shutdown.cancelled() => {
        // 排空：把已入队、但还没写出去的帧全部写掉（GoAway / 最后一个 Response 都在里面）
        while let Ok(msg) = rx.try_recv() {
            if let Err(e) = sink.send(msg).await { mark_egress(Closed); fail_all_pending(Io); return }
        }
        break
    }
    msg = rx.recv() => match msg {
        None => break,                                   // 所有 Sender 已 drop
        Some(m) => if let Err(e) = sink.send(m).await { mark_egress(Closed); shutdown.cancel(); fail_all_pending(Io); break }
    }
  }
}
// 退出即 drop(sink) → 触发 L1 的 drop 契约
```

- **单一写者**：`Sink` 是 `&mut self`，天然只有一个 task 能写（L1 文档 2.3）。
- **为什么是「取消后排空」而不是「等所有 `Sender` drop」（v0.10）**：`Session` 在 `Arc` 里，`close(&self)` 拿不到 `&mut`，**没有任何办法从 `Arc` 里单独 drop `tx` 这个字段**；改 `Option::take` 要求独占，改 `Mutex<Option<Sender>>` 则每帧一把锁。反过来做才成立：关闭时只需 `shutdown.cancel()`，writer 收到取消后自己把 `rx` 排空再退。这条路径不依赖 sender 计数，也不会漏掉排在 `GoAway` 后面的帧。
- **排空期的 `send` 失败**：直接按 `Io` 收尾（连接已经坏了，没必要再尝试）。
- writer 也是「transport 差异的吸收点」：内核缓冲满、shm 环满，在这里都表现为一次 `await`。
- `select!` 必须同时监听 `shutdown.cancelled()`——否则对端不读导致 `send()` 永久挂住时关不掉。

---

## 6. 一条请求的完整路径

### 6.1 时序与每步失败处理

| 步 | 动作 | 失败时 |
|---|---|---|
| ① | `next_id.fetch_add(1)` → `id`（非 0） | — |
| ② | `outbound.try_reserve(id, entry, max_async_in_flight)` | 满 → `Err(Overloaded)`（本地错误，请求没发出去） |
| ③ | `tx.send(bytes).await` | 队列满则等待（这是背压，不是错误）；`tx` 已关 → `Closed` |
| ④ | 服务端 reader：头解析 → `registry[service_id]` → `inbound.try_acquire(shape)` → 登记 cancel → spawn | 服务缺失 框架码 `2`；配额满 框架码 `1`；方法缺失（handler 内 match 落空）框架码 `3` |
| ⑤ | handler task：`codec.decode(payload)` → 执行 | 解码失败 `PeerRejected{4}`；panic → `PeerRejected{0}` |
| ⑥ | 编码响应 → `tx.send()` | 发送失败 → 连接废弃，pending 全部 `Closed` |
| ⑦ | 客户端 reader：`pending.remove(id)` → `oneshot.send` | 调用方已放弃 → 忽略，guard 已归还 |
| ⑧ | 超时 / 取消：guard drop → `remove` + 归还 permit + 发 `Cancel` | 见 6.3 |

**第 ③ 步的 `tx` 在 `Draining` 时也必须可用**——关闭途中要回的 `Response` 和框架码 `7` 都走它（`INV-09`）。

### 6.2 dispatch 内部（服务端）

1. `let svc = registry.get(service_id)` → 无 → `send_error(id, 2, "unknown service")`（回包侧直接陈述，见 `02` 7.3）；
2. 扣额度：`svc.schedule(method)` 给出 `Slot::Async | Slot::Blocking` → `inbound.try_acquire(slot, opts)`。**必须在 spawn 之前**：等 `call` 被 poll 才知道走哪个分支，那时额度已经扣过了。抢不到 → `send_error(id, 1, "overloaded" | "blocking pool overloaded")`，不登记 cancel token（v0.19）；
3. 方法匹配在 handler 内做（宏生成 `match method { ... }`）→ 落空 → `Err(HandlerError::Framework { code: 3, message: "unknown method" })`（`Service::call` 的失败面就是 `HandlerError`，不必绕 `RpcError`）；
4. `tokio::spawn(async move { svc.call(method, payload).await })`，**句柄必须 await**：实现上是「外层 task await 里层句柄」，每个请求多一次 spawn，换来「里层 panic 也有人回包」；
5. 结果映射：`svc.call` 交出 `Result<Bytes, HandlerError>`，**三分支各自成帧**（`02-l2-protocol.md` 7.1）：
   - `Ok(bytes)` → `Response`，flags = `0`；
   - `Err(HandlerError::Business(tail))` → `Response`，flags = `0x09`，payload 原样就是 `tail`；
   - `Err(HandlerError::Framework { code, message })` → `error_body::encode(code, &message)` + flags = `0x01`。`code` 只允许 `0..=7`：`RpcError` 若在 handler 内部被 `into` 成 `HandlerError`，由 `wire()` 决定码，`Io`/`Protocol`/`RuntimeDropped` 落 `0`（**不许静默丢帧**）；
   - 外层 `Err(JoinError)`（panic）→ 框架码 `0` + panic 文本（`panic!` 的 `&str` / `String` 两种 payload 都认）。
6. `#[spile(blocking)]` 分支的 `JoinError` 在 dispatch 内部就变成 `HandlerError::Framework { code: 0, .. }`，L3 不必再判一次「这是不是 panic」——`spawn_blocking` 由宏生成，L3 只看得到一个 `BoxFuture`。
7. `Drop` 句柄才会静默丢失 panic——这是「客户端只等到超时、日志里什么都没有」的经典形态。所以句柄必须 await；本端可见性：v0.19 起 panic 走 `tracing::error!(service_id, method, request_id, %text)`，不再只有默认 panic hook 的 stderr 一行。

> **不要再写「`Timeout → 6`、`Overloaded → 1`」这种散装映射（v0.10）**：它在本文、`02`、`04`、`design.md` M2 验收里演化出了四份互不相同的定义。现在只有 `error::wire()` 一张表，且它只服务「`RpcError` 被 `into` 成 `HandlerError`」这一条通道：`Timeout → 6` / `Overloaded → 1` 之所以成立，是因为这两条出现在**回包侧**；发起侧的 `Timeout` / `Overloaded` 永远不会被编码。方向规则见 `02-l2-protocol.md` 7.3。

### 6.3 取消与超时

| 机制 | 实现口径 | 语义强度 |
|---|---|---|
| 客户端超时 | `tokio::time::timeout` 包住 `oneshot` 的 await；超时由 `CallGuard` 回收 entry + permit | **准确**（本端判定） |
| 调用方放弃 | `call` 的 future 被 drop（`abort` / 上层 `select!` 抢先）同样触发 `CallGuard`：摘表 + 归还配额 | **准确** |
| 协议 `deadline_ms` | 客户端按「**发送时刻起还剩多少**」填剩余预算；服务端从**解析完 24B 头**开始计时，因此实际可用 = 头部值 + 传输延迟 | **尽力**（能停就停，停不下就跑完丢结果） |
| `Cancel` 发送 | `PendingEntry::drop` 里 `try_send`：走到 drop 说明结果从未送达（超时 / 放弃 / 断连），告诉对端别算了。**用 `try_send` 不用 `send`**——为一条「别算了」卡在背压上比不发更糟；队列满时对端仍会按 deadline 自行收尾 | 尽力 |
| `Cancel` 到达 | `async fn` handler：`select!` 到 `token.cancelled()` → 中止，回 `PeerRejected{5}` | 有效 |
| `Cancel` 到达 | `fn` / `#[spile(blocking)]` handler：标记已取消，**继续跑完、结果丢弃、不回 `Response`** | 只丢结果 |
| 慢 handler 告警 | debug 构建下从 `call` 开始 poll 到结束计时，超过 100 µs 打 `tracing::warn`，提示该方法应加 `#[spile(blocking)]` | 诊断，非语义 |

必须写进文档的三条：

- **同步 handler 不可中止**——同步代码里没有可中断点，任何声称能「停掉」它的机制都是假的。所以长耗时计算不要写成 sync 方法。
- **超时与 `Cancel` 的分工**：超时是本端判定，一定准确；`Cancel` 是尽力通知，可能因队列满而丢掉。只靠 `deadline_ms` 让服务端停下也成立（它从解析完头开始计时），`Cancel` 的价值在于「客户端已放弃、服务端还没醒来」的那段——客户端判超时的时刻早于服务端 deadline 到期。
- **服务端 deadline 只是尽力**——给每个请求挂一个 `sleep`，在万级 QPS 下就是万级 timer 注册。要精确只能换粗粒度 timer wheel，那是优化不是语义承诺。
- **`deadline_ms` 填的是剩余预算，不是原始超时**——如果调用方在队列里等了 20 ms 才真正写出去，头里就必须扣掉这 20 ms。填原始值的实现会让服务端在「客户端早就放弃了」之后还在算，浪费且掩盖真实超时。

### 6.4 急停（`GoAway` bit2 = 1）

跳过「等两类归零」：立即 `shutdown.cancel()` → 两个 task 被取消（**writer 不做排空**）→ drop 两端 → 所有 pending 置 `Closed`。即使 `tx` 里还有没写出去的 `Response` 也一并丢弃，并在日志里记下丢弃条数（否则「急停后客户端收到 `Closed` 但服务端以为回了包」永远查不清）。

发起入口是 `Session::abort()`（不挂到 `SessionHandle` 上：句柄的意义是「发起请求」，单方面给整条连接判死不在它的职责内）。顺序写死：

1. `goaway_sent` 去重后 `try_send(goaway_frame(true))`——急停不能被自己的写队列背压拖住，塞不进去就记一条 `debug` 直接断；
2. `abrupt.store(true)`。**必须在取消之前**：writer 是被取消唤醒的，先置位才能保证它醒来时看得到「这次不排空」；
3. 取消所有 handler token（它们的结果已经没有接收方）；
4. `fail()`：`phase = Closed`、drain pending、`shutdown.cancel()`。

> 急停与优雅关闭的差别只有一处：**writer 是否排空**。实现用的是 `Arc<AtomicBool> abrupt`，而不是给取消信号带 payload——token 是 `CancellationToken`，没有 payload 可带，加一个原子位比造两套 token 便宜。writer 在取消分支里读它：`false` → 逐帧 `send` 完再退；`true` → 全丢，并 `tracing::debug!(dropped, "急停：丢弃未写出的帧")`。丢弃是允许的，**静默**丢弃不是。

---

## 7. 关闭状态机

### 7.1 两方向状态 + 两个宣告位

| 方向 | `Open` | `Draining` | `Closed` |
|---|---|---|---|
| 出站（本端 → 对端） | 接受新请求入 `mpsc` | 拒绝新请求（`RpcError::Closed`），已入队照常写 | `sink` 已 drop |
| 入站（对端 → 本端） | 正常 dispatch | 不再 dispatch 新 `Request`（回框架码 `7`），仍收在飞 `Response` | `source` 已 drop |

```rust
struct ClosingState {
    ingress:     AtomicU8,   // 0 Open / 1 Draining / 2 Closed
    egress:      AtomicU8,
    self_goaway: AtomicBool, // 本端已发过 GoAway
    peer_goaway: AtomicBool, // 已收到对端 GoAway
    ingress_draining: AtomicBool, // 双方都宣告过之后才置位（代码里就是这个名字）
}
```

两个宣告位是 v0.10 补的：`ingress` 什么时候进 `Draining` 不能只看「本端关了没有」或「对端关了没有」，必须看**双方是否都宣告过**（理由见 7.2）。

### 7.2 触发点（写入代码前先看这张表）

| 事件 | egress | ingress | 备注 |
|---|---|---|---|
| 本端调 `close()` | `Draining` + `self_goaway = true` | **保持 `Open`** | 只停发起，不停收活：对端在收到我们 `GoAway` 之前发出的请求仍是合法请求 |
| 收到对端 `GoAway` | `Draining` + `peer_goaway = true` | **保持 `Open`** | 此后到达的都是对端在 `GoAway` 之前发出的，属于必须回完的那类 |
| **双方都已宣告**（`goaway_sent && peer_goaway`，实现里叫 `ingress_draining`） | — | `Draining` | 到这里「还会有合法在飞请求」的前提不成立；此后到达的 `Request` 回 框架码 `7` |
| 收尾超时（默认 5 s）或急停 | `Closed` | `Closed` | 强制收尾 |
| reader 出错 / EOF | — | `Closed` | 同 `03` 4.3 |
| writer 出错 | `Closed` | — | — |

**没有「只关一边」的公开 API**：半关闭是 transport 实现细节，不是会话层 API。

> **为什么 `close()` 也不能立刻让 `ingress` Draining（v0.10 修正 v0.9）**：v0.9 的六步关闭第 1 步写「两个方向同时 Draining」，同时又在验收里承诺「在飞全部完成」。这两句在竞态下必然冲突——对端在收到 `GoAway` 前 1 µs 发出的请求，到本端时 `ingress` 已 Draining，会被打成 框架码 `7`，而它明明是一个合法请求。所以 `close()` 只影响 `egress`；`ingress` 一律等到「双方都宣告过」或超时兜底。**框架码 `7` 只在这个窄窗口里出现**（双方都宣告关闭之后仍有请求冒出来），而不是关闭一开始就到处回它。

### 7.3 优雅关闭六步（`flags` bit2 = 0）

1. 把 `GoAway` **写进 `tx`**（先进队列，保证它前面的 `Response` 都能出去）；`egress = Draining`、`self_goaway = true`。**入站不动**，继续 dispatch 并回包（`INV-10a`）。
2. 等两类在飞工作归零：`outbound.pending` 与本端 `handlers`。期间 `tx` 照常可用。实现是 1 ms 轮询 `is_drained()`（语义等价；将来换 `Notify` 是优化，不是契约）。**发帧与关 egress 的顺序写死为「先关 egress、再发帧」**：反过来的话，两者之间进来的请求发得出去但没人会等它；发帧本身由 `goaway_sent` 去重，且**已经收到过对端宣告也照发**。
3. 归零后**只做一件事：`shutdown.cancel()`**（同时通知 reader 与 writer）。**不要试图 drop `Session` 的 `tx`**——它在 `Arc` 里，`&self` 拿不到 `&mut`；通道收口交给 writer 的排空退出（见第 5 节）。
4. 两个 task 各自退出：writer **先排空 `rx` 再退**、退出即 drop `sink`；reader 退出即 drop `source`。**不需要 `flush`**——最后一次 `send().await` 返回 `Ok` 即数据已交给内核缓冲。注意 drop `sink` 只是**加速路径**：能让对端尽早读到 EOF，但不是关闭契约的一部分（L1 2.2）。无半关闭的实现上这一步等价于空操作，收尾照样靠第 1 步的 `GoAway` 和第 6 步的超时。
5. `close()` 里从 `Mutex<Option<JoinHandle>>` 里 `take()` 出两个句柄并 await；`JoinError` 必须当场处理并归为断连，不能 `let _ =`。
6. 兜底：`opts.close_timeout`（默认 5 s）未走完 → 强制 `shutdown.cancel()` + `abort` 两个 task，所有 pending 与 handler guard 置 `Closed`。
7. 最后一步 `set_closed()`：`phase = Closed` + `closed.notify_waiters()`。**这一处和 `fail()` 是仅有的两个写点**，其他任何地方都不许直接改 `phase`——漏掉通知会让 `serve` 循环永远认为连接还活着。

> **第 3 步之后还有没有人能写？** 第 2 步已经保证 pending 与 handler 双零，此后没有合法的 `tx.send()` 来源。剩下的竞争只有两种：某个 handler 恰好在第 2 步读到零的瞬间完成（它的 `Response` 已经在队列里或正在 `send`，writer 排空照样写出去），以及应用层在 `close()` 之后硬塞请求（被 `egress = Draining` 挡掉，返回 `Closed`）。排空循环之后的极少数迟到帧会拿到 `send` 错误——那是「队列已关」，不是数据丢失。

### 7.4 收到 `GoAway` 的一侧做什么（写死，避免再出现方向歧义）

- 本端 `egress → Draining`（新请求 `Closed`），`peer_goaway = true`；
- **入站保持 `Open`，继续 dispatch 并继续回包**（`INV-10a`）；只有「本端也发过 `GoAway`」之后，`ingress` 才进 `Draining`；
- 若本端也要收尾，自己再发一条 `GoAway`（双方各发一条即可完成双向关闭）。**即便本端已经收到过对端的宣告，`close()` 照样发自己这条**——`ingress` 的触发条件是「双方都宣告过」，少发一条，这个条件永远不成立；
- **不要靠「对端把连接断掉」来判断自己该退出了**：收到宣告就起一个 `spawn_finish_when_drained` 后台任务，等 `pending` 与 `handlers` 双零（`close_timeout` 兜底）后 `shutdown.cancel()`。socket 上的 EOF 只是加速路径，named pipe / shm 上根本没有 EOF；少了这条，收到 `GoAway` 之后会话会一直挂着，直到用户自己再调 `close()`。

`GoAway` 的语义是**连接级开始收尾**（双向停发起、双向继续回包），不是「我只关入站」（v0.8 的写法，与它自己的第 1 步冲突，已废弃）。但**不要**把它类比成 gRPC `GOAWAY`：后者靠 `last_stream_id` 水位划边界，本协议 v0.7 已删掉该字段（对称会话下没有良定义），所以没有「按 id 判断哪些还算 in-flight」的能力，能承诺的只有「已进入 handler 的请求一定回包」。

### 7.5 无半关闭的实现上的关闭差异

半关闭是**实现能力**，不是契约（L1 2.2）。因此这里的差异按能力位判定，不按 transport 名字判定——L3 不认识具体 transport，也不该认识。

| 能力 | 收尾表现 | 测试怎么断言 |
|---|---|---|
| 有半关闭（`tcp` / `local`(Unix) / `memory` / `shm`） | writer drop `sink` 后对端读到 EOF，可能比 `GoAway` 更早收敛 | 可以断言「drop 后对端在 N ms 内读到 `UnexpectedEof`」 |
| 无半关闭（Windows named pipe，尚未实现） | `Draining → Closed` 对端**完全不可见**，唯一信号是 `GoAway` | 不能断言 EOF；断言「对端收到 `GoAway` 后在本端 `close_timeout` 内退出」 |

与能力无关、任何实现都必须成立的三条：

- **`close_timeout`（默认 5 s）从本端调 `close()` 的时刻起算**，不是从对端回 `GoAway` 起算——否则对端不配合时要白等一个完整周期；
- **「双方宣告后才拒」这条规则更重要**：本端只能靠对端的 `GoAway` 判断它是否还会发请求，丢掉这个信号就没有任何依据（`INV-10b`）；
- **框架码 `7` 的窄窗口不变**：只在双方都宣告关闭之后仍有 `Request` 冒出来时出现。

能力位本身只出现在 `tests/transport.rs` 的 `Harness::PEER_SEES_EOF` 上：声明 `true` 的实现必须真给 EOF，声明 `false` 的必须真给不出（断言「读不到结束信号」）。**不要**把它提进 `Transport` trait 让 L3 分支。

## 8. 错误映射总表（L3 的唯一权威）

**所有「上线」的框架错误都过 `error::wire()`（或 `send_error` 的三码白名单）；本地变体在发起侧永不上线。**

| 场景 | 用户看到（发起侧视角） | 是否断连 | 生成方式 |
|---|---|---|---|
| 本端出站配额满 | `RpcError::Overloaded` | 否 | 本地构造，**不上线** |
| `egress` 已 `Draining`/`Closed` | `RpcError::Closed` | 否（已有连接仍可收尾） | 本地构造，**不上线** |
| 发送超对端 `max_message_len` | `RpcError::InvalidInput` | **否** | 本地构造（L1 标记文本识别，见 L1 7.2） |
| 帧长超本端上限 / 未知 kind / 保留位 / 握手不符 | `RpcError::Protocol` | 是 | 本地构造 |
| socket / pipe / 环错误 | `RpcError::Io` | 是 | 本地构造 |
| 本端超时 | `RpcError::Timeout` | 否 | 本地构造 |
| blocking client 的 runtime 已 drop | `RpcError::RuntimeDropped` | 否（仅同步客户端路径） | 本地构造 |
| 对端 handler panic | `PeerRejected { code: 0 }` | 否 | 回包侧 `error_body::encode(0, text)` |
| 对端入站配额满 | `PeerRejected { code: 1 }` | **是**（退避） | 回包侧 `error_body(1)` |
| 服务未注册 | `PeerRejected { code: 2 }` | 否 | 回包侧 `send_error(.., 2, ..)` |
| 方法未注册 | `PeerRejected { code: 3 }` | 否 | `HandlerError::Framework { code: 3 }`（宏生成的分支） |
| payload 解码失败 / 参数不合法 | `PeerRejected { code: 4 }` | 否 | 回包侧 `error_body(4)` |
| 对端按 `Cancel` 中止 | `PeerRejected { code: 5 }` | 否 | 回包侧 `HandlerError::Framework { code: 5 }` |
| 对端自己判 deadline | `PeerRejected { code: 6 }` | 否（换更长期限是调用方策略） | 回包侧 `wire(Timeout)` |
| 双方宣告关闭后仍有 `Request` / 对端在优雅关闭 | `PeerRejected { code: 7 }` | **是**（重连后） | 回包侧 `send_error(.., 7, ..)` |
| 用户业务错误（v0.31） | `CallError::Rejected(E)`，**不是 `RpcError`** | 由 `E` 自己表达 | handler 交出 `Err(HandlerError::Business(tail))`，tail 就是 `E` 的编码 |

铁律：**发起侧的本地变体永不因为「对端说了什么」而被构造**；`PeerRejected { code: 1 }` 不许在客户端被翻译成 `Overloaded`。回包侧陈述自己的 `HandlerError` 是**允许的**（否则服务端根本没法表达「我忙」），方向区别见 `02-l2-protocol.md` 7.3。

---

## 9. 观测

| 项 | 内容 | 成本 |
|---|---|---|
| 请求 span | 服务端 `spile.request`：`service_id` + `method` + `method_name` + `request_id` + `code`（成功写 `0`，失败写错误体里的码）。在 `dispatch` 里**进 handler 之前**建好，`code` 回包时回填；只 `instrument` 里层 task（外层只是 panic 兜底）。`method_name` 来自 `Service::method_name`（宏用名字表覆盖，手写实现默认 `None` → `<unknown>`） | 零（全是常量；`instrument` 只多一层 poll 包装） |
| 调用 span | 客户端 `spile.call`：字段与服务端**同名同值**；`method_name` 是宏的编译期常量，`request_id` 由 `Session::call` 分配后 `Span::current().record` 回填（字段不存在时是 no-op） | 零 |
| 队列深度 | `tx.capacity()` / 已排队条数，超阈值 warn | 低 |
| pending 大小 | 定期采样 `outbound.lock().pending.len()`，接近配额时 warn | 低 |
| 入站配额 | `inbound.async_used` / `blocking_used` 采样；与 pending 分开报，否则看不出是哪个方向过载 | 低 |
| handler 耗时 | debug 构建下 > 100 µs 打 warn，提示该加 `#[spile(blocking)]` | 仅 debug |
| shm 槽位占用时长 | ZeroCopy 路径下 payload 从交出处到 drop 的时长，做指标 | M5 |

只带 `request_id` 的 span 在排障时基本没用——一个数字对不上任何业务语义；只有 `service_id + method` 也一样（哈希认不出方法名）。所以名字必须由宏那一侧交出来（`Service::method_name`），L3 自己反推不了。

**span 的边界就是请求的边界**：客户端 span 覆盖「encode 之后到 decode 之前」，服务端 span 覆盖「进 handler 到回包入队」。handler 内部打的日志靠 `instrument` 落进后者——`tests/tracing_spans.rs` 断言的就是这件事，而不是「span 建出来了」。

---

## 10. 验收

| 项 | 判据 | 阶段 |
|---|---|---|
| 并发乱序 | 100 个并发请求、响应乱序返回，互不串包 | M1 |
| 首帧强制 | 首帧非 `Handshake` → 200 ms 内断连，错误 `Protocol` | M1 |
| 换 transport | 同一套会话用例在 `memory` 与 `local` 上各跑一遍（8 条 × 2），`session.rs` 一行不改 | M1 |
| 零拷贝形状 | `memory` 上返回 `Bytes::from_owner`，收发后指针不变（`tests` 里断言 `as_ptr` 相等） | M1 |
| handler panic | 服务实现 panic → 调用方拿到 `PeerRejected{0}` + panic 文本，会话不断连 | M1 |
| 对端关闭 | 对端 `close()` 后新调用 200 ms 内变 `Closed`，不挂在 pending 表上 | M1 |
| 上限方向性 | 对端声明 1 MiB、本端 64 KiB 时 200 KiB 请求发得出去（写上限取对端声明值） | M1 |
| 杀进程 | 服务端被 kill，客户端 100 ms 内收到明确错误而非挂死 | M3 |
| 握手失败 | `codec_id` / 版本不等、`reserved != 0`、`max_message_len < 24` → 建连期断连 | 已覆盖（v0.22：`handshake_rejects_bad_fields`） |
| 配额 | 并发压测后 `outbound.pending` 从未越界；客户端 `max_blocking_in_flight = 0` 仍正常；入站与出站配额互不影响 | M3 |
| `GoAway`（发起侧） | 任一侧发送后，两侧**新请求**均 `Closed`（`INV-10b`） | M3 |
| `GoAway`（收活侧） | 对端 `GoAway` 之后到达、但属于「对端 `GoAway` 之前发出」的请求**必须被 dispatch 并回包**，不得回框架码 `7`（`INV-10a`） | M3 |
| 关闭不丢包 | 双方宣告关闭之后到达的 `Request` 收到 `PeerRejected { code: 7 }`（明确拒绝，不挂死）（`INV-10c`） | M3 |
| 关闭超时 | 对端完全不配合时，`close()` 在 `close_timeout` 内返回，不永久挂起 | M3 |

---

## 11. 已知取舍

- **`GoAway` 不带 `last_request_id`**：对称会话下它没有良定义（对方 id 空间 vs 本端计数器）。
- **不做严格 client/server 角色机**：两侧都能发 `Request`；`Server` / `Client` 只是用法封装。
- **handler 内部反向调用对端**（需要 `&ServiceContext` 首参）不做：会改动所有 handler 签名，留到 M3 之后。
- **shm 免 spawn 快路径**不做：会推翻 5.3「spawn 只在 L3 发生一次」，且端到端目标 1–3 µs 已可接受。
