# L1 传输层详细设计

> 上位文档：`docs/design.md` 2 节、附录 B；线格式权威：`docs/PROTOCOL.md`（L1 的长度前缀不进线格式，只在本文件第 5 节定义）。
> 本层唯一产出：**一条完整消息（`Bytes`，`Send + 'static`）**。除此之外不承担任何语义。

---

## 1. 职责边界

**做**：建立通道、把字节流切成消息、把消息写出去、提供背压、在 drop 时完成关闭。

**不做**：不认识 `request_id` / `service_id` / `method`；不认识 `Codec`；不做路由、不做重试、不做超时判定；不解释 `max_message_len` 的**来源**（只比较数字）。

一条判据：如果把 L1 单独拿去做一个「本地 echo 程序」，它应当**完全够用且完全不需要 L2**——`examples/echo.rs` 就是这条判据的物证（M0）。

---

## 2. trait 契约

### 2.1 三个 trait

```rust
pub trait MessageSource: Send {
    fn recv(&mut self) -> impl Future<Output = io::Result<Bytes>> + Send;
    /// 本端接收侧的上限旋钮（初值 64 KiB）。L3 在 move 本半身进 task 之前取出，握手后写入。
    fn recv_limit(&self) -> Arc<AtomicU32>;
}

pub trait MessageSink: Send {
    fn send(&mut self, msg: Bytes) -> impl Future<Output = io::Result<()>> + Send;
    /// 本端发送侧的上限旋钮（初值 64 KiB）。语义上是「对端声明的值」，但 L1 不知道谁写入。
    fn send_limit(&self) -> Arc<AtomicU32>;
}

pub trait Transport: Sized {
    type Source: MessageSource;
    type Sink: MessageSink;

    fn bind(ep: &Endpoint, opts: &TransportOptions)
        -> impl Future<Output = io::Result<Self>> + Send;
    fn connect(ep: &Endpoint, opts: &TransportOptions)
        -> impl Future<Output = io::Result<(Self::Source, Self::Sink)>> + Send;
    fn accept(&self, opts: &TransportOptions)
        -> impl Future<Output = io::Result<(Self::Source, Self::Sink)>> + Send;
}
```

> **为什么这两个访问器必须在 trait 上（v0.10）**：v0.9 只写了「L3 在握手后写 `FramedSource` / `FramedSink` 里的 `Arc<AtomicU32>`」，可 L3 手里只有一个 `S: MessageSource` 泛型值——**没有任何办法从 trait 对象里拿到那个 `Arc`**，于是「协商出来的上限」永远写不进去。访问器是同步的、零成本的（返回 `Arc` 克隆），且不破坏铁律②：L1 只交出「一个可写的数字」，不知道它代表协议协商。**不要**改成 trait 上的 `fn set_limit(&mut self, u32)`：那样必须独占借用，而 L3 已经把半身 move 进 task 了。

四条必须逐字遵守的约束：

| 编号 | 约束 | 违反后果 |
|---|---|---|
| `L1-C1` | `recv` 只返回**完整**消息，绝不返回半条 | L2 会解析到半截头，整条连接只能废弃 |
| `L1-C2` | 返回的 `Bytes` 必须 `Send + 'static` | L3 无法 `spawn` 去 decode，多路复用退化成串行 |
| `L1-C3` | `recv` / `send` 只允许**终态取消**（连接关闭、调用方放弃整条连接）被打断，不允许单次超时 | `send` 被打断会在流里留半帧 → 对端长度前缀永远读不满 |
| `L1-C4` | `bind` / `connect` / `accept` **三者皆异步**；`opts` 是参数不是成员 | 同步 `bind` 会把「必须在 runtime 内」藏进注释，漏了的调用方在构造期 panic 而不是拿到 `Err` |
| `L1-C5` | 两个上限旋钮必须能从 `recv_limit()` / `send_limit()` 取到，且**同一份 `Arc` 贯穿半身生命周期** | L3 拿不到旋钮 → 协商形同虚设（`INV-19`） |

### 2.2 drop 契约（**验收项，不是说明**）

没有 `close`、没有 `flush`（`design.md` 2.1）。关闭的唯一触发是 **drop**，因此 drop 必须在网络上可观测：

| transport | drop `Sink` | drop `Source` | 是否满足半关闭契约 |
|---|---|---|---|
| `tcp` | `OwnedWriteHalf::drop` → `shutdown(SHUT_WR)`，对端读侧 EOF | `OwnedReadHalf::drop` 关读方向 | 满足 |
| `local`（`interprocess`，Unix UDS） | 两半同处一个 `Arc<RawPipeStream>`，drop 一半只减引用计数 | 同左 | **不满足**，只能整连接关闭 |
| `local`（Windows named pipe） | **无半关闭**：关写句柄 = 结束整个 pipe 实例 | 同左 | **不满足**，降级为整连接关闭 |
| `memory` | drop 写半 = drop `mpsc::Sender` → 对端 `recv` 得到 `None` | drop 读半同理 | 满足（通道语义天然单向） |
| `shm` | drop 写半 = 置段头 `state` 并停写 `head` | drop 读半 = 停推进 `tail` | 满足（但**必须显式做**，否则对端静默停住，见第 8 节） |

契约写法：**`drop Sink` 必须尽力半关闭写方向；`drop Source` 关闭读方向；做不到半关闭的实现降级为「drop 任一半即整连接关闭」，并在实现文档里注明。** `local`（两种平台）属于降级档，因此：

- 优雅关闭在 `local` 上**只能**靠应用层 `GoAway` 传达，不能指望 EOF；
- 反向依赖成立：正因为存在无半关闭的平台，`GoAway` 才是必选控制帧，不是可选优化。

### 2.3 所有权决定「并发模型」

- `Sink` 是 `&mut self` + 非 `Sync`：**一条连接只有一个写者**。L3 用单一 writer task 满足它，不需要 internal lock。
- `Source` 同理，只有 reader task。
- 因此 L1 内部**不需要任何同步原语**（shm 的原子操作属于对端共享，不属于本端互斥）。

---

## 3. 模块与文件职责

```
src/transport.rs          trait 定义、Endpoint、TransportOptions、错误构造 helper
src/transport/
  framing.rs    FramedSource<S> / FramedSink<S>：长度前缀、读满、双向上限旋钮
  tcp.rs        TcpTransport（tokio bind + nodelay + into_split）
  local.rs      LocalTransport（interprocess，tokio feature）
  memory.rs     MemoryTransport + Memory::pair(opts) -> ((Source, Sink), (Source, Sink))
  any.rs        AnySource / AnySink / AnyTransport（枚举门面，按 feature 裁剪变体）
  shm.rs        （M5）段管理、SPSC 环、块池、心跳；内部真拆子模块时须回填 1.3 的树
```

> **模块文件风格（2024 风格）**：入口是 `transport.rs`，不是 `transport/mod.rs`。子模块文件放在同名目录 `transport/` 里，`transport.rs` 用 `pub mod framing;` 声明它们。两种写法 rustc 都接受，但 `foo.rs` 与 `foo/mod.rs` 不能并存；本项目统一用前者（见 `design.md` 1.3 第 5 条布局约束）。

| 文件 | 关键点 |
|---|---|
| `transport.rs` | 只放契约与数据结构；不放任何平台 `cfg` 分支的具体实现 |
| `framing.rs` | 唯一持有 `Arc<AtomicU32>` 上限旋钮的地方（两个方向各一个），并通过 `recv_limit()` / `send_limit()` 交出 |
| `tcp.rs` | 直接用 **`tokio::net::TcpListener::bind(addr).await`**：它是 `async fn`，内部完成 fd 注册，与 `L1-C4` 的异步签名天然一致。**不要**回头用 `std::net::TcpListener::bind` + `from_std`——那条路径要求调用线程恰好处在 runtime 上下文里，而同步签名无法在类型上表达这件事 |
| `local.rs` | 全平台走 framing；不要按「消息式」设计背压与上限 |
| `memory.rs` | 唯一的「零 syscall」实现，是 L1 抽象的压力测试器 |
| `any.rs` | 返回 future 的五个方法（`recv` / `send` / `bind` / `connect` / `accept`）各需一次 `Box::pin`；`recv_limit` / `send_limit` 与建段类动作不 box |

---

## 4. Endpoint、选项与默认值

### 4.1 `Endpoint`

| 变体 | 载荷 | 语义 | 解析方 |
|---|---|---|---|
| `Tcp` | `SocketAddr` | 已解析好的地址，不做 DNS | `tcp` |
| `Local` | `LocalSocketAddr` | Unix 路径或 Windows pipe 名（`interprocess` 抽象） | `local` |
| `Memory` | `MemoryAddr` | **不参与查找**，只用于日志与 `Debug` | `memory` |
| `Shm` | `{ name: String }` | **段基名**；实际两段 `{name}-a2b`、`{name}-b2a` | `shm` |

规则：

- `Endpoint` 只携带**寻址信息**，不放任何行为开关（行为开关在 `TransportOptions` / `SessionOptions`）。
- `Tcp` 的 `SocketAddr` 必须是已解析值——`connect` 里不做 DNS，避免同步解析混进 async 路径。
- `Shm` 的 `name` 是基名，**不允许**调用方自己拼 `-a2b`：拼接规则归 `shm` 实现，调用方只给基名，否则两侧可能拼出不同的名字。
- `Memory` 变体留着只为日志统一；`Memory::pair` 是它的唯一构造入口，**不存在全局注册表**（`design.md` 2.7）。

### 4.2 `TransportOptions`（L1 独占）

| 字段 | 类型 | 默认 | 语义 |
|---|---|---|---|
| `connect_timeout` | `Duration` | 5 s | 仅 `connect` 用；`accept` 不设超时（由 L3 的握手超时兜底） |
| `spin_hint` | `bool` | `false` | 仅 shm：busy-poll 还是 eventfd 唤醒（B.5） |

**`max_message_len` 不在 `TransportOptions` 里（v0.10 迁出）**：它是「本端声明 + 握手协商」的会话属性，归 `SessionOptions`。理由是它的两个消费方全在 L3——握手要填这个值、旋钮写入要拿 `Arc`——而 `Session::new` 拿不到 `TransportOptions`，L1 也不需要它（握手前恒 64 KiB，无可配置）。留在 L1 只会制造「同一份值两处真相」。

**不许放进 `TransportOptions` 的东西**（每条都是踩过的坑）：

- `ring_capacity`：必须两侧一致，且协商它要走这条环 → 归 shm 段头（B.3）。
- `max_in_flight`：那是 L3 的配额（`design.md` 2.7 的反例）。
- `payload_mode`：那是 dispatch 决策，归 `SessionOptions`。
- `max_message_len`：同上，归 `SessionOptions`（v0.10）。

### 4.3 两个上限旋钮（`INV-05` / `INV-19` 的实现形态）

`FramedSource` 与 `FramedSink` 各持一个 `Arc<AtomicU32>`，**初值都是 64 KiB**，通过 `recv_limit()` / `send_limit()` 对外交出：

| 旋钮 | 握手后写入 | 检查点 | 超限行为 |
|---|---|---|---|
| `FramedSource`（`recv_limit()`） | 本端声明的 `max_message_len` | 读到 4 字节长度前缀之后、分配缓冲之前 | 返回 `io::ErrorKind::InvalidData`（协议层映射为 `RpcError::Protocol`），**立即断连**，不尝试续读 |
| `FramedSink`（`send_limit()`） | 对端声明的 `max_message_len` | `send` 进入时、写任何字节之前 | 返回 `io::ErrorKind::InvalidInput` + 固定标记文本（L3 映射为 `RpcError::InvalidInput`），**连接保持可用** |

两条容易写反的地方：

1. 读侧上限用**本端**值（我们承诺过的接收能力），不是对端的；写侧才用对端值。
2. 写侧超限是**本地错误**，不能靠「让对端断连」来表达——那会让调用方看到一个无法归因的 `Io` 错误。

还有一条**时序**约束：`SessionOptions::max_message_len` 是「我方声明」，它在握手完成前**不落到 reader 旋钮上**——握手前的旋钮值恒为 64 KiB。否则一个恶意的 `Handshake`（或恶意的大帧先于握手到达）就能让你按 64 MiB 去分配缓冲，协商机制自己成了攻击面。声明值只在 `Handshake` 收发完成、状态进 `Ready` 之后才写入旋钮；**写入的唯一位置在 L3**，L1 只做比较。

**使用顺序错一步就拿不到旋钮**：`Session::new` 必须在把 `S` / `K` move 进 reader/writer task **之前**调 `source.recv_limit()` / `sink.send_limit()` 存到 `Session.limits`；move 之后就只剩 task 里的私有值，`&self` 再也取不出来。测试里想临时放宽上限（例如 §9 的 1 MiB 用例），两条合法路径：走 `SessionOptions::max_message_len` 正常协商，或直接对拿到的 `Arc` 做 `store`——**不提供第三种（比如全局静态变量）**。

---

## 5. 流式 transport 的 framing

### 5.1 线格式（L1 内部约定，不进 `PROTOCOL.md`）

```
[len: u32 小端 = 后续字节数 | 消息体(len 字节)]
消息体 = [24B 语义头 | payload]
```

- `len` 只算消息体，不含自身 4 字节。
- 与全文一致用小端（`design.md` 3.2 的统一口径），不做字节序协商。
- 一条消息必须有 `len >= 24`；`len < 24` 视为协议错误（正常实现不会产生，但这是唯一的「头不完整」防线）。

### 5.2 `recv` 步骤（`FramedSource`）

1. 若内部缓冲不足 4 字节，`read_buf` 直到凑满 4 字节（EOF 在**任何**未满处都返回 `UnexpectedEof`）。
2. 从缓冲**头部**读出 4 字节并解析 `len`（`u32::from_le_bytes`），然后**立刻 `buf.advance(4)` 把这 4 字节消费掉**。
   - 漏掉这一步是照抄最容易写出的错：不 `advance`，后面 `split_to(len)` 拿到的就是「4 字节前缀 + 前 len-4 字节」，消息体整体错位 4 字节，`kind` / `flags` 全部读错位，而且**不会 panic、只会静默解析出垃圾**。
   - 不要用「先 `len = buf.len()` 再 `split_to(len)`」绕：那会把前缀本身当消息体返回。
3. 与 `recv_limit()` 当前值比较，超限 → `InvalidData` 并断连（此时前缀已消费，缓冲里剩下的部分是脏的，所以必须断连而不是续读）。
4. `reserve(len)` 一次，`read_buf` 直到 `len` 字节齐（EOF 中断同样返回 `UnexpectedEof`，**不允许**返回半条）。
5. `split_to(len).freeze()` 得到 `Bytes`，返回。此时缓冲里已经没有任何属于这条消息的字节——这条断言应当写进单测。

缓冲区策略（M4 的免费收益，现在就写死）：

- **不要**「先 `read_exact` 到临时 `Vec` 再拷进 `BytesMut`」。直接 `read_buf(&mut BytesMut)`，读完 `split_to` + `freeze()`，一次落地。
- `BytesMut` 容量按「上一条消息长度」增长并保留（一条连接内的消息尺寸通常同量级），避免每帧重新分配。
- 缓冲增长有硬上限：`reserve` 之前已经校验过 `len`，所以最坏分配 = `max_message_len`；不允许无上限 `reserve`。

### 5.3 `send` 步骤（`FramedSink`）

1. 检查 `msg.len() <= sink 上限`（见 4.3，超限 `InvalidInput`）。
2. 写 4 字节长度前缀 + 消息体。**两次 `write_all` 或一次 vectored write 均可**，但必须保证「前缀与消息体不会被别的写插入」——由 `&mut self` 独占保证。
3. `write_all` 返回 `Ok` 之后**不 flush**：TCP 由内核缓冲承托，`flush` 是 no-op；shm 没有 flush 概念。这一条是 `design.md` 2.1 删掉 `MessageSink::flush` 的直接依据。

### 5.4 消息式 transport

`memory` 与 `shm` 跳过长度前缀：`recv` 直接拿一条 `Bytes`，`send` 直接放一条。**但上限检查照样要做**（在 `send` 入口、`recv` 拿到长度后立即做），否则 `INV-05` 在这两个实现上不成立。

---

## 6. 各实现的具体要求

### 6.1 `tcp`（M0 主力）

- `bind`：`tokio::net::TcpListener::bind(addr).await`，一步到位（内部已设非阻塞并把 fd 注册进当前 runtime）。**不要**改走 `std::net` + `from_std`。
- `connect`：`TcpStream::connect` + `connect_timeout`；成功即 `set_nodelay(true)`（loopback 上 Nagle 会平白加延迟）。
- 分半：`into_split()` 得 `OwnedReadHalf` / `OwnedWriteHalf`，两半各自包 `FramedSource` / `FramedSink`。
- `accept`：`listener.accept()` 循环由 L3 调用，L1 每次只产出一对半身。

### 6.2 `local`（M1 起首选，仍是可选 feature）

- `interprocess` 的 tokio 封装，Windows → named pipe，Unix → UDS。
- **Windows 上是纯字节流**：`interprocess` 的 tokio named pipe 实现里消息读取被显式关闭（源码注释 `MESSAGE READING DISABLED`，原因是要处理 `ERROR_MORE_DATA` 而 Mio 不支持），所以不能指望消息模式划边界 → **必须走 5 节的 framing**。
- **drop 不能半关闭**（2.2 表）：`Draining → Closed` 在对端不可见。因此：
  - `local` 上**必须**用 `GoAway` 传达收尾（不能像 `tcp` 那样「drop 就完事」）；
  - 测试里对 `local` 不能断言「drop 后对端 N ms 内 EOF」，只能断言「收到 `GoAway` 后对端在 100 ms 内退出」（`INV-16` 的例外条款）。
- Unix 上虽然底层是 UDS（本可半关闭），但 `interprocess` 把两半包在同一个 `Arc<RawPipeStream>` 里，drop 一半只减引用计数 → 与 Windows 同档处理。**不要**为 Unix 单独做半关闭优化，那会让平台行为分叉。

### 6.3 `memory`（一致性测试替身）

- 形状：`Memory::pair(opts) -> ((Source, Sink), (Source, Sink))`——第一组给 A 侧，第二组给 B 侧，每组都是完整一对（`design.md` 2.7）。
- 内部：两条单向 `mpsc`（A→B、B→A），容量默认 1024（实现默认值，不是协议约定）。
- `recv`：`rx.recv().await` → `Option`，`None` 即对端写半已 drop，返回 `UnexpectedEof`（不是 `Ok` 空帧）。
- `send`：`tx.send(msg).await`，队列满即等待（这是背压，不是错误）。
- 设计意图：**如果连进程内 channel 都塞不进 `Bytes` 零拷贝，shm 一定白做**。所以 M1 要在它上面专门验一次 `Bytes::from_owner` 路径（owner 持 chunk，切片零拷贝，整体 `'static + Send`）。

### 6.4 `shm`（M5，附录 B 的落地口径）

拓扑（`design.md` B.3 定死）：**一个段 = 一条单向环**；双向会话 = 两个段 `{name}-a2b` / `{name}-b2a`。`bind` 侧建两个并发布，`connect` 侧按基名拼出两条再 attach。

段头（第一页，创建方写、attach 方读并校验）：

| 字段 | 宽度 | 说明 |
|---|---|---|
| `magic` | u64 | 固定值，识别「这不是我们的段」 |
| `protocol_version` | u16 | 与 `Handshake` 版本一致 |
| `state` | u8 | 段级总开关（`Free / Writing / Ready / Reading`） |
| `reserved` | u8 | 必须 0 |
| `generation` | u64 | 每次 channel 重置 +1，旧数据靠它被跳过 |
| `ring_capacity` | u32 | 槽位数，**必须是 2 的幂**（取模用 `&`） |
| `slot_size` | u32 | 每槽字节数，含 24B 头，按 64 对齐 |
| `head` / `tail` | `CachePadded<AtomicU64>` | 各占一条 64B 线，防伪共享 |
| `peer_heartbeat` | `CachePadded<AtomicU64>` | 对端周期性写入，超阈值未更新判定对端消失 |

实现要点（每条都会单独炸）：

1. **SPSC 无 CAS**：生产只写 `head`、消费只写 `tail`，各自 `store(Release)` / `load(Acquire)`，没有重试循环。
2. **段内不许存裸指针**：一律相对 base 的偏移（`RelPtr`）。
3. **缓存对端索引**：判满不每次都读对端 `tail`，本地缓存旧值、逼近上限才真读（Disruptor 技巧）。
4. **`attach` 先校验 `magic` + 版本 + `generation`**，再按 `ring_capacity` / `slot_size` 映射；读到的值写进 `Handshake` 做断言（`design.md` 6 节机制 2）。
5. **drop 必须显式发信号**：socket 断了有 `EPIPE`，shm 不会给任何信号；对端进程消失后环会**安静停住**。所以 `Sink::drop` 要置 `state` / 写一条终止标记，`Source` 靠心跳超时判定。
6. **槽位状态机**（B.7）：`Free → Writing → Ready → Reading → Free`，状态变化与数据写入之间必须有 release/acquire 配对，否则消费者可能读到半个消息。
7. **数据放置两档**（B.4）：档 A 直接写槽位（小消息 + 同步 `fn` handler，payload 24B 头对齐 + 槽位 64 对齐成立）；档 B 块池 + 索引（大消息）。**档 A 的「对齐」只有在 payload 绝对地址 8 对齐时才成立**，而 shm 正是少数能保证它的 transport（见 `PROTOCOL.md` §1 第 3 条）。
8. **busy-poll vs eventfd**（B.5）：`spin_hint = true` 时纯自旋（100–200 ns 量级），否则 eventfd 唤醒（+1–2 µs）。
9. **`slot_size` 与声明的 `max_message_len` 必须相容**：`max_message_len ≤ slot_size`（否则声明了单帧能力而槽位装不下，对端按声明发满一槽，本端永远读不出来）。这条在 attach 之后、握手之前校验，不满足直接拒绝建会话；下限 `max_message_len ≥ 24` 由握手规则统一管（`PROTOCOL.md` §9 第 17 条）。

---

## 7. 背压与错误分类

### 7.1 背压来源

| 实现 | 满/忙时 | 完整链路 |
|---|---|---|
| `tcp` / `local` | `send().await` 等内核发送缓冲腾出空间 | socket → writer task → bounded mpsc → 调用方 |
| `shm` | 环满 → 等消费者推进 `tail` | 环 → writer task → bounded mpsc → 调用方 |
| `memory` | mpsc 满 → await | mpsc 直接对接 |

L3 的 writer task 是这层差异的**唯一吸收点**：上层永远只看到同一个 bounded mpsc（`design.md` 2.4、4.1）。所以 L1 各实现的背压差异**不允许**泄漏到 L3——L3 里出现任何「如果是 shm 就…」的分支都是设计错误。

### 7.2 `io::Error` → 上层映射（L3 负责，L1 只负责给对 `ErrorKind`）

| 场景 | L1 返回 | L3 映射 |
|---|---|---|
| 对端正常关闭（EOF / 通道 `None`） | `UnexpectedEof` | 断连 → 所有 pending 置 `Closed` |
| 帧长超本端上限 | `InvalidData` | `RpcError::Protocol` + 断连 |
| 发送超对端上限 | `InvalidInput` + 标记文本 | `RpcError::InvalidInput`，**不断连** |
| shm 段头校验失败 | `InvalidData` | `RpcError::Protocol` + 断连 |
| 其它系统错误（`ECONNRESET`、`EPIPE`、pipe 断开） | 原 kind | `RpcError::Io` + 断连 |

**`InvalidInput` 的标记文本是契约的一部分**：L1 用固定前缀（如 `"message exceeds peer max_message_len"`），L3 按「`ErrorKind::InvalidInput` 且带该前缀」识别。否则 L1 与 L3 之间没有别的通道能区分「我方拒发」与「系统层的 `EINVAL`」。

---

## 8. `AnySource` / `AnySink` 门面

```rust
pub enum AnySource { Tcp(TcpSource), Local(LocalSource), Memory(MemorySource), Shm(ShmSource) }
```

规则：

- 变体按 feature 裁剪：关掉某个 feature 就少一个变体，`match` 完备性随之变化——**代码里不许假设四个变体永远都在**。
- 返回 future 的方法一律要 `Box::pin`：`recv` / `send` / `bind` / `connect` / `accept`。不返回 future 的纯本地动作（建段、校验段头、注册服务）**不 box**，`match` 各调一次即可。
- `recv_limit()` / `send_limit()` 在门面上同样是同步 `match`：直接返回内部 `Arc` 克隆，**不 box**。
- `AnyTransport` 只用于「配置驱动」与测试矩阵；生产热路径用泛型。

**三项代价，别混成一句「一次 box」（对应 `design.md` 2.5 的表）：**

| 代价源 | 量级 | 触发点 |
|---|---|---|
| `dyn` 间接调用 | 每次 2–5 ns | 整个会话生命周期（每次 `recv`/`send` 过一层 vtable） |
| 每条消息一次 `alloc` + `free` | 20–40 ns | 每条 `recv` / `send`（`Box::pin` 的 future 生命周期 = 一条消息） |
| 单态化代码体积 | 构建期 | 泛型组合 `O(transport × handler)`，门面压回 `O(transport + handler)` |

一个 RPC 往返有 **4 个 box 点**（客户端 `send`、服务端 `recv`、服务端 `send`、客户端 `recv`）合计 **80–160 ns**：放在 3 µs 端到端目标里是 3–5%，放在 300–600 ns 裸传输里是 13–27%。所以——

- **`tests/transport.rs` 的性能用例与 `benches/` 一律禁止用 `AnySource` / `AnySink`**（`INV-18`）；要测门面开销就单开一个「门面对比」bench，并明确标注「这不是框架开销」。
- 门面只消除**单态化组合爆炸**，不消除**泛型传染**：`Session<S, K>` 会把类型参数一路带到 `Server` / `connect` / 生成的客户端上。要真正擦除，得在每个边界上都擦（`Session<AnySource, AnySink>`），只擦一半等于没擦。

---

## 9. 验收与测试矩阵

`tests/transport.rs` 是 L1 的唯一验收文件，**不依赖 `Session`**（M1 才引入）。同一套用例在多个实现上跑，一行不改，这才是抽象成立的证据。

| 用例 | tcp | local | memory | shm | 说明 |
|---|---|---|---|---|---|
| 单帧往返 | ✓ | ✓ | ✓ | ✓ | 24B 头由测试自己拼，L1 不解释 |
| 连续 100 帧顺序不乱 | ✓ | ✓ | ✓ | ✓ | 边界正确性 |
| 1 MiB 消息（上限内） | ✓ | ✓ | ✓ | ✓ | **必须先用 `recv_limit()` / `send_limit()` 的 `Arc` 把两侧上限提到 ≥ 1 MiB**（或走 `SessionOptions::max_message_len` 协商），否则默认 64 KiB 下这条用例本来就应该被拒——测试里不许出现「测 1 MiB 却依赖默认上限」的写法 |
| 超本端上限被拒 | ✓ | ✓ | ✓ | ✓ | `InvalidData` + 断连 |
| 超对端上限本地拒发 | ✓ | ✓ | ✓ | ✓ | `InvalidInput`，连接仍可用 |
| 上限旋钮真的生效 | ✓ | ✓ | ✓ | ✓ | 同一对半身：先 `store` 到 1 MiB 发 500 KiB（通过），再 `store` 回 64 KiB 发 500 KiB（本地 `InvalidInput`） |
| 对端 drop 后本端收到 EOF | ✓ | ✗（降级档） | ✓ | ✓（心跳/状态） | `INV-16` 的平台差异 |
| 并发 32 个写者排队 | — | — | — | — | 由 L3 的 writer task 保证，L1 不测 |
| `Bytes::from_owner` 零拷贝落地 | ✓ | ✓ | ✓ | ✓ | owner 归还语义必须配对；`bytes >= 1.10.1` |

M0 的落地项：trait + `framing.rs` + `tcp` + `memory` + `examples/echo.rs` 跑通，`cargo deny` 从 M0 就开（查的是「依赖死掉」而不是许可证）。

---

## 10. 已明确不做（写在这里防止被「顺手加上」）

- `close` / `flush`：删过两次，别再回来（所有权论证见 `design.md` 2.1）。
- `unix` 原生 transport / `SOCK_SEQPACKET`：`local` 已覆盖 UDS，特化留到真需要 fd 传递时。
- 消息模式 named pipe：`interprocess` 读侧不支持，Mio 也没有。
- 压缩：协议层不留位，L1 更不做。
- 大消息分片协议（chunking）：`max_message_len` 是硬上限，超了就说清楚，不做隐式分片——隐式分片会把「一条完整消息」的契约打破（`L1-C1`）。
- TLS / 认证：本地 IPC 场景无收益，属非目标。
