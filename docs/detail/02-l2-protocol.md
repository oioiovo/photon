# L2 协议层详细设计

> 上位文档：`docs/design.md` 3 节、6 节「机制 2」。
> **线格式唯一权威是 `docs/PROTOCOL.md`**；本文件只解释「怎么实现、怎么校验、出错怎么处理」，不重新定义字节。
> 本层产出：把一条 `Bytes` 解析成「语义头 + 裸 payload」，以及把语义头 + payload 拼回 `Bytes`。

---

## 1. 职责边界

**做**：24 字节头的编解码与逐 kind 校验、payload 的序列化（`Codec`）、method id 的稳定性、错误体的编解码、控制帧的手写编解码。

**不做**：不认识连接与 task（那是 L3）；不认识 `Service` 与 handler（那是 L4）；不判断 `service_id` 是否存在（那是 `ServiceRegistry`）；不做压缩、不做加密、不做分片。

一条判据：把 L2 单独拿去做「两条 `Bytes` 之间的转换函数」，它应当不需要任何 async、不需要任何 IO。

---

## 2. 24 字节语义头

### 2.1 字段与偏移（小端）

| 偏移 | 宽度 | 字段 | 类型 | 备注 |
|---|---|---|---|---|
| 0 | 1 | `kind` | u8 | 0–6 有效，7–255 未定义（收到即断） |
| 1 | 1 | `flags` | u8 | bit0–2 有定义，bit3–7 保留（置位即断） |
| 2 | 2 | `service_id` | u16 | 路由键的第一段 |
| 4 | 8 | `request_id` | u64 | **按方向解释**，见 2.3 |
| 12 | 8 | `method` | u64 | 编译期哈希，见第 5 节 |
| 20 | 4 | `deadline_ms` | u32 | 相对毫秒，0 = 无期限，上限约 49 天 |
| 24 | — | payload | 变长 | 固定在消息偏移 24；**绝对地址是否 8 对齐取决于 transport**（见 2.2） |

### 2.2 为什么必须是 24

- 20 字节版让 payload 落在 `offset ≡ 4 (mod 8)`，`rkyv` 在非对齐缓冲上读是 UB，M5 的零拷贝直接被堵死。
- 24 字节后 `payload` 在**消息内**从偏移 24 开始（`24 % 8 == 0`）。这是**唯一**必须现在付的 4 字节代价（一条 1 KiB 消息多 0.4%）。
- **但它只保证「消息内偏移」，不保证「绝对地址对齐」**（v0.10 收窄）：流式 transport 若用 `read_buf` + 复用 `BytesMut` + `split_to` 取帧，第二帧起的基址可能落在任意偏移——外层 4 字节长度前缀也不会改变这一点。所以「`rkyv` 原地读成立」**只对 `memory` / `shm` 成立**；`tcp` / `local` 上开 rkyv 必须先把 payload 拷进对齐缓冲（这会抵消大半收益，M5 选型时按这条判断）。`INV-02` 已按此拆分。
- `length` 字段已删（v0.2 决定保留）：边界由 L1 保证，头里再存一份只会制造「谁说了算」的边界条件。

### 2.3 `request_id` 的方向性（最容易写错的一条）

- id 由**发起方**分配，只在本端 pending 表内唯一；`next_id` 从 1 起，0 保留。
- `Response` / `Cancel` 里的 id 是对端空间里的引用，**按连接方向解释**：两侧各有独立计数器，永远不比较。
- 因此「A 发 5、B 发 5」同时存在是合法的，两条连接方向各自维护 map。
- `Response` 的 `request_id` 必然非 0（因为它一定回填某个非 0 请求）。

### 2.4 内存中的形状

解析**不分配**：头就是一个固定偏移视图（`&msg[0..24]`），字段通过常量偏移 + 小端读取取出来；payload 是 `msg.slice(24..)`。禁止「把整帧反序列化成 serde 结构体」——那会让取消 / trace / 分流三条控制路径都付一次完整解析代价（`design.md` 3.1 的原话）。

拼接时**必须预留 24 字节头空间**：

1. 分配 `BytesMut::with_capacity(len + 24)`（或从 L1 读缓冲 `split_to` 出带 headroom 的段）；
2. 先写 24 字节占位（或 `advance(24)`）；
3. `codec.encode_into(payload_struct, &mut buf)` 直接写进同一块；
4. 回填头。

顺序反过来（先 encode payload 再拼头）就是每帧多一次 memcpy，1 KiB 消息多拷 1 KiB，M4 实测会平白低 10–20%。

---

## 3. 逐 kind 校验

### 3.1 kind 表

| kind | 值 | 方向 | payload | 是否走 `Codec` |
|---|---|---|---|---|
| `Request` | 0 | 双向 | 请求参数 | 是 |
| `Response` | 1 | 双向 | 结果 或 错误体（`flags` bit0 = 1） | 是 / 错误体手写 |
| `Notification` | 2 | 双向 | 通知内容 | 是 |
| `Cancel` | 3 | 双向 | **空** | 否 |
| `StreamChunk` | 4 | 双向 | — | **预留，不实现**，收到即协议错误 |
| `GoAway` | 5 | 双向 | **空** | 否 |
| `Handshake` | 6 | 双向 | 12 字节手写 | 否 |

`StreamChunk`（kind 4）与 flags bit1 是预留位：**当前实现收到就必须报协议错误**，不能静默忽略，也不能当空帧处理。

### 3.2 「必须为 0」的字段矩阵（`INV-03`）

| kind | `request_id` | `service_id` | `method` | `deadline_ms` |
|---|---|---|---|---|
| `Request` | 非 0 | 有效 | 有效 | 0 或非 0 |
| `Response` | 回填非 0 | **0** | **0** | **0** |
| `Notification` | 非 0 | 有效 | 有效 | 0 或非 0 |
| `Cancel` | 要取消的 id（非 0） | **0** | **0** | **0** |
| `GoAway` | **0** | **0** | **0** | **0** |
| `Handshake` | **0** | **0** | **0** | **0** |

违反任一格的后果：**`RpcError::Protocol` + 立即断连**，不回错误体。

理由是「必须为 0」不是风格约定，而是消除解析歧义：如果 `Response` 允许带 `service_id`，那么「用哪个服务的错误码空间」就变成第二个真相来源；如果 `Cancel` 允许带 `method`，那么「取消的是 id 还是方法」在任何一侧都能自圆其说。**歧义在协议层不能用「实现方自行忽略」来收场**——对端可以据此构造出你两种解释都成立的消息。

### 3.3 校验顺序（一次遍历，不许来回扫）

**校验清单以 `PROTOCOL.md` §9 为准，逐条实现，不要在这里另写一份更短的。** 本文件 v0.9 的清单少了五条（`Response.request_id == 0`、`Request/Notification.method == 0`、`Handshake` payload ≠ 12、Err payload < 8、`max_message_len < 24`），而少一条就意味着少一个反例 fixture。实现时按 §9 的编号顺序做，顺序本身有依赖：先长度、再 kind、再 flags、再逐 kind 字段，最后才是 payload 级检查。

1. **长度 ≥ 24 → 否则 `Protocol`。无条件检查，不是 `debug_assert`（v0.10 修）。** L1 只承诺「一条完整消息」，并不承诺「消息体内含合法 24B 头」：`memory` / `shm` 这些 L1 根本不解析头的 transport，完全可能把一条 10 字节的 `Bytes` 交上来（应用层自己塞的、或对端实现有 bug）。`debug_assert` 在 release 下被编译掉，随后读 `&msg[0..24]` 就越界 panic（甚至读到垃圾字段继续往下跑）。这一条必须 `if msg.len() < 24 { return Err(Protocol) }`。
2. `kind` 属于 0–6 → 否则 `Protocol` + 断连。
3. `flags` 保留位（bit1、bit3–7）为 0 → 否则 `Protocol` + 断连；bit2 仅允许出现在 `GoAway`。
4. 按 3.2 的表检查该 kind 的「必须为 0」字段，外加 `PROTOCOL.md` §9 第 8/13/14 条（`request_id = 0` 的三个变体、`Response.request_id = 0`、`Request/Notification.method = 0`）。
5. 控制帧的定长约束：`Handshake` payload 必须恰 12 字节；`Cancel` / `GoAway` payload 必须为空；Err `Response` 的 payload ≥ 8。
6. 通过后按 kind 分流：控制帧走手写解码，数据帧把 payload 交给 L3（**decode 不在 L2 做**，见 3.4）。

### 3.4 payload 解码归 L3，不归 L2

L2 只负责「头 + 裸 payload 的边界」。`codec.decode` 发生在 L3 spawn 出来的 task 里（`design.md` 4.2 第 ⑤ 步），因为反序列化可能耗时（大 payload），留在 reader loop 里会堵住整条通道的多路复用。

**解码失败的错误码是 `4`（`InvalidArgs`），不是 `Protocol`**：帧格式没问题，是业务参数不合法。这个区分决定「要不要断连」——解析失败断连会把一条好好的连接因为一条坏请求打死。

---

## 4. flags

| bit | 允许出现在 | 含义 | 违规 |
|---|---|---|---|
| 0 | `Response` | payload 是错误体 | 其它 kind 上置位 → `Protocol` |
| 1 | `StreamChunk`（未实现） | 最后一帧 | 任何 kind 上置位 → `Protocol` |
| 2 | `GoAway` | 急停，不等 in-flight | 其它 kind 上置位 → `Protocol` |
| 3–7 | — | 保留 | 任何置位 → `Protocol` |

**协议不留压缩位。** 本地 IPC 上压缩是 CPU 换带宽、而带宽免费；将来真要压缩走「新增 kind + 独立协商」，这样旧实现对未知位报错而不会误读。

---

## 5. method id

### 5.1 规则

- 哈希对象：**只有方法名**（不含 trait 名、不含服务名）。
- 算法：xxh3，64 位，**在 proc-macro 展开时算成字面量**。
- 用 u64 不用 u32：碰撞概率 2⁻⁶⁴，省掉运行时碰撞检查表。
- 同一服务内：方法名不得重复；不同方法不得撞到同一值（宏在编译期做常量断言）。
- 改名即协议变更——这条写死，不做兼容层。

### 5.2 为什么去掉服务名

`service_id` 已经承担路由，`method` 只在服务内唯一。而改 trait 名是最常见的一次重构（`Calc` → `Calculator`），若服务名进哈希，改名就等于全量改线上 method id，那「显式 `service_id` 保证稳定」这条理由被稀释一半。

### 5.3 golden 向量

必须有一条：`"add"` → 一个固定的 u64 字面量。位置在 **`photon-macros` 的单元测试**（或 `tests/fixtures/method_ids.txt` 纯文本），**不要混进 `tests/fixtures/*.bin`**——那里放的是线格式帧，而这是哈希常量，不是帧。

用途：一旦有人「优化」哈希实现（换 xxh3 版本、改 `Hasher::write` 的字节布局），测试立刻红。没有它，某次 `xxhash-rust` 升级就会静默改掉全部线上 method id，而且没有任何现象能提示你。

### 5.4 方法名表

宏顺手生成 `const METHOD_NAMES: &[(u64, &'static str)]`（或 `fn method_name(u64) -> Option<&'static str>`）。请求 span、`UnknownMethod` 日志、panic 报告全靠它；成本为零（全是编译期字符串字面量）。只有 `request_id` 的 span 在排障时基本没用，必须带方法名。

---

## 6. `Codec`

```rust
pub trait Codec {
    const ID: u16;   // postcard = 0，rkyv = 1（M5）

    fn encode_into<T: Serialize, B: BufMut>(&self, value: &T, dst: &mut B) -> Result<(), RpcError>;
    fn decode<T: DeserializeOwned>(&self, src: &Bytes) -> Result<T, RpcError>;

    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, RpcError>; // 便利包装，默认实现
}
```

三条硬约束：

| 约束 | 原因 |
|---|---|
| **构建期选定，无 `Box<dyn Codec>`** | `encode_into` / `decode` 带泛型参数 → 非 dyn-safe；`codec::Default` 是编译期选定的单态化类型（v0.10 起 `Session` 不带 `C` 参数），握手后「换 codec」在类型上不成立 |
| 握手只带 `codec_id` 做 fail-fast | 双方各自只编译进一种 codec，位图永远只可能有一位 |
| `encode_into` 的 `dst` 必须已有 24 字节 headroom | 否则每帧多一次 memcpy（见 2.4） |

`const ID` 是补 v0.8 的缺口：`Handshake` 要填 `codec_id`，而之前没说这个数从哪来。现在握手逻辑直接读 `codec::Default::ID`（v0.10 起 `Session` 不再有 `C` 参数），不在 dispatch 或宏里维护第二份映射。**这个常量是线上协议的一部分，值不许改**。

`postcard` 已知取舍：无 schema 演进能力、反序列化必然产出 owned 结构。`rkyv` 在 M5 作为**独立编译产物**引入，不能与 postcard 同时服务同一组 handler——它要求 handler 参数写成 `&Archived<T>`，业务代码形状会变。`bincode` 不采用（RUSTSEC-2025-0141，3.0.0 是只含 `compile_error!` 的墓碑）。

---

## 7. 错误体

### 7.1 布局（`flags` bit0 = 1 的 `Response`）

```
[ code: u32 | retryable: u8 | reserved: u24 | message: 剩余字节 UTF-8 ]
   0..4        4              5..8           8..
```

共 8 字节前缀，`message` 直接读到 payload 末尾（长度由 L1 保证，不做第二套长度字段），小端。

### 7.2 code 空间

| code | 名称 | 谁产生 | 客户端拿到后 |
|---|---|---|---|
| 0 | `Internal` | 框架（handler panic、JoinError、不可恢复） | `Remote { code: 0 }` |
| 1 | `Overloaded` | 对端配额满 / 队列满 / 线程池排不上 | `Remote { code: 1 }`（`retryable` 通常 1） |
| 2 | `UnknownService` | 对端：`service_id` 未注册 | `Remote { code: 2 }`，重试无意义 |
| 3 | `UnknownMethod` | 对端：服务在但方法未注册 | `Remote { code: 3 }`，重试无意义 |
| 4 | `InvalidArgs` | 对端：payload 解码失败 / 参数不合法 | `Remote { code: 4 }`，重试无意义 |
| 5 | `Cancelled` | 对端：收到 `Cancel` 并按此中止 | `Remote { code: 5 }` |
| 6 | `DeadlineExceeded` | 对端：它自己判定的 deadline 到期 | `Remote { code: 6 }` |
| 7 | `ShuttingDown` | 对端：优雅关闭中，不接新请求 | `Remote { code: 7 }` |
| 8..=0xFFFF | — | 现在无定义 | **原样装进 `Remote`，不许拍成 `Internal`** |
| ≥ 0x1_0000 | 用户 | 业务自定义 | `Remote { code }`，语义由用户定义 |

`message` 非法 UTF-8 用 `from_utf8_lossy` 并保留原 `code`——错误体是排障通道，不能因为文本坏了就把整个错误降级成 `Internal`。`retryable` 只认 0/1，其它值按「非 0 即可重试」宽松处理，**不报协议错误**：错误体位于收尾路径，在这里断连只会让排障更难。

### 7.3 本地 / 远端分界（`INV-12`）

**规则是方向相关的，不是「本地变体一律不上线」这种一刀切（v0.10 修）：** 一刀切没法自洽——服务端要回 `code = 1/4/6`，手里只有本地变体，而 `Remote` 按定义「只能由错误体构造」。

1. **只有处理请求的一侧**（正在为某个 `request_id` 生成 `Response`）可以主动构造 `Remote` 表达自己的状态，也**可以**把自己的本地变体经 `to_wire` 编成错误体。
2. **发起请求的一侧**只能从收到的错误体得到 `Remote`；它的 `Closed` / `Io` / `Protocol` / `RuntimeDropped` 永远不出现在线上，也不与 `Remote` 互相转换。

唯一出口函数（`proto/error_body.rs`，全项目只允许这一处拼错误体）：

```rust
pub fn to_wire(e: &RpcError) -> Option<ErrorBody>
```

| `RpcError` 变体 | 上线 code |
|---|---|
| `Internal` | `0` |
| `Overloaded`（**入站配额满**） | `1` |
| `InvalidInput`（handler 解码失败 / 参数不合法） | `4` |
| `Cancelled`（ handler 被 `Cancel` 中止） | `5` |
| `Timeout`（handler 自己的 deadline 到期） | `6` |
| `Remote { code, .. }` | 原码（用户业务错误即 `code ≥ 0x1_0000`） |
| `Closed` / `Io` / `Protocol` / `RuntimeDropped` | `None`（回包路径上出现属框架异常，记 error 日志后按 `0` 兜底，不静默丢帧） |

**`UnknownService`（2）/ `UnknownMethod`（3）/ `ShuttingDown`（7）没有对应的 `RpcError` 变体**：由**回包侧**用 `RemoteError::new(code)` 造 `Remote`，再经 `to_wire` 透传（`Remote` 走原码直通）。只允许三处调用：查不到 `service_id`、查不到 `method`、`ingress == Draining`。这条同时堵住两个已经写歪的地方：`04-l4-api.md` 的未知方法分支曾自己 `RemoteError::new(3)` 却没说明「这是回包侧允许的行为」，`03-l3-session.md` 曾把 `Timeout → 6`、`Overloaded → 1` 写成散装映射、绕过白名单。

`RpcError` 必须 `#[non_exhaustive]`：公开枚举不加这个标记，每加一个变体都是 semver 破坏。

---

## 8. 控制帧（手写定长，永不经过 `Codec`）

理由是鸡生蛋：`Handshake` 要交代「我用哪个 codec」，用它自己去解自己等于循环依赖。

### 8.1 `Handshake`：12 字节

| 偏移 | 宽度 | 字段 | 规则 |
|---|---|---|---|
| 0 | 2 | `protocol_version` | 当前为 1；不等即断 |
| 2 | 2 | `flags` | 保留，必须 0 |
| 4 | 4 | `max_message_len` | 本端**愿意接收**的单帧上限（含 24B 头）；`≥ 24`，`shm` 上还须 `≤ slot_size`；取值来自 `SessionOptions`，握手前本端上限恒 64 KiB |
| 8 | 2 | `codec_id` | `Codec::ID`；不等即断 |
| 10 | 2 | `reserved` | 必须 0（v0.8 的 `payload_mode` 从这里移除） |

### 8.2 `GoAway` / `Cancel`

- `GoAway`：payload 为空，全部字段按 3.2 为 0；`flags` bit2 = 1 表示急停（不等 in-flight）。
- `Cancel`：payload 为空，`request_id` 指向要取消的请求；未知 id / 已完成 id **静默忽略**（它天然是竞态，报错只会刷屏）。

v0.7 的 `GoAway{last_request_id}` 已删：对称会话下它没有良定义——发送方说的「最后已处理 id」指的是对方 id 空间，而两侧各有一套计数器。

---

## 9. 模块布局

```
src/proto/
  mod.rs        kind / flags 常量、Header 视图与存取器、校验入口（`validate()`）
  header.rs     24B encode / decode + 逐 kind 校验（M0 就做，否则端到端无法验证）
  error_body.rs code 表、8B 前缀编解码、RemoteError ↔ 错误体、`to_wire()` 白名单
  control.rs    Handshake / GoAway / Cancel 的手写编解码（12B / 0B / 0B）
  codec/
    mod.rs      Codec trait + `codec::Default` 别名（由 feature 决定）
    postcard.rs postcard 实现（默认，ID = 0）
    rkyv.rs     M5，ID = 1，独立 feature
src/error.rs    RpcError / RemoteError 定义（`#[non_exhaustive]`）
```

**`RpcError` 不在 `codec/mod.rs` 里（v0.10 修）**：v0.9 的模块表把它塞在 codec 下，与 `design.md` 1.3 的目录树（`src/error.rs`）不一致。错误类型是 L2/L3/L4 共用的公共类型，挂在 codec 下会让「关掉 codec feature 就没有错误类型」这种荒唐依赖出现。

红线：`proto` 目录里不许出现 `TcpStream`、`tokio::spawn`、`Service`。

---

## 10. `PROTOCOL.md` 与 fixtures

- `PROTOCOL.md` 是本项目**线格式唯一权威**；`design.md` 3.3.1 只是设计视角速查表，与它同源。任何字段改动先改 `PROTOCOL.md`，再同步速查表与黄金样例。真正要照着写解析器时，看它第 2 节（头）、第 4 节（逐 kind 语义）、第 6 节（错误体）、第 7 节（控制帧）、第 9 节（校验清单）。
- `PROTOCOL.md` 最小结构：头字段表（含偏移与宽度）、逐 kind 语义、flags、错误体、控制帧、每条 kind 一段 hex 样例与字段解读。
- `tests/fixtures/*.bin` 七种：`Request`、`Response Ok`、`Response Err`、`Notification`、`Cancel`、`GoAway`、`Handshake`。
- 生成顺序（写死）：**先用实现生成 → 人工对照 `PROTOCOL.md` 逐字节审 → 冻结进仓库 → 测试只做 decode/encode 比对，运行时绝不重新生成**。运行时重生成等于把当前实现的 bug 固化进 fixture。
- 反例 fixtures（同等重要，没有它们硬约束就没有测试）：`Cancel.service_id != 0`、`Notification.request_id == 0`、`Handshake.reserved != 0`、`flags` bit3 置位、`kind = 7`，每条都断言「必须报 `Protocol`」。

---

## 11. 验收

| 项 | 判据 | 阶段 |
|---|---|---|
| 头往返 | 任意字段组合 encode → decode 字段一致；fixtures 双向比对 | M0 |
| 逐 kind 校验 | 反例 fixtures 全部被拒，且**不误伤合法帧** | M0 |
| 错误体 | `code 0..7` 编解码；`message` 非 UTF-8 时 lossy 且 code 不变 | M2 |
| 未知 kind / 保留位 | 断连且不产生 `Response` | M2 |
| method id | golden 向量稳定；同名不同服务不冲突 | M2 |
| codec fail-fast | 两侧 `codec_id` 不等 → 100 ms 内断开，错误为 `Protocol` | M3 |
| 对齐 | payload **消息内偏移 24** 恒成立；**绝对 8 对齐**只在 `memory` / `shm` 上断言（`debug_assert`），`tcp` / `local` 禁止开 rkyv 原地读 | M5 |

---

## 12. 明确不做

- 运行期 codec 协商 / `supported_codecs` 位图（构建期已定死）。
- `AnyCodec` 门面（真要支持多条 decode 路径时再引入，代价是每个方法两条解码路径）。
- 压缩位、TLS、跨语言 schema、schema 演进（postcard 本身不具备）。
- payload 内部分片（`max_message_len` 是硬上限）。
- 用 `length` 字段做校验（边界归 L1）。
