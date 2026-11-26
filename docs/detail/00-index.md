# photon 分层详细设计 —— 索引与全局约束

> 本组文档是 `docs/design.md`（**v0.22**）的**实现层展开**，不新增任何架构决定。v0.10 与本文档相关的改动集中在三处：`Session::new` 签名（补 `registry`、删 codec 泛型）、出站/入站配额拆两张表、关闭序列（`ingress` 延迟 Draining + writer 排空退出）。v0.11 只动模块布局：入口文件改为 `transport.rs` / `proto.rs` / `proto/codec.rs` / `session.rs`（2024 风格，不用 `*/mod.rs`）。v0.12 改样例承载方式：七个黄金样例从 `tests/fixtures/*.bin` 改为 `photon/tests/protocol.rs` 内联字节数组，目录与 `include_bytes!` 一并取消；线格式与接口签名均未变化。v0.13 把 `Transport::bind` 改为异步（`L1-C4` 随之从「bind 同步」改为「三者皆异步」），`Server::bind` 因此变成 `async fn`，`detail/04` 的 `pub fn bind` 与装配清单同步加上 `.await`；线格式零改动。v0.14 落地 `memory` 传输并新增 `tests/transport.rs`：以 `Harness` 只抽象「怎么接上一对半身」，6 条用例体单份、`tcp` 与 `memory` 各跑一遍；`Memory::pair` 同时去掉 `opts` 参数（`TransportOptions` 的两项对它无意义），且 `memory` 不实现 `Transport`。v0.15 落地 `local` 传输（Unix 手搓 UDS，保留半关闭；Windows 未实现、开启即编译失败），并把 drop 契约从「必须尽力半关闭」改判为「本端 drop 后不再写」——对端能否观测 EOF 降级为能力声明（`PEER_SEES_EOF`），正确性由 `GoAway` + 收尾超时兜底；`INV-16` 随之重写。v0.16 落地 L3 `Session` 骨架与 `proto/error_body.rs`：`pending` 表带 `Drop` guard（超时/取消/panic 三条路径都回收配额）、reader/writer 双 task、首帧强制 `Handshake`、`GoAway` 六步收尾；错误出口函数签名与实现对齐为 `to_wire(&RpcError) -> Option<(u32, bool, String)>`（`Closed → 7`，`Io`/`Protocol`/`RuntimeDropped → None` 按 `0` 兜底）；`proto::parse` 拆出 `parse_with_limit`，帧长上限改为握手协商值；handler panic 用「外层 task await 内层句柄」兜住并回 `Remote{0}`；`tests/session.rs` 用例体一份、`memory` 与 `local` 各跑一遍（8 条 × 2）。v0.17 落地 M2 第一步：`codec.rs` 与 `codec/postcard.rs`（`Codec` 改成无 `self` 的关联函数，`postcard` 直写 `BufMut`）、`#[photon::service(id = N)]` 展开为 `CalcClient<T>` / `CalcService<S>` / `register_calc` / `mod __photon_<svc>`（三处都是 v0.17 才定的：包装类型绕开 `E0210`、`async fn` 改写为带 `Send` 的 RPITIT、`SERVICE_ID` 不挂 trait 上）；`tests/service_macro.rs` 8 条 + `tests/compile-fail/` 3 条（trybuild）。v0.18 收尾 M2：`session/server.rs`（`Server<T>` 按值持注册表、accept 循环区分连接级/ listener 级错误）、`session/client.rs`（`ConnectOptions` + `connect::<T>` 等到握手 Ready）、`Session::wait_closed()`（`closed: Arc<Notify>`，先建 waiter 再查状态）；客户端 stub 改判为只泛型化传输半身 `CalcClient<S, K>`（`Memory::pair` 不实现 `Transport`）；三个示例输出与 `design.md` 10.5 的 M2 四条逐字一致；`tests/concurrency.rs` 在 `memory` 与 `tcp` 上各跑一遍。v0.19 取 M3 第一步：三档声明形态落地（`async fn` / `fn` / `#[photon(blocking)] fn`，逐方法写死调度策略）、`Service::schedule(method)` 把「占哪类额度」交给 L3、入站配额拆成 `async` / `blocking` 两张表、debug 慢 handler 告警与 handler panic 的 `tracing::error!`；线格式与接口签名（除 `Service` 新增一个带默认实现的方法）均未变化。v0.20 取 M3 第二步：`CallGuard`（超时或 future 被 drop 都摘表、归还配额）+ `PendingEntry::drop` 发 `Cancel`（`try_send`）、`Notification` 与 `Request` 共用 `dispatch`（`want_reply = false`）、`SessionHandle::notify`；`INV-21` 新增「通知永不回包」。v0.21 取 M3 第三步：`close()` 按 4.6 顺序（先关 `egress` 再发帧 → 等 `pending`/`handlers` 双零）、收到对端 `GoAway` 后 `spawn_finish_when_drained` 排空即收尾（不依赖 EOF）、`ingress_draining` 只在双方宣告后置位并回 `Remote{7}`、`Session::abort()` 发 bit2 急停且 writer 依 `abrupt` 丢弃未写帧；`INV-22` 新增「急停必须真的丢活」。v0.22 只补握手协商的验收（`limits.0` 本端声明 / `limits.1` 对端声明各一条用例 + 三种建连期拒绝），代码零改动；`INV-23` 钉住「上限方向不混用」。
>
> 三类文档的优先级从高到低：**`docs/PROTOCOL.md`（线格式唯一权威）> `docs/design.md`（架构与决策）> `docs/detail/*.md`（实现细节）**。任何冲突以高优先级为准；本组文档若与 `design.md` 打架，是本文档的 bug，不是 `design.md` 的。

---

## 1. 文档索引

| 文档 | 覆盖范围 | 对应 `design.md` | 里程碑 |
|---|---|---|---|
| `docs/PROTOCOL.md` | 线格式**唯一权威**：24B 头、逐 kind 语义与「必须为 0」矩阵、flags、错误体、控制帧、七种 hex 样例、parser 校验清单、扩展规则 | 3.2 / 3.3 / 3.4 / 3.8 / 6 节机制 2 | M0（头 + 样例）、M2（错误体）、M3（握手协商） |
| `01-l1-transport.md` | 通道抽象、framing、`tcp` / `local` / `memory` / `shm`、`AnySource`、Endpoint、`TransportOptions`、帧长旋钮（`recv_limit()` / `send_limit()`） | 2 节、附录 B | M0、M1、M5 |
| `02-l2-protocol.md` | 24 字节头、逐 kind 校验、flags、Codec、method id、错误体 | 3 节、6 节机制 2 | M0（头）、M2（错误体）、M5（rkyv） |
| `03-l3-session.md` | 出站 pending 表与入站配额、reader/writer、取消与超时、握手、关闭状态机 | 4 节、6 节机制 1/2 | M1、M3 |
| `04-l4-api.md` | `Service` trait、宏语法与展开产物、三档调度、两个客户端变体、本地直连 | 5 节、10 节 | M2、M3 |

阅读顺序建议：先 01 → 02（一帧从字节到语义）→ 03（一帧从语义到 handler）→ 04（从 handler 到用户代码）。四份文档里出现的类型名、字段名、常量值必须逐字一致，靠本文档第 3 节的全局表锁住。

---

## 2. 分层交接契约（跨文档唯一的接口面）

四层之间只有三个交接面。任何实现只要不破坏这三条，就允许自由重组内部模块。

| 交接 | 交出方 | 交出什么 | 接收方拿到后**不允许**做什么 |
|---|---|---|---|
| **H1** | L1 | `recv() -> io::Result<Bytes>`，一条**完整**消息，`Send + 'static` | L2 及以上不得自己从字节流里切消息、不得认识 fd / `AsyncRead` |
| **H2** | L2 | 24 字节头已解析为固定偏移上的视图 + 裸 payload `Bytes` | L3 不得认识 `fd` / transport 类型；不得假设 payload 已反序列化 |
| **H3** | L3 | `Service::call(self: Arc<Self>, method: u64, payload: Bytes) -> BoxFuture<'static, Result<Bytes, RpcError>>`（v0.10 把 `self: Arc<Self>` 补进交接面，`design.md` 5.1 早已是它） | L4 不得认识 `request_id`、连接、`Sender` |

三条铁律（`design.md` 1.2）在这三面上的具体形态：

- 铁律①：`transport` 模块之外不得出现 `AsyncRead` / `AsyncWrite` / `RawFd` 类型名。
- 铁律②：`session` 与 `proto` 模块里不得出现 `TcpStream` / `Endpoint` 的**具体变体**匹配（只允许 `Transport` 泛型参数与 `Endpoint` 值传递）。
- 铁律③：L1 交出的载体固定是 `Bytes`，不是 `&[u8]`、不是 `Vec<u8>`、不是自定义 `Frame` 结构体。

---

## 3. 全局不变量总表

每一条都必须能被至少一个测试或一个编译期断言守住；标「文档」的表示当前只能靠评审守。

| ID | 不变量 | 守它的手段 |
|---|---|---|
| `INV-01` | 协议头恰好 24 字节，字段偏移固定，全文小端 | 黄金样例 decode/encode 双向比对 |
| `INV-02` | 消息内 payload 起始偏移恒为 24；**绝对地址 8 字节对齐只对 `memory` / `shm` 成立**（流式 transport 从第二帧起基址任意） | `memory`/`shm` 路径的 `debug_assert`；`tcp`/`local` 路径禁止开 rkyv 原地读（M5 的单测必须按 transport 分别断言） |
| `INV-03` | `Request` / `Notification` / `Cancel` 的 `request_id != 0`；`Request` / `Notification` 的 `method != 0`；`Response` 的 `request_id != 0`；`Response` / `Cancel` / `GoAway` / `Handshake` 的 `service_id` / `method` / `deadline_ms` 必须为 0 | 反例用例（字段填错必须被拒）；清单以 `PROTOCOL.md` §9 为准 |
| `INV-04` | `request_id` 由发起方分配、只在本端 pending 表内唯一；`Response` / `Cancel` 里的 id 按连接方向解释 | 双向并发测试（两侧各自从 1 开始仍不串包） |
| `INV-05a` | `max_message_len` 双向独立：reader 上限 = 本端声明，writer 上限 = 对端声明；握手前两侧固定 64 KiB | 协商测试：A 声明 1 MiB / B 声明 64 KiB，B 发 500 KiB 必须被 A 接受、A 发 500 KiB 必须被本地拒 |
| `INV-05b` | `max_message_len >= 24`；`shm` 上 `<= slot_size` | 建会话前的拒绝测试（两个越界值各一条） |
| `INV-06` | `Handshake` 恰 12 字节、首帧、每连接一次；`codec_id` 不匹配或 `reserved != 0` 即断开 | 首帧非握手断连测试；错 `codec_id` 断连测试 |
| `INV-07` | 出站配额检查与 `pending` 插入在同一临界区（`try_reserve`），入站同理由 `try_acquire` + guard 保证，永不超过上限 | 并发压测后断言 `pending.len() + 已完成数 == 已提交数` 且从未越界 |
| `INV-08` | 客户端只有一组出站配额；`blocking_in_flight` 只在服务端生效 | 客户端把 `max_blocking_in_flight = 0` 仍能正常发包 |
| `INV-09` | 关闭期间 `tx` 保持可用，直到「本端 pending + 对端在飞 handler」双双归零；通道收口由 writer 排空退出完成，不靠 drop 字段 | Draining 测试：关闭途中收到的 `Request` 仍被 dispatch 并回包，在飞 `Response` 仍送达 |
| `INV-10a` | **已进入 handler 的请求一定回包**：只要它在本端 `ingress` 进 `Draining` 之前到达并被 dispatch，就必须拿到 `Response`（成功或错误体） | 关闭竞态测试：发起 `close()` 的同时并发发请求，落在「已 dispatch」集合里的请求零丢失 |
| `INV-10b` | `GoAway` = 连接级开始收尾：**出站**在两个方向上都立即停止发起（`close()` 与收到 `GoAway` 都触发），**入站**只在「双方都宣告过关闭」或超时兜底时才 `Draining` | 双向对等测试：任一侧 `GoAway` 后，两侧新请求都返回 `Closed`；对端 `GoAway` 之前发出的请求不被误拒为 `Remote{7}` |
| `INV-10c` | 双方都宣告关闭之后到达的 `Request` 回 `Remote{7}`（明确拒绝，不静默丢弃）；超时兜底同样如此 | 半开连接测试：发 `GoAway` 但对端继续发请求 → 必须收到 `Remote{7}` 而不是挂死 |
| `INV-11` | 控制帧（`Handshake` / `GoAway` / `Cancel`）payload 手写定长，永不经过 `Codec` | 关掉 codec feature 仍能完成握手与关闭 |
| `INV-12` | 错误只有一个出口 `to_wire`；规则**方向相关**：回包侧可主动构造 `Remote` 并把自己的本地变体编成错误体，发起侧只能由错误体得到 `Remote`、本地变体永不上线路 | 错误映射表的单测 + 注入测试（服务端配额满 → 客户端拿到 `Remote{code:1}`，不是 `Overloaded`；发起侧 `Closed`/`Io` 永不出现在线上） |
| `INV-13` | `Service::call` 的接收者是 `self: Arc<Self>`；`spawn` 每请求只在 L3 发生一次 | 编译通过即证明（`&self` 版本编不过 blocking 分支） |
| `INV-14` | `#[photon::service]` 的 `id` 必填、`u16`、注册撞号报错不覆盖 | 编译失败测试 + 注册期单测 |
| `INV-15` | method id 只哈希方法名，编译期常量，golden 向量锁死 | `photon-macros` 单元测试 |
| `INV-16` | `drop Sink` 后**本端**不再写、`drop Source` 后不再读；对端能否观测到 EOF 是实现能力，**不是契约**（正确性由 `GoAway` + `close_timeout` 兜底） | 各 transport 按 `Harness::PEER_SEES_EOF` 的声明断言：`true` 必须读到 `UnexpectedEof`，`false` 必须读不到结束信号 |
| `INV-17` | `recv` 与 `send` 只允许终态取消，不得用于单次超时 | 代码评审 + 文档；单测里禁止 `timeout(send())` 写法 |
| `INV-18` | 所有基准测试与性能验收禁止使用 `AnySource` / `AnySink` 门面，必须走泛型单态化路径 | `benches/` 里出现 `AnySource` 即评审否决；M4/M5 数字必须标注所用具体 transport 类型 |
| `INV-19` | 只有 `recv_limit()` / `send_limit()` 交出的 `Arc<AtomicU32>` 能承载协商后的帧长上限；`Session` 不得自己再存一份 | `max_message_len` 的唯一真相点检查（代码评审）；1 MiB 协商测试必须走过旋钮 |
| `INV-20` | 入站额度分 `Async` / `Blocking` 两类，占哪类由 `Service::schedule(method)` 给出；两类互不挤占（阻塞池满时异步请求照常放行） | `tests/session.rs::blocking_quota_is_separate`（只给 1 个阻塞额度：一次成功、两次过载，同批异步请求成功） |
| `INV-23` | `max_message_len` 两个方向不混用：reader 用本端声明、writer 用对端声明，且都在 `Ready` 时写入 | `tests/session.rs::own_declared_limit_governs_recv`（客户端声明 1 MiB 收 200 KiB）+ `peer_declared_limit_governs_sending`（服务端声明 1 MiB，客户端发 200 KiB） |
| `INV-22` | 急停（`GoAway` bit2 / `Session::abort()`）必须真的丢活：已进入 handler 的活被取消、未写出的帧被丢弃（丢弃条数进日志），而不是排空后才断 | `tests/session.rs::force_goaway_cancels_running_handlers`（handler 是 `pending()`，只有被取消才会 drop；优雅宣告下必然红）+ `abort_fails_inflight_without_waiting` |
| `INV-21` | 通知（`Notification`）永不产生 `Response`：服务端未注册服务 / 额度不足 / handler 失败一律只记日志 | `tests/session.rs::notification_reaches_handler`（含「指向未注册服务不断连」） |

> 编号只在本文档内使用。`design.md` 第 8 节的风险条目引用按它自己的规则（1–36 条 + 具名锚点），两套编号不互抄。`INV-10` 在 v0.10 拆成 10a/10b/10c，因为它们原本被一条写死，结果与 `03-l3-session.md` 的关闭流程互相打架（「双向停发起」与「在飞一定完成」不能同时成立）。

---

## 4. 术语（四份文档统一口径）

| 术语 | 定义 | 易混淆点 |
|---|---|---|
| **消息 / 帧** | L1 交出、L2 解析的最小单位：`24B 头 + payload` | 流式 transport 上的「帧」还包含外层 4 字节长度前缀，那是 L1 内部实现，不属于线格式 |
| **payload** | 24 字节头之后的部分，长度 = 消息总长 − 24 | 控制帧的 payload 是手写定长字段，不是 codec 编码结果 |
| **in-flight** | 已被接受但尚未完成的工作，两类要分开数：本端**已发出**的（等 `Response`）、对端发来且**已进入 handler** 的（该我回 `Response`）。v0.10 明确「已写出但还没被对端 `recv` 到的请求」不算，也不可观测 | 「in-flight 清空」必须说清是哪一类；`GoAway` 的承诺只覆盖「已进入 handler」那类 |
| **pending** | 本端 `outbound` 表里「发出去了、还没拿到 `Response`」的条目 | 与 in-flight 不等价：对端发来的请求不在本端 `outbound` 里，而在 `InboundQuota` 的计数里 |
| **Draining** | 某一方向「不再接受新的、但把已有的做完」 | 不是「已关闭」，也不是「半关闭」；半关闭是 transport 细节 |
| **半关闭** | 只关掉一个传输方向（写方向 `shutdown`） | `local` / Windows named pipe 做不到，必须降级 |
| **档 A / 档 B** | shm 的两种放置：直接写槽位 / 块池 + 索引 | 档 A 配同步 `fn`，档 B 配大消息 |
| **owned payload / 借用 payload** | `tcp`/`local`/`memory` 交出的 `Bytes` 是 owned；shm 档 A 交出的指向槽位 | `PayloadMode::Copy` 只对后者有成本 |

---

## 5. 与 `design.md` 第 12 节待决项的关系

第 12 节列了 17 条待决。其中**会直接改动本组文档**的是这四条，实现前必须先收口：

| 待决 | 影响哪份文档 | 当前实现口径（在定案前照此执行） |
|---|---|---|
| 协作式取消（handler 加 `&ServiceContext` 首参） | `04-l4-api.md` 第 3、6 节 | 不做。`Cancel` 对 `async fn` 生效，对 `fn` 只丢弃结果 |
| `SOCK_SEQPACKET` 特化 | `01-l1-transport.md` 第 6 节 | 不做。`local` 已经覆盖 UDS |
| `AnyCodec` 门面 | `02-l2-protocol.md` 第 6 节 | 不做。构建期单 codec，握手只做 fail-fast |
| shm 免 spawn 快路径 | `03-l3-session.md` 第 6 节 | 不做。端到端目标 1–3 µs |

---

## 6. 实施顺序（四份详细设计都按它校准验收）

| 阶段 | 先落哪一层 | 关键理由 |
|---|---|---|
| M0 | L1 + L2 的**头** | 没有帧就没有会话；头是唯一会被 M5 复用的线格式，必须先冻结 |
| M1 | L3 | 换 transport 不改 L3，是这个项目最重要的架构验收 |
| M2 | L4 | 宏与错误体同时落地，否则「未知服务/未知方法/参数错」三个码无法观测 |
| M3 | L3 收尾 + L4 其余形态 | `GoAway` / `Cancel` / `blocking` 都是收尾路径，必须一起做 |
| M5 | L1 的 shm + L2 的 rkyv | 二者互为前提：没有 rkyv，shm 只省 syscall；没有 shm，rkyv 省不了唤醒 |

> 一条贯穿四份文档的判据：**任何「只有 M5 才需要」的字段或形状，如果现在不占位就会迫使线格式二次变更，那就现在占位**（`PayloadMode`、`#[photon(zero_copy)]`、段头的 `generation` / `peer_heartbeat` 都是按这条加的）；反之，凡是「现在加了也验证不了」的机制（协作式取消、`AnyCodec`、免 spawn 快路径）一律推迟，并在文档里明确写「不做」而不是留白。
