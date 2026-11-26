# Photon 设计文档

> 一个用 Rust 实现的轻量 RPC / IPC 框架。目标：小巧、高效、可扩展。
>
> 当前阶段：**M3 进行中 — 声明形态 + 超时/取消/通知 + 关停 + 握手协商 + 三传输示例**（trait 一次声明即得 client stub / dispatch / 注册函数；`local_calc`（memory）、`calc_server`+`calc_client`（TCP）、`calc_local`（UDS，两进程）三条链路都跑通；并发用例在 `memory` 与 `tcp` 上各跑一遍）。M3 已落八块：① 同步 handler 与调度分流；② 超时 / 取消 / 通知；③ `GoAway` 双向收尾 + 急停；④ 握手协商的双向验收；⑤ `calc_local` 两进程 UDS；⑥ `local_calc` 八行输出覆盖 10.3 全部七种形态（含 stub 的 `with_deadline`）；⑦ 业务错误映射 `#[photon::service(id = N, error = MyError)]`；⑧ `PayloadMode` 的类型与方法级 opt-in（`#[photon(zero_copy)]` 只对同步 `fn` 开放，全局开关随 shm 一起落）。剩下 `BlockingClient` / `Interceptor` 仍在 M3。
>
> 版本：**v0.26**（M3 第八步：`PayloadMode` 的接口面落地。`PayloadMode { Copy, ZeroCopy }` 进 `photon`，`Service::payload_mode(method)` 与 `schedule` 并列给出编译期属性；宏接受 `#[photon(zero_copy)]`，但**只对同步 `fn` 放行**——async 与 `#[photon(blocking)]` 都在展开期报错。v1 的两种取值在 owned payload 上都是 no-op，这一步落的是类型与护栏，不是性能。)

> 上一版（v0.25）（M3 第七步：业务错误映射。`#[photon::service(id = N, error = MyError)]` 之后方法返回 `Result<T, MyError>`，生成的 dispatch 在每个 handler 上 `into_error`（并在 debug 断言 `code >= 0x1_0000`），生成的 stub 在每个方法上 `from_error`——包括本端的 `Timeout` / `Closed`。只要求一个 `ServiceError` impl、两个方法，而不是两个方向相反的 `From` 实现。)

> 更早（v0.24）（M3 第六步：`local_calc` 补齐 10.3 的七种形态，一条命令打出八行。新增 `with_deadline` —— 期限挂在客户端上（`CalcClient::new(h).with_deadline(50)`），生成的每个方法把 `self.deadline_ms` 填进头里；这是 4.3 的 per-request timeout 在类型化 API 上的唯一入口，此前只能绕到 `SessionHandle::call`。示例同时新增 `notify`（无回包）、`slow_op`（50 ms 打出 `Timeout`）、同步 handler `fast_hash`，以及「服务端宣告关闭时留一个在飞请求」的优雅收尾。)

> 更早（v0.23）：`examples/calc_local.rs` —— 同一个二进制 `server` / `client` 两个角色，UDS 上跨进程跑通。业务代码与 `calc_client` 逐行同构，只换 `Endpoint::Local` 与 `LocalTransport`，是「换 transport 不改业务代码」的第一份可执行证据；同时验证了「上次进程崩溃留下的 socket 文件会被 bind 清掉、活着的监听者会被拒绝覆盖」这条本地 IPC 最容易踩的边角。）

> 更早（v0.22）：M3 第四步：握手协商补验收。`max_message_len` 的机制 v0.16 已落（`Ready` 时 `limits.0 = 本端声明`、`limits.1 = 对端声明`），但此前只有「对端声明决定发送」这一条证据；本版补上「本端声明决定接收」（客户端声明 1 MiB 收下服务端 200 KiB 的回包）与三种建连期拒绝（codec / 版本 / `max_message_len < 24`）。代码零改动，只加用例与文档）

> 更早（v0.21）：M3 第三步：`close()` 按 4.6 走「宣告 → 等在飞双零 → 停 task」，收到对端 `GoAway` 后自己起一个「排空即收尾」的后台任务（不等 EOF，named pipe / shm 上也能收敛）；`ingress` 只在**双方都宣告过**之后才进 Draining，此后的 `Request` 回 `Remote{7}`；新增 `Session::abort()` 发 bit2 急停，writer 看到 `abrupt` 就不排空、丢弃帧数进 `tracing::debug!`。线格式、字节布局与 17 条校验零改动）

> 更早（v0.20）：M3 第二步：客户端放弃（超时或 future 被 drop）时 `CallGuard` 摘表、归还配额，并由 `PendingEntry::drop` 发 `Cancel`（`try_send`，不因背压卡住放弃路径）；`Notification` 与 `Request` 共用 `dispatch`，只差「不回包」，未注册服务与额度不足只记日志；`SessionHandle::notify` 补齐。线格式、字节布局与 17 条校验零改动）

> 更早（v0.19）：M3 第一步：`fn` / `#[photon(blocking)] fn` 两种展开（`MethodKind` 三分支，`blocking` 的 `decode` 在闭包里）、`Service::schedule` 把调度类别交给 L3 用于扣额度、入站配额拆成 `async`/`blocking` 两张表、debug 慢 handler 告警与 panic 的 `tracing::error!`。线格式、字节布局与 17 条校验零改动）

> 更早（v0.18）：M2 收尾——`Server` 装配与 `photon::connect::<T>` 落地、三个示例跑通（输出与 10.5 逐字一致）、`tests/concurrency.rs` 在 `memory`/`tcp` 上各跑一遍；同时改判三处接口——客户端 stub 只泛型化到**传输半身**（`CalcClient<S, K>`，否则 `Memory::pair` 这种不实现 `Transport` 的路径用不上同一份 stub）、`Server` 按值持注册表并暴露 `registry()`/`transport()`、`Session` 新增 `wait_closed()` 作为「连接结束」的唯一可观测点。

> 更早（v0.17）：`codec.rs` + `codec/postcard.rs`（`ID = 0`）、`#[photon::service(id = N)]` 展开成 `CalcClient` / `CalcService<S>` / `register_calc` / `__photon_calc` 工具模块，method id 用 xxh3 只哈希方法名并冻结 golden 向量，宏与 dispatch 的验收用例与 trybuild 编译期拒绝一并到位。

## 变更记录

| 版本 | 变更 |
|---|---|
| v0.1 | 初稿：四层切分、24 字节帧头、`Transport: AsyncRead + AsyncWrite` |
| **v0.2** | **L1 由「字节流」改为「消息」**；协议头 24 → 20 字节，删除 `length` 字段（边界改由 L1 提供）；`Codec` 预留 `encode_into`；锁定「一切消息以 `Bytes` 为载体」的不变量；新增 `AnySource/AnySink` 类型擦除门面与 `PayloadMode` 策略；新增附录 B（共享内存方案详解与兼容性论证）。**注：v0.7 又把头改回 24 字节，但字段构成与 v0.1 完全不同**——不是回滚，是补齐 `service_id` 与 `deadline_ms` 并顺带解决对齐 |
| **v0.3** | **修正 1.3 的自相矛盾**（正文说「不拆 workspace」，同一节又说 macros 必须独立 crate）→ 定为**最小双 crate workspace**；补齐 `examples/` `tests/` `benches/`；新增 feature 划分表与默认集原则；**新增第 10 节「示例与运行」**，把可运行示例定为正式产物，含清单、验收门槛与运行方式；里程碑把示例纳入验收 |
| **v0.3.1** | 修正第 0 节「单 crate」的错误表述（与 1.3 的双 crate workspace 冲突）；把「依赖 ≤ 8 个」的口径写明（直接依赖 / 默认 feature / 排除 macros crate），记下 M3 `tower` 与 M5 `crossbeam-utils` 两处潜在冲突；补「重型栈」黑名单，并把「小巧」的实测指标改为二进制体积 |
| **v0.4** | **全文一致性审计**，修 20 处。硬错误：2 处交叉引用指错章节、1.3 目录树漏 `examples/calc_local.rs`、M0 验收误引尚不存在的 `Session`、待决第 4 条与第 10 条互相矛盾。**实质矛盾**：2.2 把 `memory` 说成「可直传指针、零序列化」，与铁律 ③（载体固定 `Bytes`）冲突——已拆清 `memory`（走 `Bytes`）与「本地直连」（不走通道）两件事。口径冲突：shm RTT 三处数字不一致 → 统一为按配置分档的表；`Handshake` 字段两处写法不同 → 归一到 6 节；`local` 被误称「默认」→ 改「首选（仍为可选 feature）」。另补 `TransportOptions` 定义、`max_in_flight` 字段、附录 A 数据来源、`unix` 实现去留（不做） |
| **v0.5** | **支持同步方法声明与同步调用**。把原先被 `async fn` 绑死的三件事拆开：**声明形态**（`async fn` / `fn` / `fn + #[photon(blocking)]`）、**调度策略**（编译期决定 `spawn` 还是 `spawn_blocking`）、**调用形态**（异步客户端 / `BlockingClient`）。协议与传输零改动：无新 kind、无新字段；`Service::call` 的参数与返回类型不变（接收者 `&self` → `self: Arc<Self>` 这一步 v0.6 才真正落到签名上）。配套新增：`Service` 必须持 `Arc`、双 `in_flight` 计数、同步 handler 不可中止的语义（4.3）、`BlockingClient` 必须绕开 `Handle::block_on`（5.5）、**第 5 节改造**（5.2 重写为三档声明、新增 5.3 调度分流、原 5.3/5.4 顺延为 5.4/5.6、新增 5.5 客户端双变体）、8 节第 18–22 条新坑、9 节 M2/M3 任务、12 节第 12–15 条待决、10.3/10.5 示例补同步 handler 形态 |
| **v0.6** | **全文一致性审计**，修 14 处。**设计缺陷 3 处**：① 5.1 原文说「`&self` 编译不过」但签名就写着 `&self`——接收者改为 `self: Arc<Self>`（`spawn_blocking` 要所有权，`&self` 无法重建 `Arc<Self>`）；② 5.3 的 dispatch 分支直接 `tokio::spawn` 返回 `JoinHandle`，与 5.1 的 `BoxFuture` 返回类型不符，且会变成每请求多次 spawn——改为「每分支返回 `Box::pin`，spawn 只在 L3 发生一次」；③ 4.5 的「`ZeroCopy` 超时就把 payload 拷出来」不可实现（所有权已移交 handler），改为**交付前**按方法标注/大小决策。**协议一致性 2 处**：3.3 明确 `StreamChunk` 为预留不实现；3.4 bit2 压缩改为「当前不实现，置位即协议错误」（原文引用了 Handshake 里并不存在的压缩协商项）。**依赖线 2 处**：5.1 补 `BoxFuture` 自实现说明（避免引入 `futures` 破 8 依赖线）、1.3.1 补缺失的 `interceptor` feature 行。**其余**：5.5 补 `T: Send + 'static` 与「`try_current` 必须在 spawn 前」「`Runtime` 必须被持有」三条；4.2 超时错误名 `Elapsed` → `Timeout` 统一；2.5 补枚举变体按 feature 条件编译；0 节可扩展加「1 个预留调度插槽」、非目标鉴权补 Interceptor 出口；8.10 与 B.5 的延迟口径澄清；12 节第 2 条与 5.3 对齐 |
| **v0.7** | **线格式定稿 + 修外部事实**。协议头 20 → **24 字节**：`kind(1) flags(1) service_id(2) request_id(8) method(8) deadline_ms(4)`——一次改动同时解决三件事：多服务路由（原来 `reserved: u16` 空着，`method` 里没有服务维度）、per-request deadline 无处安放（4.3 承诺过但没有字段）、rkyv 对齐（payload 起点从 4 mod 8 变成 8 对齐）。新增 **3.3.1 逐 kind 字段语义表**、**3.8 错误体与本地/远端错误分界**、**6 节控制帧编码 + 握手状态机**（控制帧不走 Codec，否则与「协商 codec」鸡生蛋）。修收尾路径三处空转：4.2 的超时泄漏（`Drop` guard 显式回收 permit）、`MessageSource::close`（reader task 可被唤醒）、`BlockingClient` 的 `recv_timeout`（原来会永久阻塞，与 M3 验收矛盾）。修三处外部事实：`bytes` 的 `from_owner` 实为 **1.9.0** 引入（且 1.10.1 才修掉它的内存泄漏）、`interprocess` 的 tokio named pipe **MESSAGE READING DISABLED**（`local` 必须全平台走 framing）、**bincode 已停止维护**（3.0.0 是只含 `compile_error!` 的墓碑，RUSTSEC-2025-0141）→ 默认 codec 换 `postcard`。另修 `GoAway.last_request_id` 在对称会话下的语义混乱、宏的 `blocking` 按使用方 feature 无法展开、handler panic 无人回包、`try_current` 误杀 blocking 线程、M2 验收不可能通过等 |
| **v0.8** | **收口五项「写进代码就改不动」的设计**。① `MessageSource::close` 删掉——`recv(&mut self)` 独占借用期间没人拿得到 `&self`，而 4.1 的 `select!` + 取消 token 已经解决了唤醒，`close` 是第二套重复机制；改为「取消 token 唤醒 → break → drop source 即关闭」，writer 侧同样 select。② **codec 改为构建期选择**：`Codec` 的方法是泛型的、不是 dyn-safe，因此「握手协商 codec 位图」根本无法实现——握手只带 `codec_id` 做 fail-fast，位图留给将来引入 `AnyCodec` 门面时再说。③ 协商项补通路：`max_message_len` 通过 `Arc<AtomicU32>` 旋钮由 L3 在握手后调整（L1 不看语义，不破铁律②）；`ring_capacity` 从 Handshake 移入 shm 段头，由创建方写入、attach 方校验（原来它要走这条环才能协商容量，是鸡生蛋）。④ 错误码定死 `1..7` 保留段，并引入 `RpcError::Remote` 与本地变体**永不合并**，解决「服务端过载/未知服务/未知方法无码可发」；`ShuttingDown` 也进保留段。⑤ 性能口径拆开：B.2 的 100–600 ns 是**裸传输 ping-pong**（不含 codec/session/dispatch），端到端目标另给 1–3 µs——原先把两者混成一个数，按每请求 `Box::pin + spawn + oneshot + 两次 mpsc` 的固定成本算，300 ns 端到端结构上不可能。**其余**：`TransportOptions` 补 `bind`/`connect` 入口并拆出 `SessionOptions`；`PayloadMode::Copy` 明确为非借用 transport 的 no-op、`ZeroCopy` v1 只对 `fn` handler 开放；`service_id` 的宏语法定为 `#[photon::service(id = 7)]`、方法哈希只对方法名；`next_id` 从 1 起；全文统一小端；M2 提前落地错误体；`flush` 从 trait 删除；黄金样例补 `Cancel`/`Notification`；`full` 的 CI 启用时点改到 M3；补 Closing 状态表与 `Session` 持 `JoinHandle` |
| **v0.9** | **修 v0.8 新文本里的四个真 bug + 闭合两处删除的后果**。① **4.6 的关闭序列与「一条 tx 跑请求与响应」冲突**：v0.8 用「`tx = None`」表达「不再准入」，结果在途 `Response` 与 `Remote{7}` 全发不出去（若 clone 了 `Sender` 又永远等不到 `None`）。改为——`tx` 是普通可 clone 的 `Sender`，准入由 `state`（两个方向各一个）判断，关队列靠「最后一个 sender 消失」自然发生。② **`max_message_len` 取小写错了方向**：A 声明收 1 MiB、B 声明 64 KiB，取小会让 A 拒收 B 合法发来的 500 KiB。改为**两个方向各用各的**：reader 上限 = 本端声明，writer 上限 = 对端声明（新增 writer 侧检查，超了本地 `InvalidInput`）。③ **GoAway 语义与步骤方向相反**：v0.8 说「不表示我不再发 Request」，下一步却把出站置 Draining。统一为「连接级开始收尾：双向都停止发起、双向都继续收尾」，收到 `GoAway` 的一侧出站也进 Draining。④ **drop 契约没写、且不是所有 transport 都能半关闭**：补「drop `Sink` 必须尽力半关写方向、drop `Source` 关读方向，做不到就降级为整连接关闭」，并在 2.6 写明 Windows named pipe 没有半关闭；`send` 与 `recv` 一样只允许终态取消（打断 `send` 会在流里留半帧）。**另修**：`payload_mode` 从 `Handshake` 移除（纯本端策略，两边不一致时原本无定义）；配额检查与插入并入同一把锁（`try_reserve`），并写明客户端只有一组出站配额；shm 段头补 `generation` / `state` / `peer_heartbeat`，并定死「一个段 = 一条单向环、双向用两个段」；M5 的 3 µs 补齐测量条件；`Codec` 加 `const ID` 供握手填 `codec_id`；`Memory::pair` 改为返回两份半身 + 新增 `Session::new`；`bind` 同步签名的实现备注（`std::net` + `from_std`）。**小问题**：10.3 形态 2 的 `Service` 变体残留改成 `Remote{code≥0x10000}`、四处 `#[photon::service(blocking)]` 补 `id`、3.2 的「v0.1 是 20 字节」纠正（v0.1 就是 24）、13.1 的字节序口径改为「以 3.2 正文为准」、`tests/protocol.rs` 的 M0 范围缩到头 + 长度前缀、method 哈希 golden 向量从 `.bin` 挪到单元测试、`PROTOCOL.md` 定为唯一权威（3.3.1 降为速查表）、fixtures 定「先实现生成→人工审→冻结，测试不重生成」并补反例断言、8 节新增条目改用具名锚点、4.6 的 Draining 触发点改为**不对称**（收到 `GoAway` 只动 `egress`，`ingress` 保持 `Open`，否则在飞请求会被本地打成 `Remote{7}`） |
| **v0.10** | **把「照文档写代码会做错」的六处接口逐个钉死，并与 PROTOCOL / detail 01–04 对齐**。① **4.6 关闭序列重写**：`close()` 只把 `egress` 置 `Draining`、`ingress` 保持 `Open` 继续 dispatch，`ingress` 只在「双方都宣告过关闭」或 5 s 兜底时才进 `Draining`（此后到达的 `Request` 回 `Remote{7}`）；「在飞」定义为「已进入 handler」；`GoAway` 第 1 步先入队再改状态；第 3 步不再 drop `tx`（`Arc` 里做不到），改为 `shutdown.cancel()` + writer 收到取消后 `try_recv` 排空再退；`reader`/`writer` 的句柄改 `Mutex<Option<JoinHandle>>` 以便 `take()`；删掉「与 gRPC `GOAWAY` 一致」的类比（v0.7 已删 `last_request_id`，没有水位就没有按 id 划边界的能力）。② **`max_message_len` 从 `TransportOptions` 迁入 `SessionOptions`**（握手与旋钮写入都在 L3，L1 拿不到也不需要；握手前恒 64 KiB），并给 `Source`/`Sink` 各加 `recv_limit()`/`send_limit()` 交出 `Arc<AtomicU32>`（v0.9 只写了「L3 去写旋钮」却没给通路）；补握手校验「`≥ 24`」与「shm 上 `≤ slot_size`」。③ **删 `Session<S,K,C>` 的 `C`**：`Service::call` 无 `C`、宏固定用 `codec::Default`，`C` 只贡献握手 `C::ID`，会导致「宣告 A、线上 B」的静默错线格式；`Session::new` 补 `registry` 参数。④ **出站/入站配额拆两张表**：`OutboundTable{pending,async_used}` + `InboundQuota{async_used,blocking_used}`，入站用 `try_acquire` + guard（原来一张表既挤占额度又没法在入站入口原子检查）。⑤ **错误边界定唯一出口 `to_wire(&RpcError) -> Option<ErrorBody>` 白名单**，规则改为方向相关两句话（回包侧可主动构造 `Remote`、发起侧只能由错误体得到），`UnknownService/UnknownMethod/ShuttingDown` 由回包侧经 `RemoteError::new(code)` 构造 `Remote`（只允许这三处调用，`Remote{3}` 是合法的而不是伪造），M2 的 `0x10001` 明确为示例 handler 自造用户码。⑥ **`deadline_ms` 口径统一**为「发送方填剩余预算、接收方从解析完头开始计时，实际可用 = 值 + 传输延迟」。**另**：2.5 的 `AnySource` 代价拆成「间接调用 / 每消息 alloc / 单态化体积」三项并量化到一个往返 4 个 box 点 80–160 ns，明确**基准测试禁止用门面**且门面只消除组合爆炸不消除泛型传染；8 节新增两条具名条目（`Arc` 里置空字段、假的 codec 泛型）并把「1–36 编号 + 新增用具名锚点」写成引用规则。**同步改动**：`docs/PROTOCOL.md`（§1 对齐断言收窄到 memory/shm、§2.2 `deadline_ms`、§9 补 5 条校验）、`docs/detail/01`（`advance(4)`、1 MiB 测试与旋钮）、`02`（无条件长度检查并改引 PROTOCOL §9、模块表 `error.rs`）、`03`（结构/配额/关闭/错误映射四节重写）、`04`（decode 移入闭包、宏命名空间、补服务端装配）、`00-index`（`INV-02`/`INV-10` 拆分、H3 补 `self: Arc<Self>`） |
| **v0.11** | **工程约定定稿，线格式零改动**。① **模块文件统一 2024 风格**：`foo.rs` + `foo/`，禁 `foo/mod.rs`。两种写法 rustc 都合法（`foo.rs` 与 `foo/mod.rs` 不能并存），官方自 1.30 起鼓励新写法，本项目为「入口文件与子目录并列、一眼可见」而统一采用；1.3 目录树按此重画（`transport.rs` / `proto.rs` / `proto/codec.rs` / `session.rs`）。② **`edition = "2024"` + `resolver = "3"`**，根 `Cargo.toml` 定为纯 virtual workspace（只有 `[workspace]`，不再同时挂 `[package]`，否则没有 target 会直接报 `no targets specified in the manifest`）。③ **成员名定死 `photon-macros`（复数）**，且它是 `proc-macro = true` 的 **lib** crate——proc-macro 不能是 bin，`src/main.rs` 的写法编译不过。④ **布局约束从四条变五条**，新增第 5 条即模块文件风格。**同步改动**：`docs/detail/01` §3 文件表、`docs/detail/02` §9 模块布局改为 `transport.rs` / `proto.rs` / `codec.rs`，并各加一段「为什么不是 `mod.rs`」。`PROTOCOL.md` 只更新版本指向，字节布局与 17 条校验一字未动。 |
| **v0.12** | **样例承载方式改为源码内联，线格式零改动**。① 七个黄金样例从 `tests/fixtures/*.bin` 改为 `photon/tests/protocol.rs` 里的内联字节数组（`assert_golden` 的 `expected` 参数），删除 `tests/fixtures/` 目录与 `include_bytes!` 这层路径管理。理由：`.bin` 在 code review 的 diff 里不可见，改坏一个字节没人看得出来；内联字面量逐行可审、可逐段注释字段名与宽度，冻结效果完全等价。② 布局约束第 4 条随之重写（原「`tests/fixtures/` 从 M0 就存在」→「样例内联在 `tests/protocol.rs`」），1.3 目录树删掉 fixtures 子目录。③ **`PROTOCOL.md` §9 第 12 条（帧长上限）在 M0 落地**：新增 `proto::MAX_MESSAGE_LEN = 64 KiB`，`parse` 在解码与切片之前先查长度（`check_len`），恰好等于上限放行、多 1 字节拒绝，两个用例都只差长度、字段全合法。④ 同步 `PROTOCOL.md` §8.8 表头与 §11 生成/冻结流程、`detail/02` §10 与测试矩阵。 |
| **v0.13** | **`Transport::bind` 改为异步，线格式零改动**。① 原同步签名与实现的真实前置条件不符：tokio 的 `TcpListener::from_std` 必须把 fd 注册进当前 runtime 的 driver，取不到 `Handle` 时 `Handle::current()` 直接 panic（`there is no reactor running`），而签名返回 `io::Result` 又让调用方以为失败一定可处理——错误类型错位。② 改为返回 `impl Future<Output = io::Result<Self>> + Send`：实现可直接用 `tokio::net::TcpListener::bind().await`，不再需要 `std::net::TcpListener::bind` + `set_nonblocking` + `from_std` 三步，也不需要运行时的 `try_current` 补丁——「必须被某个 executor poll」成为类型上的前置条件，漏掉是编译错误而不是构造期 panic。③ 三个发起动作（`bind` / `connect` / `accept`）由此统一在 async 世界，`detail/01` 的 `L1-C4` 改为「三者皆异步」。④ 门面代价口径修正：`AnyTransport::bind` 现在同样要一次 `Box::pin`，但一个进程只调一次，不在热路径。**同步改动**：`design.md` 2 节 trait 片段与 2.5 门面代价段、`detail/01` `L1-C4` / 模块表 / §6.1 / §7.2、`detail/04` `Server::bind` 签名与装配清单、`detail/00` 版本指向、`PROTOCOL.md` 版本指向（字节布局与 17 条校验一字未动）。 |
| **v0.14** | **`memory` 落地，`Memory::pair` 去掉 `opts`，线格式零改动**。① 产出 `transport/memory.rs`：`pair()` 造两条单向 `mpsc`（容量 `DEFAULT_QUEUE_CAPACITY = 1024`，实现常量不是协议约定）；读半把通道 `None` 映射 `UnexpectedEof`（返回空消息会让 L2 误诊成「头不完整」），读侧超上限报 `InvalidData`，写侧超上限报 `InvalidInput` 且连接保持可用。② **不实现 `Transport`**：`bind` / `connect` / `accept` 对「已经接好的一对 channel」没有对应动作，造一个按名字查找的注册表等于给它加一套不该有的寻址。③ `pair(opts)` → `pair()`：`TransportOptions` 的 `connect_timeout` 与 `spin_hint` 对进程内配对都无意义，收下不用就是坏参数。④ 新增 `tests/transport.rs`：以 `Harness` 只抽象「怎么接上一对半身」，6 条用例体只写一份，`tcp` 与 `memory` 各跑一遍（12 条全绿）——这是 M0 对 L1 抽象的唯一客观证据。**同步改动**：`design.md` 2.7 与 10.5、`detail/01` §3 目录树与 §6.3、`detail/00` 与 `PROTOCOL.md` 版本指向（字节布局与 17 条校验一字未动）。 |
| **v0.16** | **M1 落地：`Session` 骨架 + 错误体，线格式零改动**。① 产出 `session.rs`：`pending` 表（`PendingEntry` 带 `Drop` guard，超时/取消/panic 三条路径都回收配额）+ reader/writer 两个 task（`Source`/`Sink` 都是 `&mut self` 且非 `Sync`，拆开之后「谁来保证不并发写」在结构上不存在）+ 首帧强制 `Handshake` + `SessionOptions{max_message_len, close_timeout, handshake_timeout, max_async_in_flight}`。② 产出 `proto/error_body.rs`：8B 前缀编解码与 `to_wire`；**签名对齐实现**：`to_wire(&RpcError) -> Option<(u32, bool, String)>`，`Io`/`Protocol`/`RuntimeDropped` 返回 `None` 由调用方按 `0` 兜底（不静默丢帧），`Closed` 映射 `7`（回包侧说「正在关闭」是准确陈述）。③ `proto::parse` 拆出 `parse_with_limit`：帧长上限来自握手且两方向可以不同，写死 64 KiB 会让「对端声明 1 MiB」永远收不到大帧（真 bug，测试抓到）。④ handler panic 兜底：外层 task `await` 里层 `JoinHandle`，`JoinError` → `Remote{0}` + panic 文本；代价是每请求多一次 `spawn`（换 `catch_unwind` 要引入 `futures`，留待压测后再议）。⑤ `memory` 零拷贝形状验证：`Bytes::from_owner` 收发后 `as_ptr` 不变。⑥ `tests/session.rs` 改成「用例体一份 + `Harness` 两种实现」，8 条用例在 `memory` 与 `local` 各跑一遍（16 条）——M1 的「换 transport 不改上层」由此变成可执行断言。⑦ `limit_knob` 套 10 s 超时：卡住要变失败，而不是让整个测试二进制挂着。**同步改动**：`detail/02`（`parse` 双入口、`to_wire` 签名与 code 表、模块表）、`detail/03`（§6.2 的 spawn/JoinError 口径、§10 验收表补 3 行并纠正首帧窗口）、`detail/00` 与 `PROTOCOL.md` 版本指向、`design.md` M1 清单与 M2 错误体项勾选。 |

| **v0.17** | **M2 第一步落地：codec + 宏 + dispatch，线格式零改动**。① **codec**：`Codec` 的方法改成**不带 `&self` 的关联函数**（实现无状态，`codec::Default` 是类型而不是值），于是宏与业务代码可以照文档直接写 `Default::decode(&payload)`；`postcard` 用 `to_extend` + 自定义 `Extend<u8>` 适配器直接写进 `BufMut`，不再摊一次临时 `Vec`；编码失败与解码失败都落 `InvalidInput`（线上 `4`），不因参数错断连。② **宏**：`#[photon::service(id = N)]` 要求 `id` 显式（自动编号会跨 crate 撞号），`blocking` 参数在 M2 直接报编译错误而不是生成半成品；方法级 `#[photon(...)]`、同步 `fn`、非 `&self` 接收者、非 `Result` 返回、参数写成模式解构，全部在展开期报错。③ **`async fn` 与 `Send` 的死结**：trait 里的原生 `async fn` 不保证 future 是 `Send`，而 dispatch 的 `BoxFuture` 要求 `Send`——宏把签名改写成 `fn(...) -> impl Future<Output = R> + Send + '_`（RPITIT），实现侧照旧写 `async fn`，`Send` 由编译器逐 impl 验证。④ **dispatch 只能落在本地类型上**：`impl<S: Calc> Service for S` 在用户 crate 里过不了孤儿规则（`Service` 来自 photon，`S` 是裸类型参数，`E0210`），所以生成 `CalcService<S>` 包一层 `Arc<S>` 再实现 `Service`；副作用是**同一实现类型可以同时实现多个服务 trait**，v0.9 的「一类型一服务」限制随之消失。⑤ **`SERVICE_ID` 只放在能被命名的地方**：trait 里带默认值的关联常量无法写成 `Calc::SERVICE_ID`（`E0790`），因此只在 `CalcClient::SERVICE_ID` 与 `mod __photon_calc::SERVICE_ID` 暴露，两处都取同一个字面量。⑥ **验收**：`tests/service_macro.rs` 8 条（dispatch 往返、用户码 `0x10001` 透传、未知方法 `Remote{3}`、截断 payload → `InvalidInput`、注册与撞号、方法名表与 golden 哈希）；`tests/compile-fail/` 3 条（同步 `fn`、缺 `id`、`blocking` 参数）用 `trybuild` 冻结报错文本；`photon-macros` 3 条（`xxh3("add")` 冻结向量、命名敏感性、snake_case）。全量 39 + 9 + 8 + 1 + 16 + 21 + 3 全绿，`fmt --check` 与 `clippy --all-targets --features local` 干净。**同步改动**：`detail/02`（§6 trait 签名改为无 `self`、§9 模块表把 codec 移到 `src/codec.rs`）、`detail/04`（§4 产物表改为 `CalcService<S>` 并补孤儿规则与 `Send` 改写两段、§5 dispatch 样例对齐实现、§9 编译期检查补两行）、`detail/00` 与 `PROTOCOL.md` 版本指向（字节布局与 17 条校验一字未动）。

| **v0.18** | **M2 收尾：服务端装配、客户端连接、示例与并发验收，线格式零改动**。① **`Server<T>`**：`bind(ep, transport_opts, session_opts)` → `register`（撞号 `Err` 不覆盖）→ `serve` / `serve_with`。注册表**按值**持有、`serve` 时才包 `Arc`：把「运行中改注册表」从类型上排除，比事后加锁便宜；`registry()` 交出 `&mut ServiceRegistry` 让宏生成的 `register_calc(server.registry(), svc)` 直接可用，`transport()` 交出 listener（端口写 `0` 的测试靠它拿真地址）。accept 循环里连接级错误（`ConnectionAborted` / `ConnectionReset` / `Interrupted` / `WouldBlock`）记一行继续，listener 级错误才返回 `Err`——`while let Ok(..)` 会把 listener 死亡静默吞掉。② **`Session::wait_closed()`**：`closed: Arc<Notify>` + 进 `Closed` 的唯一写点；`serve` 循环靠它知道「这条连接不用再持有」。**先建 waiter 再看状态**，反序会丢通知并留下永久等待。③ **客户端拆成两步**：`connect::<T>(ep, ConnectOptions)` 拿 `SessionHandle`，`CalcClient::new(handle)` 包一层。原因是 `CalcClient` 只泛型化到半身 `CalcClient<S, K>`——`Memory::pair` 不实现 `Transport`，若 stub 绑在 `Transport` 上，`local_calc` 根本用不了同一份 stub；`connect` 本身还要等握手到 `Ready`（`codec_id`/版本不符在 connect 上就报错，而不是第一次业务调用超时）。④ **示例**：`examples/calc_service/mod.rs`（共用 `Calc` 服务，两个 example `#[path]` 各引一次）、`local_calc`（memory pair，四条输出与 10.5 逐字一致）、`calc_server`/`calc_client`（TCP，客户端 50 ms 间隔重试、2 s 预算）。⑤ **`tests/concurrency.rs`**：32 并发 + 100 乱序，同一套断言在 `memory` 与 `tcp` 上各跑一遍，比对「每个请求拿到的是不是自己的值」，整体套 10 s 超时（卡住必须变失败）。变异验证：把 dispatch 的已注册分支改成不可达 → 两条用例同时红。⑥ 全量 39 + 1 + 2 + 9 + 8 + 16 + 21 + 3 全绿。**同步改动**：`detail/03`（`wait_closed` / `set_closed` 的写法与原因）、`detail/04`（§4 stub 改为半身泛型、§10 三小节按实现重写）、`detail/00` 与 `PROTOCOL.md` 版本指向（字节布局与 17 条校验一字未动）。
| **v0.19** | **M3 第一步：三档声明形态与调度分流，线格式零改动**。① **宏**：`async fn` / `fn` / `#[photon(blocking)] fn` 三档都接受，形态判定在展开期写死（`MethodKind::{Async, Sync, Blocking}`），`rewrite_sigs` 只改 `async fn`（同步 `fn` 本来就没有 future 可改写）；方法级 `#[photon(...)]` 展开后从 trait 上摘掉——留着 rustc 会去解析一个不存在的属性并报错。② **dispatch 三分支**：`async` 分支 await；`fn` 分支包进 async 块首 poll 就地跑完（不换线程）；`blocking` 分支 `spawn_blocking`，**decode 搬进闭包**（它也是 CPU 工作），`JoinError` 映射 `Internal`（线上按 `0` 兜底）。③ **`Service::schedule(method) -> Schedule`**：额度必须在「进 handler」之前扣，那时只有 `service_id + method`，还看不到实现内部分支——所以调度类别由生成代码交出来（默认 `Async`，只有 `blocking` 方法生成 `Blocking`；没有 blocking 方法时干脆不生成，用 trait 默认实现）。④ **入站配额拆两张**：`InboundQuota{async_used, blocking_used}` + `try_acquire(Slot)` 一次持锁完成「检查 + 计数」，`InboundGuard` 按 slot 归还；阻塞池满时异步请求照跑。⑤ **debug 慢 handler 告警**：超过 100 µs 打 `tracing::warn` 提示加 `#[photon(blocking)]`；顺手把 handler panic 也改成 `tracing::error!`（原来只回包不记日志，服务端看起来「什么都没发生」）。⑥ **验收**：`tests/service_macro.rs` 12 条（同步就地执行、blocking 走线程池、blocking panic 不挂住、`schedule` 逐方法正确）、`tests/session.rs` 18 条（新增「阻塞额度只有 1 个 → 一次成功两次过载、异步不受影响」，`memory`/`local` 各一遍）、`tests/compile-fail/` 换成 `blocking_on_async` / `unknown_method_attr`（`sync_method` 已合法，删除）。变异验证：把 `Schedule::Blocking` 也映射到 `Slot::Async` → 两条配额用例同时红。全量 39 + 1 + 2 + 9 + 12 + 18 + 21 + 3 全绿。**同步改动**：`detail/04`（§3 属性表标未实现、§4 产物补 `schedule`、§5 三分支与 `schedule`、§9 检查表与 trybuild 清单）、`detail/03`（§6.2 补 slot 选择与阻塞分支的 `JoinError`、§6.3 补慢 handler 告警）、`design.md` 5.1 / 5.3 片段、`detail/00` 与 `PROTOCOL.md` 版本指向（字节布局与 17 条校验一字未动）。
| **v0.26** | **M3 第八步：`PayloadMode` 接口面与 `zero_copy` 护栏，线格式零改动**。① **类型先落**：`photon::PayloadMode { Copy（默认）, ZeroCopy }`，`Service::payload_mode(method) -> PayloadMode` 默认 `Copy`——放在 `schedule` 旁边，因为两者都是「进 handler 之前就得知道、且只有宏知道」的编译期属性。② **只放行同步 `fn`**：宏现在接受 `#[photon(zero_copy)]`，但 `async fn` 与 `#[photon(blocking)] fn` 都在展开期报错（两条 `.stderr` 冻结进 trybuild）。理由沿用 4.5：跨 `await` 的 handler 会让 shm 槽位占用不可预期；blocking 方法虽然一次性跑完，但时长由用户计算量决定，同样不适合。③ **全局开关延后**：删掉「`SessionOptions::payload_mode` 是全局默认」这一段——v1 的 payload 全是 owned + `'static`，全局开关没有任何可依附的行为，收下不用就是坏参数（与 `Memory::pair` 去掉 `opts` 同一条理由）。方法级 opt-in 与类型先落，M5 加全局开关时用户代码不用改，因为属性已经存在。④ **验收**：`tests/service_macro.rs` 新增 `payload_mode_reflects_method_annotation`（标注方法 `ZeroCopy`、未标注 `Copy`、未知 method 回落 `Copy`）；trybuild 5 条（`unknown_method_attr` 改用真正未支持的 `cache`、新增 `zero_copy_on_async` / `zero_copy_on_blocking`）。变异：删掉生成的 `payload_mode` impl → 标注方法回落 `Copy`，用例红。全量 39 + 1 + 2 + 9 + 17 + 36 + 21 + 3 全绿，`fmt` / `clippy` 干净。⑤ 全篇按现状改写：4.5 的开关段落、5.x 的握手「`payload_mode` 不该进握手」段落仍成立（它讲的是「为什么不在握手里」，与本步无关）。**同步改动**：`detail/04`（§3.2 属性表、§9 编译期检查补两条、新增 §5.2 说明「两种取值在 v1 都是 no-op」）、`detail/00`（`INV-27`）、`PROTOCOL.md` 版本指向。 |
| **v0.25** | **M3 第七步：业务错误映射，线格式零改动**。① **契约只有一个 impl**：新增 `photon::ServiceError { fn into_error(self) -> RpcError; fn from_error(err: RpcError) -> Self }`，服务级写 `#[photon::service(id = N, error = E)]` 后方法返回 `Result<T, E>`，dispatch 在 handler 返回处 `into_error`、stub 在 `call`/`encode`/`decode` 三处 `from_error`。**为什么不是 `impl From<E> for RpcError` + `impl From<RpcError> for E`**：两个 `From` 分散在两个方向，只写一半时编译器报的是「某处 `?` 无法转换」，而这里一行 impl 就交代了两侧语义（尤其 `from_error` 必须显式决定「本端 `Timeout` 怎么收敛」）。② **debug 断言业务码段**：`assert_business_error` 在 debug 构建里要求 `into_error` 结果必须是 `Remote { code >= 0x1_0000 }`——用户把「余额不足」写成 `code 3` 时，客户端看到的是「方法不存在」这种假象，而这类错误一旦上线极难回溯。release 下整个调用编译掉。③ **展开期校验错误类型**：`error = E` 声明后方法返回别的东西、或没声明却返回非 `RpcError`，都在展开期报错（`error_type_mismatch` 冻结进 trybuild）。④ **验收**：`tests/service_macro.rs` 新增 3 条——dispatch 直调拿 `Remote{0x2_0001}`、整条会话上 `withdraw(200)` 回到 `BizError::Code` 且关闭后回到 `Transport(Closed)`（证明本端错误也经 `from_error`）、`code 3` 在 debug 下 panic；变异：删掉 `assert_business_error` 调用 → 第三条红。全量 39 + 1 + 2 + 9 + 16 + 36 + 21 + 3 全绿，`fmt` / `clippy` 干净。⑤ `local_calc` 仍用 `RpcError`（七形态聚焦在传输与会话，不混业务错误展示）；`design.md` 3.8 末段、5.2 属性表按实现重写。**同步改动**：`detail/04` 补 `ServiceError` 展开块与三处 `from_error` 位置、`detail/03` 错误表补一行「用户码段由 handler 的 `into_error` 产出」、`detail/00`（`INV-26`）、`PROTOCOL.md` 版本指向。 |
| **v0.24** | **M3 第六步：示例补齐七种形态，新增 stub 的 per-client 期限**。① **`with_deadline`**：`CalcClient` 多一个 `deadline_ms: u32` 字段（`new` 置 0），`with_deadline(self, ms)` 换值返回新客户端、`deadline_ms()` 读当前值；生成的每个方法把 `self.deadline_ms` 填进 `handle.call(..)` 的第 4 个参数。**为什么不是 `&mut self` 或全局**：期限是调用点的选择，挂在 `&self` 的可变状态上会让并发调用互相影响；而「一条连接上的所有请求共用一个期限」在本框架里是常态（重试与预算都在上层）。这也补上一个真实的 API 缺口——v0.20 落了 `deadline_ms` 的线格式与 `CallGuard`，但类型化 stub 一直硬传 `0`，用户想要超时只能绕到 `SessionHandle::call` 自己 encode。② **`calc_service` 加三个方法**：`ping`（通知目标，无参 → 空 payload）、`slow_op(ms)`（`sleep` 后返回）、`fast_hash(data)`（`fn` 声明，FNV-1a，就地跑完）。③ **`local_calc` 八行**：M2 四行不变，M3 四行是「`notify ping (no reply)`」「`slow_op -> Err(Timeout after 50ms)`」「`fast_hash(data) = 0x40b1d64aff3aac17`」「`shutdown (in-flight drained)`」。优雅收尾那段刻意先 spawn 一个 `slow_op(30)`、睡 5 ms 再 `server.close()`，因此 `close()` 返回时 handler 必已清空，回包也在其之前进了 writer——这是 4.6 的「在飞」定义在示例里的可观测形态。④ **两个不能当测试的点**写进注释：超时行里客户端与服务端期限同为 50 ms（与 `tests/session.rs` 那条「必须用 0 才测得到 `Cancel`」是同一类坑的反面），以及 `fast_hash` 在 reader task 上同步执行、必须足够短。⑤ **`tests/service_macro.rs` 新增 `client_deadline_reaches_header`**：同一个 `nap(300)` 在期限 0 下拿到 `Ok(300)`、在 `with_deadline(30)` 下 300 ms 之前就拿到 `Timeout`。变异验证一条：把生成代码里的 `self.deadline_ms` 改回 `0` → 该用例红（拿到 `Ok(300)`）。全量 39 + 1 + 2 + 9 + 13 + 36 + 21 + 3 全绿，`fmt` / `clippy` 干净。**同步改动**：5.5 补 `with_deadline` 用法、10.1 表标「v0.24 已落地」、10.5 的 M3 段换成实跑八行、M3 清单勾「七种形态」、`detail/04` stub 代码块补字段与新方法、`detail/00` 与 `PROTOCOL.md` 版本指向。线格式零改动。 |
| **v0.23** | **M3 第五步：`calc_local` 两进程 UDS**。① 一个二进制两个角色（`server` / `client`，默认 `/tmp/photon-calc.sock`），省掉第三个 example 文件；`Cargo.toml` 的 `[[example]]` 用 `required-features = ["local", "codec-postcard"]`，默认 feature 集不会试着编译它（Windows 上 `local` 本就是 `compile_error!`）。② 业务代码与 `calc_client` 同构：`connect::<LocalTransport>` + `CalcClient::new`，客户端侧三行输出一致——这就是「换 transport 不改业务代码」的证据，而不是一句口号。③ 顺带把本地 IPC 最容易踩的边角写进示例注释并在真机上验过：`bind` 遇到**上次崩溃留下的 socket 文件**会删掉重建（示例跑了两轮，第二轮正常起来）；遇到**还活着的监听者**会拒绝覆盖并给出「已有活跃监听者」——前面那次 `pkill` 只杀了 `cargo run` 包装进程、真正的二进制还在听，第二次 bind 正是被这条规则挡住的。这两句行为写在这里，是因为用户第一次撞上时看到的只会是一句 `EADDRINUSE` 级别的天书。④ 运行方式进 `10.4`，预期输出进 `10.5`，`10.1` 表标「v0.23 已落地」。**同步改动**：`detail/04` 示例表同标；线格式零改动。
| **v0.22** | **M3 第四步：握手协商补验收（代码零改动）**。① 此前只有 `peer_declared_limit_governs_sending`（服务端声明 1 MiB → 客户端能发 200 KiB），证明的是「writer 用对端声明」；新增 `own_declared_limit_governs_recv`（客户端声明 1 MiB → 收得下服务端 200 KiB 回包），证明「reader 用本端声明」。两条合起来才是「两个方向各用各的」，缺一条都只证明了一半。② 新增 `handshake_rejects_bad_fields`：codec 不等 / 版本不等 / `max_message_len = 20` 三种都要在**建连期**断连（300 ms 内 `is_closed`），不能拖到第一次业务调用。第三条不能走 `Handshake::encode`——它对 `< 24` 有 `debug_assert`（本端的编程错误），对端的坏值只能用 `raw_handshake_frame` 手写字节造出来；这个区分本身也值得记下来：`debug_assert` 拦的是「我方发错」，不是「对方发错」。③ 变异验证：删掉 `limits.0` 的写入 → 接收方向用例双传输红；删掉 `check_against` → 拒绝用例双传输红。**同步改动**：`detail/03`（2.3 补「两条证据」与 `debug_assert` 的分工）、`detail/00`（`INV-23` 上限方向不混用）、`PROTOCOL.md` 版本指向；字节布局零改动。
| **v0.21** | **M3 第三步：`GoAway` 双向收尾与急停落地**。① `close()` 顺序钉死：**先关 `egress` 再发帧**（反过来会留一个「发得出去但没人等」的窗口），发帧由 `goaway_sent` 去重，即便本端已收到对端宣告也照发——否则对端永远看不到「双方都宣告过」这个条件；随后按 `close_timeout` 等在飞双零（`pending` + `handlers`）。② **收到对端 `GoAway` 不再等 EOF**：起一个 `spawn_finish_when_drained` 后台任务，排空即 `shutdown.cancel()`，`close_timeout` 兜底。这条对没有半关闭的实现（named pipe / shm）是唯一收敛路径，socket 上的 EOF 只是加速。③ **`ingress` 只认「双方都宣告过」**：新增 `ingress_draining`，置位后新到 `Request` 回 `Remote{7}`（`send_error(...,7,true,...)`），此前一律照常 dispatch——早拒会把对端合法在飞请求打死。④ **`Session::abort()`** 发 bit2 急停：`try_send` 帧、取消所有 handler token、置 `abrupt` 后 `fail()`；writer 依 `abrupt` 决定排空还是丢弃，丢弃条数进日志。⑤ **验收** 4 条 × 2 实现（`session` 32 条）：「优雅关闭等在飞回完」、**「急停后 3 s 的 handler 也在 1 s 内结束」**、「双方宣告后迟到 `Request` 收到 `Remote{7}`」（手写假对端才能造出这段窗口）、**「收到 bit2 宣告时正在跑的 handler 必须被 drop」**（`pending()` + Drop 计数，优雅宣告下必然红）。变异验证三条全部双传输变红：去掉 `dispatch` 的 Draining 检查 / 去掉 `close()` 的等待 / 去掉急停分支的 token 取消 + `fail()`。**同步改动**：`detail/03`（`ingress_draining` 字段与 7.2/7.3/7.4 三处、6.4 急停实现口径）、`detail/04`（`Session::abort` 与「为什么不上句柄」）、`detail/00`（`INV-22` 急停必须真正丢活）、`PROTOCOL.md` 版本指向；字节布局零改动。
| **v0.20** | **M3 第二步：超时 / 取消 / 通知，线格式零改动**。① **放弃即取消**：`Session::call` 里插入 pending 之后立刻挂一个 `CallGuard`，超时或 future 被 drop（`abort` / 上层 `select!` 抢先）时它摘表并归还配额；`PendingEntry::drop` 在「结果从未送达」时用 `try_send` 发 `Cancel`（`send` 会卡在背压上，为一条「别算了」阻塞放弃路径比不发更糟）。② **`Notification` 走完整 dispatch**：与 `Request` 共用一个 `dispatch(header, payload, want_reply)`，差别只有不回包；未注册服务、额度不足一律只记日志——给通知回 `Response` 是协议噪声（对端没有对应 pending）。③ **`SessionHandle::notify`** 补齐发起入口（不插 pending、不占出站配额、无超时）。④ **验收**：`tests/session.rs` 新增 4 条 × 2 实现——「超时准时暴露且会话仍可用」、**「丢 future 必须发 Cancel」**（刻意用 `deadline_ms = 0`，否则服务端自己会在同一时刻超时把 handler 丢掉，等于什么都没测）、「通知进 handler 且连发多条都到」、「通知指向未注册服务不断连」。变异验证两条：去掉 `Cancel` 的 `try_send` → 放弃用例红（2 s 后断言失败）；去掉 `CallGuard` → 同样红。⑤ 顺带记一句教训：最初那版「超时后 handler 被取消」用例是**假的**，它的断言被服务端同刻的 deadline 满足，与 `Cancel` 无关——已拆成两条并把隔离条件写进注释。全量 39 + 1 + 2 + 9 + 12 + 24 + 21 + 3 全绿。**同步改动**：`detail/03`（6.2 的 `want_reply`、6.3 的超时/放弃/Cancel 三行与分工说明、4.1 的通知语义）、`detail/04`（10.3 补 `SessionHandle` 三个入口与 `notify` 的代价）、`detail/00` 与 `PROTOCOL.md` 版本指向（字节布局与 17 条校验一字未动）。
| **v0.15** | **`local` 落地 + drop 契约改判**。① 产出 `transport/local.rs`：Unix 上手搓 `tokio::net::UnixStream`（零新依赖，约 240 行含测试），`OwnedWriteHalf::drop` 免费拿到 `SHUT_WR`；`Endpoint::Local(LocalAddr)` + `local` feature（不进默认集）；Windows 开启该 feature 直接 `compile_error!`（named pipe 无半关闭、server 需 per-connection 新建，没有测试环境不写）。② **把 2.1 的 drop 契约从「必须尽力半关闭写方向（验收项）」改判为「drop 后本端不再写」**，对端能否观测 EOF 降级为**实现能力**，正确性改由 `GoAway` + `close_timeout` 兜底——原写法需要给 named pipe 开例外条款，而需要例外才成立的契约不是契约。③ `tests/transport.rs` 的 `Harness` 增加 `const PEER_SEES_EOF: bool` 能力位，EOF 用例改为「按声明断言」，同一份用例体覆盖 3 个实现（21 条）。④ 顺带修一处测试自身的缺陷：`limit_knob` 先写 500 KiB 再读，在 UDS 缓冲区上会自堵（tcp 的缓冲够大所以没暴露），改为 `join!` 并发推进。 |

---

## 0. 目标与非目标

### 目标

| 维度 | 具体要求 |
|---|---|
| 小巧 | **两个 crate**（`photon` + `photon-macros`，proc-macro 硬约束），主 crate 内按 L1–L4 分层；**默认 feature 下直接依赖 ≤ 8 个**（口径见下）；不引入 HTTP/2、TLS、gRPC 等重型栈（黑名单见下） |
| 高效 | 单连接多路复用；头部定长手工编解码；`Bytes` 贯穿全链路，为共享内存零拷贝预留 |
| 可扩展 | Transport / Codec / Interceptor 三个 trait 边界，另有 1 个预留的调度插槽；本地调用与远程调用共用同一套 `Service` 抽象；**L1 可替换为共享内存而不改 L2 以上任何代码** |
| 类型安全 | proc-macro 从 trait 定义生成 client stub 与 server dispatch，参数错误在编译期暴露 |

#### 「小巧」的可验证口径

「依赖不超过 8 个」如果不说清怎么数，就是个不可证伪的魔数。三个限定：

- 只算 **直接依赖**（`Cargo.toml` 里显式列出的），不算传递依赖——后者由 tokio / serde 决定，我们控制不了
- 只算 **默认 feature**：`shared_memory` + `crossbeam-utils`（`shm`）、`criterion`（dev）、`tracing-subscriber`（示例）全部不计；`local` 手搓 `tokio::net::UnixStream`，**不引入新依赖**
- `syn` / `quote` / `proc-macro2` 住在 `photon-macros`，不计入主 crate

当前默认集正好 8 个：`tokio` / `bytes` / `serde` / `postcard` / `thiserror` / `tracing` / `tokio-util` / `parking_lot`。

**这个数字的用途是警报线，不是自我约束。** 零余量是刻意的：一旦默认集需要第 9 个直接依赖，就说明有人在往核心里塞不该有的东西，应当先问「能不能做成可选 feature」，而不是顺手加进去。

已知的两处潜在冲突，提前记账：

| 阶段 | 可能新增的直接依赖 | 处理方式 |
|---|---|---|
| M3 | `tower`（Interceptor 插槽想复用 `tower::Service` / `Layer`） | 做成可选 feature `interceptor`，不进默认集 |
| M5 | `crossbeam-utils`（`CachePadded`，仅 `shm` 需要） | 已锁在 `shm` feature 内，不破线 |

**MSRV：1.85。** 这是 `edition = "2024"` 的下限，也是 `impl Trait` in trait、`Box`/`Arc` 接收者等本设计用到的特性都能满足的最低版本。文档里说「Rust 1.75 之后有 AFIT」只是说明不必用 `async-trait`，不代表 MSRV 是 1.75——**两件事不要混**。CI 固定跑 1.85 与当前 stable 两档。

#### 「重型栈」的黑名单

比数依赖更有效的判据是：**这个依赖会不会把编译时间、二进制体积或平台支持面显著拖坏**。明确不引入：

- `hyper` / `h2`（HTTP/1.1、HTTP/2 栈）
- `rustls` / `ring` / `openssl`（TLS）
- `tonic` / `prost` 全家桶（gRPC）
- 任何异步文件系统或信号处理运行时（本框架不需要）

反过来，`tower` 核心（只有 `Service` / `Layer` 两个 trait，无重型依赖）**允许**，但按上表走可选 feature。

> 补充：依赖个数是**可维护性**指标，不是「小巧」的可感知指标。用户能感知的是**二进制体积**和**编译时间**。建议从 M4 起把「release + strip 后的 `local_calc` 示例二进制 ≤ 2 MB」作为一条实测门槛——**2 MB 是首版估值（对标 tokio+serde+postcard 的典型体积），M4 实测后按真实基线校准**。它比数依赖更能反映有没有偷偷引入重型栈。

### 非目标（当前阶段明确不做）

- 不做跨语言互通（但 Codec 抽象为将来接 protobuf 留口子）
- 不做 HTTP/2 兼容（不做 gRPC 竞品）
- 不做 TLS / 鉴权体系（先假设进程在同一信任域；鉴权若要，通过 `Interceptor` 插槽由用户自己接，框架不内建）
- 不做服务发现、负载均衡、注册中心
- 不做 io_uring（`tokio-uring` 生态停滞；且队列深度为 1 时 io_uring 反而比 epoll 慢，见附录 A）
- **不在 M5 之前实现共享内存传输**（M4 只做性能基准，不做 shm；但 M0 的接口必须为它预留，见附录 B）

---

## 1. 总体架构

### 1.1 四层切分

```
┌──────────────────────────────────────────────────────────┐
│  L4  类型安全 API                                         │
│      #[photon::service(id = 7)] -> client stub + dispatch │
├──────────────────────────────────────────────────────────┤
│  L3  Session（会话层）                                    │
│      多路复用 / request_id 表 / 取消 / 超时 / 优雅关闭     │
├──────────────────────────────────────────────────────────┤
│  L2  Codec（协议层）                                      │
│      24 字节语义头 ↔ 结构；payload 的 serde 序列化         │
├──────────────────────────────────────────────────────────┤
│  L1  Transport（消息通道层）                              │
│      MessageSource / MessageSink                          │
│      TCP / 本地 socket / 共享内存环 / 进程内 channel       │
└──────────────────────────────────────────────────────────┘
```

### 1.2 三条架构铁律

> **① L2 以上不认识 `AsyncRead` / `AsyncWrite` / 任何 fd 概念。**
>
> **② L1 以下不认识 `request_id` / `method` / 任何协议语义。**
>
> **③ L1 交出的永远是「一条完整消息」，且载体固定为 `Bytes`。**

前两条是分层纪律，第三条是**为共享内存预留的核心契约**，理由见附录 B：只有「完整消息 + `Bytes` 载体」这个组合，才能同时满足三种互相冲突的需求——流式 socket 的帧拼装、共享内存的零拷贝借用、以及 dispatch 时消息可以 `spawn` 到独立 task（要求 `Send + 'static`）。

`Bytes` 之所以能承载共享内存的零拷贝切片，是因为 `bytes ≥ 1.9`（建议 **≥ 1.10.1**）提供 `Bytes::from_owner`：只要传入的 owner 实现 `AsRef<[u8]> + Send + 'static`，`Bytes` 就会持有 owner、切片直接指向 owner 的字节区，`Bytes` 被 drop 时 owner 随之 drop（正好用来把 chunk 归还给共享内存池）。

两个版本细节，写在这里免得以后再查：`from_owner` 是 **1.9.0（2024）** 引入的，不是早期版本；**1.10.1（2025-03）修掉了它与 `to_vec` 组合时的内存泄漏**，所以下限取 1.10.1 更稳。owner 必须 `Send`——否则 `Bytes` 就不再是 `Send`，而「消息必须 `Send + 'static`」是铁律③的一部分。

### 1.3 模块布局：最小双 crate workspace

v0.2 的这节原本写着「先不拆 workspace，单 crate + module 分层」，但树里又把 `macros` 画成 `src/` 下的子目录、标注「必须独立 crate」——**这两句是矛盾的**。Rust 不允许 proc-macro 与普通代码共存于同一个 crate，所以只要用 `#[photon::service]`，就至少是两个 `[package]`。

修正后的原则：**只做硬约束要求的拆分，不做架构性的拆包。**

- **必要**：`photon` + `photon-macros`（proc-macro 的硬约束，无法规避）
- **暂不做**：`photon-core` / `photon-transport` / `photon-codec` / `photon-rt` 这类按层拆包的方案（个人项目过早拆，每次改签名都要跨 crate 改版本，收益接近零）

```
photon/                          # workspace 根
├── Cargo.toml                   # [workspace] members = ["photon", "photon-macros"]，resolver = "3"
├── docs/
│   ├── design.md
│   ├── PROTOCOL.md              # 线格式权威定义 + hex fixture 说明（见 13 节）
│   └── detail/                  # 分层详细设计：把本文件按 L1/L2/L3/L4 展开成实现细则
│       ├── 00-index.md          # 索引、全局不变量、跨层交接契约
│       ├── 01-l1-transport.md
│       ├── 02-l2-protocol.md
│       ├── 03-l3-session.md
│       └── 04-l4-api.md
├── photon/                      # 主 crate，对外名字就是 photon
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs               # 门面：re-export 公共 API
│   │   ├── error.rs             # RpcError 统一错误类型（含 Remote，见 3.8）
│   │   ├── transport.rs         # L1 入口：MessageSource / MessageSink / Transport
│   │   ├── transport/
│   │   │   ├── framing.rs       # 流式 transport 的长度前缀拼装（L1 内部）
│   │   │   ├── tcp.rs           # feature = "tcp"（默认）
│   │   │   ├── local.rs         # feature = "local"
│   │   │   ├── memory.rs        # feature = "memory"（默认，接口验收用）
│   │   │   ├── shm.rs           # feature = "shm"（M5，附录 B）
│   │   │   └── any.rs           # AnySource / AnySink 类型擦除门面
│   │   ├── proto.rs             # L2 入口：kind / flags 常量、Header 视图与校验入口
│   │   ├── proto/
│   │   │   ├── header.rs        # 24 字节语义头 + 手工编解码
│   │   │   ├── error_body.rs    # 错误体 8B 前缀 + code 表（见 3.8）
│   │   │   └── handshake.rs     # Handshake 12B 编解码；GoAway / Cancel 的 payload 恒空
│   │   ├── codec.rs             # Codec trait（含 const ID）+ codec::Default 别名（L2 的能力，独立于 proto）
│   │   ├── codec/
│   │   │   ├── postcard.rs      # 默认实现，ID = 0
│   │   │   └── rkyv.rs          # feature = "codec-rkyv"，M5，ID = 1
│   │   ├── session.rs           # L3 入口：Session（pending map + 读写任务 + Closing 状态）
│   │   ├── session/
│   │   │   ├── client.rs        # 异步客户端
│   │   │   ├── blocking.rs      # feature = "blocking"：同步客户端（见 5.5）
│   │   │   ├── server.rs
│   │   │   └── dispatch.rs      # method_id → handler；含 spawn / spawn_blocking 分流
│   │   └── router.rs            # ServiceRegistry
│   ├── examples/                # 可直接 cargo run 的示例，见第 10 节
│   │   ├── echo.rs              # M0：裸字节往返
│   │   ├── local_calc.rs        # M2：单进程全链路（一条命令跑通）
│   │   ├── calc_server.rs       # M2：两进程，服务端
│   │   ├── calc_client.rs       # M2：两进程，客户端
│   │   └── calc_local.rs        # M3：两进程本地 IPC（架构验收）
│   ├── tests/                   # 跨 transport 的一致性测试
│   │   ├── transport.rs         # M0：不依赖 Session，直接测 L1 收发与帧边界
│   │   ├── protocol.rs          # M0：24B 头 + 逐 kind 校验 + 帧长上限 + §8 逐字节样例
│   │   ├── service_macro.rs     # M2：宏展开产物的行为验收（dispatch / 错误码 / 注册）
│   │   ├── compile-fail/        # M2：trybuild 冻结的编译期拒绝用例
│   │   ├── roundtrip.rs         # M1+
│   │   ├── concurrency.rs       # M1+
│   │   └── shutdown.rs          # M3：两个方向的 GoAway、JoinHandle 必须结束
│   └── benches/                 # M4
│       └── latency.rs
└── photon-macros/               # L4 代码生成，独立 package
    ├── Cargo.toml
    └── src/
        └── lib.rs               # #[photon::service(id = N)] 展开
```

五条布局约束：

1. **`examples/` 从 M0 就存在**，不是"以后再补"。没有能跑的示例，抽象就没有被验证过（见第 10 节）。
2. **`tests/` 按"跨 transport 一致性"组织**，不按"按模块"组织。同一个测试要在 `tcp` / `memory` / `local` / `shm` 上全绿——这是 transport 抽象成立的唯一客观证据。
3. **`benches/` 留到 M4**，提前建空目录只会变成噪音。
4. **黄金样例内联在 `tests/protocol.rs`，不设 `tests/fixtures/` 目录**（v0.12）。线格式是唯一不能靠「代码写得一致」保证的东西——它需要一组逐字节冻结的样例来锁死（见 13.2）。但这些样例以**源码内联的字节数组**形式存在，而不是 `.bin` 文件：二进制在 code review 的 diff 里不可见，改坏一个字节没人看得出来；内联字面量逐行可见、可逐段注释字段名与宽度，冻结效果完全等价（两者都是「运行时不再重新生成」）。代价只是测试文件长一些，比多一层文件路径管理划算。
5. **模块文件用 2024 风格：`foo.rs` + `foo/`，不用 `foo/mod.rs`**（v0.11）。两种写法在 rustc 里都合法，但官方从 1.30 起鼓励新写法（「more consistent, and avoids having many files named `mod.rs`」）。同一仓库只允许一种：`foo.rs` 与 `foo/mod.rs` 并存会直接报错（`foo` 的模块文件被定义两次），且混用会让「模块入口在哪」变成靠猜。`Cargo.toml` 的 `edition = "2024"` 与 `resolver = "3"` 也在同一轮定死。

> **`docs/detail/` 的分工**：本文件只保留「决策 + 论证 + 线格式速查」；逐层的实现细则（模块职责、数据结构、算法步骤、错误映射、测试矩阵）在 `docs/detail/` 里展开。那里的内容不构成新决策，与本文冲突时以本文为准；反过来，若实现过程中发现本文缺一条必须定死的约定，先改本文、再同步 detail。
>
> **版本对齐（v0.12）**：detail 五份（`00-index.md` + `01-l1-transport.md` / `02-l2-protocol.md` / `03-l3-session.md` / `04-l4-api.md`）与本文 v0.12 同步修订，行数为 112 / 344 / 312 / 415 / 323；`INV-01…19` 编号以 `00-index.md` 为准（`INV-05`、`INV-10` 已拆为 a/b/c），引用时写全编号，别只写 `INV-10`。

### 1.3.1 Feature 划分

| feature | 默认 | 内容 | 说明 |
|---|---|---|---|
| `tcp` | ✅ | `transport/tcp.rs` | 跨平台、零额外依赖，M0 的默认通道 |
| `memory` | ✅ | `transport/memory.rs` | 常开：接口验收与单元测试的基础 |
| `local` | ❌ | `transport/local.rs` | Unix 手搓 UDS（零新依赖）；Windows 未实现，开启即编译失败 |
| `shm` | ❌ | `transport/shm.rs` | M5；依赖 `shared_memory` + `crossbeam-utils` |
| `blocking` | ❌ | `session/blocking.rs` | 生成 `BlockingClient`，支持同步调用，见 5.5 |
| `interceptor` | ❌ | 接 `tower::Service` / `Layer` | M3 评估后再引入；它会把直接依赖推到 9 个，所以不进默认集（见 0 节） |
| `codec-postcard` | ✅ | `proto/codec.rs` | 默认 codec |
| `codec-rkyv` | ❌ | 零拷贝 codec | M5，与 `shm` 配套引入 |
| `full` | ❌ | **随里程碑长大**：M3 之前 = `local`；M3 起 = `local` + `blocking` + `interceptor` | 便于 CI 一次性覆盖。**不含 `shm` / `codec-rkyv`**——M5 之前 `--all-features` 必然编译失败，别把它当成 `--all-features` 的替身 |

> **`full` 不能写死成一个固定集合（v0.8 修）。** 它的成员里有 `blocking` / `interceptor`，而这两个的源码到 M3 才存在；而 CI 从 M0 就跑 `--features full`。写死成三项的结果是 M0–M2 的 CI 全红。两种解法：让 `full` 按里程碑演进（本文采用），或者让 CI 从 M3 才跑 `full`（M0–M2 只跑 `default` + `local`）。**不要在 M0 的 `Cargo.toml` 里提前声明一个空 feature 来糊弄**——空 feature 会让 `--features full` 假装成功，掩盖真实缺口。

**默认 feature 的选择原则**：默认集必须能在 Windows 与 Linux 上**零额外系统依赖**跑通，且**不引入需要用户主动防范的失败模式**。所以：

- `local`（需要 named pipe 适配）和 `shm` 不进 default
- **`blocking` 也不进 default**——它要额外维护一套 runtime 生命周期与告警面，属于「该由用户显式要」的能力（理由见 5.5）

保证一个 `cargo run --example local_calc` 在干净环境下就能成功。

---

## 2. L1 传输层

### 2.1 Trait 定义

L1 不再描述「字节流」，而是描述「**消息通道**」。一个通道由独立的收发两半构成——这正是共享内存环的天然形态（一个方向一条 SPSC 环），也是 socket 的 `split()` 形态。

```rust
pub trait MessageSource: Send {
    /// 返回一条完整消息。实现方保证「完整」，不做半条。
    /// shm 实现可以直接返回指向共享内存的零拷贝 Bytes。
    /// 关闭语义 = drop；没有独立的 close 方法（理由见下）。
    fn recv(&mut self) -> impl Future<Output = io::Result<Bytes>> + Send;

    /// 本端 reader 的帧长上限旋钮。L3 在握手成功后把「本端声明值」写进去（见 2.3）。
    /// 没有这个访问器，L3 就够不到那个 Arc<AtomicU32>——它只认识 Source/Sink，不认识 transport。
    fn recv_limit(&self) -> Arc<AtomicU32>;
}

pub trait MessageSink: Send {
    /// 发送一条完整消息。必须是 async：shm 环满时要能等待空位。
    /// 没有 flush：收尾由 sink 自己在 drop 时做（见 4.6）。
    fn send(&mut self, msg: Bytes)
        -> impl Future<Output = io::Result<()>> + Send;

    /// 本端 writer 的帧长上限旋钮。L3 在握手成功后把「对端声明值」写进去（见 2.3）。
    fn send_limit(&self) -> Arc<AtomicU32>;
}

/// 只负责「建立通道」，不负责收发
pub trait Transport: Sized {
    type Source: MessageSource;
    type Sink: MessageSink;

    /// 服务端：占住地址、准备接受连接。listen 端必须走它，而不是 accept 自己猜。
    /// 异步不是因为它需要等待——绑定本身没有等待——而是因为实现要把 fd 注册进
    /// 当前 runtime 的 driver。这个前置条件只能由 `async` 在类型上表达：写成同步
    /// 签名时，漏了 runtime 的调用方会在构造期 panic，而不是拿到一个 `Err`。
    fn bind(
        ep: &Endpoint,
        opts: &TransportOptions,
    ) -> impl Future<Output = io::Result<Self>> + Send;

    fn connect(ep: &Endpoint, opts: &TransportOptions)
        -> impl Future<Output = io::Result<(Self::Source, Self::Sink)>> + Send;

    fn accept(&self, opts: &TransportOptions)
        -> impl Future<Output = io::Result<(Self::Source, Self::Sink)>> + Send;
}
```

**没有 `close`，也没有 `flush`——这两个都是 v0.7 加错的东西。**

`close` 的问题不在语义，而在所有权：`source` 被 move 进 reader task，`recv(&mut self)` 在整个 await 期间独占它，**这时没有任何其他任务拿得到 `&self` 去调 `close`**；而 trait 只约束 `Send` 不约束 `Sync`，「拿 `Arc` 共享再关」也过不了编译。更要紧的是 4.1 的 reader 已经是 `select! { recv(), shutdown.cancelled() }`——取消 token 触发时那个 `recv` future 被 drop，循环退出，task 结束时 `source` 随之 drop，**关闭自动发生**。再挂一个 `close` 就是第二套机制，而且是一套调不动的机制。

`flush` 同理：TCP 写本来就由内核缓冲承托、shm 根本没有 flush 概念、memory 是 channel——三种里两种是 no-op，留着只会让每个新 transport 都多实现一个空函数。收尾交给 sink 的 `drop`。

**但「收尾交给 drop」必须先说清它能承诺什么（v0.15 改判）。** 既然没有 `close`，那 `Draining → Closed` 在网络上能不能被对端看见，就完全取决于 `drop` 对底层的效果——而这个效果**逐平台不同**。所以契约只能承诺本端行为：

**契约（唯一可作为验收条件的一条）：drop `Sink` 之后本端不再向这条连接写任何字节；drop `Source` 之后本端不再读。其余一概不承诺。**

「对端能否观测到收尾」是**实现能力**，不是契约：

| transport | drop `Sink` 的实际效果 | 对端可见 EOF |
|---|---|---|
| `tcp`（tokio 分裂半身） | `OwnedWriteHalf::drop` → `shutdown(SHUT_WR)` | 是 |
| `local`（Unix，手搓 UDS） | 同上 | 是 |
| `local`（Windows named pipe，未实现） | 无半关闭：关写句柄 = 结束整个 pipe 实例 | 否 |
| `memory` | drop `mpsc::Sender`，对端 `recv` 得到 `None` | 是（不是 EOF，是通道关闭） |
| `shm` | 置段头 `state` 并停写 `head` | 是（靠状态位） |

**为什么不再写成「必须尽力半关闭」（v0.15）**：原来那条契约要求四个实现里有两个（`interprocess`、Windows named pipe）开例外条款——**一个需要例外才成立的契约不是契约，是偏好**。硬写进验收条件，只会让那个平台要么永远红着，要么逼实现撒谎。正确性改挂在这里：

- **由 `GoAway` + 收尾超时保证**（4.6 的默认 5 s 兜底）：没有 EOF 时收敛慢一点，但不会卡死；
- **半关闭降级为推动收敛的加速路径**：实现拿得到就拿（Unix 上 `tokio::net::UnixStream` 免费给 `SHUT_WR`），拿不到就不拿，L2 以上一行代码都不用改；
- **能力位只出现在测试里**（`tests/transport.rs` 的 `PEER_SEES_EOF`），用来验证实现没撒谎；**不要**把它提到 `Transport` trait 上让 L3 分支——L3 一旦开始分辨具体 transport，铁律 ①（L2 以上不认识 fd 与底层概念）就已经破了。

由此推出第二条使用约束（原来只写了 `recv`）：**`recv` 与 `send` 都只允许在「终态取消」时被打断，不能用来做单次超时。** `recv` 被打断会丢掉已读进内部缓冲的字节；`send` 被打断更糟——它会在流里留下**半帧**，对端解析到一半的长度前缀，整条连接只能废弃。把 `send` 的 future 拿去做 `timeout`，等于在生产环境里随机制造「连接无端损坏」。

关键设计取舍：

- **收发拆成两个 trait，而不是一个 `Io`**。socket 用 `io.split()` 得到两半；共享内存是两条独立的 SPSC 环（方向上天然分离）。如果保留「一个双向 Io」的形状，shm 就得假装自己是一个双向设备，白白多一层适配。
- **`recv` 返回 `Bytes` 而不是 `&[u8]`**。借用的切片会带上生命周期，紧接着就过不了 `spawn`（dispatch 要求 `Send + 'static`）。`Bytes::from_owner` 让零拷贝和 `'static` 同时成立，详见 3.7。
- **`send` 必须是 async**。socket 靠内核缓冲区 + bounded mpsc 提供背压；shm 环满了只能等空位。统一成 async 才能让两种背压共用一个接口。
- **`accept` 语义泛化**。对 socket 是 OS `accept`；对 shm 是「等待对端 attach 到同一段共享内存」。概念上都是「产出一个已就绪的双向通道」，所以接口可以统一。
- **不用 `Box<dyn>`，用泛型 + 枚举门面**。这些方法的返回类型是 `impl Future`，非 dyn-safe。泛型实现在热路径上零开销；需要类型擦除时用 `AnySource` / `AnySink`（见 2.5），代价是每条消息一次 box（约 20–40 ns）。
- **`bind` 必须显式存在**。原来只有 `connect` / `accept`，服务端「先占地址、再等连接」这一步无处表达——shm 尤其明显（要先建段并发布元数据，才谈得上 attach）。`accept(&self, opts)` 拿到的是**每个连接**的选项，`bind` 拿到的是**监听端**的选项，两者分开，不要混成一个。
- **trait 上必须有 `recv_limit()` / `send_limit()` 两个访问器**（v0.10）。v0.9 规定「L3 在握手成功后写 `FramedSource`/`FramedSink` 的 `Arc<AtomicU32>`」，但 L3 只认识 `Source`/`Sink`，**根本没有通道去拿那两个 `Arc`**——文档写了、代码够不到。所以把旋钮的句柄作为同步、零成本的方法暴露出来：L3 在把半身 move 进 task **之前**取出 `Arc` 存着，`Ready` 之后写入。这不违反铁律②——L1 只交出一个 `Arc<AtomicU32>` 和一个数字，完全不知道它代表「协议协商」。
- **opts 是参数，不是成员**。v0.7 定义了 `TransportOptions` 却没有入口，等于写在文档里没人用；现在 `bind` / `connect` / `accept` 都显式接收它。

### 2.2 各实现的选择与权衡

| 实现 | 平台 | 优点 | 缺点 | 定位 |
|---|---|---|---|---|
| `tcp`（127.0.0.1） | 全平台 | 零平台差异、最省事 | 比 UDS 慢约 20–30%（量级估计，非实测） | **M0 打通链路用这个** |
| `local`（手搓 UDS） | Unix only | 零新依赖（`tokio::net::UnixStream` 已在默认依赖里）；**天然半关闭**，与 `tcp` 同语义；比 TCP loopback 少一层协议栈 | Windows 不可用；路径受 `sun_path` 长度限制（约 104 字节） | **M0 起落地的本地 IPC**（可选 feature，不进默认集）；M1 用它做「换 transport 不改上层」的架构验收 |
| `local`（Windows named pipe） | Windows only | 系统原生 | 无半关闭；server 需 per-connection 新建并用 `first_pipe_instance` 规避 `ERROR_PIPE_BUSY`；无 `into_split`；**当前无测试环境** | **推迟**：实现代码没人验证过就是负债，等有 Windows CI 再谈（见 2.6） |
| `unix`（`SOCK_SEQPACKET` 特化） | Unix only | 原生带消息边界；支持 fd 传递 | Windows 不可用 | **当前不做**：`local` 已覆盖 `SOCK_STREAM`；仅当需要 fd 传递/seqpacket 时再加，见 12 节第 11 条 |
| `memory` | 全平台 | 零 syscall；**与 `tcp` 走同一套 `Bytes` 载体**，是最低成本的一致性测试替身 | 不能跨进程 | **单测 + 消息导向接口的验收 + 零拷贝形状验证** |
| `shm` | 全平台（实现各异） | 建立后完全不经内核，RTT 100–600 ns（配置相关，见附录 B.2） | 实现复杂（一致性 / 崩溃检测 / 生命周期） | **M5，见附录 B** |

注意：`memory` 不只是测试替身。它是**在没有共享内存的情况下，验证「消息导向 + 零拷贝 `Bytes`」接口是否成立的最低成本手段**——如果连进程内 channel 都塞不进去，说明接口形状有问题，这时还没写 shm，返工代价为零。

> **一个必须澄清的混淆点**：`memory` **不提供**「跳过序列化、直传指针」的能力。它和 `tcp` / `local` 一样，消息载体必须是 `Bytes`（铁律 ③），否则它就无法承担"一致性测试替身"的职责。
>
> 真正的「零序列化进程内直连」**不是 transport**，而是 5.6 说的**本地调用路径**——同一个 `Service` 实现被同进程的调用方直接调，跳过整个 L1/L2/L3。两者不是一回事：`memory` 是「用 channel 模拟一条 IPC 通道」，本地直连是「根本不走通道」。v0.2 曾把两者混在 `memory` 一个实现里，是错的。

### 2.3 流式 transport 的帧拼装下沉到 L1

这是 v0.2 的一处重要调整。既然 L1 的契约是「交出完整消息」，那**长度前缀的拼装就必须由 L1 内部完成**，不能指望上层从字节流里自己切消息。

```
流式 transport（TCP / UDS / named pipe）：
   wire:  [ len: u32 | 完整消息体（含 24B 语义头 + payload） ] ...

  MessageSource::recv 内部：读 4 字节长度 → 校验上限 → 读满 len 字节 → 冻结成 Bytes

消息式 transport（SOCK_SEQPACKET / 共享内存环）：
   边界由 OS 或环本身提供，跳过长度前缀，直接一条消息一个 Bytes
```

**注意：若将来实现 Windows `local`，它属于「流式」这一档，不是消息式。** 原因见 2.6：named pipe 在消息超出缓冲时返回 `ERROR_MORE_DATA`，读侧不替我们切消息，长度前缀不能省。Unix 上本来就是流，无此问题。

**读路径上有一个容易做错的地方**：拼帧时不要「先 `read_exact` 到一个临时 Vec、再拷进 `BytesMut`」。直接用 `read_buf(&mut BytesMut)` 让数据一次落进目标缓冲，读完再 `split_to(len).freeze()`。前者对一条 1 MiB 消息会多一次整块拷贝，而且两者都发生在 reader task 的关键路径上——这是 M4 里最容易拿到的一笔免费收益。

**两个方向各有一个上限旋钮，否则协商出来的 `max_message_len` 会用错方向（v0.9）。** `FramedSource` 与 `FramedSink` 各持一个 `Arc<AtomicU32>`，**初值都固定 64 KiB**（握手完成前不能信任何声明值，见 6 节）；握手成功后：

- `FramedSource` ← **本端声明的** `max_message_len`（这是我们承诺过的接收能力）
- `FramedSink` ← **对端声明的** `max_message_len`（`send` 前检查，超了立刻本地 `RpcError::InvalidInput`，别等对端断连）

这不违反铁律②——L1 只是持有并比较一个数字，完全不知道它代表「协议协商」。（`max_message_len` 是**本端声明**、由 `Handshake` 带给对端，不是本端 reader 初值；声明值放在 **`SessionOptions`**，不放在 `TransportOptions`——v0.10 迁移，理由见 2.7。）

写旋钮的唯一位置是 L3，而 L3 拿到旋钮的唯一途径是 `recv_limit()` / `send_limit()`（见 2.1）：`Session::new` 在把半身 move 进 task 之前先把两个 `Arc` 取出来存着，`Ready` 之后 `store` 进去。

这样带来的收益：

- 协议层（L2）不再关心「消息边界」这件事，只解析语义头
- 顺带**删掉了协议头里的 `length` 字段**（详见 3.2），去掉冗余
- 将来加 shm 时，L2 一行不用改

`transport/framing.rs` 就是把「长度前缀 + 读满 + 上限校验」做成一个可复用的 `FramedSource<S>` 包装器，流式实现都复用它。

### 2.4 各实现的背压来源

| 实现 | 满/忙时的行为 | 背压链路 |
|---|---|---|
| `tcp` / `local` | `send().await` 等内核发送缓冲区腾出空间 | socket → writer task → bounded mpsc → 调用方 |
| `shm` | 环满 → 等消费者推进 `tail` | 环 → writer task → bounded mpsc → 调用方 |
| `memory` | mpsc 满 → await | mpsc 直接对接 |

L3 的 writer task（见 4.1）是这层差异的**吸收点**：它把「很多调用方并发要发」和「transport 一次只能发一条」解耦，所以 L3 不需要知道 transport 的背压细节。

### 2.5 `AnySource` / `AnySink`：类型擦除门面

```rust
pub enum AnySource {
    Tcp(tcp::Source),
    Local(local::Source),
    Memory(memory::Source),
    Shm(shm::Source),
}
```

> 各变体按 feature 条件编译：关掉某个 feature 就少一个变体，`Endpoint` 同理。所以 `AnySource` 的 `match` 完备性也会随 feature 变化——写代码时不要假设四个变体永远都在。

**门面的代价只来自「返回 `impl Future`」这一件事，和「类型擦除」本身无关。** 凡是返回 future 的方法（`recv` / `send` / `connect` / `accept` / `bind`）在门面上都要付一次 `Box::pin`；不返回 future 的纯本地动作（建段、校验段头、注册服务）不必 box，`match` 各变体各调一次即可。`bind` 之所以也在异步侧（v0.13），是因为它要把 fd 注册进当前 runtime——这个前置条件只能由 `async` 在类型上表达，用同步签名就得靠注释和运行时检查补，漏了会在构造期 panic。它一个进程只调用一次，这次 box 不在热路径上。

用途：运行时才知道用哪种 transport 的场景（配置驱动、测试矩阵），避免 `Session<T>` 的泛型在调用方层层传染。

代价要拆成三项，别混成一句「一次 box」（v0.10 细化）：

| 代价源 | 量级 | 触发点 | 说明 |
|---|---|---|---|
| `dyn` 间接调用 | 每次 2–5 ns | 整个会话生命周期 | 每个 `recv`/`send` 从 `Box::pin` 的 future 上走一层 vtable，可预测分支不友好 |
| 每条消息一次 `alloc` + `free` | 20–40 ns | 每条 `recv` / `send` | `Box::pin` 的 future 生命周期 = 一次消息，分配器往返就在这里 |
| 单态化代码体积 | 编译时间/产物体积，不是运行时 | 构建期 | 泛型组合 `O(transport × handler 数)`，每个都要独立代码生成；门面把它压成 `O(transport + handler 数)` |

一个 RPC 往返有 **4 个点**要 box：客户端 `connect` 后的第一次 `send`、服务端 `recv`、服务端 `send`（回包）、客户端 `recv`（收响应）——合计 **80–160 ns**。放在 3 µs 的端到端目标里是 3–5%；放在 300–600 ns 的裸传输 ping-pong 里是 13–27%，那就不是「可接受」而是「测错了」。

所以策略定死：

- 追求极限性能 / 任何基准测试 → 直接 `Server<TcpTransport>`，泛型特化，零开销
- 需要运行时选择（配置驱动、测试矩阵、示例的 `--transport` 开关）→ `AnySource` / `AnySink`，接受上面那张表的代价

不要两头都想要。这个取舍写死在文档里，避免后面反复摇摆。

> **一个容易误判的地方**：`AnySource` 消除的是**单态化组合爆炸**，不是**泛型传染**。`Session<S, K>` 仍会把 `S`/`K` 一路带到 `Server`、`connect`、生成的客户端类型上；想真正擦除，必须在每个边界上都擦（`Session<AnySource, AnySink>` + `Server::bind` 返回 `Session<AnySource, AnySink>` 的入口）。只擦一半，调用方照样被 `S` 绑住。

### 2.6 Windows 平台的现实约束（`local` 的 Windows 分支推迟）

必须先明确的事实：

- `tokio::net::UnixStream` / `UnixListener` 都标了 `Available on Unix only`，Windows 上没有。
- Windows 10 1803+ 内核支持 `AF_UNIX`，但 Rust 标准库与 tokio 都**没有**提供封装。
- Windows 的本地 IPC 正解是 **named pipe**（`tokio::net::windows::named_pipe`）。OS 与 tokio 都能**创建**消息模式的 pipe（`PipeMode::Message`），**但读侧不替我们切消息**——一条消息超出缓冲时返回 `ERROR_MORE_DATA`，得自己续读拼完。所以即使将来做，长度前缀也不能省。

路线（v0.15 重写）：

1. **M0 用 TCP loopback 打通全链路**。内核 loopback 绕过大部分网络栈，吞吐 GB/s 级、延迟微秒级。用它能先把 L2/L3 调对，不被平台差异干扰。
2. **M0 同时落地 `local`（Unix UDS）**：直接拿 `tokio::net::UnixStream` + `FramedSource/FramedSink`，零新依赖，`OwnedWriteHalf::drop` 免费给出 `SHUT_WR`。
3. **M1 做架构验收**：把 transport 从 TCP 换成 `local`，`Session` 一行不改。
4. **Windows 分支明确推迟**（v0.15）。四条理由，按严重程度排：
   - **没有半关闭**：关掉写句柄等于结束整个 pipe 实例，进程间看不到「我先不发、但仍能收」；
   - **server 侧要 per-connection 新建 pipe**，且第一个实例必须 `first_pipe_instance(true)`，否则两个进程先后 bind 会撞 `ERROR_PIPE_BUSY` 这种「看起来像随机失败」的竞态；
   - **没有 `into_split()`**，只能 `tokio::io::split`（内部 `Arc<Mutex>`，读写抢同一把锁，也就永远拿不到半关闭）；
   - **没有测试环境**：手搓一段没人跑过的 Windows 代码，不是「提前布局」，是给 CI 埋一颗只在特定机器上炸的雷。

   落地方式：`local` feature 在非 Unix 目标上直接 `compile_error!`。**把「不支持」写成编译期事实，而不是运行时惊喜。**

**Windows 上的关闭差异，仍然要提前记下来（v0.9 提出，v0.15 改判表述）。** tokio 的 `TcpStream::into_split()` 两半是真正独立的半身，drop 写半会 `shutdown(SHUT_WR)`；named pipe **没有半关闭**。因此 2.1 的 drop 契约只能承诺「本端 drop 后不再写」，对端能不能看见收尾交给 `GoAway` + 收尾超时。这也正是为什么 `GoAway` 必须是显式控制帧——在没有半关闭的平台上，它是唯一的优雅关闭信号。

### 2.7 Endpoint 抽象

```rust
pub enum Endpoint {
    Tcp(SocketAddr),
    Local(LocalAddr),                        // Unix：socket 文件路径；Windows pipe 名待扩
    Memory(MemoryAddr),                      // 进程内直接配对的通道标识（无注册表，见下）
    Shm { name: String },                    // 段基名；双向 = 两个段（见附录 B.3），容量写在段头
}
```

endpoint 必须携带足够的寻址信息，让 L3 完全无感——L3 拿到 endpoint 就直接调 `T::connect(ep, opts)`。

> **`Endpoint::Memory` 是唯一一个「不参与查找」的变体（v0.8 补）。** `memory` 走的是「进程内直接配对」，而且返回的是**两份半身**而不是一份：`Memory::pair() -> ((Source, Sink), (Source, Sink))`——第一组给 A 侧、第二组给 B 侧。v0.8 只写「返回两半」，但一条双向会话需要 `(Source, Sink)` **各一份**，`Session` 没法只靠一个 `Source` 和一个 `Sink` 跑起来（reader 要读、writer 要写，缺一半就是单向）。示例把两组分别交给 server 与 client，全程没有地址解析，也没有全局注册表。
>
> 顺带定死 `Session` 的构造入口（v0.10 补 `registry`）：`Session::new(source: S, sink: K, registry: Arc<ServiceRegistry>, opts: SessionOptions) -> (Arc<Session>, SessionHandle)`——`local_calc` 与测试矩阵需要「从一对现成的 source/sink 直接建会话」，服务端还需要在同一入口上装配注册表；客户端传一个空注册表即可。这一条不定，示例就得绕到 transport 里去拿通道，`memory` 的整个 pair 设计白做，而服务端根本没有入口挂服务。

**选项要分成两层，塞在一起是 v0.7 的错：**

| 结构 | 归属 | 字段 |
|---|---|---|
| `TransportOptions` | L1 | `connect_timeout`、`spin_hint`（shm，见 B.5） |
| `SessionOptions` | L3 | `max_message_len`（**v0.10 从 L1 迁来**）、`max_async_in_flight`、`max_blocking_in_flight`、`payload_mode`（见 4.5）、`handshake_timeout`、`close_timeout`（默认 5 s） |

`max_in_flight` 是 Session 自己的配额（4.1 的 pending 表在 Session 里），`payload_mode` 是 dispatch 的决策（4.5 明说「交给 handler 之前拷」），两者都不该由 L1 持有。

**`max_message_len` 必须搬到 `SessionOptions`（v0.10 修）**：它不是 channel 的构造参数，而是「本端声明 + 握手协商」的会话属性。留在 `TransportOptions` 里会同时坏三件事——① 握手是 L3 的事，`Session::new(source, sink, opts)` 拿不到 `TransportOptions`，连本端声明值都填不出来；② 握手后要把值写进两个旋钮，而旋钮的句柄也只有 L3 能拿（`recv_limit()` / `send_limit()`，见 2.1）；③ 同一份值出现两处真相（`TransportOptions` 一份、`Handshake` 一份）。L1 侧不需要它：**握手前上限恒为 64 KiB**，本来就不用配置。

> 三方分工原则（v0.10 收敛）：**寻址信息进 `Endpoint`，L1 行为进 `TransportOptions`，L3 行为进 `SessionOptions`，双方必须一致的东西进 `Handshake`**（见 6 节「机制 2」）。
>
> 反例三个：`ring_capacity` 曾写在 `TransportOptions` 里，但环容量必须两侧一致，而且**协商它本身要走这条环**——所以它最终落在 shm 段头，由创建方写入、attach 方校验（见 6 节机制 2）。`max_in_flight` 也曾塞在这里，但它是 Session 的配额，L1 根本读不到它。`max_message_len` v0.9 还留在 `TransportOptions`，但它的两个消费方（`Handshake`、旋钮写入）全在 L3，留在 L1 等于让两个消费方都拿不到它。

---

## 3. L2 协议层

### 3.1 核心原则：语义头必须「不解包 payload 就能处理」

很多框架把整个信封当成一个 serde 结构体反序列化，结果是**取消一个请求、打一条 trace、判断一个类型，都要付出完整解析的代价**。

正确做法：**定长头 + 手工编解码 + 裸 payload**。

### 3.2 协议头：24 字节

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    kind (u8)  |   flags (u8)  |        service_id (u16)       |   字节 0-3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        request_id (u64)                       |   字节 4-11
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          method (u64)                         |   字节 12-19
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     deadline_ms (u32)                         |   字节 20-23
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        payload (变长)                         |   字节 24..
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**v0.7 把 20 字节扩到 24 字节**，一箭三雕：

| 新增/变化 | 解决什么问题 |
|---|---|
| `reserved: u16` → **`service_id: u16`** | 原来头里只有 `method`，没有服务维度，而 6 节又要求「动态注册多个服务」——结果是「一个端口只能跑一个服务」这个隐含约束从来没写出来。现在 `ServiceRegistry` 按 `service_id` 查找，`method` 只在服务内部唯一 |
| 新增 **`deadline_ms: u32`** | 4.3 写了「协议侧可选带 deadline」，但头里没有字段能放它，`reserved` 最多表达 65 秒。现在 0 = 无期限，非 0 = 相对毫秒（≤ 49 天） |
| 头长 24 → **payload 起点 8 字节对齐** | 20 字节头让 payload 落在 `offset ≡ 4 (mod 8)`；`rkyv` 在非对齐缓冲上读是 UB，这会直接把 M5 的零拷贝堵死。24 字节头让 payload 从 8 对齐处开始，`rkyv` 的原地读路径才能成立 |

代价是每帧多 4 字节。在多路复用下这是可忽略的：一条 1 KiB 消息多 0.4%，而换来的是「不必在 M5 为了对齐再改一次线格式」。

**为什么不用 flags + TLV 扩展段代替**：TLV 要额外一次扫描才能取到字段，且必须在解析头之后才能判断「这帧有没有扩展」，正好破坏了 3.1 那条「不解包就能处理」的原则。

**v0.2 删除了原 `length` 字段**，这个决定保留，理由：

- 消息边界由 L1 保证（流式用长度前缀，消息式由 OS/环提供），头里再存一份是冗余
- shm 消息的 payload 长度 = 消息总长 − 24，可直接算出
- 少一处需要两侧同步的约定：`length` 与实际长度不一致时，谁赢、怎么报错，是个永远吵不清的边界条件
- **不再有「头更短所以更快」这个理由**。v0.1 的头就是 24 字节（`v0.2` 缩到 20，`v0.7` 又扩回 24），所以「现在比 v0.1 短」这句话本身不成立；真正的收益是「少一处冗余约定 + 边界下沉到 L1」，不要拿性能当借口

**全文统一小端。** 24 字节头、错误体（3.8）、控制帧（6 节）一律小端，不做字节序协商——本项目只跑同机 IPC，跨字节序的场景不存在，为此多一个协商维度不值得。这条要写进 `PROTOCOL.md`，也是黄金样例比对的基准。

定长 24 字节意味着所有字段都在**固定偏移**上：`&msg[4..12]` 就是 `request_id`，`&msg[0..4]` 里 `kind` / `flags` / `service_id` 各就各位，不需要任何解析器，也不需要 serde。这是取消 / trace / 分流这三条控制路径能做到接近零成本的前提。

### 3.3 消息类型

| kind | 值 | 方向 | 说明 |
|---|---|---|---|
| `Request` | 0 | 双向 | 需要响应 |
| `Response` | 1 | 双向 | `flags` bit0 标记 Ok/Err；`payload` 是结果或错误体（见 3.8） |
| `Notification` | 2 | 双向 | 单向，无响应 |
| `Cancel` | 3 | 双向 | 取消某个 in-flight 请求 |
| `StreamChunk` | 4 | 双向 | **预留，不实现**（见下） |
| `GoAway` | 5 | 双向 | 优雅关闭；`payload` 为空，急停由 `flags` bit2 表示 |
| `Handshake` | 6 | 双向 | 协商连接参数，字段见 6 节「机制 2」，**首帧必须是它** |

> `StreamChunk`（kind 4）与 flags bit1 是**预留位，当前阶段不实现流式响应**——M0–M5 全是「一问一答」。留在表里是为了让 kind 编号从一开始就稳定，避免将来插入新类型时改动线上语义。不要把它当作已设计好的功能去实现。

### 3.3.1 逐 kind 字段语义（哪一列在哪种 kind 下有效）

**本表的定位（v0.9 澄清）：设计视角的速查表，不是线格式的第二权威。** 线格式的**唯一权威**是 `docs/PROTOCOL.md`（见 13 节），本表与它同源，任何字段改动都必须先改 `PROTOCOL.md`、再同步本表与黄金样例。两份手写表格必然漂移，所以只能有一个说了算——否则实现者按哪份写都可能对、也可能都错。

**「必须为 0」不是建议，是对端可以直接拒绝的硬约束**——否则解析歧义会变成安全问题。

| kind | `request_id` | `service_id` | `method` | `deadline_ms` | `payload` |
|---|---|---|---|---|---|
| `Request` | 发起方分配，非 0 | 目标服务，有效 | 目标方法 | 相对毫秒，0=无 | 请求参数 |
| `Response` | 回填请求里的 id | **必须 0** | **必须 0** | **必须 0** | 结果或错误体 |
| `Notification` | 发起方分配，非 0 | 目标服务 | 目标方法 | 相对毫秒，0=无 | 通知内容 |
| `Cancel` | 要取消的 id | **必须 0** | **必须 0** | **必须 0** | 空 |
| `GoAway` | **必须 0** | **必须 0** | **必须 0** | **必须 0** | 空 |
| `Handshake` | **必须 0** | **必须 0** | **必须 0** | **必须 0** | 见 6 节 |

**`request_id` 的命名空间必须写死：id 由发起方分配，只在本端的 `pending` 表内唯一。** Response / Cancel 里的 id 是对端空间里的引用，**按连接方向解释**，两侧永远不需要比较各自的计数器。不写这条，读者很容易理解成「一张全局表」，那时 A 发的 5 和 B 发的 5 就会互踩。

### 3.4 flags 位定义

| bit | 含义 |
|---|---|
| 0 | `Response` 为 Err（payload 按 3.8 的错误体解析） |
| 1 | 预留：`StreamChunk` 最后一帧。当前不实现，置位即协议错误 |
| 2 | `GoAway` 为急停（不等待 in-flight，立即断连）；其它 kind 上置位即协议错误 |
| 3-7 | 保留。**任何置位都视为协议错误**，不要静默忽略 |

> v0.7 明确：**协议不留压缩位。** 压缩在本地 IPC 场景的收益为负（CPU 换带宽，而带宽免费），如果将来真要，走「新增 kind + 独立协商」而不是借用 flags——这样旧实现对未知位报错，不会误读。

### 3.5 method id：编译期稳定哈希

- 对**方法名**（不含 trait / 服务名）在 **proc-macro 展开时**算 64 位哈希（xxh3），生成字面量。路由已经靠 `service_id`，method 只在该服务内部唯一，因此**没有理由把服务名混进哈希**
- 运行时零开销，且**跨进程、跨版本稳定**
- 为什么去掉服务名：改 trait 名是最常见的一次重构（`Calc` → `Calculator`），若服务名进了哈希，改个名字就等于全量改线上 method id，那「显式 `service_id` 保证稳定」这条理由就被稀释了一半
- 用 u64 而非 u32：碰撞概率 2⁻⁶⁴，省掉运行时碰撞检查表。多出的 4 字节在多路复用下可忽略
- 改名即协议变更，这个语义是正确的，写死在文档里
- **宏顺手生成一张方法名表**：`const METHOD_NAMES: &[(u64, &'static str)]`（或 `fn method_name(u64) -> Option<&'static str>`）。请求 span、`UnknownMethod` 日志、panic 报告全靠它，成本是零——都是编译期字符串字面量。只有 `request_id` 的 span 在排障时基本没用
- **必须有一条 golden 向量**：`method_name = "add"` → 固定的 u64 字面量，放在 **`photon-macros` 的单元测试**里（或纯文本常量），**不要混进 `photon/tests/protocol.rs` 的 §8 帧样例**——那里放的是线格式帧，而这条是哈希常量，不是帧。哈希实现（xxh3 版本、`Hasher::write` 的字节布局）一旦被人「优化」，这条测试立刻红；没有它，某次升级 `xxhash-rust` 就会静默改掉全部线上 method id

> dispatch 的键是 **`(service_id, method)`**，不是单独的 `method`。`service_id` 由 `ServiceRegistry` 查表得到 `Arc<dyn Service>`，`method` 再在该服务内部 `match`——两层都不需要哈希表。
>
> 由此得一条硬约束：**同一个服务内，方法名不能重复，也不接受两个不同方法哈希撞到同一值。** 后者概率可忽略，但宏应该给一条 `const` 断言式的检查（编译期算完就是常量，重复直接报错），免得将来改用与「方法名」平行的新命名规则时踩到。

> 注意：shm 上 `request_id` 仍然需要（用来配对响应），但它不再是「防止队头阻塞」的手段——环形队列天然支持并发在飞。原本为 socket 队头阻塞设计的机制在这里自动退化成更简单的形态。

### 3.6 payload 序列化

```rust
pub trait Codec {
    /// 这个 codec 在线格式里的身份。`Handshake` 里的 `codec_id` 就填它。
    const ID: u16;

    /// 只保留这个核心方法：可以写进调用方提供的任意缓冲
    fn encode_into<T: Serialize, B: BufMut>(&self, value: &T, dst: &mut B)
        -> Result<(), RpcError>;

    fn decode<T: DeserializeOwned>(&self, src: &Bytes) -> Result<T, RpcError>;

    /// 便利包装
    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes, RpcError> {
        let mut buf = BytesMut::new();
        self.encode_into(value, &mut buf)?;
        Ok(buf.freeze())
    }
}
```

**`const ID` 是补 v0.8 的一个缺口（v0.9）**：6 节的 `Handshake` 要让本端填 `codec_id`，但 v0.8 没写这个数从哪来。加一个关联常量，`postcard = 0`、`rkyv = 1`，`Session` 的握手逻辑直接读 `codec::Default::ID`（v0.10：不再有 `C` 泛型参数，见 4.1），不需要在 dispatch 或宏里再维护第二份映射。常量必须是 `u16` 且值稳定——它就是线上协议的一部分。

**v0.2 的改动**：把 `encode` 从「返回 `Bytes`」改为「核心是 `encode_into`」。

原因是 shm 的 tier-2 路径需要**把响应直接编码进共享内存槽位**，省掉「编码到堆上、再拷进环里」这一次拷贝。如果 `Codec` 只会返回 `Bytes`，这条路就永远堵死。改成 `encode_into` 后，默认实现在堆上分配，成本与 v0.1 完全一样；但将来只需给 shm 提供一个自定义 `BufMut` 指向槽位，即可零改动接上。

这是典型的「现在改一行、将来省一次重构」。

**这个形状同时决定了另一件事：codec 只能是构建期选择，不能运行期协商。** `encode_into` / `decode` 都带泛型参数，泛型方法不是 dyn-safe，所以**不存在 `Box<dyn Codec>`**；而 `codec::Default` 是编译期选定的单态化类型（v0.10 起 `Session` 不再带 `C` 参数），握手完再「换一个 codec」在类型上就不成立。v0.7 写「握手带 `supported_codecs` 位图、取共同支持的第一个」是错的：那张位图永远只可能有一位。

所以：**codec 在构建期由 feature / 泛型参数选定，握手只带一个 `codec_id`，不匹配就 fail fast（`Protocol` 错误，直接断连）。** 位图留给将来「同一构建内真的要支持多条 decode 路径」时再引入，且那时必须先加一个 `AnyCodec` 枚举门面（照 2.5 的 `AnySource` 做法，每个变体静态调用自己的泛型方法）——代价是每个方法要生成两条解码路径，这正是 5.3 编译期分流已经接受的那类代价，文档届时必须明说。

**rkyv 更是同一编译产物内不可能与 postcard 共存服务于同一组 handler**：它要求 handler 参数写成 `&Archived<T>`，而 postcard 路径是 owned `T`。这不是「加个 feature 就行」，是业务代码形状变了。所以 M5 的 rkyv 只能作为**独立的、编译期选定的 codec**，不能作为「shm 上才启用的旁路」。

**但 `encode_into` 有个容易漏的前提：缓冲区必须预留 24 字节头空间。** 如果先 `encode` 出 payload 再拼头，每帧就多一次 memcpy（payload 越大越亏，1 KiB 消息多拷一次就是 1 KiB）。正确形状是「先写 24 字节占位（或 `advance` 跳过），再 `encode_into` 写 payload，最后回填头」——一次分配、一次编码、零额外拷贝。`BytesMut` 默认容量很小，所以要么用 `with_capacity(len + 24)`，要么从 L1 的读缓冲里 `split_to` 出带 headroom 的段。这条不做，M4 的性能数字会莫名其妙比预期低 10–20%。

选型：

| crate | 优点 | 缺点 | 结论 |
|---|---|---|---|
| **`postcard`** | serde 生态、no_std、体积小、维护活跃 | 无 schema 演进能力；反序列化必然产出 owned 结构 | **第一版就用这个** |
| `wincode` | bincode 兼容字节格式、性能好 | 生态较新 | 备选：仅当需要与 bincode 线上兼容时 |
| ~~`bincode`~~ | — | **已停止维护**（RUSTSEC-2025-0141；3.0.0 是只含 `compile_error!` 的墓碑发布） | **不采用，连可选 feature 都不要** |
| `rkyv` | 真正零拷贝：可在别人的字节上直接读 | `Archived` 生命周期侵入性强；与 `DeserializeOwned` 不兼容（见下） | **M5 与 shm 配套引入** |
| `protobuf` / `capnp` | 跨语言、schema 演进 | 引入 codegen 与运行时 | 远期，若做跨语言 |

**v0.7 换掉了 bincode，这是个必须修的外部事实。** bincode 已停止维护，3.0.0 的 `lib.rs` 只有一行 `compile_error!`，`cargo deny` 会直接报 RUSTSEC-2025-0141。继续用它等于项目第一天就带一个死依赖。`postcard` 在同一个 serde 抽象下工作，`Codec` trait 一行都不用改，依赖数也不变（仍是 8 个）。

**第一版不要上 rkyv，但要知道它和当前 `Codec` 形状不完全兼容。** `decode<T: DeserializeOwned>(&Bytes) -> T` 必然产出 owned `T`；而 rkyv 的价值恰恰是返回 `&Archived<T>`，让 handler 直接在别人的字节上读。两者不是同一个函数签名。M5 想走完全零拷贝，得给 `Codec` 加一条 `decode_archived` 路径，并把 handler 参数类型改成 `&Archived<_>`——**那会把 codec 泄漏进业务代码**。这是 M5 的真实代价，不要以为「换个 crate 就行」。详见附录 B.6。

### 3.7 不变量：消息交给 L2 时必须 `Send + 'static`

一条硬约束，写在这里作为所有 transport 实现的验收条件：

> `MessageSource::recv` 返回的 `Bytes` 必须满足 `Send + 'static`。

因为 L3 拿到消息后要 `spawn` 到独立 task 做反序列化和 dispatch（见第 8 节第 1 条）。如果为了实现零拷贝而返回 `&'a [u8]`，这一步就过不了编译，只能退化成「在 reader loop 里同步反序列化」——那会把一个大 payload 卡住整条通道的多路复用。

`Bytes::from_owner` 正是为解开这个死结而存在的：owner 持有 chunk 并负责归还，切片零拷贝，同时整体是 `'static + Send`。

### 3.8 错误体：跨进程的错误长什么样

`flags` bit0 置位表示 `Response` 为 Err，此时 payload 不是业务结果，而是一个**定长头 + 变长文本**的错误体：

```
[ code: u32 | retryable: u8 | reserved: u24 | message: 剩余字节，UTF-8 ]
```

共 8 字节前缀，`message` 直接读到 payload 末尾，不做额外长度字段——长度由 L1 保证。

**code 空间划分。v0.7 把 `1..=0xFFFF` 整段留白，结果是服务端自己产生的错误（过载、未知服务、未知方法、参数解码失败）一个码都没有，只能全拍成 `0`——这篇文档里最典型的「设计时留白，实现时卡死」。现在把常用的先定死：**

| code | 名称 | 谁产生 | 客户端拿到后 |
|---|---|---|---|
| `0` | `Internal` | 框架（handler panic、不可恢复） | `Remote { code: 0, .. }` |
| `1` | `Overloaded` | 对端：配额满 / 队列满 / 线程池排不上 | `Remote { code: 1, .. }`，`retryable` 通常 `1` |
| `2` | `UnknownService` | 对端：`service_id` 未注册 | `Remote { code: 2 }`，**重试无意义** |
| `3` | `UnknownMethod` | 对端：服务存在但方法未注册 | `Remote { code: 3 }`，**重试无意义** |
| `4` | `InvalidArgs` | 对端：payload 解码失败 / 参数不合法 | `Remote { code: 4 }`，**重试无意义** |
| `5` | `Cancelled` | 对端：收到 Cancel 并中止了 handler | `Remote { code: 5 }`，等价本地放弃 |
| `6` | `DeadlineExceeded` | 对端：它自己的 deadline 判定到期 | `Remote { code: 6 }` |
| `7` | `ShuttingDown` | 对端：正在优雅关闭，不再接新请求 | `Remote { code: 7 }`，看 4.6 |
| `8 ..= 0xFFFF` | — | 现在无定义 | **原样装进 `Remote`，不要拍成 `Internal`**（拍平会丢掉未来版本的信息） |
| `≥ 0x1_0000` | 用户 | 业务自定义 | `Remote { code, .. }`，语义由用户定义，框架不解释 |

`message` 非法 UTF-8 时用 `String::from_utf8_lossy` 并保留原 `code`——错误体是排障通道，**不能因为文本坏了就把整个错误降级成 `Internal`**。

错误体也是**小端**（与 24 字节头、控制帧统一，见 3.2）；`retryable` 只认 `0`/`1`，其它值按「非 0 即视为可重试」宽松处理，不要报协议错误——错误体是收尾路径，在这里断连只会让排障更难。

**错误分本地 / 远端两类，但边界要重述一遍，v0.7 的说法太绝对：**

- **本地错误**（本端自己产生）：`Timeout` / `Overloaded`（本端配额拒绝）/ `Closed` / `Io` / `Protocol` / `RuntimeDropped` / `InvalidInput` / `Internal`。它们**在发起侧永不上线路**；在回包侧经 `to_wire` 白名单才可能变成错误体（见下）
- **远端错误**（只能由错误体构造）：`Remote(RemoteError { code, retryable, message })`，**唯一一个变体**，用户业务错误也是它（`code ≥ 0x1_0000`）

规则不是「对端不许说任何话」，而是：**对端可以陈述它自己的状态（我过载了、我判超时了、我要关了），但本端的控制流结论必须由本端产生。** 区别在于：`Remote { code: 1 }` 只是「对方说它忙」，本端仍可决定继续排队或换路；而 `RpcError::Overloaded` 是「本端配额表满了」，请求压根没发出去。两者永不合并、也永不互相转换——合并了就分不清「谁该为放弃负责」。

v0.7 之所以自相矛盾，是因为它既写了「4.3 服务端超配额返回 `Overloaded`」，又在 3.8 把 `Overloaded` 钉成「永不上线路」。现在服务端超配额直接回 `code = 1`。

**「本地变体永不上线」这句话有个隐含前提，v0.10 必须写明：它约束的是「发起请求的一侧」，不是「处理请求的一侧」。** 否则没法自洽——`Remote` 是唯一远端变体，可服务端要回 `code = 1/4/6` 就得有个本地变体当输入；而 `Remote` 又「只能由错误体构造」，服务端手里没有错误体。所以规则改成**方向相关**的两句话：

1. **只有处理请求的一侧**（正在为某个 `request_id` 生成 `Response` 的一侧）可以主动构造 `Remote` 表达自己的状态，也可以把本地变体编成错误体发出去。
2. **发起请求的一侧**只能从收到的错误体得到 `Remote`，它的本地变体（`Closed` / `Io` / `Protocol` / `Timeout` / `Overloaded` / …）留在本地，不上线路、也不与 `Remote` 互相转换。

落地成**唯一的出口函数**，全项目不允许别处手搓错误体：

```rust
fn to_wire(e: &RpcError) -> Option<ErrorBody>
```

白名单（返回 `None` = 「这个错误不能上线，就地降级或直接断连」）：

| `RpcError` 变体 | 上线 code | 场景 |
|---|---|---|
| `Internal` | `0` | handler panic（`JoinError`）|
| `Overloaded` | `1` | **入站配额满**（这是回包侧，所以它能上线）|
| `InvalidInput` | `4` | handler 解码失败 / 参数不合法 |
| `Cancelled` | `5` | handler 被 `Cancel` 中止 |
| `Timeout` | `6` | handler 自己的 deadline 到期 |
| `Remote { code, .. }` | 原码 | 透传；用户业务错误（`code ≥ 0x1_0000`）走这条 |
| `Closed` / `Io` / `Protocol` / `RuntimeDropped` | `None` | 这些是「本端连路坏了」，没有可回的对象；在回包路径上出现说明框架自身异常，记 `tracing::error!` 后按 `Internal` 兜底（不要静默丢帧）|

**`UnknownService`（2）/ `UnknownMethod`（3）/ `ShuttingDown`（7）没有对应的 `RpcError` 变体**，由**回包侧**用统一构造入口 `RemoteError::new(code)` 造一个 `Remote` 再交给 `to_wire` 透传（`Remote` 走原码直通，不重映射）。**只允许三处用它**：查不到 `service_id`、查不到 `method`、`ingress == Draining`（L3 内部那两处实际走 `send_error` 直接写错误体，不必绕成 `RpcError`；构造器留给必须交出 `RpcError` 的调用点，目前是宏的 unknown-method 分支）。这条定死是为了堵两个已经写歪的地方：`04` 的未知方法分支曾自己 `RpcError::Remote(RemoteError::new(3))` 而不说明「这是回包侧允许的行为」（看起来像客户端伪造远端错误）；`03` 又曾把 `Timeout → 6`、`Overloaded → 1` 当成散装规则写，等于绕开了白名单。

> M2 验收里的 `0x10001` 业务错误也归这条：它是**示例 handler 自己**构造 `Remote { code: 0x10001 }`（用户码段），与 M3 的 `#[photon(error)]` 派生的映射无关——M2 不需要宏支持错误派生，这也是 M2 能提前验收的原因。

`RpcError` 标 `#[non_exhaustive]`。它是公开枚举，不加这个标记，每加一个变体都是 semver 破坏。

**业务错误映射（v0.25 落地）**：服务级写 `#[photon::service(id = N, error = E)]`，方法返回 `Result<T, E>`，实现一个 trait 交代两侧语义：

```rust
pub trait ServiceError: Sized {
    fn into_error(self) -> RpcError;      // 回包侧：业务错误 → 线上 Remote{ code >= 0x1_0000 }
    fn from_error(err: RpcError) -> Self; // 发起侧：Remote / Timeout / Closed / … → 业务错误
}
```

只要一个 impl，不必写两个方向相反的 `From`。**契约只有一句**：`into_error` 必须给出 `Remote { code >= 0x1_0000 }`；debug 构建里 `assert_business_error` 会断言它，release 下编译掉。`from_error` 则必须显式处理本端错误（`Timeout` / `Closed` / …）——它们不该被当成业务错误，这一点只能由用户决定怎么收敛（通常是一个 `Transport(..)` 变体）。

`RpcError::Remote` 仍是唯一的远端变体、`code` 语义仍由用户定义：映射只是把「用户错误 → 这个变体」和「这个变体 → 用户错误」各写一次，不新增第四种错误类别。

---

## 4. L3 会话层

### 4.1 结构

`Session` 由**两个 task + 一张 pending 表 + 一份关闭状态**组成：reader 进来、writer 出去、pending 记录谁在等，状态记录这条连接的两个方向各走到哪一步（见 4.6）。

```rust
pub struct Session<S, K>
where S: MessageSource, K: MessageSink
{
    next_id: AtomicU64,                              // 从 1 起；0 被 3.3.1 保留，只服务本端发出的请求
    outbound: Arc<parking_lot::Mutex<OutboundTable>>, // { pending: HashMap<u64, PendingEntry>, async_used }
    inbound:  Arc<parking_lot::Mutex<InboundQuota>>,  // { async_used, blocking_used }，只有服务端用
    handlers: AtomicUsize,                           // 对端发来、还没回 Response 的 handler 数
    cancels:  parking_lot::Mutex<HashMap<u64, CancellationToken>>, // 服务端专用
    registry: Arc<ServiceRegistry>,                  // service_id → Arc<dyn Service>；客户端传 empty()
    shutdown: CancellationToken,                     // 同时覆盖 reader 与 writer 两个 task
    tx: mpsc::Sender<Bytes>,                         // bounded(1024)，可 clone（每个回包 task一份）
    state: ClosingState,                             // ingress / egress 各一个 AtomicU8，见 4.6
    limits: (Arc<AtomicU32>, Arc<AtomicU32>),        // 握手后写：本端 reader 上限 / 对端 writer 上限
    reader: Mutex<Option<JoinHandle<()>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
}
```

> 只有两个泛型参数：transport 的 `S` / `K`。**codec 不再是参数**（v0.10 删）——`Session<S, K, C>` 里的 `C` 是死参数：`Service::call` 没有 `C` 槽位，宏在编译期别名 `codec::Default` 上做 en/decode，热路径上真正的字节从来不用 `C`，`C` 只在握手时贡献一个 `C::ID`。于是只要有人写 `Session<S, K, OtherCodec>`，握手会宣告 `OtherCodec::ID`、线上却是 `Default` 的字节——**静默错线格式**，比编译不过坏十倍。现在全项目只用 `codec::Default`（由 `codec-rkyv` feature 决定是 postcard 还是 rkyv），握手填 `Default::ID`；将来真要同构建支持第二种 codec，先引入 `AnyCodec` 门面（见 3.6）。用 `AnySource` / `AnySink` 时退化成枚举门面（见 2.5）。

- **`next_id` 从 1 开始**：`fetch_add` 返回旧值，若从 0 开始首个请求就是 `request_id = 0`，而 3.3.1 规定 Request 的 id 非 0。这是个必然踩到的 off-by-one，初始化时写 1 即可（`AtomicU64::new(1)`）
- **出站 pending 与入站配额必须是两张表**（v0.10 拆）。v0.9 只有一张 `PendingTable`，把「我发出去等响应的」和「对端发来等我回的」混在一组计数里，后果有两个：① 双向会话里本端 outbound 与对端 inbound 互相挤占同一组额度，两个方向的过载语义糊在一起；② 服务端入口根本没有 entry 可插，`try_reserve` 在入站侧调不起来。正确的形状是——`OutboundTable { pending, async_used }` 提供 `try_reserve(id, entry)`（检查 + 计数 + 插入同一次持锁）；`InboundQuota { async_used, blocking_used }` 提供 `try_acquire(shape)`，handler 结束时由 guard 归还。关闭时分别等两个零（见 4.6）。不要为了「少一把锁」把计数改成 `AtomicUsize` + CAS 循环，那是给自己造 ABA 和顺序问题
- **`tx` 是普通 `Sender`，不是 `Option`**（v0.9 修）。v0.8 想用「把 `tx` 置 `None`」表达「不再接受新请求」，这在一条同时承载请求、响应、控制帧的队列上根本行不通：置 `None` 会让在途 handler 的 `Response` 和 Draining 期要回的 `Remote{code:7}` 一起发不出去。准入由 `state` 判断，通道本身一直活到 `Session` 被整体 drop
- **关队列不能靠「drop 掉 `Session` 的 `tx`」**（v0.10 修）。v0.9 的第 3 步写「drop `Session` 的 `tx` 与所有临时 clone」，但 `Session` 活在 `Arc` 里、`close(&self)` 只有 `&self`——**没有任何安全手法能从 `Arc` 里单独 drop 一个字段**；改成 `Option` + `take()` 就退回 v0.8 的坑，改成 `Mutex<Option<Sender>>` 又等于每帧一次锁。正确做法是反过来：**writer 自己的退出由取消信号 + 排空队列决定**，不依赖 sender 计数（见 4.6 第 3 步）
- **`reader` / `writer` 的 `JoinHandle` 必须放进 `Session`，而且要包在 `Mutex<Option<_>>` 里**。v0.7 的关闭流程写「关闭两端」，但 Session 里没有任何句柄能操作那两个 task——既不能 await 它们退出，也不能确认 sink/source 已经 drop，这是「文档写了、代码做不到」的典型。而 `close(&self)` 要从 `&self` 里**拿走**句柄去 await，普通字段同样做不到，所以必须是 `Option`（`take()` 之后 await）；`Drop` 里拿不到 `.await`，只能 `abort`
- **两个 task 都要 `select!` 取消信号**，不只是 reader。writer 卡在 `sink.send().await`（对端不读、socket 缓冲满）时，仅取消 reader 是关不掉的
- **reader task**：`loop { select! { msg = source.recv() => ..., _ = shutdown.cancelled() => break } }`，按 kind 分流——`Response` 查 pending 并 `oneshot.send()`；`Request` 按 `service_id` 找服务、再按 `method` 分派；`Cancel` 触发对应 token
- **writer task**：`select! { msg = rx.recv() => sink.send(msg).await, _ = shutdown.cancelled() => { while let Ok(msg) = rx.try_recv() { sink.send(msg).await? } ; break } }`——收到取消后**先把队列排空再退出**，单一写者避免竞争，退出即 drop `sink`（见 4.6）
- **writer task 是 transport 差异的吸收点**：无论底层是 socket 的内核缓冲还是 shm 的满环，上层看到的都是同一个 bounded mpsc

`pending` 里存的不能是裸 `oneshot::Sender`，而是带 **`Drop` guard** 的 `PendingEntry`：guard 被 drop 时（无论因为拿到响应、超时、取消还是调用方被 panic unwind）都要 `pending.remove(id)` 并把 permit 还给配额计数。不这么做就会出现 4.2 第 ⑧ 步那种「以为自然清理、其实永久泄漏」。

注意 `tx` 里流动的是 `Bytes` 而不是 `Frame`——L2 已经把语义头拼进 `Bytes` 了，L3 不需要再理解结构。

### 4.2 一条 RPC 的完整生命周期

```
① Client: next_id.fetch_add(1) → id（非 0）
② Client: outbound.try_reserve(id, PendingEntry { tx })   // 出站配额检查 + 插入，同一把锁
③ Client: tx.send(bytes)                        ──[ 通道 ]──►
④ Server: header.parse → registry[service_id] → inbound.try_acquire(shape) → 登记 Cancel token → spawn
⑤ Server: （在 spawn 出来的 task 内）codec.decode(payload) → handler 执行
⑥ Server: 编码响应并发回                        ◄──[ 通道 ]──►
⑦ Client: pending.remove(id) → tx.send(resp)；guard 归还 permit
⑧ 超时/取消：guard 被 drop → remove + permit 归还 + 顺手发 Cancel
```

**第 ④ / ⑤ 步的顺序是 v0.8 写死的，两处都不能挪：**

- **decode 不在 reader loop 里**。8 节第 1 条说了「反序列化不要占住 reader」，而 v0.7 的 4.2 却写成「按 5.3 分到 async / blocking」——读起来像是在 reader 里同步 decode。正确的是：reader 只做头解析与路由，`Bytes` move 进 spawn 的 task 后再 decode；`#[photon(blocking)]` 方法则在 `spawn_blocking` 的闭包里 decode + 执行，一次线程切换干两件事
- **Cancel token 必须在 reader 侧登记完再 spawn**。如果等 spawn 出来的 task 起来才登记，紧跟 `Request` 到达的 `Cancel` 会扑空（找不到 id），取消就静默丢失了。改法是：reader 解析出 `request_id` 后先 `cancel_map.insert(id, token)`，再 spawn；handler 结束时由 Drop guard 移除。未知 id / 已完成的 `Cancel` **静默忽略**，不回错误、不记 error 级日志（它天然是竞态，报错只会刷屏）
- **入站配额在第 ④ 步用 `try_acquire` 抢，抢不到就当场回 `code = 1`**（v0.10，配合 4.1 的出站/入站两表拆分）。入站没有 entry 可插，所以不能套出站那套 `try_reserve`；正确形态是「计数 +1 或返回 `Overloaded`」，成功则拿到一个 guard，handler 结束（无论正常、报错、panic）由 guard 减 1。这一步在 spawn 之前，避免先起 task 再发现超额。**注意此时是回包侧**，`Overloaded` 走 `to_wire` 白名单变成 `code = 1`（见 3.8），这与「发起侧 `Overloaded` 永不上线」不冲突——那是同名的两个东西。

**第 ⑧ 步是 v0.7 修掉的一个真实泄漏。** 原文写「超时则 drop(oneshot)，`pending` 里的 sender 因发送失败自然清理」——不成立：drop 的是 receiver，map 里的 entry 要等响应真的回来才会发送失败；服务端永不回包，entry 和配额就永久留着。必须由 `Drop` guard 显式回收（见 4.1），并且顺手发一条 `Cancel`，给还能停下来的 `async` handler 一个机会。

> 之所以不用左右双栏的 ASCII 图：中文字符是双宽，竖线永远对不齐，每次改文案都要手工数空格。编号步骤流没有这个维护税。

### 4.3 并发、取消、超时

**并发**：一条通道天然支持任意多 in-flight 请求，但必须有**显式上限**（`pending` 是 `HashMap`，本身不设界）。检查发生在**插入前**：超出配额立即返回 `Err(Overloaded)`，而不是靠 `mpsc` 的 bound 间接反压——后者会让调用方卡在 `send()` 里无声排队，丢掉"过载"这个明确语义。

> **同一个词在两端的含义必须分开（v0.8）**：本端配额满 → `RpcError::Overloaded`（本地错误，请求根本没发出去）；服务端配额满 → 回 `code = 1`，客户端拿到的是 `Remote { code: 1, .. }`。不要把它在客户端再映射成 `Overloaded`，否则「谁该负责放弃」这件事就糊了（见 3.8）。

配额是**两组而不是一组**：`async_in_flight` / `blocking_in_flight`。理由见 5.3——同步方法吃的是 blocking pool 的线程，异步方法吃的是 task，共用一组额度会让慢的同步请求饿死异步请求。

> **但这只对服务端成立（v0.9 补）**：`blocking_in_flight` 约束的是「本端要开多少个 blocking 线程去跑 handler」，**客户端不跑 handler，它只有一组出站配额**。`SessionOptions` 里两个字段都保留是为了让同一份配置结构能被两侧复用，但客户端构造时 `max_blocking_in_flight` 不生效、也不该因此拒绝请求。不写这条，实现者很容易在客户端上莫名其妙地收到 `Overloaded`。
>
> **这组入站配额与出站配额是两张表，不是一张（v0.10）**：`max_async_in_flight` / `max_blocking_in_flight` 只约束**入站**（对端发来、本端要跑 handler 的请求），`OutboundTable.async_used` 只约束**出站**（本端发出、等响应的请求）。双向会话里两边都在发也都在收，混成一张表会让「我方过载」与「对方过载」共用一个数字——正好是过载语义最需要区分的地方（见 4.1、3.8）。

配额检查与插入的**原子性**由 4.1 的出站 `try_reserve` 保证：计数和 `pending.insert` 在同一把 `parking_lot::Mutex` 内完成；入站侧没有 entry 可插，用 `try_acquire` + guard 保证「检查成功才 +1、guard drop 才 -1」。两侧都不要用 `AtomicUsize` 的「先读、再 CAS」写法——两个并发调用可以同时通过检查然后一起超额，而且 CAS 失败重试会让过载判定变得不可复现。

**取消**：`Cancel{request_id}` 到达服务端后，dispatch 层通过为该请求挂的 `CancellationToken`（`tokio_util`）中断 handler。**这条只对 `async fn` handler 成立**，同步 handler 的语义见下。

> 这个位置必须在设计阶段就留出来。事后补补不了——那时 handler 已经把 `&self` 拿走了，没有中断点。

**同步 handler 不可中止（v0.5 起）。** 这是 `fn` 方法与 `async fn` handler 的语义分界，必须写死：

- `Cancel` 到达 → 标记为已取消，handler **继续跑完**，结果丢弃，不回 `Response`
- 请求方超时 → 同上，服务端仍在计算
- 因此 **长耗时的计算不要写成 sync 方法**：几毫秒以上的要么改成 `async fn` 配 `select!`，要么自己拆成可中断的片段

> 协作式取消（handler 多接一个 `&ServiceContext` 首参、内部轮询 `is_cancelled()`）是可选逃逸口，但它会改变**所有** handler 的签名形状，放到 M3 之后（见 12 节第 14 条）。
>
> 这条限制不是设计缺陷，是被迫的诚实：**同步代码里没有可中断点，任何声称能「停掉」它的机制都是假的。** 与其给一个静默失效的 Cancel，不如明说它只丢弃结果。

**超时**：两层。
- 客户端侧：`tokio::time::timeout` 包住 oneshot 的 await；超时或 future 被 drop 时由 `CallGuard` 回收 entry 与 permit，`PendingEntry::drop` 再发一条 `Cancel`（不是靠「sender 发送失败自然清理」，那条是错的，见 4.2）
- 协议侧：由头里的 `deadline_ms`（见 3.2）表达，0 表示无期限，非 0 是**剩余预算**：发送方按「从发送时刻起还剩多少毫秒」填，接收方**从解析完头开始**计时（所以实际可用 = 值 + 传输延迟，见 12 节第 17 条）。服务端尽力中止（`async fn` 能真停；同步 handler 只能丢弃结果，见上）。客户端应在超时前留出一点余量，否则等自己判超时的时候服务端还在算

> **「尽量」这两个字必须写进文档，不然 M3 实现会以为自己在做保证（v0.8）**：给每个请求挂一个 `tokio::time::sleep`，在万级 QPS 下就是万级 timer 注册，成本不便宜。所以第一版的口径是——**客户端超时是准确的（本端 `timeout` 包住 oneshot），服务端 deadline 只是尽力（能停就停，停不下来就继续跑完再丢结果）**。真要精确，M3 可以换粗粒度 timer wheel，但那是优化，不是语义承诺。

**背压**：出站 `mpsc` 必须 bounded。写满时 `send().await` 阻塞生产者——这是唯一正确的背压来源。注意 shm 场景下是**两级队列串联**（mpsc + 环），总容量应显式配置，否则会出现「以为设了 1024 实际只有 16」的错觉。

### 4.4 双向对等，不做严格的 client/server 角色机

两侧都能发 `Request`（服务端回调客户端、推送事件）。`Session` 必须对称设计，`Server` / `Client` 只是用法上的便利封装，底层是同一个 `Session`。

但要说清一个当前能力的边界：**「双向对等」指的是通道能力，不是 handler 的签名。** 服务端可以在 handler 之外拿着 `Session` 句柄主动向对端发 `Request`（推送事件、回调），这条现在就成立；而「handler 内部反过来调用对端」需要 handler 多接一个 `&ServiceContext` 首参才能拿到 `Session`——那会改动所有 handler 的签名，**当前不做**（见 12 节第 14 条）。别把这两件事混成一句「完全对称」。

> 与 4.6 的关系要理清（v0.10）：`GoAway` 是**连接级**的「开始收尾」——两边都停止发起，但两边都继续把已进入 handler 的活回完，所以不破坏「双向对等」。它不是「客户端命令服务端关掉」，两侧都可以发，也都是同一个语义。注意状态机不是对称推进的：`egress` 一收到 `Open → Draining`，`ingress` 则一直保持 `Open` 到「双方都宣告过关闭」为止（见 4.6 的触发表）。

### 4.5 `PayloadMode`：零拷贝的代价必须显式选择

**先把一个容易误解的地方说清（v0.7）：`ZeroCopy` 在 v1 和 M5 里指的不是同一件事。**

| 阶段 | `ZeroCopy` 的实际含义 | 省掉的是什么 |
|---|---|---|
| v1（serde codec） | 入站 payload 不额外拷到独立堆缓冲，直接带着 `Bytes` 进 decode | 一次 memcpy；**decode 仍产出 owned `T`** |
| M5（`rkyv`） | handler 直接读 `&Archived<T>`，decode 也不产 owned | memcpy + 反序列化两次开销 |

所以 v1 的 `ZeroCopy` 收益有限（省一次拷贝），它的主要价值是**提前验证接口形状**——如果连 `memory` 上的 `Bytes` 直传都做不到，shm 阶段必然要返工。别指望 v1 打开它就有数量级提升。

这是 v0.2 新增的一条，因为零拷贝引入了一个 v0.1 没有的问题：

> **零拷贝把 buffer 的生命周期和 handler 的执行时长耦合在了一起。**

shm 的零拷贝路径下，`payload` 是一块指向环形队列槽位的 `Bytes`。如果 handler 把它 `await` 很久才处理，槽位就一直不能释放，环很快被填满，整条通道停摆。

因此必须给用户一个显式开关：

```rust
pub enum PayloadMode {
    /// 默认。借用型 payload（shm 槽位）先拷到独立堆缓冲再交给 handler；
    /// 对 owned payload（tcp / local / memory 的 Bytes）是 no-op
    Copy,
    /// 高风险高收益。仅对声明为同步 `fn` 且显式标注的方法开放，见下方护栏
    ZeroCopy,
}
```

**`Copy` 只对借用型 payload 才有成本，这一点必须写在枚举的注释里（v0.8 修）。** v0.7 把 `Copy` 定义成「先拷到独立堆缓冲」，可 TCP / local / memory 交出来的 `Bytes` 本身就是 owned + `'static`（`from_owner` 或 `BytesMut::freeze`），**再拷一次没有任何收益**——而 `Copy` 偏偏是默认值，等于在默认 transport 上白付一次 memcpy。正确表述是：

| transport | `Copy` 的实际行为 |
|---|---|
| tcp / local / memory | **no-op**，`Bytes` 直接进 dispatch |
| shm + 档 A（借用槽位） | 真拷一次到堆缓冲 |
| shm + 档 B（块池） | 归还块之前拷出，或改为「handler 结束时归还」，见附录 B.4 |

所以「默认 Copy」不是「默认慢」，只是「默认安全」；只有 M5 的 shm 零拷贝路径才需要为它付钱。

策略：**默认 `Copy`**，`ZeroCopy` 必须显式开启并在文档里写明约束。理由是「框架不该在用户不知情时把内存池的生命周期绑到他的业务代码上」。

**v1 先落方法级，全局开关随 shm 一起落（v0.26 改判）。** 原计划是两层：`SessionOptions::payload_mode` 全局默认 + `#[photon(zero_copy)]` 逐方法 opt-in。实际做的时候发现全局那一层**没有任何可依附的行为**：v1 的 tcp / local / memory 交出来的 `Bytes` 全是 owned + `'static`，拷一次没有收益，所以「全局 Copy」不会让任何一条路径变慢，也不会让任何一条路径变安全——收下不用的参数就是坏参数（与 `Memory::pair` 去掉 `opts` 同一条理由）。

已经落地的部分：

- `PayloadMode { Copy（默认）, ZeroCopy }` 进 `photon`；
- `Service::payload_mode(method) -> PayloadMode`，默认 `Copy`，宏只给显式标注的方法生成覆盖；
- `#[photon(zero_copy)]` **只对同步 `fn` 放行**，`async fn` 与 `#[photon(blocking)] fn` 在展开期报错（理由见下一条）。

M5 加全局开关时**用户代码不用改**——属性已经存在，只是「全局默认」与「方法 opt-in」两个条件都要成立才真正走零拷贝：全局是 `Copy` 时方法标了也没用，全局是 `ZeroCopy` 但方法没标仍然 Copy。这样「默认关、逐方法开」的语义在 v1 就已经定死，只剩一个还没落地的总闸。

**但不要承诺「超时就把 payload 拷出来」——那做不到。** 字节的所有权一旦移交给 handler，dispatch 就没有办法从外部把它收回来，更没法把它「换成一份已拷贝的副本」：拷贝的前提是你还持有来源，而此时来源就在对面那个 `Bytes` 里。这条 v0.4 及以前写过的兜底是假的，已删除。

真正的兜底只有两个，而且都必须**在交给 handler 之前**决策：

1. **按 handler 形态**：v1 的 `ZeroCopy` **只对 `fn`（无 await 点的同步方法）开放**，且方法必须显式标注 `#[photon(zero_copy)]`。这看似和「慢 handler」无关，实际恰恰相关——真正会长期攥着 chunk 的不是 `#[photon(blocking)]`（那是线程池里一次性跑完的短任务），而是**跨 `await` 的 async handler**：它可以在等数据库的时候把槽位占上几毫秒。v0.7 用 `#[photon(blocking)]` 判定「慢方法」，**选错了对象**。
2. **按 payload 大小**：超过阈值（如 4 KiB）强制 Copy，只有小消息才走零拷贝——大 payload 占槽位的时间长，收益/风险比反而更差。

任何跨 `await` 的 async handler 一律 Copy，不提供开关。这不是保守，是「槽位归还」这件事本身要求 handler 在可预期的短时间内结束，而 `await` 天然不可预期。

事后只剩检测：debug 下记录 payload 从交出处到 drop 的时长，超过阈值打 `tracing::warn`，并把「槽位平均占用时长」做成指标。慢 handler 在 `ZeroCopy` 下的后果是环被填满、整条通道停摆——这个症状迟早会暴露，但暴露得太晚，所以必须先有指标。

### 4.6 优雅关闭

**先把一个说法纠正掉（v0.10）：`GoAway` 的「效果」是双向停止发起，但关闭状态仍按方向各记一份。** 两侧对称（4.4），各有自己的出站与入站，所以有两条半关闭要分别推进；v0.7 只写「发起方关两端」，等于假设了严格的 client/server 角色机，v0.8 又矫枉过正成「只关入站」。这两句话在 v0.9 的正文里同时残留过（本节开头一处、8 节「GoAway 只关一方向」一处），现在是同一个口径：**报文表达的是「我开始收尾」，落到状态上就是两个方向各自推进。**

状态按方向各记一份：

| 方向 | `Open` | `Draining` | `Closed` |
|---|---|---|---|
| 出站（本端 → 对端） | 接受新请求入 `mpsc` | 拒绝新请求（`RpcError::Closed`），已入队的照常写 | `sink` 已 drop |
| 入站（对端 → 本端） | 正常 dispatch | 不再 dispatch 新 `Request`（回 `Remote { code: 7 }`），仍收 in-flight 的 `Response` | `source` 已 drop |

两个方向进入 `Draining` 的触发点**并不对称，而且入站要晚得多（v0.10 定案）**：

| 触发 | 出站 `egress` | 入站 `ingress` | 理由 |
|---|---|---|---|
| 本端主动 `close()` | `Draining` | **保持 `Open`** | 只停「发起」，不停「收活」。此刻若把 ingress 也置 Draining，对端在收到我们 `GoAway` **之前**发出的请求就会被本地打成 `Remote{7}`——那是它合法发出的请求，我们没有任何依据拒绝 |
| 收到对端 `GoAway` | `Draining` | **保持 `Open`** | 同上，而且更明确：对端一旦发出 `GoAway` 就不再发起，此后到达的都是它在 `GoAway` 之前发出的，属于必须回完的那一类 |
| 双方都已发过 `GoAway`（双向收尾） | — | `Draining` | 到这里「还会有合法的在飞请求」这个前提已经不成立：两侧都宣告过停止发起。此后到达的 `Request` 回 `Remote{7}`（**明确拒绝，不静默丢弃**），然后随 reader 退出转 `Closed` |
| 收尾超时（默认 5 s）或急停 | `Closed` | `Closed` | 强制收尾 |

**所以 `Remote{7}` 不是「关闭一开始就到处回」的东西**，它只在「两侧都宣告关闭之后还有请求冒出来」这个窄窗口里出现——那种流量要么是协议违规的对端，要么是迟滞到最后一刻的极端情况。早回 `Remote{7}` 会把合法在飞请求打死，这正是 v0.9 第 1 步「双向同时 Draining」与本节承诺第 ③ 条打架的地方。

`GoAway` 的语义定死为：**「本端开始关闭这条连接」——① 我不再接你发来的新 `Request`（收尾窗口内也不再 dispatch）；② 我也不再发新 `Request`；③ 但你已发来的请求，我仍会把 `Response` 回完。** 换句话说：**「在飞」的定义是「已进入 handler」，不是「已写到线上」**——后者不可观测，前者可以计数，也只有前者能给出可验证的保证。

> **不要再写「与 gRPC 的 `GOAWAY` 语义一致」（v0.10）。** gRPC/HTTP2 的 `GOAWAY` 带 `last_stream_id` 水位，客户端据此判断哪些流还算 in-flight、哪些要重试；本项目 v0.7 正是把这个字段删掉了（它在对称会话下没有良定义：两侧各有一套 id 空间）。没有水位，就没有「按 id 划边界」这个能力，能承诺的只有「已进入 handler 的请求一定回包」。写「一致」会让实现者以为有同样的边界保证。

由此，收到 `GoAway` 的一方要做的事也明确了：**本端出站立即进 `Draining`（新请求返回 `RpcError::Closed`），入站继续 dispatch 并收在飞 `Response`**。要等反方向的 `Response` 也收完，双方各自发一条 `GoAway` 即可——`GoAway` 不再承担「对方不许再发」以外的任何语义。

这一句话同时解掉 v0.7 空着的三个问题：

- **收到 `GoAway` 的一侧怎么处理新到的 `Request`**：继续 dispatch，直到「双方都已宣告关闭」才拒（回 `Remote { code: 7 }`）。已经在 UDS / pipe 缓冲里的请求不可能「撤回」，既不能静默丢弃（调用方会吊死在超时上），也不能提前拒绝（会把合法请求打死）
- **发起方的出站 pending 要不要等**：要等。清空的是「**本端发出的**、还没拿到 `Response` 的请求」，它们的响应来自对端的**出站**方向，而 `GoAway` 只让对端停止「新发起」，不禁止它回包。v0.7 写「本端 in-flight 清空」没说方向，容易实现成「等对端发来的请求处理完」，方向反了
- **收到 `GoAway` 的一方怎么知道自己该退出了**：排空即退，**不等 EOF**。socket 上 EOF 只是加速路径，named pipe / shm 连 EOF 都没有；所以收到宣告后要起一个后台收尾任务（`spawn_finish_when_drained`）：等 `pending` 与 `handlers` 双零（`close_timeout` 兜底）→ `shutdown.cancel()`。没有这条，会话会一直挂着，直到用户自己再调 `close()`
- **谁关哪半边**：谁 `Draining` 完谁关自己的那一半。出站 `Draining` 结束时等队列排空 → writer 退出 → drop `sink`；本端 pending 与 handler 都清零后取消 reader → reader 退出 → drop `source`

步骤（优雅关闭，`flags` bit2 = 0）：

1. 写一条 `GoAway` 进 `tx`（**先进队列、排在已有帧之后，才能保证它前面那些 `Response` 都能出去**）；本端 `state.egress = Draining`——新请求直接 `Closed`，**不动 `tx`**（通道还要用来回包）。**入站不动**：`state.ingress` 保持 `Open`，继续 dispatch、继续回包（理由见上表第 1 条）
2. 等待两类在飞工作归零：本端发出的 outbound `pending`（等 `Response`）、对端发来的 in-flight handler（要回 `Response`）。这期间 `tx` 照常可用
3. 两类都归零后，**只做一件事：`shutdown.cancel()`**。不要试图 drop `Session` 的 `tx`——`Session` 在 `Arc` 里，`close(&self)` 拿不到 `&mut`，从 `Arc` 里单独 drop 一个字段在 Rust 里做不到；改回 `Option::take` 或加 `Mutex` 都只是把问题换了个地方（前者要求独占所有权、与回包任务并发冲突，后者每帧一次锁）。**通道的收口交给 writer 自己**：writer 收到取消后 `while let Ok(msg) = rx.try_recv() { sink.send(msg).await }` 排空再退出。这条路径不依赖任何 sender 计数，也不会漏掉排在 `GoAway` 之后的帧
4. 两个 task 各自退出：writer 排空队列、退出并 drop `sink`；reader 退出并 drop `source`。**writer 不需要任何 `flush`**——最后一次 `send().await` 返回 `Ok` 就意味着数据已交给内核缓冲，紧接着 drop `sink`（见 2.1 的 drop 契约）。**这正是 2.1 删掉 `MessageSink::flush` 的依据**
5. `Session::close()` 里 **从 `Mutex<Option<JoinHandle>>` 里 `take()` 出来 await 两个句柄**。`JoinError`（panic / 被 abort）必须当场处理并归为断连，不能 `let _ = handle.await` 吞掉
6. 兜底：`close_timeout`（默认 5 s）内仍没走完 → 强制 `shutdown.cancel()` + `abort` 两个 task，把所有 pending 与 handler guard 置 `Closed`。这一步保证「对端根本不配合」时 `close()` 也会返回，而不是永远挂着

> **排空与「谁还可能再写」的关系要说清。** 第 3 步之所以敢在第 2 步之后就排空退出，是因为第 2 步已经保证 pending 与 handler 双零——此后没有任何合法的 `tx.send()` 来源。仍然存在的竞争只有两种：某个 handler 恰好在第 2 步读到零的瞬间完成（它属于「已进入 handler」，此时它的 `Response` 已经在队列里或正在 `send`，`writer` 的排空照样会写出去），以及应用层在 `close()` 之后硬塞请求（那会被 `egress = Draining` 挡掉，返回 `Closed`）。排在排空循环之后的极少数迟到帧会拿到 `send` 错误——那是「队列已关」，不是数据丢失。

急停（`flags` bit2 = 1）：跳过 1–3，直接 cancel 两个 task、drop 两端、所有 pending 立刻 `Closed`。

发起急停的入口是 `Session::abort()`（没有对应到 `SessionHandle` 上——句柄是「发起请求」用的，允许它单方面把整条连接打断没有意义）。它做三件事：`try_send` 一条 bit2 的 `GoAway`（不排队等，急停不该被自己的背压拖住）、取消所有已进入 handler 的 token、置 `abrupt` 后 `fail()`。**`abrupt` 必须在取消之前置位**，否则 writer 醒来时可能还没看到它，把一批本该丢掉的帧写出去。writer 因此有两套收尾：优雅关闭排空，急停丢弃并把条数写进 `tracing::debug!(dropped, ...)`——丢弃本身是允许的，静默丢弃不是。

> **`source.close()` 不再是必需的，因为 `close` 已经不存在（2.1）。** 打断挂在 `recv` 上的 reader 只需要取消 token：`select!` 的另一个分支一旦完成，`recv` 这个 future 就被 drop，`source` 随之 drop。v0.7 里「必须有 `close`」的论证，本质上是「必须有办法让 `recv` 退出」，取消 token 已经满足它。

> v0.7 删掉了 `GoAway{last_request_id}`。原因是它在对称会话下没有良定义：发送方说的「最后已处理 id」指的是**对方**的 id 空间，可两侧各有一套计数器（见 3.3.1），这个字段到底该填谁的值根本说不清。真正需要的信息本端都有，没必要放进线格式。

shm 场景下这一步比 socket 更需要显式做——socket 断了有 `EPIPE`，共享内存不会给你任何信号，对端进程消失后你的环会**安静地停在那里**。详见附录 B.7。

---

## 5. L4 类型安全 API

### 5.1 Service trait

```rust
pub trait Service: Send + Sync + 'static {
    fn call(self: Arc<Self>, method: u64, payload: Bytes)
        -> BoxFuture<'static, Result<Bytes, RpcError>>;

    /// 这个方法占哪一类入站额度；默认 `Async`，宏只给 `#[photon(blocking)]` 的方法生成 `Blocking`。
    fn schedule(&self, method: u64) -> Schedule { Schedule::Async }
}
```

> `schedule` 是 v0.19 加的，理由是**扣额度的时点早于进 handler**：L3 在 `service_id + method` 决定要不要放行时，还看不到实现里的 `spawn_blocking` 分支。把它做成「由生成代码交出的一个枚举」比「让 L3 猜」或「让 handler 自己扣额度」都便宜——没有 blocking 方法时宏干脆不生成这个方法，trait 的默认实现接管。

> 接收者是 `self: Arc<Self>` 而不是 `&self`。v0.5 说「`&self` 编译不过」却把签名写成 `&self`，v0.6 修正了这个自相矛盾。`Arc<Self>` 是稳定支持的接收者类型，`Arc<dyn Service>` 依然可调，不损 dyn 兼容性。

> `BoxFuture` 是自己定义的类型别名（`type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>`），**不要为了这个别名引入 `futures` crate**——那会让默认依赖数从 8 变 9，纯属为一行代码买单。

> Rust 1.75 之后已有 AFIT，**不要再用 `async-trait` 宏**。但 trait object 场景仍需显式 `BoxFuture`。

**v0.2–v0.4 里这个签名一个字没改；v0.5 指出了接收者必须换，v0.6 才真正改到签名上。** 参数与返回类型始终没变，原因是 `Bytes::from_owner` 让「零拷贝」和「`'static` 签名」可以共存——shm 送来的 `Bytes` 内部指向共享内存，但类型上仍是普通 `Bytes`，于是 L4 完全不必知道自己收到的 payload 来自哪个 transport。这是整个设计的收敛点。

**响应侧暂时不做零拷贝（明确的范围收缩）**：`Service::call` 返回 owned `Bytes`。handler 若想让响应也直接指向 shm 槽位，得自己构造 `Bytes::from_owner`，前提是它能拿到 transport 的 chunk 分配器——那会打破 L4 对 L1 的无知。所以 M5 只做**入站零拷贝**，出站先走堆。不要误以为两边都零拷贝。

**接收者必须是 `Arc<Self>`。** 同步方法要走 `tokio::task::spawn_blocking`，它的闭包要求 `'static + Send`——需要把服务对象**所有权**移进闭包。而 `&self` 是借用，永远拿不到 `'static`，所以 `fn call(&self, ...)` 这一版在 blocking 分支上根本编译不过。

可选的做法只有两种：让注册表存 `Arc<dyn Service>` 再在分支里想办法克隆（做不到——`&self` 无法重建 `Arc<Self>`），或直接改接收者。所以选后者：`self: Arc<Self>`，dispatch 里 `let me = self.clone();` 就能 move 进闭包。

这条看起来只是类型细节，但它决定了 dispatch 层的形状，**必须在 M2 就按最终形态写**，否则后面要返工一次。

**`Service` 不负责服务路由。** `service_id` 到服务的映射在 `ServiceRegistry`（6 节机制 1），`Service::call` 只处理 `method`。这样同一个 `Session` 能挂多个服务，而每个服务的实现完全不知道自己是第几号——路由是会话层的事，不是业务层的事。

**`Service::call` 的签名与 handler 形态无关。** 无论 handler 是 `async fn` 还是 `fn`，返回的都是 `BoxFuture`——同步方法在 dispatch 里被包成一个 async 块，并在**首次 poll 内就地执行完毕**。也就是说**调度差异不进这个签名**，只体现在 dispatch 的分支里（见 5.3）。

### 5.2 声明形态：三档

```rust
#[photon::service(id = 7)]
pub trait Calc {
    /// ① 异步 handler：会 await（查缓存、调下游 RPC）
    async fn add(&self, a: i64, b: i64) -> Result<i64, RpcError>;

    /// ② 同步 handler：纯计算、不 await、很轻
    fn fast_hash(&self, data: Vec<u8>) -> Result<u64, RpcError>;

    /// ③ 同步但会阻塞：读文件、同步 DB 驱动
    #[photon(blocking)]
    fn load(&self, path: String) -> Result<Vec<u8>, RpcError>;
}
```

**`id` 是宏的必填参数（v0.8 补）。** v0.7 只在 6.1 说「`service_id` 由用户显式分配」，5.2 的示例却写裸 `#[photon::service]`——宏从哪拿 id、生成的 client stub 怎么知道要填哪个 `service_id`，都没写。现在定死：

- `#[photon::service(id = N)]`，`N` 是 `u16`，**必须显式写**，不自动编号（自动编号会在「不同 crate 各自注册」时撞号，且顺序一变线上 id 就变）
- 可选 `error = MyError`：声明后方法写 `Result<T, MyError>`，`MyError` 需实现 `photon::ServiceError`；不写则方法返回 `Result<T, RpcError>`。两种写法不能混在同一 trait 里
- 宏把 `N` 同时嵌进 client stub（发请求时填 `service_id`）和 `register(registry, svc)`（注册时占号），一处写、两处用，不存在不一致
- 同一个 `ServiceRegistry` 里撞号 → **注册时直接 `Err`**，不是覆盖（见 6 节机制 1）
- `id = 0` 允许但建议避开，`0` 没什么特殊含义，只是最容易手滑重复的那个值

**唯一的语义规则，必须记牢：**

> **`fn` 表示「服务端实现同步完成」，不是「客户端阻塞调用」。**

不守这条就会绕回 v0.4 那个把两件事绑死的错误。声明形态描述的是**服务端契约**；生成的客户端永远是异步的，同步客户端是另一件事（见 5.5）。

这个语义和 Rust 语言本身一致：`fn` 就是同步完成，`async fn` 就是可能挂起。用户不需要额外记一套规则，只需要接受一个提醒——**trait 写的是服务端长什么样，不是调用方长什么样。**

宏展开产出三样东西：

1. **Client stub** `CalcClient`：**三个方法全部是 `async fn`**（线上都是异步，与声明形态无关）
2. **Server dispatch**：三分支，调度策略在生成期固定（见 5.3）
3. **`BlockingCalcClient`**：仅当用户写 `#[photon::service(id = 7, blocking)]`（且 crate 开了 `blocking` feature）时生成，三个方法全部是 `fn`（见 5.5）

同一个 trait 里混写三档是允许的，宏逐方法读 `async` 关键字与属性即可。

> 注意两个 `&self` 是两回事：**trait 里照常写 `&self`**（用户视角的服务对象），宏生成的 `Service::call` 才用 `self: Arc<Self>`（见 5.1）。用户不需要为框架的调度需求改自己的签名。

**`blocking` 不能靠 `#[cfg(feature = "blocking")]` 生成（v0.7 修正）。** proc-macro 展开出的 `cfg` 检查的是**使用方 crate** 的 feature 集合，不是 photon 的。所以「`feature = "blocking"` 时生成 `BlockingCalcClient`」按字面根本不成立——用户开 `photon/blocking` 并不会让这段 `cfg` 变成真。

解法只有两个，选后者：

- 在 photon 侧按 feature 重导出两个宏入口（`service` / `service_with_blocking`），用户换宏名
- **给属性加参数**：`#[photon::service(id = N)]` 默认只生成异步客户端，`#[photon::service(id = N, blocking)]` 额外生成同步客户端。这是**用户显式要求**的能力，语义上比「自动跟随 feature」更清楚，也不存在 cfg 跨界问题

所以 `blocking` feature 控制的是 **photon 库内那套支持设施**（`session/blocking.rs`、打包 runtime 的代码），而「这个服务要不要同步客户端」由宏参数决定——两件事不要混。

### 5.3 调度分流：编译期决定，运行期零判断

宏展开出的 dispatch：

```rust
// 每个分支都返回 BoxFuture，形状与 5.1 的 call 一致
match method {
    HASH_ADD => Box::pin(async move { me.add(a, b).await }),

    // fn：包成 async 块，首 poll 就执行完 —— 不换线程、不加调度
    HASH_HASH => Box::pin(async move {
        let data: Vec<u8> = codec::Default::decode(&payload)?;
        let out = me.fast_hash(data)?;        // 同步 `fn` 直接出 Result，不 await
        codec::Default::encode(&out)
    }),

    // 唯一会换线程的分支：decode 也在闭包里，JoinError 映射成 RpcError
    HASH_LOAD => Box::pin(async move {
        let me = me.clone();
        tokio::task::spawn_blocking(move || {
            let path: String = codec::Default::decode(&payload)?;
            let out = me.load(path)?;
            codec::Default::encode(&out)
        })
        .await
        .map_err(|join| RpcError::Internal(format!("blocking handler 未正常结束: {join}")))?
    }),
}
```

**注意 spawn 的位置**：每个请求的 `tokio::spawn` 只发生一次，在 L3 里（`tokio::spawn(svc.call(method, payload))`），不在这些分支里。三个分支回答的只是「这次执行落在哪个线程池」，不是「各自再 spawn 一次」。

**代价要说清**：`fn` 分支不换线程，意味着它在 poll 它的那个 worker 上**就地跑完**。微秒级无所谓，毫秒级就是 worker 被占住——这正是 5.3 下面那道 debug 告警存在的理由。

**`spawn` 出去的 `JoinHandle` 必须被 await，否则 handler panic 会静默丢失（v0.7）。** L3 那个 `tokio::spawn(svc.call(...))` 如果直接把句柄丢掉，handler 一 panic，客户端只会等到超时——日志里什么都没有，排障时完全看不出是 panic。正确做法是 reader/dispatch 侧 `match handle.await`：`Ok(Ok(resp))` 正常回包，`Ok(Err(e))` 回错误体，`Err(join_err)`（panic 或取消）**编码成 `code = 0`（`Internal`）回包**（客户端拿到的是 `Remote { code: 0, .. }`，见 3.8），本端同时记 `tracing::error!`。`spawn_blocking` 分支同理，`JoinError` 一样要处理。

> 这条的代价是每个请求多一次 `await` 点——本来就在 task 里，等于零成本。省掉它换来的却是「panic 无声」。不值。

> **sync/async 是编译期属性，不是运行期元数据。** 每条分支的调度策略在宏展开时就写死，运行期零判断、零查表；协议里也不需要任何「这是不是同步方法」的标记。

顺带把 5.4 的理由补全：`match` 的每个分支是具体类型，编译期就能把调度策略内联进去；换成 `HashMap<u64, Box<dyn Fn>>` 之后，这些信息在运行期只剩一个函数指针，没法分流。

#### 默认 `spawn` 还是 `spawn_blocking`？

这是个真取舍，没有免费答案：

| | 默认 `spawn`（包成 Ready future） | 默认 `spawn_blocking` |
|---|---|---|
| 微秒级纯计算 | 不换线程、不加调度 | 多一次跨线程池调度，反而更慢 |
| 毫秒级阻塞 | 拖慢 runtime（starvation） | 正确 |
| 代价 | 依赖用户记得标注 | 为罕见场景给所有轻量 handler 加税 |

**决策：默认 `spawn`。** 理由是 RPC handler 绝大多数就是「内存里算一下 + 查个缓存」，都在微秒级；而 `spawn_blocking` 的调度开销比 `spawn` 高一个量级。给 99% 的路径免票、让 1% 显式买票，才符合零成本抽象。

但安全不能押在「用户记得标注」上，所以加一道兜底：

> **debug 构建下测量 handler 实际耗时，超过阈值（建议 100 µs）打 `tracing::warn`，提示「这个 handler 应加 `#[photon(blocking)]`」。**

运行时诊断比编译期猜测可靠得多——宏没法知道你会不会在 handler 里读文件。

#### 配额必须拆成两个

`async_in_flight` 与 `blocking_in_flight` 分开计数，`blocking_in_flight` 默认等于 blocking pool 大小。

原因：同步方法占的是 **blocking pool 的线程**（默认上限 512），异步方法占的是 **task**。共用一组额度会出现一种很难查的故障——一批慢的 `#[photon(blocking)]` 请求把额度占满，此时异步请求明明完全跑得动，却也收到 `Overloaded`。两类资源混进一个计数器，过载信号就失去意义了。

### 5.4 为什么用 `match` 而不是 `HashMap<u64, Box<dyn Fn>>`

- 编译器能把密集 `match` 优化成跳转表，比哈希查找快
- **method id 重复能在编译期报错**（宏生成时检测）
- 不需要类型擦除，handler 可直接持有泛型参数

### 5.5 客户端：异步与同步两个变体

```rust
// 默认：异步客户端（无论方法声明是 async 还是 fn）
let handle = connect::<TcpTransport>(&ep, ConnectOptions::default()).await?;
let c = CalcClient::new(handle);
c.add(2, 3).await?;

// 这个客户端上所有请求的期限（毫秒，0 = 不设期限）
let strict = CalcClient::new(handle.clone()).with_deadline(50);
strict.slow_op(200).await?;   // 50 ms 处返回 Err(Timeout)

// #[photon::service(id = 7, blocking)]：额外生成同步客户端
let c = BlockingCalcClient::connect(ep)?;
c.add(2, 3)?;
```

> 注意这里没有「`feature = "blocking"` 就生成」——那种写法在 proc-macro 里不成立（见 5.2）。`blocking` feature 管的是框架侧支持设施，宏参数管的是「这个服务要不要同步客户端」。

#### 内部实现必须绕开 `Handle::block_on`

`Handle::block_on` 有两条死路：

- 在 `current_thread` runtime 里 → **死锁**（future 永远没机会被 poll）
- 在 multi_thread 的 worker 线程里 → **panic**：`Cannot block the current thread from within a runtime`

正确做法是「投递 + 阻塞等待」，而不是「在本线程跑 future」：

```rust
fn blocking_call<T: Send + 'static>(
    &self, fut: BoxFuture<'static, Result<T, RpcError>>, timeout: Duration,
) -> Result<T, RpcError> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    self.handle.spawn(async move {
        // 超时必须包在这里：外面 recv 一旦卡死，future 也没人管
        let r = tokio::time::timeout(timeout, fut).await
            .unwrap_or(Err(RpcError::Timeout));
        let _ = tx.send(r);
    });
    // 比内层多留一点余量，优先让内层给出更准确的 Timeout
    match rx.recv_timeout(timeout + Duration::from_millis(50)) {
        Ok(r) => r,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(RpcError::RuntimeDropped),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(RpcError::Timeout),
    }
}
```

> **`rx.recv()` 是 v0.6 的一个硬伤（v0.7 修正）：服务端被杀，它会永久阻塞。** 这直接和 M3 验收的「杀掉服务端 100ms 内收到明确错误」矛盾。两个超时必须都设：内层 `tokio::time::timeout` 管 future，外层 `recv_timeout` 管「连 runtime 都没回话」的兜底。少了外层，runtime 线程池被饿死时照样挂住。

区别是关键：**`block_on` 在当前线程执行 future**（所以会和 runtime 打架）；**`spawn + recv` 在 runtime 线程执行 future，当前线程纯等待**（不 panic、不死锁）。

三个容易漏的细节：

- `T` 必须 `Send + 'static`：`std::sync::mpsc` 的发送端要求 `T: Send`，`handle.spawn` 要求 future `'static`
- **告警要在进入阻塞等待之前打，而且只打一次**（用 `AtomicBool` 去重）。每次调用都 warn 会把日志刷废，反过来等于没有告警
- **自建的 `Runtime` 必须被 client 结构体持有**。只存 `Handle` 是不够的：`Runtime` 一 drop，所有 task 被取消，`recv_timeout` 立刻返回 `Disconnected`，映射成 `RuntimeDropped`。这个错误变体正是为这种情况准备的

#### 安全阀：`try_current()` 只能告警，不能硬拒绝（v0.7 修正）

v0.6 的设计是「检测到 runtime 上下文就直接拒绝，debug panic / release 返回 `Err`」。**这条会误杀一个完全合法的用法**：在 `spawn_blocking` 闭包里调 `BlockingClient`。

原因：`spawn_blocking` 的线程**是有 runtime 上下文**的（否则它内部没法 `tokio::spawn`），所以 `Handle::try_current()` 返回 `Ok`——而阻塞池里做同步等待恰恰是官方推荐的「从 async 里调阻塞 API」的写法。Tokio 没有公开 API 区分「worker 线程」和「blocking 线程」，所以任何硬拒绝都会把对的用法一起毙掉。

改成三层：

1. **不做硬拒绝。** `blocking_call` 照常执行
2. **debug 下告警**：在 runtime 上下文里调用时打 `tracing::warn`，附一句「若你确定自己在 `spawn_blocking` 内，可忽略」
3. **文档里写清真正的危险场景**：`current_thread` runtime 的 worker 上做同步等待 → 必死锁（只有一个线程，future 永远没机会被 poll）。multi_thread 上只是浪费一个 worker，不会死

硬拒绝唯一真正想拦的就是第 3 条，但它拦不准，代价却是把合法用法打死。**宁可告警 + 文档，也不要一个会误杀的 panic。**

同步客户端最危险的用法不是「用错」，而是**被人顺手在 async 函数里调了一下**。告警比 panic 更合适：前者让人知道发生了什么，后者让人去查一个并不存在的 bug。

#### runtime 从哪来

两个构造入口，避免 N 个 client 起 N 套线程：

- `BlockingCalcClient::connect(ep)` —— 自建一个只有 1 个 worker 的 multi_thread runtime（它只跑 IO，1 线程足够）
- `BlockingCalcClient::with_runtime(&shared, ep)` —— 多个 client 复用一个 runtime

#### 为什么不默认生成

同步客户端**不是默认产物**：它需要 `blocking` feature（框架侧设施）+ 用户显式写 `#[photon::service(id = N, blocking)]`（宏侧参数）。两个开关都打开才存在。多数 async 服务永远不需要它，而它的代价是「多一套 runtime 生命周期 + 一个告警面」——这类能力不该默认存在（同 1.3.1 的默认集原则）。

#### 明确不做

**不给 `fn` 声明自动生成 `fn` 客户端方法。** 那会把「handler 形态」和「调用形态」重新绑死，正好绕回 5.2 想拆开的那个错误。要同步客户端，是用户的**显式选择**（宏参数），不是声明形态的自动推论。

### 5.6 本地 / 远程透明

同一个 `Service` trait，既可以被远程 client 调用，也可以被**本地直接调用**（跳过序列化）。注意这个「本地直连」不是 transport——它根本不走通道，与 2.2 里澄清过的 `memory` 实现是两件事。把 transport 换成 `Memory` 或 `Shm`，调用方代码一行不改——这是这个框架长期最有价值的部分。

三档声明形态不影响这一点：本地直连同样按声明形态分派——`async fn` 直接 `await`，`fn` 直接调；若标了 `#[photon(blocking)]`，**本地直连时由调用方自己负责线程池**，框架不替它调度（那时框架根本不在链路上）。

---

## 6. 可扩展性机制

### 插槽 1：`Transport`

换通道：TCP / 本地 socket / 共享内存 / 进程内。**v0.2 已把它从「字节流」改造成「消息通道」，这是为了真正装得下 shm**（论证见附录 B.8）。

### 插槽 2：`Codec`

换序列化：postcard / rkyv / protobuf。`encode_into<B: BufMut>` 的形状让 shm 可以直接编码进槽位。注意 rkyv 不只是「换 crate」——它要把 handler 参数类型改成 `&Archived<_>`（见 3.6）。

### 插槽 3：`Interceptor`

中间件链：tracing、metrics、超时、重试、鉴权。

> 不想自己写就直接复用 `tower::Service` / `tower::Layer`，tokio 生态的超时、限流、重试、`tracing` 中间件全是现成的。建议 M3 接进来，不要自己撸一套。

### 插槽 4（预留，未实现）：调度策略

当前的调度策略（v0.5 起）是**每个方法编译期固定**（`spawn` / `spawn_blocking`）。若将来要支持「某组方法走专用线程池」（独立 DB 池、限并发的 CPU 密集池），扩展方向是给 trait 或方法加 `#[photon(executor = "db")]`，由 `ServiceRegistry` 按名字查一张 executor 表。

**当前不做，但结构上不挡**：调度决策在 5.3 的 `match` 分支里就是一个字面量，改成按名字查表只是把那一行换成「查表 + 分发」，`Service::call` 的签名仍然不动。

### 机制 1：`ServiceRegistry`

`service_id: u16` → `Arc<dyn Service>` 的表，支持动态注册 / 注销，为插件化与热更新留位置。

**`service_id` 由用户显式分配，不自动编号。** 自动编号会让「同一个服务在两次运行里拿到不同 id」变成默认行为，而 id 一旦出现在线上，重启就意味对端全部路由失败。显式分配还有一个好处：它天然是一份「服务清单」，重复 id 在注册时直接报错。

第一版可以静态分发（编译期把表建成常量），但**数据结构**要按可动态增删的形状留出来。

### 机制 2：版本协商与控制帧

**先定一条硬规则：所有控制帧（`Handshake` / `GoAway` / `Cancel`）的 payload 都是手写定长字段，永不经过 `Codec`。**

理由是鸡生蛋：`Handshake` 要交代的就是「我用哪个 codec / 哪个协议版本」，用它自己去解自己等于循环依赖。控制帧字段少、格式稳定，手写编解码的成本几乎为零，还顺带避免了「codec 未定时无法通信」的死锁。

`Handshake` payload 定长 **12 字节**（小端，v0.8 从 16 缩到 12，v0.9 换掉 `payload_mode`）：

| 字段 | 宽度 | 含义 |
|---|---|---|
| `protocol_version` | u16 | 当前定为 1；对端不同直接断开 |
| `flags` | u16 | 保留，必须 0 |
| `max_message_len` | u32 | **本端愿意接收**的单帧上限（含 24B 头），取自 `SessionOptions::max_message_len`（v0.10 迁入），见下方规则 3 |
| `codec_id` | u16 | 由 `Codec::ID` 提供（见 3.6）。0 = postcard，1 = rkyv（M5）。**不是位图**：codec 是构建期选择，泛型方法不是 dyn-safe，不存在「运行期挑一个」 |
| `reserved` | u16 | 必须 0。v0.8 的 `payload_mode` 从这里移除——理由见下 |

**`payload_mode` 不该出现在握手里（v0.9）。** 它回答的是「**本端**怎么处理入站 payload（要不要拷贝）」，纯本端策略，对端既不需要知道、也没有能力影响。把它放在握手里，等于制造一个「两边不一致时怎么办」的未定义行为——而 v0.8 的三条 fail-fast 规则根本没覆盖它。正确的位置只有两个：`SessionOptions::payload_mode`（全局默认）+ `#[photon(zero_copy)]`（逐方法 opt-in），见 4.5。握手因此省下 1 字节，`reserved` 从 u8 变 u16 仍保持 12 字节总长，不用改对齐。

**协商规则，三条 fail-fast + 一条纯信息，没有「取交集」这种柔性逻辑：**

1. `protocol_version` 不等 → 断开（`Protocol`）
2. `codec_id` 不等 → 断开。不要试图「降级到共同支持的那个」——双方各自只编译进了一种 codec，不存在第二个选项
3. `max_message_len` **不是「取小」，而是两个方向各用各的**（v0.9 修正方位错误）。v0.8 写「取小后写进本端 `FramedSource`」，反例很简单：A 声明能收 1 MiB、B 声明 64 KiB，取小得 64 KiB，于是 **A 的 reader 会拒收 B 发来的 500 KiB 帧**——而 A 明明声明过自己能收，B 也完全有权发。正确关系是：

   - **本端 reader 上限 = 本端自己声明的值**（握手后从 64 KiB 放宽到它；「取小」这一步根本不存在）
   - **本端 writer 上限 = 对端声明的值**（`send` 前检查，超了立刻 **本地** `RpcError::InvalidInput`，不要等对端断连才发现）

   这两个数都要落到 `FramedSource` / `FramedSink` 的 `Arc<AtomicU32>` 旋钮上。**通路是 trait 上的两个访问器**（`recv_limit()` / `send_limit()`，见 2.1）：L3 在把半身 move 进 task 之前取出 `Arc`，`Ready` 之后 `store`；L1 只负责比较数字。v0.8 完全没有 writer 侧检查，等于「声明了能收多少，但自己发多少没人管」。
4. `reserved` 非 0 → 断开（`Protocol`）。未知位不要静默忽略，这条与 3.4 的 flags 规则一致
5. **`max_message_len` 还有下限，shm 上还有上限**（v0.10 补）：下限 `≥ 24`（否则连空 payload 的帧都收不了）；shm transport 上还要求 **`max_message_len ≤ slot_size`**——否则会声明一个自己根本装不下的单帧上限，对端按你的声明发满一槽，这边永远读不出来。两处都在建会话前校验（shm 用段头的 `slot_size`），不满足直接拒绝，不要等跑起来才报帧超限。

**`ring_capacity` 不在 Handshake 里（v0.8 移出）。** 它原来是鸡生蛋：容量必须两侧一致，可协商容量的这条 `Handshake` 本身要走那个还没建好的环。正确做法是——**容量写在 shm 段头的元数据区**（见附录 B.3），创建方建段时写入，attach 方读出来后校验，`Handshake` 只做一次「我读到的容量 = 我实际能用的容量」的断言。控制帧这条路因此完全不需要知道自己跑在什么 transport 上。

**握手状态机**（这套规则不写死，M0 的 echo 和 M1 的 Session 会各写一套错误处理）：

| 状态 | 允许的动作 | 违规处理 |
|---|---|---|
| `Connected` | 只能收/发 `Handshake` | 收到任何其它 kind → 立刻断开（`Protocol` 错误） |
| `Negotiating` | 等对端 `Handshake` 回包 | 超时 5s → 断开 |
| `Ready` | 所有 kind | 收到第二个 `Handshake` → 断开 |

**握手前的帧长上限必须是保守固定值（64 KiB），握手成功后才按上面的规则各就各位。** 否则一个恶意的 `Handshake` 就能让你按 64 MiB 去分配缓冲，协商机制自己成了攻击面。写入方式只有一处：L3 在 `Ready` 之后把两个值分别写进 `FramedSource` / `FramedSink` 的 `Arc<AtomicU32>`（见 2.3）——L1 不认识握手，只认识一个上限数字。

> 握手阶段带版本号，比后期写一堆兼容补丁便宜一百倍。

---

## 7. 技术选型清单

| 用途 | 依赖 | 备注 |
|---|---|---|
| 异步运行时 | `tokio`（`rt-multi-thread`, `net`, `io-util`, `sync`, `time`, `macros`） | 只用一个 runtime |
| 零拷贝缓冲 | **`bytes`（≥ 1.10.1）** | `Bytes` 贯穿全链路；`Bytes::from_owner` 1.9.0 引入、1.10.1 修掉它的内存泄漏，故下限取 1.10.1 |
| 序列化 | `serde` + **`postcard`** | Codec trait 隔离，便于替换；bincode 已停止维护（RUSTSEC-2025-0141），不采用 |
| 错误 | `thiserror`（库）/ `anyhow`（示例） | 库必须给结构化错误 |
| 观测 | `tracing` + `tracing-subscriber` | 每个请求一个 span，带 `service_id` + `method_name` + `request_id`（只有 id 的 span 排障时基本没用，见 3.5）；**`tracing-subscriber` 只在示例与 dev 依赖里，不进主 crate** |
| 取消 | `tokio-util`（`CancellationToken`、`codec`） | |
| 本地 IPC | **无**（`tokio::net::UnixStream`） | Unix 手搓 UDS；Windows named pipe 推迟，不为统一抽象引第三方依赖 |
| 低开销锁 | `parking_lot` | pending map 用 |
| cache line 对齐 | `crossbeam-utils`（`CachePadded`） | **M5 shm 必需**，避免伪共享 |
| proc-macro | `syn`（`full`）、`quote`、`proc-macro2` | |
| method 哈希 | `xxhash-rust`（`xxh3`） | **只在 `photon-macros` 里用于编译期哈希**，不进 `photon` 的运行期依赖集，所以 0 节那「默认不超过 8 个」仍然成立（见 3.5） |
| 基准测试 | `criterion` | M4 |
| 共享内存（M5） | `shared_memory`（跨平台映射）或自研 `memfd_create`+`mmap` | Windows 走 `CreateFileMapping`；参考 `iceoryx2` 的实现 |

---

## 8. 风险与坑

> **引用规则（v0.10 收紧）**：1–36 保留编号（外部已有引用），**新增条目一律用具名锚点**（例「见 8 节『`Arc` 里置空字段』」），不再编号、也不再用「第 N 条」。原因是纯数字引用只要插入一条就全错，v0.7 已经踩过一次。

### 通用

1. **反序列化不要放在 reader loop 里。** 一个大 payload 会卡住整条通道的多路复用。dispatch 到独立 task——这条同时要求消息是 `Send + 'static`（见 3.7）。
2. **pending map 用 `parking_lot::Mutex`**，不要用 `tokio::sync::Mutex`——持锁期间没有 `await`，用 async mutex 白多一层调度。
3. **出站队列必须 bounded。** 否则背压全丢，慢消费者能吃穿内存。
4. **接收侧必须校验消息长度上限。** 否则一条伪造的超长帧就能打爆进程（流式 transport 在 `framing.rs` 里做）。
5. **不要在 L1 里 `match` 协议语义。** 一旦破例，`MessageSource` / `MessageSink` 就退化成摆设。
6. **不要上 io_uring。** 队列深度为 1 时它比 epoll 慢，且部署环境常被 seccomp 屏蔽（附录 A）。
7. **不要一上来做零拷贝。** 先 `PayloadMode::Copy` 把正确性做对。
8. **`Bytes` 与生命周期**：`decode` 用 `DeserializeOwned`，第一版不要引入带借用的反序列化，否则整个 API 被生命周期参数污染。
9. **Unix socket 文件残留**：进程崩溃后 `.sock` 还在，下次 bind `EADDRINUSE`，需显式 unlink 或先探测。

### 消息导向接口带来的新坑

10. **`AnySource` 的 box 会吃掉性能。** 每条消息一次堆分配，在 shm 的百 ns 级目标下不可接受（socket 路径上几十 ns 尚可容忍）。性能敏感路径直连泛型 `Server<T>`。**推论：所有基准测试禁止使用 `AnySource` / `AnySink`**（`benches/` 与 M4/M5 的全部数字都必须走泛型），否则测到的是门面开销而不是框架开销。另注意 `AnySource` 只消除「N 个 transport × M 个 handler」的单态化组合爆炸，**不消除泛型向上传染**：`Session<S, K>` 沿 `Server` → `connect` → 客户端方法 一路带下去，想擦除就得在每个边界上都擦（见 2.5）。
11. **`impl Future` 的 trait 不可 dyn。** 如果想在别处用 `Box<dyn MessageSource>`，编译不过。方案只有两个：接受每次 box，或用枚举门面。不要试图绕。
12. **两级队列串联的容量错觉。** mpsc bound × shm 环容量，实际在飞上限是两者较小值，需显式对齐。

### shm 特有（M5，见附录 B.7）

13. **槽位状态机必须用 release/acquire**，先写完 payload 再置 Ready，否则消费方读到半条消息。
14. **伪共享会静默吃掉一半性能。** `head` / `tail` 各占一条 64B cache line，用 `CachePadded` 包。
15. **共享内存里绝不能存裸指针。** 两个进程映射到不同虚拟地址，所有引用必须是相对 base 的偏移。
16. **崩溃检测必须自己做。** 对端进程消失不会给你 `EPIPE`，环会安静停住，需要心跳 + generation 重置。
17. **`/dev/shm` 残留**：进程被 kill 后共享内存段还在，启动时要能识别并清理。

### 同步方法与同步调用带来的新坑（v0.5）

18. **`spawn_blocking` 的闭包要 `'static + Send`。** 所以 `Service::call` 的接收者必须是 `self: Arc<Self>`——`&self` 是借用，无法重建所有权，编译不过。这条在 M2 就要按最终形态写。
19. **`Handle::block_on` 在 runtime 内会 panic 或死锁。** `BlockingClient` 必须用「`spawn` + `recv_timeout`」，并把 `try_current()` 只当告警信号——它在 `spawn_blocking` 线程上同样是 `Ok`（见 5.5、8 节第 24 条）。
20. **共用一组 `in_flight` 会让 blocking 请求饿死 async 请求。** 拆成 `async_in_flight` / `blocking_in_flight` 两组（见 5.3）。
21. **同步 handler 的 `Cancel` 只丢弃结果，不会停机。** 长耗时计算不要写成 `fn`（见 4.3）。
22. **`spawn_blocking` 里不能 `.await`。** 所以 `#[photon(blocking)]` 的 handler 必须是纯同步的——内部绝不能再 `block_on` 一个异步 API，那会变成嵌套等待，比直接阻塞更容易死锁。

### 线格式与收尾路径（v0.7）

23. **handler panic 会静默变成「客户端超时」。** `tokio::spawn` 的 `JoinHandle` 不 await，就没人知道 task 是 panic 了还是还在跑。所有 spawn 点都必须 `match handle.await` 并映射 `JoinError`（见 5.3）。
24. **`try_current()` 在 `spawn_blocking` 线程上返回 `Ok`。** 拿它当「是否在 async 上下文」的判据会误杀官方推荐的用法，只能告警不能硬拒绝（见 5.5）。
25. **超时不回收 `pending` 就是泄漏。** 不要指望「sender 发送失败自然清理」——响应不来就永远不清理，配额也不会释放。必须 `Drop` guard（见 4.1/4.2）。
26. **`GoAway` 里放「最后已处理的 id」在双向会话下说不清。** 两侧各有自己的 id 空间，这个字段没有良定义，已删（见 4.6）。
27. **控制帧不能用 `Codec` 编码。** `Handshake` 要交代 codec 身份，用它解自己就是循环依赖；而且握手阶段 codec 还没确认。控制帧一律手写定长（见 6 节机制 2）。
28. **Windows named pipe 没有可信的消息边界，也没有半关闭。** 消息模式能创建，但读侧一条消息超出缓冲就返回 `ERROR_MORE_DATA`，边界仍要自己算；关写句柄又等于结束整个 pipe 实例。所以将来做 Windows `local` 时：必须走 framing、必须靠 `GoAway` 收尾（见 2.6）。现在这段推迟，不写没人验证的代码。
29. **`Bytes::from_owner` 的 owner 必须 `Send`，且 `bytes` 下限是 1.10.1。** 少一个 `Send`，`Bytes` 就不再是 `Send`，铁律③当场破；用 1.9.x 会踩它和 `to_vec` 的内存泄漏。
30. **选依赖前先查 RUSTSEC。** bincode 的死法是「维护者直接发一个只含 `compile_error!` 的 3.0.0」，不是没人维护那么简单——`cargo deny` 会拦，CI 会红。核心依赖每年复查一次。**line format 里出现的对齐假设同理**：20 字节头让 rkyv 无法原地读，这类「不是 bug 但堵死未来」的约束，越早发现越便宜。

### 写进代码就改不动的地方（v0.8）

31. **带泛型方法的 trait 不是 dyn-safe，所以「运行期协商 codec」永远做不成。** `Codec::encode_into<T>` / `decode<T>` 一旦带泛型参数，`Box<dyn Codec>` 就不存在，`codec::Default` 也必然在编译期定死（`Session` 不带 `C` 参数，v0.10）。想做「按连接挑 codec」，只能先把泛型方法收进 `enum AnyCodec` 门面（每个变体静态调用），代价是每个方法生成两条解码路径。所以 v0.8 直接定成构建期选择（见 3.6）。
32. **错误码留白等于没有错误码。** 「`1..=0xFFFF` 全留给框架、未知一律 `Internal`」听起来很保守，实际结果是服务端过载、未知服务、未知方法、参数解码失败**全部没有码可发**，客户端只能看到 `Internal`——排障时连「你调用错了」和「框架坏了」都分不开（见 3.8）。
33. **协商出来的参数如果没有通路，就等于没协商。** `max_message_len` 是在 `FramedSource` 里执行的，而按铁律② L1 不认识握手——于是「握手成功后放宽上限」这句话没有任何代码能实现。解法是给 L1 一个 `Arc<AtomicU32>` 的不透明旋钮（见 2.3）：L1 只比较数字，不知道这个数字从哪来。
34. **别拿传输层数字当端到端目标。** 共享内存能到 300 ns 的是「裸 ping-pong」；端到端 RPC 每请求要过 `Box::pin` + `spawn` + oneshot + 两次 mpsc，固定成本就是 100–200 ns。把两个数混成一个验收标准，等于给自己定一个结构上不可能达到的 KPI（见 9 节 M5 与 B.2）。
35. **SPSC 环的顺序推进会让慢 handler 堵住整条环。** `tail` 只能顺序前进，请求 1 慢、请求 2 快，2 的槽位也不能先归还。「环形队列天然支持并发在飞」只在 payload 不被借用时成立；借用了就必须上块池 + 自由链表（见 B.4）。
36. **`GoAway` 是「连接级开始收尾」，但两个方向的 `Draining` 触发点并不对称。** 只写「发起方关闭两端」等于偷偷假设了严格 client/server 角色，与 4.4 的双向对等直接冲突；反过来写成「一收到 `GoAway` 就把 ingress 也置 Draining」又会在对端 `GoAway` 之前发出的请求上误回 `Remote{7}`。正确形态：两个方向各有一份 `Open → Draining → Closed` 状态，`egress` 在 `close()` 或收到 `GoAway` 时立即 Draining，`ingress` 只在「双方都宣告过关闭」或超时兜底时才 Draining（见 4.6）。
- **「关闭时把字段置空」在 `Arc` 里做不到（v0.10）。** v0.9 的关闭第 3 步写「drop `Session` 的 `tx`」，但 `Session` 被 `Arc` 共享、`close(&self)` 只有 `&self`：`Sender` 是普通字段，既不能单独 drop，`Option::take` 又要 `&mut`；套 `Mutex<Option<Sender>>` 则每帧一次锁。同类问题还有 `reader`/`writer` 的 `JoinHandle`（要从 `&self` 里取出 await）。正确形态：`tx` 保持普通 `Sender`、通道收口交给 writer 自己（取消后排空 `try_recv` 再退），句柄放 `Mutex<Option<JoinHandle>>` 以便 `take()`（见 4.6 第 3/5 步）。**凡是「关闭时把某个字段置空」的写法，先问一句这个类型是不是在 `Arc` 里。**
- **`Session` 的 codec 泛型参数是假的（v0.10）。** `Service::call` 签名里没有 `C`，宏在 `codec::Default` 上做 en/decode，`C` 只贡献握手的一个 `C::ID`——写 `Session<S, K, OtherCodec>` 会宣告 A 的 id、线上却是 B 的字节，静默错线格式。已删该参数，全项目只用 `Default::ID`（见 4.1、3.6）。

---

## 9. 里程碑

按「能独立验证」的顺序切，不按「功能完整性」切。

### M0 — 传输与消息帧（1–1.5 天）

- [ ] **`MessageSource` / `MessageSink` / `Transport` trait（消息导向形状）**
- [ ] `transport/framing.rs`：长度前缀 + 读满 + 上限校验
- [ ] `Tcp` 实现
- [ ] `proto/header.rs`：**24 字节**语义头的手工 `encode` / `decode` + 逐 kind 字段校验（属 L2，但 M0 一并做掉，否则无法验证端到端）
- [ ] **`memory` 变体（进程内 channel）**——用它验证消息导向接口能不能真的塞进零拷贝
- [ ] `examples/echo.rs` 可 `cargo run --example echo` 直接跑通（见 10.4）

**验收**：`Tcp` 与 `memory` 两个实现共用**同一套传输层测试**（`tests/transport.rs`，不依赖 `Session`——M1 才引入），一行不改。这一步过了，说明 L1 抽象成立；如果 `memory` 塞不进去，现在返工代价为零。

### M1 — 多路复用（1–2 天）

- [x] `Session`：pending map（**带 `Drop` guard 的 `PendingEntry`**）+ reader task + writer task
- [x] `Request` / `Response` 语义
- [x] `Handshake` **最小版**：固定 12 字节、版本 1、首帧强制、握手超时与握手前 64 KiB 上限（完整协商留到 M3）
- [x] `Transport` 换成 `local`（Unix 手搓 UDS，M0 已落地；Windows 分支推迟，见 2.6）
- [x] 用 `cargo test --features local` 跑通本地 IPC：`tests/transport.rs` 21 条 + `tests/session.rs` 8 条（同一套会话用例在 `memory` 与 `local` 上各跑一遍）
- [x] `shutdown` token 覆盖 reader **与 writer** 两个 task，让两边都能被唤醒并非阻塞退出（v0.8 删掉了 `MessageSource::close`，见 2.1）
- [x] 并发测试：100 个并发请求乱序返回，互不串包
- [x] **零拷贝形状验证**：在 `memory` 上返回 `Bytes::from_owner`，确认全程没有额外拷贝
- [x] `GoAway` / `close()` 六步：对端关闭后新调用在毫秒级内变 `Closed`，不挂在 pending 表上
- [x] handler 的 panic 兜底：服务实现 panic 时回 `code 0`，连接不被一条坏请求打死

**验收**：把 transport 从 TCP 换成本地 IPC，`Session` 代码一行不改；`memory` 的 zero-copy 路径跑通；首帧非 `Handshake` 会被断连。

### M2 — 类型安全 API（2–3 天，最小集）

- [x] `photon-macros`：`#[photon::service(id = N)]`（独立 crate，`id` 必填，见 5.2）——v0.17 落地
- [x] 生成 client stub + `match` dispatch——v0.17 落地（`CalcClient` / `CalcService<S>` / `register_calc`）
- [x] method 哈希（**只哈希方法名**）碰撞与重复检测 + **方法名表** + golden 哨兵测试——v0.17 落地（`xxh3("add") = 0xffb352e13fd28e80`）
- [x] **`Service::call` 用 `self: Arc<Self>` 接收者**（为 `spawn_blocking` 预留，见 5.1；用 `&self` 后面要返工）
- [x] `ServiceRegistry` 按 `service_id` 路由（表先静态构建，结构留动态；撞号注册报错）
- [x] **错误体提前落地**（3.8 的 `code` 表 + `RpcError::Remote`）——M1 已随 `proto/error_body.rs` 完成：只有把 `2` / `3` / `4` 这几个码做出来，M2 的「未知服务 / 未知方法 / 参数解码失败」才有可观测结果，否则只能都拍成 `Internal`，验收没法写
- [x] 示例组 `local_calc` / `calc_server` / `calc_client`，覆盖 10.3 的**第 1、2、5 三种形态**——v0.18 落地，输出与 10.5 的 M2 四条逐字一致
- [x] `tests/concurrency.rs`：同一套测试在 `tcp` 与 `memory` 上全绿——v0.18 落地（32 并发 + 100 乱序 × 2 条传输）
- [x] 本阶段**只支持 `async fn` 声明**；`fn` 与 `#[photon(blocking)]` 放到 M3（见 5.2）——v0.17 由宏在展开期强制

**验收**：两进程跑通；改方法签名后编译失败提示清晰。**注意 M2 不包含 Notification / 超时 / GoAway / 同步 handler**——v0.6 把 10.3 的七种形态全压给 M2，而 M2 又只支持 `async fn`，按现文根本不可能通过（v0.7 拆分）。

### M3 — 健壮性（3–4 天，全量形态）

- [x] `Notification`、`Cancel`（+ `CancellationToken` 贯通）、per-request timeout（`deadline_ms` 生效，口径是「客户端准、服务端尽力」，见 4.3）——v0.20 落地（`CallGuard` + `PendingEntry::drop` 发 `Cancel`、`SessionHandle::notify`）
- [x] `Handshake` **完整协商**（`max_message_len` 双向落位：reader = 本端声明、writer = 对端声明；`codec_id` 校验在 M2 之后不再变；`payload_mode` 不进握手，见 4.5）——机制 v0.16 已落，v0.22 补齐两个方向各自的证据与三种建连期拒绝
- [x] `GoAway` 优雅关闭 + 急停（`flags` bit2）+ 按 4.6 的状态表实现两个方向——v0.21 落地（`Session::abort()`、`ingress_draining` 只在双方宣告后置位、收到宣告后排空即收尾）
- [x] `PayloadMode` 策略落地（默认 Copy；`ZeroCopy` 只对 `fn` + `#[photon(zero_copy)]` 开放，见 4.5）——v0.26 落地（`Service::payload_mode` + 展开期护栏；全局开关随 shm）
- [x] 业务错误映射（`#[photon(error = MyError)]` → `code ≥ 0x1_0000` 的 `Remote`）——v0.25 落地（`ServiceError` 双向 trait + debug 断言码段）；错误体本身的 `code 0..7` 已在 M2 落地
- [ ] `Interceptor` 链（或接 `tower`，走 `interceptor` feature）
- [ ] `tracing` span 贯穿（span 带 `method_name`，见 3.5）
- [x] `examples/calc_local.rs`：两进程本地 IPC，验证「换 transport 不改业务代码」——v0.23 落地（同一二进制 `server` / `client` 两角色，默认 `/tmp/photon-calc.sock`）
- [x] **`fn` 声明 + `#[photon(blocking)]` 两种展开**（见 5.2 / 5.3）——v0.19 落地（三分支 + `schedule`）
- [ ] **`BlockingClient`**（`blocking` feature + `#[photon::service(id = N, blocking)]`）+ `recv_timeout`
- [x] 双 `in_flight` 配额（`async` / `blocking` 分开计数）——v0.19 落地（`InboundQuota` 两张表 + `try_acquire(Slot)`）
- [x] debug 模式的慢 handler 告警（阈值 100 µs，见 5.3）——v0.19 落地
- [x] 示例覆盖补齐 10.3 全部七种形态——v0.24 落地（`local_calc` 八行，含 `with_deadline` 与同步 handler）

**验收**：杀掉服务端进程，客户端在 100ms 内收到明确错误而非挂死；示例七种形态全跑通。

### M4 — 性能（按需）

- [ ] `criterion` 基准：吞吐、单次调用延迟、并发扩展性
- [ ] 与 `tarpc`、裸 `tokio` 横向对比
- [ ] 据此决定：是否上 `rkyv`、是否上共享内存

### M5 — 共享内存（附录 B）

- [ ] `shm` transport：SPSC 环 + 相对偏移 + cache line 隔离
- [ ] 块池 + chunk 索引（大消息路径）
- [ ] `rkyv` codec（与 shm 配套才有完全零拷贝）
- [ ] 心跳、崩溃检测、channel 重置
- [ ] 与 socket 路径的 A/B 基准（延迟、吞吐、CPU）

**验收分两级，两个数不能混成一个（v0.8 改口径，v0.9 补测量条件）：**

1. **传输层裸 ping-pong**（只有 L1 + 一个固定 24 字节头，不含 codec / Session / dispatch）：busy-poll + 拷贝 ≤ 600 ns；busy-poll + 零拷贝 ≤ 300 ns。这才是附录 B.2 那张表的口径
2. **端到端 RPC**（stub → Session → dispatch → codec → 返回，含每请求一次 `Box::pin + spawn`）：**≤ 3 µs**，现实值 1–3 µs

> **这两个数都必须带条件，否则验收无法判定（v0.9）。** 测量条件是：单机同 NUMA 节点、物理核 ≥ 4（**不是**容器 / vCPU 共享环境）、独占 CPU、`tokio` worker 数 = 2、单连接、payload 64 B、连续 100k 次调用、取 **P50**，busy-poll 侧固定 pin 在一个核上（不 pin 的数字会因调度迁移抖 2–3 倍）。这些条件不写，「3 µs」在 CI 的共享 runner 上永远测不出来，最后只能被悄悄改成 30 µs。

300 ns 不可能成为端到端目标：`Box::pin` + `tokio::spawn` + 一个 oneshot + 两次 mpsc 的固定成本已经是 100–200 ns 量级，剩下的编解码和唤醒还没算。真要亚微秒端到端，只能给 shm 专门设计一条免 spawn 的快路径（reader 内联 poll dispatch 或 per-worker 队列），那会推翻 5.3「spawn 只在 L3 发生一次」——**不在 M5 范围内，明确不做**。

另外两条：`memory` / `shm` / `tcp` 三套 transport 共用同一套一致性测试并且全绿；**rkyv 侧额外要求** payload 起点 8 字节对齐成立（24 字节头 + 槽位 64 对齐，见 3.2 与 B.4），否则零拷贝路径不成立。

---

## 10. 示例与运行

v0.2 及以前只在里程碑里提过「echo 测试」「CalcService 两进程跑通」，没有把示例当成交付物来设计。这不够——**一个 RPC 框架如果不能在一条命令内跑起来，它的抽象就还没有被真正验证过。**

### 10.1 示例清单

| 示例 | 进程数 | transport | 阶段 | 验证什么 |
|---|---|---|---|---|
| `echo` | 1 | `tcp` | M0 | 24 字节头编解码、长度前缀、消息边界正确 |
| `local_calc` | 1 | `memory` | M2 | **最小集**全链路（stub → session → dispatch → codec → 返回），第 1/2/5 三形态；**一条命令可跑** |
| `calc_server` / `calc_client` | 2 | `tcp` | M2 | 真·跨进程 IPC，首次端到端（同样只验最小集） |
| `local_calc`（补齐形态） | 1 | `memory` | M3（v0.24 已落地） | 七种形态全跑（Notification / 超时 / GoAway / 同步 handler） |
| `calc_local` | 2 | `local` | M3（v0.23 已落地） | 换 transport 不改业务代码（架构验收） |

### 10.2 为什么必须有一个「单进程示例」

两进程示例需要开两个终端、启动顺序不能错、端口可能被占。这对"第一次跑通"是额外摩擦，而摩擦通常被误判为"框架有问题"。

所以 `local_calc` 刻意设计成**单进程内同时起 server 与 client、走 `memory` transport、一条命令完成全部往返**。它同时承担三个角色：

1. **M2 的验收工具**：跑不通就是链路有问题，无需解释
2. **抽象的验收工具**：`memory` 与 `tcp` 共用同一套 `Session` 代码
3. **新人的第一课**：不需要理解 IPC 就能看到框架在干什么

### 10.3 示例要覆盖的调用形态

示例不该只是 `add(2, 2)`。它应当同时是一组冒烟测试，按阶段分两批覆盖：**M2 只做 1、2、5 三种**（此时还没有 Notification / 超时 / GoAway / 同步 handler），M3 才补齐全部七种：

1. 一个成功的一元调用（M2）
2. 一个返回业务错误的一元调用（M2，错误体走 3.8：`Remote { code ≥ 0x1_0000 }`）
3. 一次 `Notification`（M3）
4. 一次超时调用（M3，靠头里的 `deadline_ms`，确认拿到 `Timeout` 而不是挂死）
5. 一次并发调用（M2，同时发 32 个，确认响应不串包）
6. 一次优雅关闭（M3，`GoAway` 优雅模式，确认 in-flight 正常完成后再退出）
7. 一次**同步 handler**调用（M3，`fn` 声明，验证不 `await` 也能正常分发，见 5.2）

这样"示例能跑"就等价于"主要路径没坏"，不必再单独维护一套脆弱的集成测试。但**每阶段的预期输出要分开写**，否则 M2 的验收会永远红着。

### 10.4 运行方式

单进程（推荐先跑这个）：

```
cargo run --example local_calc
```

两进程（两个终端，TCP）：

```
cargo run --example calc_server -- --addr 127.0.0.1:7001
cargo run --example calc_client -- --addr 127.0.0.1:7001
```

两进程（两个终端，本地 IPC；只在 Unix 上，`local` 不是默认 feature）：

```
cargo run --features local --example calc_local -- server /tmp/photon-calc.sock
cargo run --features local --example calc_local -- client /tmp/photon-calc.sock
```

裸字节往返（M0）：

```
cargo run --example echo
```

约定：

- server 侧默认监听 `127.0.0.1:7001`，允许 `--addr` 覆盖
- client 侧必须**重试连接**而非固定 `sleep`，最多等 2s（否则在不同机器上时序会飘）
- 所有示例必须能在 Windows 与 Linux 上无额外系统依赖运行（这是默认 feature 集的约束，见 1.3.1）

### 10.5 预期输出与验收门槛

`local_calc` 的预期输出分两批——**M2 阶段只有前四条，M3 才补齐其余**。把两批写在一起，M2 的验收会永远红着（v0.7 修正）。

M2（最小集）：

```
[server] listening on memory (in-process pair, no endpoint)
[client] add(2, 3) = 5
[client] div(1, 0) -> Err(Remote(code=0x10001, "divide by zero"))
[client] 32 concurrent calls -> all matched, mismatched = 0
```

M3（补齐全量形态，M2 那四条仍然要在）：

```
[client] notify ping (no reply)
[client] slow_op -> Err(Timeout after 50ms)
[client] fast_hash(data) = 0x40b1d64aff3aac17   (sync handler, 未 await)
[server] shutdown (in-flight drained)
```

`fast_hash` 那一行打的是 FNV-1a 的完整 16 位（`{:#018x}`），省略号在文档里可以留白、在输出里不行——验收要能逐字比对。

`calc_server` / `calc_client` 同理，只把第一行换成 `listening on tcp://127.0.0.1:7001`；`calc_local` 换成 `local:///tmp/photon-calc.sock`，客户端侧三行与上面 `calc_client` 一致（`connected to local://…` 之后是 `add` / `div`）。

> **`local_calc` 不打印任何 `memory://…` 地址（v0.8 修）。** v0.7 的输出暗示 `memory` 有一个「按名字注册服务」的注册表，而 2.x 从头到尾只定义了「进程内直接配对的两半」，没有名字解析。诚实的形态是 `Memory::pair()` 返回 `(Source, Sink)` 两半，示例内部把它们分别交给 server 与 client——**单进程示例不需要地址，也不需要注册表**。`Endpoint::Memory` 只做日志标识与 `Debug` 输出，不参与查找。

若同时用 `--features blocking` 且服务端声明 `#[photon::service(id = 7, blocking)]`，额外打印一行 `[client] blocking client: add(2,3) = 5`——**这一行不是默认验收门槛**，因为默认 feature 集不含 `blocking`（见 1.3.1）。

**验收门槛**：对应阶段该出现的条目全部出现、**且字段值正确**，进程退出码为 0。注意「出现」不等于「正确」——所以 13.3 要求把结果输出成结构化行，CI 断言字段而不是 grep 文本。任何一条缺失或值不对，都视为该里程碑未完成——不允许用"手工验证过"代替。

### 10.6 示例的长期角色

示例不是"写完就删的演示"，它有三个长期职责：

1. **文档的可执行版本**：文档说"换 transport 不改代码"，`calc_local` 就是证据
2. **CI 冒烟测试**：`cargo test` 之外再跑一遍 `cargo run --example local_calc`
3. **API 吐槽场**：写示例时任何一处别扭，都是 API 需要改的信号，比等用户反馈便宜得多

> 经验：**示例写起来费劲，通常不是示例的问题，是 API 的问题。**

---

## 11. 参考实现

| 项目 | 该看什么 | 为什么 |
|---|---|---|
| **`tarpc`** | `transport.rs`、`client/mod.rs` | `Transport` trait 与类型安全 API 做得最干净的 Rust RPC，约 200 行读完 |
| `jsonrpsee` | 多路复用与背压细节 | 生产级实现，注意它踩过的坑 |
| `zbus` | 报文解析 | D-Bus 实现，报文编解码的教科书 |
| `tonic` | HTTP/2 流控、`GoAway` | 优雅关闭与流量控制的成熟方案 |
| **`iceoryx2`** | SPSC 环、`RelativePointer`、service registry、monitoring | **M5 的主要参考**：共享内存 IPC 的完整实现，尤其崩溃检测和生命周期管理 |
| `crossbeam` | `ArrayQueue`、`CachePadded` | SPSC 环与 cache line 隔离的工业级实现 |
| `capnp-rpc` | Promise pipelining、能力传递 | 本地 / 远程透明的思想源头 |

---

## 12. 待决问题

1. `method` 用 u64 编译期哈希？→ **已定（v0.7）**：u64 哈希 + 独立的 `service_id: u16` 路由。原来是 `(service_id: u32, method_id: u16)` 的候选，但 `method_id: u16` 会让「服务超过 65535 个方法」变成硬上限，而 u64 哈希在 1000 个方法时碰撞概率约 10⁻¹³，够用。
2. `Session` 是否需要独立 dispatch 线程池，还是 reader task 里直接 `spawn`？→ 倾向 `spawn` + 并发上限。注意「怎么调度」已在 5.3 定死（编译期分流），这里剩下的只是「由谁来 spawn」。
3. 压缩是否进 M3？→ **不进，且协议不留压缩位（v0.7 定论）**。本地 IPC 场景 CPU 换带宽、带宽免费，收益为负；将来真要，走新增 kind + 独立协商。
4. `photon-macros` 何时**填内容**？→ **M2**。注意「是否拆 crate」在 v0.3 已判定为必然拆（proc-macro 硬约束），不再是待决项；包本身按第 10 条在 M0 就建好。
5. 是否支持 `#[photon::service(id = N)]` 加在 `impl` 块？→ 先只支持 trait（`id` 已定为必填参数，见 5.2）。
6. **`MessageSink::flush` 是否从 trait 移除？** → **已定（v0.8）：移除**。理由见 2.1 与 4.6：`send().await` 返回 `Ok` 即代表数据已交给内核缓冲，收尾由 `sink` 的 `drop` 承担；三种 transport 里两种本来就是 no-op。留着它只会让每个新 transport 多实现一个空函数。
7. **`AnySource` 的 box 是否可接受？** → 只在配置驱动 / 测试矩阵场景使用，热路径走泛型。需要 M4 用基准量化后再定。
8. **`PayloadMode` 是否要分池：小消息 Copy、大消息 ZeroCopy？** → 倾向先只做全局开关，M5 实测再细化（v1 的 ZeroCopy 语义见 4.5）。
9. **示例是否进 CI？** → 倾向是：`cargo test` + `cargo run --example local_calc`，退出码非 0 即失败（**不要 grep stdout 字符串**，见 13.3）。示例兼职冒烟测试，避免维护两套东西（见 10.6）。
10. **workspace 是否在 M0 就建好双 crate？** → 倾向是。`photon-macros` 即使 M2 才写内容，M0 也先把空包和 workspace 根建起来，避免中途改目录结构打断 M1。
11. **是否实现 `unix`（原生 `tokio::net` + `SOCK_SEQPACKET`）？** → 当前不做，`local` 已覆盖 UDS。仅当 M3 实测发现长度前缀开销显著、且需要 fd 传递时才引入，作为可选 feature。
12. **`blocking` feature 是否默认开？** → **否**。它是框架侧设施，且同步客户端还要宏参数显式开启（见 5.5）；`try_current` 只告警不拒绝之后，默认开的唯一理由也没了。
13. **慢 handler 告警阈值定多少？** → 先 100 µs，M4 实测后校准；按这个数值做可配置项，不要写死在代码里。
14. **协作式取消（handler 多接 `&ServiceContext` 首参）何时做？** → **M3 之后**。它同时是「handler 内发起对端调用」的前提（见 4.4），一次改动覆盖两件事，所以要等 handler 形态稳定了再做。
15. **`#[photon(blocking)]` 是否也给 `async fn` 用？** → **否**。blocking pool 里不能 `.await`，允许就是制造嵌套等待的陷阱（见 8 节第 22 条）。
16. **`service_id` 的分配方式？** → **已定（v0.8）**：用户显式分配，宏语法 `#[photon::service(id = N)]`（见 5.2 与 6 节机制 1）。保留段暂不划——框架自己不起服务；`0` 允许但建议避开。剩下真正待决的只有「跨 crate 分配冲突怎么查」，倾向先靠注册时撞号报错。
17. **`deadline_ms` 的口径是相对还是绝对？** → **相对预算，但两侧起点不同（v0.10 统一口径）**：发送方按「**从发送时刻起还剩多少**」填「剩余预算」（它已经把自己排队/重试花掉的时间扣掉），接收方**从解析完 24B 头开始计时**。所以接收方实际可用的时长 = 头部值 + 传输延迟，比调用方看到的超时略宽松——这个偏差无害（晚一点中止而已），绝对值需要时钟同步，本地 IPC 上不值得引入这个依赖。

---

## 13. 协议文档与验收方式

设计文档讲「为什么」，还需要一份讲「是什么」的权威定义——**两者不能合并**：这份文档要能自由修改理由和权衡，而线格式一旦定稿就只允许追加。

### 13.1 `docs/PROTOCOL.md`

**它是线格式的唯一权威（v0.9 定），3.3、3.8、6 节的所有表格都是「设计视角的同源速查」，冲突时以 `PROTOCOL.md` 为准。** 反过来的流程是硬规则：**任何字段增删必须先在 `PROTOCOL.md` 改，再同步 3.x 的速查表和 `photon/tests/protocol.rs` 里的逐字节样例**。不要反过来从设计文档摘录——设计文档要能自由改理由，它天然不适合当规范。

**只写结论，不写权衡论证**（理由留在本文档里；每条结论最多允许一句必要背景，超过一句就该搬回设计文档）：

- 24 字节头的字段表（偏移、宽度、小端、有效 kind）
- 逐 kind 的字段语义与**「必须为 0」矩阵**（与 3.3.1 同源，但只保留结论）
- flags 位定义与**非法组合**
- 错误体格式与 code 区间（含「原样透传 8..=0xFFFF」）
- 三个控制帧的字节布局（`Handshake` 12 字节 / `GoAway` / `Cancel`）与协商规则
- 每种 kind 的**一段 hex 示例 + 逐字段解读**（见 13.2）
- **parser 校验清单**：哪些必须拒绝并断连、哪些**不得**断连（后者是「合法竞态」的清单，比前者更容易写错）
- **版本与扩展规则**：什么变更必须升 `protocol_version`，什么变更不升
- `photon/tests/protocol.rs` 里逐字节样例的清单、字节数与冻结规则（与 13.2 对齐）

字节序统一写死为**小端**，覆盖全部线格式：24 字节头、错误体（3.8）、控制帧（6 节），不做字节序协商。3.2 的正文是设计视角的来源，线格式层面以 `PROTOCOL.md` 的字段表为准。

### 13.2 黄金样例（golden samples）

在 `photon/tests/protocol.rs` 里以**源码内联的字节数组**给出，**七种 kind 各一个**：`Request`、`Response(Ok)`、`Response(Err)`、`Notification`、`Cancel`、`GoAway`、`Handshake`。v0.12 把承载方式从 `tests/fixtures/*.bin` 改为内联：`.bin` 在 diff 里不可见、改坏无人察觉，内联字面量逐行可审、可逐段注释字段名与宽度，冻结效果等价。**目录里没有 `tests/fixtures/`，也不使用 `include_bytes!`。**

v0.7 只列了五种（漏 `Cancel` 和 `Notification`），等于把「最容易被实现成『反正没人用』的两个帧」留在了无保护状态——`Cancel` 恰恰是有竞态的那个，`Notification` 恰恰是不带响应、最容易把字段填错的那个。

**生成顺序是硬规则（v0.9）：先用实现生成一遍 → 人工逐字段对照 `PROTOCOL.md` 审一遍 → 把审过的字节内联冻结进测试 → 测试里只 decode/encode 比对，绝不在测试运行时重新生成。** 测试时重新生成，等于把「当前实现的 bug」固化进样例，然后宣称它通过了黄金测试。

测试做三件事：

1. `decode(样例)` 得到预期结构
2. `encode(结构)` 与样例逐字节相等
3. **反例断言**：`Cancel.service_id != 0`、`Notification.request_id == 0`、`Handshake` 的 `reserved != 0` 这类「必须为 0 / 非 0」的硬约束，必须各有一条「填错就被拒」的用例。只测正例，等于硬约束没人守

**这类测试的价值在于防「善意的重构」**：某天有人把 `request_id` 改成 u32 以省 4 字节，功能测试全绿，只有黄金样例会红。协议测试不该依赖「记得同步改文档」。

### 13.3 验收用结构，不用文本

10.5 那种「打印出这几行」只能当说明书，**不能当测试**。原因很实际：日志格式一改、并发输出一交错、多打一行调试信息，CI 就红，而功能其实没坏——最后所有人学会的技能是「改期望文本让它变绿」。

正确做法：

- 示例进程把每条结果编码成一个 JSON 行写 stdout，CI 解析后**断言字段**
- 断言内容：调用成功数、错误变体的**类型**（不是 message 文本）、超时是否在预期区间、并发结果是否全部匹配、退出码
- 文本匹配只用于「有没有崩」这类粗判，且不参与 CI 门槛

### 13.4 CI 矩阵

| 维度 | 取值 | 目的 |
|---|---|---|
| OS | Windows / Linux / macOS | Windows 是本地 IPC 的关键差异点（见 2.6） |
| 工具链 | 1.85（MSRV）/ stable | 防止悄悄用到新特性 |
| feature | 默认集 / `local` / （M3 起）`full` | `full` 的成员随里程碑长大，见 1.3.1；M0–M2 只跑默认集与 `local`，**不要提前声明空 feature 假装通过** |
| 检查 | `cargo test` + `clippy -D warnings` + `cargo deny` | `deny` 会在 bincode 这类「墓碑依赖」上直接报错（见 8 节第 30 条） |

`cargo deny` 从 M0 就要开，不是 M5。它的作用不是查许可证，是**在依赖死掉的第一天就告诉你**。

---

## 附录 A：为什么第一版不引入 io_uring

### io_uring 与 epoll 的本质差别

epoll 是**就绪（readiness）模型**：内核说「fd A 可读了」，你还得自己再调一次 `read()` 把数据搬出来——一次 IO 至少两次系统调用。

io_uring 是**完成（completion）模型**：把「从 fd A 读到 buffer B，读 16KB」这个完整操作提交到共享内存里的提交环（SQ），内核做完把结果写进完成环（CQ）。一次 `io_uring_enter` 可批量提交 N 个、批量收割 M 个完成事件。它的收益本质是：**用「队列深度」换「系统调用次数」**。

### 为什么 tokio-uring 停滞

1. **完成模型与 tokio 调度器结构冲突。** 完成语义要求 buffer 所有权在操作期间交给内核，future 里绑着裸 buffer，拿不到 `Send`。tokio-uring 只能退化成 `LocalSet` 单线程运行时，等于放弃 tokio 多线程 work-stealing 的全部优势。
2. **从未进入 tokio 核心**，一直是独立 experimental crate，停在那个状态多年。

替代品 `monoio`（字节）与 `glommio`（Datadog）都是 thread-per-core + io_uring 的独立运行时，与 tokio 生态不兼容。

### 为什么对 photon 收益不确定

1. **队列深度。** photon 是单通道多路复用，一条连接上永远只有 1 个读、1 个写在飞，深度 1–2。syscall 摊薄收益 ≈ 0，却要付出 ring 管理与 buffer 注册的固定开销。**厂商自己的基准都承认：单核且连接数极少时，Monoio 延迟高于 Tokio。**
2. **实测差距。** 2021 年 ByteDance 的 100B ping-pong 峰值测试曾给出 4 核 2x、16 核 3x；但 2025–2026 年在现代内核与调优后的 tokio 上重测，16 worker 只差约 8%，且**单核时 tokio 的吞吐和每请求 CPU 都优**。
3. **CPU 占比。** thread-per-core 没有 work stealing，遇到重请求无法卸载给空闲核，straggler 会拉爆尾延迟。RPC 框架的序列化正是 CPU 密集型。
4. **平台。** io_uring 是 Linux only；Windows 用 IOCP。引入等于凭空多一条平台路径。
5. **部署风险。** io_uring 曾有一批 CVE，Google 在 Android 禁用、多家云厂商与容器默认 seccomp 配置直接屏蔽 `io_uring_setup`。依赖它在很多真实环境里连初始化都过不去。

### 数据来源

本节引用的三组数字均可查证，写进文档是为了避免反复口头论证：

- **Monoio 官方基准（2021）**：100B ping-pong 峰值 QPS，1/4/16 核分别约 1x / 2x / 3x Tokio，且官方承认「单核且连接数极少时 Monoio 延迟高于 Tokio」。来源：`bytedance/monoio` 仓库 `docs/zh/benchmark.md`
- **giong 基准（2025–2026）**：EPYC 4345P 8C/16T，独立压测机，HTTP `/json`。1 worker 时 Tokio 508k vs Monoio 489k、每请求 CPU 1.92 µs vs 2.00 µs；16 worker 时 2.33M vs 2.53M（约 8% 差距）。来源：crates.io 的 `giong` 页面（0.4.0 / 0.7.0 两个版本的 README 表）
- **statico 基准**：1024 连接，1 线程 monoio 656k vs tokio 399k；但该 README 自述 tokio 那一档跑的是完整路径（HTTP/2 + TLS + 日志），**不可直接横比**，仅作参考

> 如果只用一句话记忆：**2021 年的「3 倍」在 2026 年的软硬件上已经缩到个位数百分比，而单核低并发时 Tokio 依然占优。**

### 结论

不换 runtime；最值得投入性能的方向是**消除拷贝**（`Bytes` 贯穿 + `rkyv`）、**减少每帧分配**、**缩短头解析路径**，最后是**共享内存**。这些收益比换 runtime 大一到两个数量级，且不改架构。

---

## 附录 B：共享内存方案与兼容性论证

### B.1 为什么 socket 慢

一次 UDS ping-pong 六个阶段：

1. `sendto()` 陷入内核 —— **110–300 ns**（含 Spectre 缓解）
2. `copy_from_user` 拷进内核 socket 缓冲区
3. 内核唤醒对端：`try_to_wake_up` → 可能跨核 IPI
4. 对端被调度回来 —— **1–3 µs（致命的一段）**
5. `recvfrom()` 再拷回用户态
6. 返回用户态

一个 RTT 约 2–5 µs，**超过 80% 花在「让内核替你把消息递过去」，而不是拷贝**（memcpy 1 KB 约 50–100 ns）。io_uring 能省掉 1 和 5，但省不掉 3 和 4，所以压到 1–3 µs 就到头了。

共享内存要干的事就是**把 1–6 全部删掉**。

### B.2 机制

**建立阶段（一次性）**：`memfd_create` / `shm_open` 创建，两侧各自 `mmap(MAP_SHARED)`，内核把同一份物理页映射进两个地址空间。

**此后**：A 写自己地址空间里的几个字节，B 在自己地址空间里就看到了。没有 syscall、没有拷贝、没有内核。一个 RTT 变成一次 release store + 一次 load。

不同配置下的 RTT 量级（**均为数量级估计，不是实测值；口径是「裸传输 ping-pong」，不是端到端 RPC**）：

| 配置 | RTT | 说明 |
|---|---|---|
| UDS / TCP loopback（基线） | 2–5 µs | 见 B.1 的六段拆分 |
| shm + 拷贝 + event 唤醒 | 1–2 µs | 唤醒成本主导，收益有限 |
| shm + 拷贝 + busy-poll | 300–600 ns | postcard，仍有一次 serde 拷贝 |
| shm + 零拷贝 + busy-poll | 100–300 ns | rkyv，M5 的最终目标 |

> **这张表里的数字不含 codec、Session、dispatch，也不含每请求一次 `spawn`。** v0.7 把它和 M5 的验收混成一个数，等于承诺「端到端 300 ns」——而 `Box::pin + spawn + oneshot + 两次 mpsc` 的固定成本就已经 100–200 ns。端到端现实目标是 **1–3 µs**（见 9 节 M5 验收）。要亚微秒端到端只有一条路：给 shm 做免 spawn 的内联 dispatch，那会推翻 5.3，明确不做。

> 一个常见误读：**"共享内存 = 立刻 100 ns"不成立。** 它取决于两个独立开关——**用不用 busy-poll**（决定是否回内核唤醒）和 **codec 是否支持原地读**（决定是否还有一次拷贝）。四个组合的差距在同一个数量级内来回摆动，所以任何单一数字都必须带上配置前提。

### B.3 无锁环形队列的四个关键点

**① 必须是 SPSC。** 单生产者单消费者是唯一不需要 CAS 的拓扑，每个原子操作退化成 store+release / load+acquire，没有重试循环。这是快的第一来源。所以 shm 的天然拓扑是「一对 channel = 一个单向流」，双向通讯需要两条环——这正是 2.1 把 L1 拆成 `MessageSource` / `MessageSink` 的原因。

**② 共享内存里绝不能存裸指针。** 两个进程映射到不同虚拟地址，A 的指针值在 B 里毫无意义。所有跨进程引用必须是相对 base 的偏移（iceoryx 的 `RelativePointer` 就是干这个的）。

**③ cache line 隔离。** `head` / `tail` 必须各占一条 64B 线，否则伪共享能把性能吃掉一半。用 `crossbeam_utils::CachePadded` 即可。

**④ 缓存对端索引。** 生产者不要把每次判满都去读消费者的 `tail`，本地缓存旧值、逼近上限才真读（Disruptor 经典技巧）。正常路径下两侧一条 cache line 都不共享。

**⑤ 段头元数据区（v0.8 新增，v0.9 补齐拓扑与运维字段）。** 段的第一页固定布局，创建方写、attach 方读并校验，不参与握手协商。

**先定拓扑：一个段 = 一条单向环。** 双向会话需要两条环，因此也就要两个段——`Endpoint::Shm { name }` 里的 `name` 是**基名**，实际创建 `{name}-a2b` 与 `{name}-b2a` 两个段（`bind` 一侧建两个并发布，`connect` 一侧按基名拼出两条再 attach）。不要试图把两条环塞进一个段：那会让「段头里放几组 head/tail」变成每条环都要复制一遍的样板，而且一条环被写满时会连带影响另一条的缓存行。

| 字段 | 宽度 | 说明 |
|---|---|---|
| `magic` | u64 | 固定值，识别「这不是我们的段」 |
| `protocol_version` | u16 | 与 `Handshake` 里的版本必须一致 |
| `state` | u8 | `Free / Writing / Ready / Reading` 的段级版本（B.7 的槽位状态机在这里落一个总开关） |
| `reserved` | u8 | 必须 0 |
| `generation` | u64 | 每次 channel 重置 +1；旧数据靠它被识别并跳过（B.7 要求，v0.8 的段头里没有它的位置） |
| `ring_capacity` | u32 | 槽位个数，**必须是 2 的幂**（取模用 `&`） |
| `slot_size` | u32 | 每个槽位字节数，含 24 字节头，按 64 对齐 |
| `head` | `CachePadded<AtomicU64>` | 独占一条 64B 线 |
| `tail` | `CachePadded<AtomicU64>` | 独占一条 64B 线 |
| `peer_heartbeat` | `CachePadded<AtomicU64>` | 对端周期性写入的时间戳/计数；本端超过阈值未更新即判定对端消失（B.7） |

`attach` 方先校验 `magic` 与版本、再读容量与 `generation`，读到之后在 `Handshake` 里断言「我按 N 个槽位映射」。这样「协商容量」不再需要环本身——原来它必须先建环才能协商环的尺寸，是明显的循环依赖（见 6 节机制 2）。

> 段头一旦发布就改不动，所以 `generation` 与 `peer_heartbeat` 必须**现在**占好位置。等 M5 真做崩溃检测时再往段头里插字段，等于要求所有已部署的段重新创建——而共享内存的段名是写死在配置里的。

### B.4 数据放置：三档粒度

| 档位 | 做法 | 适用 |
|---|---|---|
| A. 直接写槽位 | payload 写在 ring 的 slot 里 | 小消息 + **同步 `fn` handler**（见下方顺序约束）；RPC 帧大多在这一档 |
| B. 块池 + 索引 | 预分配池里取 chunk、写入、只发索引 | 大消息 / 变长 |
| C. 零拷贝借用 | 消费端直接读 chunk 原地数据 | 需配合 `rkyv` |

**档 A 的对齐要求（v0.7 新增）**：槽位基址按 64 字节对齐，24 字节头之后 payload 从槽内偏移 24 起——`64 + 24 = 88`，是 8 的倍数，所以 `rkyv` 在各槽位上的读取都对齐。若哪天头长变回 20，这个对齐立刻破掉，档 C 直接不可用。**这也是「头长不许随便改」的硬理由，不是审美问题。**

**档 A 还有一个并发约束，v0.7 漏了（v0.8 补）**：SPSC 环的 `tail` 只能**按顺序**推进。请求 1 的 handler 慢、请求 2 快，2 的槽位明明已经处理完，`tail` 也不能越过 1 往前走——整条环被一个慢 handler 堵住。所以 3.5 那句「环形队列天然支持并发在飞」**只在 payload 不被借用时成立**：

- **档 A 只配同步 `fn` handler**（首 poll 内跑完，没有 await 把槽位攥住），并且仍然只对 `#[photon(zero_copy)]` 显式标注的方法开放（见 4.5）
- 一旦出现跨 `await` 的 handler，就必须走**档 B**：从块池取 chunk，槽位里只放索引，消费者归还的顺序可以乱——用自由链表 + 引用计数，而不是推进环尾

档 B 的 chunk 基址同样要 ≥ 8 字节对齐（建议 64），否则跨 chunk 的 `Archived` 引用会 UB。

### B.5 通知：唯一必须回内核的地方

| 方式 | 延迟 | CPU 代价 |
|---|---|---|
| Busy-poll | 100–200 ns | 独占一个核 |
| `eventfd` / `futex` 唤醒 | +1–2 µs | 空闲零开销 |
| 混合（短自旋 + 超时挂起） | 200 ns ~ 2 µs | 可控 |

> 表里的数字是**单次通知成本**，不是 RTT——一个往返至少两次通知，所以 B.2 的 busy-poll RTT 是 300–600 ns 量级。两个表不要混着记。

**这是产品决策不是技术决策**：默认走 event 唤醒，提供 `spin_hint` 让延迟敏感场景显式打开。框架不该在用户不知情时烧掉一个核。

### B.6 shm 与 rkyv 的配套关系

> **只上 shm 不换 codec，也能拿到约 80% 的收益。**

因为边际成本最大的是「陷入内核 + 调度唤醒」，不是 memcpy。shm + postcard 已能把 RTT 从 2–5 µs 拉到 300–600 ns。但要走完最后一公里（彻底零拷贝），序列化格式必须支持「在别人的字节上直接读」，所以 **shm 与 rkyv 必须一起上**——而且 rkyv 要求 payload 起点 8 字节对齐，这正是 v0.7 把协议头从 20 改回 24 字节的原因之一（见 3.2）。

### B.7 真正的难点：崩溃与一致性

1. **槽位状态机**：`Free → Writing → Ready → Reading → Free`，先写完 payload 再 release 置 `Ready`，否则消费方读到半条消息。
2. **对端崩溃**：socket 断了有 `EPIPE`，共享内存什么都不给——环会安静停住。必须有**心跳 + 存活检测**，检测到对端消失后触发 channel 重置（用 generation 计数区分旧数据）。心跳与 generation 的位置已经定在段头（B.3 的 `peer_heartbeat` / `generation`），不要事后再往段里找地方——段头是发布即冻结的。
3. **崩溃时正在写的槽位**：SPSC 没有锁泄漏，但会留一个永远停在 `Writing` 的槽位，重置时必须能跳过。
4. **chunk 引用计数**：块池场景需要在共享内存里做原子引用计数，才知道何时能归还。
5. **`/dev/shm` 残留**：进程被 kill 后段还在，启动时要能识别并清理。

这也是为什么 iceoryx2 体量不小，以及它需要一整套 service registry + monitoring 子系统。

### B.8 原设计能不能装下 shm ——逐条核对

| # | v0.1 现状 | 与 shm 的冲突 | 结论 |
|---|---|---|---|
| 1 | `Transport: AsyncRead + AsyncWrite` | shm 是消息队列不是字节流；硬塞进去要合成连续字节流，等于废掉零拷贝 | **必须改** → 拆成 `MessageSource` / `MessageSink` |
| 2 | 帧头 24B 自带 `length` | 边界应由 L1 提供，头里再存一份冗余；shm 消息长度可直接算出 | **必须改** → 删 `length`，头缩为 20B（v0.7 又扩回 24B，但那是为了 `service_id`/`deadline_ms`/对齐，不是把 `length` 加回来） |
| 3 | `accept()` = OS accept | shm 没有 accept，是「等对端 attach」 | 语义泛化为「产出已就绪通道」，接口可统一 |
| 4 | `send` 的背压靠内核缓冲 + mpsc | shm 环满时无法「await 空位」 | **必须改** → `send` 统一为 async |
| 5 | `Codec::encode -> Bytes` | tier-2 需要直接把响应编码进 shm 槽位，返回值形状堵死这条路 | **必须改** → 核心改为 `encode_into<B: BufMut>` |
| 6 | 消息无类型约束 | 借用切片会破坏 dispatch 的 `spawn`（要求 `Send + 'static`） | 锁定不变量：消息一律 `Bytes`，靠 `from_owner` 实现零拷贝 |
| 7 | `Session<T: Transport>` 泛型 | 泛型膨胀；而 `impl Future` 的 trait 又不可 dyn | 泛型为主 + `AnySource` 枚举门面（明确接受一次 box） |
| 8 | 无 buffer 生命周期策略 | 零拷贝把 payload 生命周期绑到 handler 执行时长 | 新增 `PayloadMode::{Copy, ZeroCopy}`，默认 Copy（在 owned transport 上是 no-op，见 4.5） |
| 9 | 无协商机制 | 容量 / 版本 / codec 这类「两侧必须一致」的参数无处放 | 新增 `Handshake` 控制帧（12 字节，见 6 节）；`ring_capacity` 不进协商，改走段头 |
| 10 | `Codec` 是泛型方法 | 运行期挑 codec 不存在 dyn-safe 实现 | codec 构建期选定，握手只带 `codec_id` fail-fast（见 3.6） |

**不需要改的部分**（这是设计成立的证据）：

- `Service` trait 的参数与返回类型（`Bytes::from_owner` 让零拷贝与 `'static` 共存）。接收者改成 `Arc<Self>`（v0.6）是为 `spawn_blocking`，与 shm 无关
- pending map / `request_id` 命名空间 / 取消 / 超时（`GoAway` 的载荷在 v0.7 改过，但那是双向语义问题，与 shm 无关，见 4.6）
- `ServiceRegistry` / `service_id` 路由 / method 哈希 / proc-macro 产物
- `RpcError` 的本地/远端分界（见 3.8）——shm 下错误照样只能由本端产生，语义不变

**结论**：核心骨架可以容纳共享内存。但第 1、2、4、5 条会在 M0–M1 就被写死，**必须现在改**；第 6、7、8 条是新增约束，成本很低但漏掉就堵死路径。

### B.9 给 photon 的落地顺序

1. **M0（成本≈0，必做）**：`MessageSource` / `MessageSink` 形状 + `memory` 变体验证
2. **M1 顺手**：`memory` 上跑通 `Bytes::from_owner` 的零拷贝
3. **M3 想尝鲜**：Linux 上试 `SOCK_SEQPACKET`——自带消息边界、支持 fd 传递，几乎零改造
4. **M4 之后**：真 shm，先只做档位 A（小消息直写槽位），不做块池；同时上 `rkyv`。**注意档 A 只能配同步 `fn` handler**（B.4 的顺序约束）——一旦有跨 `await` 的 handler 走零拷贝，就必须补档 B 的块池，否则一条环会被一个慢 handler 堵死
5. **永远保留 socket 路径做对照**：共享内存出问题时没有 `strace` / `tcpdump` 可用，只能 hex dump，且不能跨机器。两条路径跑同一套测试是唯一可靠的验证手段——这也正是给 transport 做抽象最大的回报
