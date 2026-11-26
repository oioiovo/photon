# L4 类型安全 API 详细设计

> 上位文档：`docs/design.md` 5 节、10 节。
> 本层是**唯一对用户可见**的一层：一个 trait + 一个属性宏 + 两个客户端变体。
> 产出：用户写 `#[photon::service(id = 7)] pub trait Calc { … }` 之后得到的全部代码。

---

## 1. 职责边界

**做**：把用户 trait 变成「可注册的服务实现 + 可调用的 stub」；把三档声明形态编译期映射到三种调度；生成方法名表与 method id 常量；生成可选的同步客户端。

**不做**：不认识 `request_id`、不认识连接、不认识 `tx`、不认识 `Endpoint`（除了 `connect` 那一层薄封装）；不解析 24 字节头；不做序列化（交给 `Codec`）。

红线：`photon-macros` 里不许出现任何运行期逻辑；用户的 `trait` 里不许出现任何框架类型（`Bytes` / `RpcError` 只在返回值里出现，这是 5.1 已经接受的例外）。

---

## 2. `Service` trait

```rust
pub trait Service: Send + Sync + 'static {
    fn call(self: Arc<Self>, method: u64, payload: Bytes)
        -> BoxFuture<'static, Result<Bytes, RpcError>>;
}
```

| 要点 | 规定 | 理由 |
|---|---|---|
| 接收者 | **`self: Arc<Self>`**，不是 `&self` | `spawn_blocking` 的闭包要求 `'static + Send`，需要把服务对象**所有权**移进闭包；`&self` 拿不到 `'static`，`fn call(&self, …)` 在 blocking 分支上根本编不过 |
| dyn 兼容 | `Arc<dyn Service>` 可存可调 | `ServiceRegistry` 只存 `Arc<dyn Service>` |
| `BoxFuture` | 自己定义 `type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>` | **不要**为这个别名引入 `futures`（默认依赖数会从 8 变 9） |
| 与 handler 形态无关 | 无论 `async fn` 还是 `fn`，返回都是 `BoxFuture` | 同步方法在 dispatch 里包成 async 块、首次 poll 内执行完；**调度差异不进签名** |
| 响应 | 返回 owned `Bytes` | 出站零拷贝不做（M5 只做入站），否则 handler 要拿到 transport 的 chunk 分配器，破坏 L4 对 L1 的无知 |
| 服务路由 | 不在 `Service` 里 | `service_id → Arc<dyn Service>` 在 `ServiceRegistry`（L3） |

`Service::call` 的 `payload` 是**已过 24 字节头剥离的裸 payload**。反序列化发生在 `call` 内部（宏生成的代码里），不发生在 L3——L3 只负责「把它搬进 task」。这一点与 4.2 第 ⑤ 步一致：读者只需记住「decode 不在 reader loop 里」。

> 编解码器从哪来：宏生成的 `call` 用编译期别名 `photon::codec::Default`（由 `codec-*` feature 决定是 postcard 还是 rkyv）。**`Session` 不再有 `C` 泛型参数（v0.10）**——它只持 `S: MessageSource` / `K: MessageSink`，握手固定填 `codec::Default::ID`。v0.9 的 `Session<S, K, C>` 里 `C` 是死参数：`Service::call` 没有 `C` 槽位，热路径的字节从来不用 `C`，`C` 只在握手贡献一个 id——于是 `Session<S, K, OtherCodec>` 会「宣告 A、线上却是 B」，**静默错线格式**。将来若真要同构建内支持第二种 codec，必须先引入 `AnyCodec` 门面。

---

## 3. 宏的输入语法

### 3.1 服务级属性

```rust
#[photon::service(id = 7)]                    // 必填
#[photon::service(id = 7, blocking)]          // 额外生成同步客户端
```

| 参数 | 是否必填 | 类型 | 规则 |
|---|---|---|---|
| `id` | **必填** | `u16` | 显式分配，不自动编号；`0` 允许但建议避开 |
| `blocking` | 可选 | 标志 | 生成 `BlockingXxxClient`；还需 crate 开 `blocking` feature |

`id` 必填的三条理由：宏否则无从填 `service_id`；自动编号会在「不同 crate 各自注册」时撞号；顺序一变线上 id 就变。

### 3.2 方法级属性

| 属性 | 适用 | 语义 |
|---|---|---|
| `#[photon(blocking)]` | 仅 `fn` | 该 handler 走 `spawn_blocking` |
| `#[photon(zero_copy)]` | 仅 `fn` | 该方法的入站 payload 允许直传（v1 无行为差异，语法先定） |
| `#[photon(error = MyError)]` | 服务级/方法级 | M3：把业务错误映射成 `code ≥ 0x1_0000` 的 `Remote` |

**不合法的组合必须在编译期报错**，不能降级为警告：`async fn` + `blocking`、`async fn` + `zero_copy`、`fn` 不带 `blocking` 却出现在 blocking client 里（后者由生成逻辑自然规避）。

### 3.3 三档声明（写进用户脑子的唯一规则）

> **`fn` 表示「服务端实现同步完成」，不是「客户端阻塞调用」。**

| 声明 | 服务端执行 | 生成的客户端方法 | 客户端实际行为 |
|---|---|---|---|
| `async fn` | 在当前 task 里 await | `async fn` | 异步 |
| `fn` | 就地执行完（不换线程） | `async fn` | 异步 |
| `fn` + `#[photon(blocking)]` | `spawn_blocking` | `async fn`（`BlockingXxxClient` 里是 `fn`） | 异步 / 同步 |

混写三档是允许的，宏逐方法读 `async` 关键字与属性。

---

## 4. 宏展开产物

对一个 `#[photon::service(id = 7, blocking)] trait Calc { … }`，展开必须产出**恰好**下列项（名字规则固定，避免用户猜）：

| 产物 | 形状 | 说明 |
|---|---|---|
| `CalcClient` | 持 `SessionHandle`，三个方法全是 `async fn` | 线上永远异步，与声明形态无关 |
| `impl Service for Calc` | `fn call(self: Arc<Self>, method, payload)`，内部 `match method` | 三分支，`Box::pin`，见第 5 节 |
| `register_calc(registry, svc)` | 用 `photon::service` 的 `7` 占号 | 与 stub 共用同一个 `id` 字面量，一处写两处用 |
| `CalcClient::SERVICE_ID` / `Calc::SERVICE_ID` | `const u16` | 排障与日志用 |
| `mod __photon_calc` 内的 `METHOD_NAMES` | `&[(u64, &'static str)]` | span / `UnknownMethod` 日志 / panic 报告 |
| `mod __photon_calc` 内的 `HASH_<METHOD>` 常量 | `const u64` | 编译期哈希值，`match` 的键 |
| `BlockingCalcClient` | 持自建或共享的 runtime + `SessionHandle`，方法全是 `fn` | 仅 `blocking` 参数 + feature 同时成立时生成 |

**所有辅助项必须包进一个按服务命名的模块（v0.10 修）**。v0.9 的写法是在模块级直接生成 `METHOD_NAMES` / `HASH_ADD` / `SERVICE_ID` 这些固定名字——**同一个模块里写两个 `#[photon::service]` 就必然重名编译失败**，而「一个模块放几个服务」是完全正常的用法。规则：

```rust
mod __photon_calc {          // 模块名 = __photon_ + 服务名的小写 snake 形式
    pub(super) const METHOD_NAMES: &[(u64, &'static str)] = &[...];
    pub(super) const HASH_ADD: u64 = 0x...;
}
```

- 模块名只由服务名决定，不参与任何线上语义；用户代码里**不应引用**它（生成的客户端/注册函数在父模块里 re-export 需要的项）。
- `HASH_<METHOD>` 用方法名**大写**形式；若方法名本身含非法标识符字符（极少数），退回 `HASH_0`、`HASH_1` 并按声明顺序编号。
- 不要用 `#[doc(hidden)]` 糊过去就完事——重名是编译错误，不是文档问题。

**`blocking` 不能用 `#[cfg(feature = "blocking")]` 生成**：proc-macro 展开出的 `cfg` 检查的是**使用方 crate** 的 feature，不是 photon 的，按字面根本不成立。所以用属性参数显式要求；`blocking` feature 管的是**框架侧设施**（`session/blocking.rs`、打包 runtime 的代码）。两件事不要混。

---

## 5. dispatch 展开

```rust
match method {
    HASH_ADD  => Box::pin(async move { me.add(a, b).await }),
    HASH_FAST => Box::pin(async move { me.fast_hash(data) }),
    HASH_LOAD => Box::pin(async move {
        let me = me.clone();
        // decode 必须在闭包里：一次线程切换干两件事（反序列化 + 执行）
        tokio::task::spawn_blocking(move || {
            let path: PathBuf = Default::decode(&payload)?;
            me.load(path)
        }).await?
    }),
    _ => return Box::pin(async { Err(RpcError::Remote(RemoteError::new(3))) }), // UnknownMethod：回包侧构造，合法
}
```

四条不能挪的规则：

1. **`tokio::spawn` 只在 L3 发生一次**（`tokio::spawn(svc.call(method, payload))`）。这里的三个分支只回答「这次执行落在哪个线程池」，不是「各自再 spawn 一次」。
2. **`fn` 分支不换线程**，就在 poll 它的 worker 上就地跑完。微秒级无所谓，毫秒级就是 worker 被占住——所以需要第 7 节的 debug 告警。
3. **`spawn_blocking` 的 `JoinHandle` 必须 await**，`JoinError` 映射成 `code = 0`；`tokio::spawn` 的句柄同理（在 L3 await）。丢掉句柄 = handler panic 无声，客户端只等到超时。
4. **未知 method 返回 `Remote{code:3}`，不是 `Protocol`**——帧格式没错，是方法没注册。这个 `Remote` 由**回包侧**（正在回 `Response` 的一方）构造，因而是**合法**的；`RemoteError::new(code)` 是唯一允许的构造入口，全项目只允许三处用它（`2` / `3` / `7`，见 `02-l2-protocol.md` 7.3）。**注意这条在 v0.9 被误标成「服务端伪造远端错误」——那是把「发起侧不许造 `Remote`」错当成了「谁都不许造」**；正确的规则是方向相关：发起侧只能从错误体得到 `Remote`，回包侧可以主动构造。

参数解码位置：**`async fn` / `fn` 分支在 `call` 开头** `let (a, b) = Default::decode::<(i64, i64)>(&payload)?;`；**`#[photon(blocking)]` 分支必须在 `spawn_blocking` 的闭包里 decode**（v0.10 修）——v0.9 把它写在闭包外，等于只在调用线程上解析、闭包里只剩执行，一次线程切换只干了一件事；更糟的是 decode 出错时闭包外的 `?` 会让 `payload` 在闭包捕获前就被借用，写出来的代码通常编译不过或被迫多一次 clone。解码失败一律 `code = 4`。`Vec<u8>` 等容器类型直接走 serde，不需要特殊处理。

---

## 6. 配额与线程池

| 配额 | 谁消费 | 默认 | 说明 |
|---|---|---|---|
| `max_async_in_flight` | 服务端 `async fn` / `fn` handler | 1024（与出站队列同量级） | task 资源 |
| `max_blocking_in_flight` | 服务端 `#[photon(blocking)]` handler | = blocking pool 大小 | 线程资源 |
| 客户端出站配额 | 客户端 | 一组，只用 `max_async_in_flight` | 客户端不跑 handler |

**必须拆成两个**：共用一组额度会出现「一批慢的 blocking 请求把额度占满，此时异步请求明明跑得动却也收到 `Overloaded`」——两类资源混进一个计数器，过载信号就失去意义了。

---

## 7. 同步客户端

### 7.1 实现约束（逐条都是踩过的坑）

| 约束 | 原因 |
|---|---|
| **禁用 `Handle::block_on`** | `current_thread` runtime 里必死锁；multi_thread worker 上 panic（`Cannot block the current thread from within a runtime`） |
| 用「投递 + 阻塞等待」：`handle.spawn(fut)` + `std::sync::mpsc::sync_channel(1)` + `recv_timeout` | 在当前线程**纯等待**，future 在 runtime 线程跑，不 panic 不死锁 |
| **内外两层超时** | 内层 `tokio::time::timeout` 管 future；外层 `recv_timeout(timeout + 50ms)` 管「连 runtime 都没回话」。少了外层，runtime 线程池饿死时照样挂住 |
| `Disconnected → RuntimeDropped`、`Timeout → Timeout` | 两个错误必须区分：前者是「runtime 没了」，后者是「太慢」 |
| **自建 `Runtime` 必须被 client 结构体持有** | 只存 `Handle` 的话，`Runtime` 一 drop，所有 task 被取消，`recv_timeout` 立刻 `Disconnected` → 全部变成 `RuntimeDropped` |
| 告警在**进入阻塞等待之前**打，且用 `AtomicBool` 只打一次 | 每次调用都 warn 会把日志刷废，等于没有告警 |

### 7.2 runtime 从哪来

| 构造 | 行为 |
|---|---|
| `BlockingCalcClient::connect(ep, opts)` | 自建一个 1 worker 的 multi_thread runtime（只跑 IO，1 线程足够）；`opts: ConnectOptions` 与服务端 `Server::bind` 的同名参数一致，**不能省**（v0.9 漏了 `opts`，导致 `max_message_len` / 配额 / 握手超时全部无法配置） |
| `BlockingCalcClient::with_runtime(&shared, ep, opts)` | 多个 client 复用一个 runtime，避免 N 个 client 起 N 套线程 |

### 7.3 `try_current()` 只告警，不硬拒绝

- `spawn_blocking` 的线程**是有 runtime 上下文**的（否则它内部没法 `tokio::spawn`），所以 `Handle::try_current()` 返回 `Ok`——而在阻塞池里做同步等待恰恰是官方推荐写法。
- 任何「检测到 runtime 上下文就拒绝」都会误杀这个合法用法。
- 所以：不做硬拒绝、debug 下告警（附「若你确定在 `spawn_blocking` 内，可忽略」）、文档写清唯一真正危险场景（`current_thread` 的 worker 上做同步等待 → 必死锁）。

---

## 8. 本地直连

同一个 `Service` trait，既可以被远程 stub 调用，也可以**同进程直接调**（跳过 L1/L2/L3）。

| 传播方式 | 走不走通道 | 零序列化 |
|---|---|---|
| `memory` transport | 走（用 channel 模拟一条 IPC 通道） | 否（仍是 `Bytes`） |
| **本地直连** | **不走** | 是（直接调 handler） |

两条必须澄清的点：

- 本地直连**不是 transport**，它是 5.6 的调用路径；把两者混成一个实现是 v0.2 的错误。
- 三档声明在本地直连下仍按形态分派：`async fn` 直接 await、`fn` 直接调；标了 `#[photon(blocking)]` 时**由调用方自己负责线程池**，框架不替它调度（那时框架根本不在链路上）。

---

## 9. 编译期检查与报错

| 检查 | 时机 | 报错要求 |
|---|---|---|
| `id` 缺失 / 不是 `u16` 字面量 | 展开 | 指出「必须写 `id = N`」，并给一行正确写法 |
| 方法名重复或哈希相撞 | 展开 | 常量断言式检查；报出两个方法名 |
| `async fn` + `blocking` | 展开 | 「同步方法才需要 `blocking`」 |
| `async fn` + `zero_copy` | 展开 | 「`zero_copy` 只对 `fn` 开放」 |
| 注册撞号 | 运行期（注册时） | `Err`，**不覆盖**已有服务 |
| 改方法签名 | 编译期 | 在用户侧编译失败，错误信息必须定位到具体方法 |

测试手段：`compile-fail` 用例（`trybuild` 作为 **dev-dependency**，不进默认依赖集）。

---

## 10. 服务端装配与连接建立（v0.10 新增）

v0.9 的四份文档把「宏产出什么」写得很细，却**没有一处说清这些产物怎么装成一个能跑的服务**：`Server::bind` 长什么样、accept 循环在哪、每连接怎么建 `Session`、`ServiceRegistry` 怎么共享给所有连接、客户端到底用泛型还是 `AnyTransport`。照着写的人到这一步会自己发明一套，然后与 L1/L3 的签名对不上。本节把它定死。

### 10.1 `Server` 与 `bind`

```rust
pub struct Server<T: Transport> {
    transport: T,                 // bind 的产物（可 clone 的 listener 包装，不是每连接状态）
    registry: Arc<ServiceRegistry>,
    transport_opts: TransportOptions,
    session_opts: SessionOptions, // 含 max_message_len（v0.10 从 TransportOptions 迁来）
}

impl<T: Transport> Server<T> {
    pub fn bind(ep: &Endpoint, transport_opts: TransportOptions,
                session_opts: SessionOptions) -> io::Result<Self>;
    pub fn register<S: Service>(&mut self, id: u16, svc: Arc<S>) -> Result<(), RegistryError>;
    pub async fn serve(self) -> io::Result<()>;                 // accept 循环，永不返回（除错误）
    pub async fn serve_with(self, mut on_conn: impl FnMut(Arc<Session<T::Source, T::Sink>>)) -> io::Result<()>;
}
```

规则：

- **`bind` 是同步的**（`L1-C4`）：内部 `std::net` + `from_std`，因此**必须在 runtime 上下文里调用**（`from_std` 需要 reactor）。这是已知前置条件，不要在示例里写 `let server = Server::bind(...)` 于 `main` 的同步段——放进 `#[tokio::main]` 里就没问题。
- `register` 就是宏生成的 `register_calc(registry, svc)` 的薄包装：**注册发生在 `bind` 之后、`serve` 之前**，撞号返回 `Err`、**不覆盖**已有服务（`INV-14`）。
- 三个配置各归各位，不许合并成一个 `Options`：`TransportOptions` 给 L1（`connect_timeout` / `spin_hint`），`SessionOptions` 给 L3（`max_message_len` / 配额 / `payload_mode` / 超时）。`design.md` 2.7 已经因为「塞在一起」返工过一次。

### 10.2 accept 循环与每连接的会话

```rust
while let Ok((source, sink)) = transport.accept(&transport_opts).await {
    let registry = registry.clone();               // Arc 共享，不是每连接一份
    let opts = session_opts.clone();
    tokio::spawn(async move {
        let (session, _handle) = Session::new(source, sink, registry, opts);
        // 这是骨架：持有 Arc<Session> 直到本条连接结束（对端 EOF / 双方宣告关闭），
        // 结束时按 `03-l3-session.md` 7.3 的六步调 `session.close().await`。
    });
}
```

- **`ServiceRegistry` 是 `Arc` 共享给所有连接的**：每连接复制一份注册表会让「热更新服务」变成不可能，而且白白多 N 份 map。`Server` 持一份，`accept` 时 `clone` 给每个 `Session`。
- **每连接一个 `Session`，两个 task（reader/writer）**；`serve` 本身只做 accept，不参与收发。这样 N 条连接 = N×2 个 task，与「每连接两个半身」的结构一致。
- `SessionHandle`（发起端）在纯服务端场景用不上，但**不能随手丢掉**：定死 `SessionHandle::drop` **不关连接**（它只是个请求入口），真正关闭由 `Arc<Session>::close()` 或 drop 最后一个 `Arc<Session>` 决定。这一条不写，示例里 `let (session, _handle) = ...` 会被人当成「丢了就会关」。
- 服务端驱动的收尾入口**只有一个**：`Session::close(&self)`（`03` 7.3 六步）。「等对端 `GoAway` / EOF 再收尾」的等待逻辑属于 `Session` 内部，不要在示例代码里再拼一套状态机。
- accept 循环的错误分两类：**单连接 `accept` 失败**（对端提前断开）记 `debug` 后继续；**listener 级别失败**（`EMFILE`、listener 被关）直接返回 `Err`。不要用 `while let Ok(..)` 把 listener 错误静默吞成「永远 accept 不到新连接」。

### 10.3 客户端：泛型还是 `AnyTransport`

| 场景 | 用什么 | 理由 |
|---|---|---|
| 生产 / 基准 | `CalcClient<TcpTransport>`（或 `Local`） | 泛型单态化，零门面开销；`INV-18` 禁止基准用门面 |
| 示例的 `--transport` 开关 / 测试矩阵 | `CalcClient<AnyTransport>` | 运行时选择，接受一次 box |

**`CalcClient` 的类型参数来自 transport，而不是 codec**（v0.10）：`C` 已从 `Session` 删掉，客户端自然也不该有。签名形状：

```rust
impl<T: Transport> CalcClient<T> {
    pub async fn connect(ep: &Endpoint, opts: ConnectOptions) -> io::Result<Self>;
    // ConnectOptions 把 TransportOptions 与 SessionOptions 打包，避免 connect 出现两套参数顺序
}

impl CalcClient<AnyTransport> {
    pub async fn connect_any(ep: &Endpoint, opts: ConnectOptions) -> io::Result<Self>;
}
```

- `connect` 内部顺序固定：`transport.connect(ep, &transport_opts)` → `Session::new(source, sink, empty_registry, session_opts)` → 取 `SessionHandle` 包进客户端。
- **客户端也传非空 `registry` 的情形**：4.4 的双向对等允许服务端回调客户端，若确实要暴露回调服务，就注册到自己的 registry；否则传 `ServiceRegistry::empty()`（**不是 `None`**，`Session::new` 的签名如此）。

### 10.4 完整的装配清单（照抄这一条就不会漏）

| 步骤 | 服务端 | 客户端 |
|---|---|---|
| 1 | `Server::bind(ep, transport_opts, session_opts)?` | — |
| 2 | `server.register::<Calc>(7, Arc::new(CalcImpl))?`（或 `register_calc(&mut registry, svc)`） | — |
| 3 | — | `CalcClient::connect(ep, opts).await?` |
| 4 | `server.serve().await?` | 调方法 |
| 5 | 收到 `GoAway` / 本地 `close()` → 按 `03` 7.3 六步收尾 | 收到 `GoAway` → 新请求 `Closed`，等自己的 pending 清零 |

---

## 11. 示例与验收

| 示例 | 形态 | 阶段 | 关键点 |
|---|---|---|---|
| `local_calc` | `memory`，单进程 | M2 | 最小集全链路：stub → session → dispatch → codec → 返回；一条命令可跑 |
| `calc_server` / `calc_client` | `tcp`，两进程 | M2 | 真跨进程；同样只验最小集 |
| `local_calc`（补齐） | `memory` | M3 | 七种形态：`async fn` / `fn` / blocking / `Notification` / 超时 / `GoAway` / 错误体 |
| `calc_local` | `local`，两进程 | M3 | 换 transport 不改业务代码（架构验收） |

`local_calc` 的两条预期输出是验收硬指标（不能随手改文案）：

- `[server] listening on memory (in-process pair, no endpoint)`
- `Err(Remote(code=0x10001, "divide by zero"))`

M2 的范围必须写清：**只支持 `async fn` 声明**，不含 `Notification` / 超时 / `GoAway` / 同步 handler。把七种形态全压给 M2 是 v0.6 的错误，按现文不可能通过。

---

## 12. 明确不做

- **不给 `fn` 声明自动生成 `fn` 客户端方法**：那会把「handler 形态」和「调用形态」重新绑死，正好绕回 5.2 想拆开的错误。要同步客户端必须是显式宏参数。
- 不默认生成同步客户端：需要 feature + 宏参数两个开关都打开，代价是「多一套 runtime 生命周期 + 一个告警面」。
- handler 内反向调用对端（需 `&ServiceContext` 首参）：会改动所有 handler 签名，M3 之后再说。
- 协作式取消（handler 内轮询 `is_cancelled()`）：同上。
- 用 `HashMap<u64, Box<dyn Fn>>` 代替 `match`：调度决策在运行期只剩一个函数指针，无法分流；且 method id 重复无法在编译期发现。
- 引入 `async-trait`：Rust 1.75 已有 AFIT；只有 trait object 场景才需要显式 `BoxFuture`，已经处理。
