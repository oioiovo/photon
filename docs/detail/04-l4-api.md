# L4 类型安全 API 详细设计

> 上位文档：`docs/design.md` 5 节、10 节。
> 本层是**唯一对用户可见**的一层：一个 trait + 一个属性宏 + 两个客户端变体。
> 产出：用户写 `#[spile::service(id = 7)] pub trait Calc { … }` 之后得到的全部代码。

---

## 1. 职责边界

**做**：把用户 trait 变成「可注册的服务实现 + 可调用的 stub」；把三档声明形态编译期映射到三种调度；生成方法名表与 method id 常量；生成可选的同步客户端。

**不做**：不认识 `request_id`、不认识连接、不认识 `tx`、不认识 `Endpoint`（除了 `connect` 那一层薄封装）；不解析 24 字节头；不做序列化（交给 `Codec`）。

红线：`spile-macros` 里不许出现任何运行期逻辑；用户的 `trait` 里不许出现任何框架类型（`Bytes` / `RpcError` 只在返回值里出现，这是 5.1 已经接受的例外）。

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

> 编解码器从哪来：宏生成的 `call` 用编译期别名 `spile::codec::Default`（由 `codec-*` feature 决定是 postcard 还是 rkyv）。**`Session` 不再有 `C` 泛型参数（v0.10）**——它只持 `S: MessageSource` / `K: MessageSink`，握手固定填 `codec::Default::ID`。v0.9 的 `Session<S, K, C>` 里 `C` 是死参数：`Service::call` 没有 `C` 槽位，热路径的字节从来不用 `C`，`C` 只在握手贡献一个 id——于是 `Session<S, K, OtherCodec>` 会「宣告 A、线上却是 B」，**静默错线格式**。将来若真要同构建内支持第二种 codec，必须先引入 `AnyCodec` 门面。

---

## 3. 宏的输入语法

### 3.1 服务级属性

```rust
#[spile::service(id = 7)]                    // 必填
#[spile::service(id = 7, blocking)]          // 额外生成同步客户端
```

| 参数 | 是否必填 | 类型 | 规则 |
|---|---|---|---|
| `id` | **必填** | `u16` | 显式分配，不自动编号；`0` 允许但建议避开 |
| `blocking` | 可选 | 标志 | 生成同步客户端 `<Svc>BlockingClient`（v0.27 生效，不需要额外 feature）：每个方法都是同步 `fn`，底层是自带线程与 runtime 的 `BlockingSession`，见 §7 |

`id` 必填的三条理由：宏否则无从填 `service_id`；自动编号会在「不同 crate 各自注册」时撞号；顺序一变线上 id 就变。

### 3.2 方法级属性

| 属性 | 适用 | 语义 |
|---|---|---|
| `#[spile(blocking)]` | 仅 `fn` | 该 handler 走 `spawn_blocking`（v0.19 生效） |
| `#[spile(zero_copy)]` | 仅同步 `fn` | 该方法的入站 payload 允许直传；生成 `payload_mode` 覆盖返回 `ZeroCopy`（v0.26 生效，v1 上是 no-op，等 shm） |

**服务级属性只剩 `id = N` 与 `blocking`（v0.31）**：v0.25 的 `error = MyError` 已删除，错误类型改由方法签名表达（`-> T` / `-> Result<T, E>`），宏只在展开期做两件校验——`E` 不许是 `RpcError`（否则报「契约方法不能返回 `RpcError`」），`T` / `E` 不做其它限制。

**不合法的组合必须在编译期报错**，不能降级为警告：`async fn` + `blocking`、`async fn` + `zero_copy`、`fn` 不带 `blocking` 却出现在 blocking client 里（后者由生成逻辑自然规避）。

### 3.3 三档声明（写进用户脑子的唯一规则）

> **`fn` 表示「服务端实现同步完成」，不是「客户端阻塞调用」。**

| 声明 | 服务端执行 | 生成的客户端方法 | 客户端实际行为 |
|---|---|---|---|
| `async fn` | 在当前 task 里 await | `async fn` | 异步 |
| `fn` | 就地执行完（不换线程） | `async fn` | 异步 |
| `fn` + `#[spile(blocking)]` | `spawn_blocking` | `async fn`（`BlockingXxxClient` 里是 `fn`） | 异步 / 同步 |

混写三档是允许的，宏逐方法读 `async` 关键字与属性。

---

## 4. 宏展开产物

对一个 `#[spile::service(id = 7, blocking)] trait Calc { … }`，展开必须产出**恰好**下列项（名字规则固定，避免用户猜）：

| 产物 | 形状 | 说明 |
|---|---|---|
| `CalcClient<S, K>` | 持 `SessionHandle<S, K>`，方法全是 `async fn` | 只泛型化到半身，见 v0.18 说明；线上永远异步，与声明形态无关 |
| `CalcService<S>` | 包一层 `Arc<S>`，`impl Service`，内部 `match method` | 见下方「为什么是包装类型而不是 blanket impl」，v0.17 修 |
| `<CalcService<S> as Service>::schedule` | 逐个 blocking 方法比 `method`，其余落 `Async` | 有 blocking 方法时才生成；没有就用 trait 默认实现（v0.19） |
| `register_calc(registry, svc)` | 用 `Calc` 上的 `id` 占号，内部 `CalcService::new(svc)` | 与 stub 共用同一个 `id` 字面量，一处写两处用 |
| `CalcClient::SERVICE_ID` / `mod __spile_calc::SERVICE_ID` | `const u16` | 排障与日志用；**trait 上不挂关联常量**，见 v0.17 说明 |
| `mod __spile_calc` 内的 `METHOD_NAMES` | `&[(u64, &'static str)]` | span / `UnknownMethod` 日志 / panic 报告 |
| `mod __spile_calc` 内的 `HASH_<METHOD>` 常量 | `const u64` | 编译期哈希值，`match` 的键 |
| `CalcBlockingClient<S, K>` | 持 `spile::blocking::BlockingSession<S, K>` + `deadline_ms`；方法与 `CalcClient` 一一对应但全是同步 `fn`（v0.27） | 仅服务级写了 `blocking` 时生成 |

**为什么是包装类型 `CalcService<S>` 而不是 blanket impl（v0.17 修）**。v0.9–v0.16 一直写的是 `impl<S: Calc + Send + Sync + 'static> Service for S`，看着最省事（`Arc::new(my_calc)` 直接进注册表）。实际写出来在**用户 crate 里必然编译失败**：`Service` 来自 spile、`S` 是裸类型参数，展开代码属于下游 crate，孤儿规则不允许「外部 trait + 裸参数」（`E0210`）。只有两种出路：把 `Service` 改成 phantom 参数（等于让 L3 认不出服务），或者包一层本地类型。后者多一层指针解引用与一次 `Arc::clone`，代价可以忽略，收益是**同一实现类型可以同时实现多个服务 trait**——v0.9 里「一个类型只能属于一个服务」的限制不再存在。`register_calc(registry, svc)` 的签名保持不变，包装发生在宏内部。

**`async fn` 必须改写成带 `Send` 的 RPITIT（v0.17 修）**。trait 里的原生 `async fn` 只保证「返回一个 future」，**不保证它是 `Send`**；而 dispatch 要交出 `BoxFuture`（内含 `+ Send`），于是 `Box::pin(async move { … svc.add(a, b).await … })` 直接报「future cannot be sent between threads safely」。宏因此把 trait 方法的签名改写成：

```rust
fn add(&self, a: i64, b: i64) -> impl ::std::future::Future<Output = Result<i64, RpcError>>
    + ::std::marker::Send + '_;
```

实现侧**照旧写 `async fn`**（编译器认这个形状），`Send` 由它逐 impl 验证：handler 里持有了一个 `!Send` 的东西（例如 `Rc`）会在用户自己的代码上编译失败，而不是等到运行期发现 task 起不来。这也是不用 `async-trait` 的唯一理由——`async-trait` 会把 future 装进 `Box`，与我们「不为每请求多买一次 alloc」的口径相反。

**所有辅助项必须包进一个按服务命名的模块（v0.10 修）**。v0.9 的写法是在模块级直接生成 `METHOD_NAMES` / `HASH_ADD` / `SERVICE_ID` 这些固定名字——**同一个模块里写两个 `#[spile::service]` 就必然重名编译失败**，而「一个模块放几个服务」是完全正常的用法。规则：

```rust
mod __spile_calc {          // 模块名 = __spile_ + 服务名的小写 snake 形式
    pub(super) const METHOD_NAMES: &[(u64, &'static str)] = &[...];
    pub(super) const HASH_ADD: u64 = 0x...;
}
```

- 模块名只由服务名决定，不参与任何线上语义；用户代码里**不应引用**它（生成的客户端/注册函数在父模块里 re-export 需要的项）。
- `HASH_<METHOD>` 用方法名**大写**形式；若方法名本身含非法标识符字符（极少数），退回 `HASH_0`、`HASH_1` 并按声明顺序编号。
- 不要用 `#[doc(hidden)]` 糊过去就完事——重名是编译错误，不是文档问题。

**`blocking` 不能用 `#[cfg(feature = "blocking")]` 生成**：proc-macro 展开出的 `cfg` 检查的是**使用方 crate** 的 feature，不是 spile 的，按字面根本不成立（用户开 `spile/blocking` 不会让这段 `cfg` 变真）。所以生成与否只由属性参数决定；框架侧也不设这个 feature（v0.27）——`src/blocking.rs` 复用已有 tokio、零新依赖，加一个永远为真的开关只会在文档与代码之间制造落差。

---

## 5. dispatch 展开

```rust
// `CalcService<S>` 的 call：整个 dispatch 只包一层 Box，分支里不再各自 Box::pin
fn call(self: Arc<Self>, method: u64, payload: Bytes) -> BoxFuture<'static, Result<Bytes, HandlerError>> {
    Box::pin(async move {
        let me = Arc::clone(&self.inner);            // 内层 Arc<S>：本层不再分层
        match method {
            HASH_ADD => {                                            // async fn，不可失败：-> i64
                let (a, b): (i64, i64) = Default::decode(&payload)?;  // 解码失败 → ? 自动转 HandlerError::Framework{4}
                let out = <S as Calc>::add(&*me, a, b).await;
                Default::encode(&out).map_err(HandlerError::from)
            }
            HASH_DIV => {                                            // async fn，可失败：-> Result<i64, CalcError>
                let (a, b): (i64, i64) = Default::decode(&payload)?;
                match <S as Calc>::div(&*me, a, b).await {
                    Ok(out) => Default::encode(&out).map_err(HandlerError::from),
                    Err(e) => match Default::encode(&e) {
                        Ok(tail) => Err(HandlerError::Business(tail)),  // 业务拒绝：tail 是 E 的编码
                        Err(err) => Err(HandlerError::from(err)),
                    },
                }
            }
            HASH_LOAD => {                                            // fn + #[spile(blocking)]
                let me = Arc::clone(&me);
                tokio::task::spawn_blocking(move || {                  // decode 必须在闭包里
                    let path: PathBuf = Default::decode(&payload)?;
                    let out = <S as Calc>::load(&*me, path)?;
                    Default::encode(&out)
                })
                .await
                .map_err(|join| HandlerError::Framework {             // JoinError → 框架码 0
                    code: 0,
                    message: format!("blocking handler 未正常结束: {join}"),
                })?
            }
            _ => Err(HandlerError::Framework { code: 3, message: "unknown method".into() }),
        }
    })
}

// 同一个 impl 里还要交出「这个方法占哪类额度」，L3 靠它在进 handler 之前扣额度
fn schedule(&self, method: u64) -> Schedule {
    match method {
        HASH_LOAD => Schedule::Blocking,
        _ => Schedule::Async,      // 没有 blocking 方法时，这个方法干脆不生成
    }
}
```

四条不能挪的规则：

1. **`tokio::spawn` 只在 L3 发生一次**（`tokio::spawn(svc.call(method, payload))`）。这里的三个分支只回答「这次执行落在哪个线程池」，不是「各自再 spawn 一次」。
2. **`fn` 分支不换线程**，就在 poll 它的 worker 上就地跑完。微秒级无所谓，毫秒级就是 worker 被占住——所以需要第 7 节的 debug 告警。
3. **`spawn_blocking` 的 `JoinHandle` 必须 await**，`JoinError` 映射成 `code = 0`；`tokio::spawn` 的句柄同理（在 L3 await）。丢掉句柄 = handler panic 无声，客户端只等到超时。
4. **未知 method 回框架码 `3`（`HandlerError::Framework`），不是 `Protocol`**——帧格式没错，是方法没注册。v0.31 起回包侧不再需要「构造一个假的远端错误」：它直接交出 `HandlerError::Framework { code, message }`，由 L3 编码成框架错误帧。同理，L3 内部回 `2` / `7` 走 `send_error(code, message)`，不绕 `RpcError`。**v0.9 曾把这条标成「服务端伪造远端错误」，那是把「发起侧只能读 `PeerRejected`」错当成了「谁都不许说自己失败」**；正确的规则是方向相关：发起侧只能从错误帧得知 `PeerRejected`，回包侧直接陈述自己的 `HandlerError`。

**`schedule` 为什么必须由生成代码给出（v0.19）**：配额检查发生在「reader 解析完头、准备 spawn」这一刻，此时手上只有 `service_id + method`；实现内部走哪个分支要等 `call` 被 poll 才知道，而那时额度已经扣过了。让 L3 猜不了，也不该让 handler 自己扣（那等于把会话层的资源契约下放到业务代码）。代价只是一次 `match` 比较链，且只有存在 blocking 方法时才生成。

**`method_name` 同理，只是用途是观测（v0.28）**：`dispatch` 在进 handler 之前就要建 `spile.request` span，而那时只有 `service_id + method`；`method` 是 `xxh3` 哈希，**反推不出名字**，名字只存在于宏展开的 `mod __spile_xxx::METHOD_NAMES` 里。所以 `Service` 加了一个默认返回 `None` 的方法，宏用一次 `find` 覆盖它：

```rust
fn method_name(&self, method: u64) -> Option<&'static str> {
    METHOD_NAMES.iter().find(|(hash, _)| *hash == method).map(|(_, name)| *name)
}
```

不生成时（手写 `Service`）日志里写 `<unknown>`——比什么都不写诚实：看得出「没接名字表」，而不是以为这个方法没有名字。

参数解码位置：**`async fn` / `fn` 分支在 `call` 开头** `let (a, b) = Default::decode::<(i64, i64)>(&payload)?;`；**`#[spile(blocking)]` 分支必须在 `spawn_blocking` 的闭包里 decode**（v0.10 修）——v0.9 把它写在闭包外，等于只在调用线程上解析、闭包里只剩执行，一次线程切换只干了一件事；更糟的是 decode 出错时闭包外的 `?` 会让 `payload` 在闭包捕获前就被借用，写出来的代码通常编译不过或被迫多一次 clone。解码失败一律 `code = 4`。`Vec<u8>` 等容器类型直接走 serde，不需要特殊处理。

---

## 5.1 错误面：契约零框架类型（v0.31）

方法签名只有两种形状，`E` 由契约自定义、只要求 `Codec`：

```rust
#[spile::service(id = 11)]
trait Ledger {
    async fn balance(&self) -> i64;                            // 不可失败
    async fn withdraw(&self, amount: i64) -> Result<i64, CalcError>;   // 可失败
}
```

生成的 stub 交出两种失败类型：

```rust
// balance（不可失败）：框架失败仍可能发生
pub async fn balance(&self) -> Result<i64, RpcError>;

// withdraw（可失败）：业务拒绝 / 框架失败分两臂
pub async fn withdraw(&self, amount: i64) -> Result<i64, CallError<CalcError>>;
```

`CallError<E>` 的两臂由**来源**决定，不由类型猜：

| 臂 | 来源 | 客户端该做什么 |
|---|---|---|
| `Rejected(E)` | 对端 `flags = 0x09` 的业务帧，payload 就是 `E` | 按业务语义处理（余额不足不是错误，是结果） |
| `Failed(RpcError)` | 本端（超时/断开/编解码）或对端框架帧（`PeerRejected`） | 重试/退避/放弃，看 `RpcError::retryable()` |

dispatch 侧与之对称，三处产物各有明确类型：

```rust
// handler 的返回值 → HandlerError（回包前就决定回哪种帧）
match <S as Ledger>::withdraw(&*me, amount).await {
    Ok(out) => Default::encode(&out).map_err(HandlerError::from),
    Err(e) => match Default::encode(&e) {
        Ok(tail) => Err(HandlerError::Business(tail)),
        Err(err) => Err(HandlerError::from(err)),
    },
}
```

**为什么不做「业务码段」**：v0.25 曾要求 `into_error` 产出 `Remote { code ≥ 0x1_0000 }`，并在 debug 里断言。那套东西的实际作用是把 `E` 压成一个整数 + 文本，代价是：① 契约必须依赖框架类型；② `E` 的结构不能自己定（网关要整数码就只能这样）；③ 判定「这是业务还是框架」靠比较整数，边界一改全体失效。v0.31 用一个 flags bit 替代整段码空间——**身份由协议位表达，内容由契约表达**，两边各管一件事。

**编码失败怎么办**：`E` 编码不出来（codec 是用户可替换的，理论上可能）时退化成框架码 `0` + 错误文本，**绝不再去编码错误本身**——那是递归。同理，客户端解不出 `E` 时按 `InvalidInput` 处理：那说明两端契约不一致，属于该报警的事故，不该假装成业务错误。

## 5.2 `PayloadMode`：两种取值在 v1 都是 no-op（v0.26）

```rust
#[spile::service(id = 8)]
trait Mixed {
    async fn add(&self, ..) -> Result<i64, RpcError>;
    #[spile(zero_copy)]
    fn digest(&self, data: Vec<u8>) -> Result<u64, RpcError>;   // 仅同步 fn
}

// 生成的 impl Service 里多一段
fn payload_mode(&self, method: u64) -> ::spile::PayloadMode {
    match method { HASH_DIGEST => PayloadMode::ZeroCopy, _ => PayloadMode::Copy }
}
```

**为什么这一步不产生任何性能差异**：`tcp` / `local` / `memory` 交出来的 `Bytes` 都是 owned + `'static`，`Copy` 想拷贝也没有「借用槽位」可拷。真正为 `PayloadMode` 付钱的是 M5 的 shm 借用槽位——那时 `Copy` 才等于「先拷到堆缓冲再交给 handler」，`ZeroCopy` 才等于「把槽位直接借出去，handler 结束才归还」。

所以这一步落的是三件**不涉及代码生成结果**的事：类型（`PayloadMode`）、通路（`Service::payload_mode` 与 `schedule` 并列）、护栏（`zero_copy` 只对同步 `fn` 放行）。护栏是唯一有立即收益的部分：把「async handler 攥住槽位」这种错误提前到编译期，而不是等 M5 在线上表现为「环被填满、通道停摆」。

**`SessionOptions` 不加 `payload_mode`**：v1 里没有任何路径会读它——加了就是一个收下不用的参数，和 `Memory::pair` 去掉 `opts` 是同一条理由。M5 加总闸时用户代码不用改，因为 `#[spile(zero_copy)]` 已经存在，语义（全局关时方法标了也不生效）也已经定死。

## 6. 配额与线程池

| 配额 | 谁消费 | 默认 | 说明 |
|---|---|---|---|
| `max_async_in_flight` | 服务端 `async fn` / `fn` handler | 1024（与出站队列同量级） | task 资源 |
| `max_blocking_in_flight` | 服务端 `#[spile(blocking)]` handler | = blocking pool 大小 | 线程资源 |
| 客户端出站配额 | 客户端 | 一组，只用 `max_async_in_flight` | 客户端不跑 handler |

**必须拆成两个**：共用一组额度会出现「一批慢的 blocking 请求把额度占满，此时异步请求明明跑得动却也收到 `Overloaded`」——两类资源混进一个计数器，过载信号就失去意义了。

**占哪一类由 `Service::schedule(method)` 决定**（v0.19）：`try_acquire(Slot::Async | Slot::Blocking)` 在同一次持锁里完成检查与计数，`InboundGuard` 记住 slot、drop 时归还对应那一个。抢不到时回的 `Overloaded` 消息也分开（`"overloaded"` / `"blocking pool overloaded"`）——只报「过载」而不说是哪类资源满了，排障时等于没信息。

---

## 7. 同步客户端

### 7.1 形状：会话连同 runtime 搬到专属线程

| 层 | 谁 | 关键点 |
|---|---|---|
| 调用方 | `BlockingSession::{call, notify, close}` | 只做「写入命令通道 + `recv_timeout` 等回包」，不碰 future、不碰 runtime |
| 命令通道 | `tokio::sync::mpsc::UnboundedSender<Command>` | 调用方线程非阻塞投递（`send` 是同步方法）；`Call` / `Notify` / `Close` 三种命令 |
| 会话线程 | `std::thread` + 2 worker 的 `multi_thread` runtime | 建 runtime → `block_on(握手)` → `spawn(dispatcher)` → `block_on(done)` → `drop(rt)` |
| dispatcher | 会话线程上的 task | 每条 `Call` 再 `tokio::spawn` 一个 task（多线程调用方不能被串行化）；通道关闭后 `close()` 再放行 runtime |

**为什么不是「调用方线程 `block_on`」**：`Runtime::block_on` 在已有 runtime 上下文里 panic（`Cannot start a runtime from within a runtime`），在 `current_thread` 的 worker 上等于死锁（future 唯一的 poll 者正在等自己）。而「同步库被 async 代码调用」是最常见的用法，这个错误只会在线上第一次同步调用时出现。搬到独立线程后，这两种情形在结构上都不存在。

**为什么不是「复用调用方 `Handle` + `spawn`」**：那样「同步客户端」就绑死了「调用方必须有 runtime」——`main`、`Drop`、C 回调里都没有。

### 7.2 两条期限

| 期限 | 入口 | 计时起点 | 到点后 |
|---|---|---|---|
| 协议期限 | `with_deadline(ms)` → header `deadline_ms` | 发起侧发帧 | 服务端回 `DeadlineExceeded`（`PeerRejected{6}`），或发起侧计时器先响 → `Timeout` |
| 本地期限 | `with_timeout(dur)` → `recv_timeout` + 会话线程上的 `tokio::time::timeout` | 调用方投递 | 两侧同刻到点 → `Timeout` |

- 本地期限默认 `None`：没设就等于「等到有结果为止」，与异步客户端一致；默认塞一个数字等于框架替用户决定最多等多久。
- 两个期限用**同一个值**（v0.27 改判 v0.7 的「外层 +50ms」）：会话线程上的 `timeout` 到点会 drop future，`CallGuard` 摘表并送 `Cancel`，所以调用方先返回不会漏收尾；多留 50 ms 只是让内层先说结论，而结论一样。
- 错误映射：`recv_timeout` 的 `Disconnected` → `Closed`（会话线程没了），`Timeout` → `Timeout`。命令投递失败（通道关闭）同样 → `Closed`。

### 7.2.1 名字要跟着命令一起过线程（v0.28）

`spile.call` 的 span 在**会话线程**上建（调用方线程上没有 runtime、也没有 `await`），而 `method_name` 只有宏知道。所以 `Command::Call` 多带一个 `Option<&'static str>`，公开入口是：

```rust
pub fn call(&self, service_id, method, payload, deadline_ms) -> Result<Bytes, RpcError>;
pub fn call_named(&self, service_id, method, method_name: Option<&'static str>, payload, deadline_ms)
    -> Result<Bytes, RpcError>;
```

`call` 就是 `call_named(.., None, ..)`——手写调用方拿不到名字，日志里写 `<unknown>`，语义与线格式都不受影响。宏生成的同步 stub 走 `call_named`，因此**两条路径（异步 stub / 同步 stub）的 span 同名同字段**，一个 `RUST_LOG` 过滤条件能同时捞到。

### 7.3 生命周期

- **构造必须等到握手结果**：`ready` 用 `sync_channel(1)`；握手失败或线程早死 → `Err`，绝不交出一个「看着能用、其实没握手」的客户端。
- **`Clone` 共享会话**（`Arc<Inner>`）：同一会话可以包进多个服务的同步客户端；最后一个副本 drop → 命令通道关闭 → dispatcher 优雅 `close()`（`close_timeout` 兜底）→ 线程退出。
- **`Drop` 不同步等待**：关闭最长 5 s，塞进调用方的 `Drop` 只会变成一次莫名卡顿；会话线程独立，收尾期间调用方可以继续干活。
- **runtime 必须在非 async 上下文 drop**：`drop(rt)` 放在 `block_on` 返回之后、线程函数末尾。

### 7.4 不检测 runtime 上下文

v0.7 的「`try_current()` 只告警、不硬拒绝」在 v0.27 整个取消：future 从不在调用方线程上跑，调用方在不在 runtime 里都不影响正确性。`current_thread` 的 worker 上调用照样拿到回包（`tests/blocking_client.rs::usable_inside_current_thread_runtime` 钉这条）。

### 7.5 验收矩阵（`tests/blocking_client.rs`，8 条）

| 用例 | 钉住什么 |
|---|---|
| `roundtrip_without_runtime` | 普通 `#[test]`（没有 runtime）里也能往返 |
| `timeout_cancels_handler` | 本地期限到点后 handler 的 future 被 drop（取消真的到了服务端） |
| `timeout_keeps_session_usable` | 一次超时不污染会话（pending 表项被摘掉） |
| `deadline_alone_bounds_the_call` | 只设 `deadline` 也有界（外层默认 `None` 不是漏洞） |
| `notify_is_fire_and_forget` | 通知不占回包通道 |
| `close_then_call_is_closed` | `close()` 之后的调用回 `Closed` |
| `blocking_call_also_carries_the_name`（在 `tests/tracing_spans.rs`） | 同步路径的 `spile.call` 也带 `method_name`，且与服务端 `spile.request` 的 `request_id` 对齐 |
| `business_error_survives_sync_stub` | 业务错误经 `from_error` 三处映射后仍是用户码 |
| `usable_inside_current_thread_runtime` | `current_thread` runtime 里同步调用不 panic、不死锁 |

## 8. 本地直连

同一个 `Service` trait，既可以被远程 stub 调用，也可以**同进程直接调**（跳过 L1/L2/L3）。

| 传播方式 | 走不走通道 | 零序列化 |
|---|---|---|
| `memory` transport | 走（用 channel 模拟一条 IPC 通道） | 否（仍是 `Bytes`） |
| **本地直连** | **不走** | 是（直接调 handler） |

两条必须澄清的点：

- 本地直连**不是 transport**，它是 5.6 的调用路径；把两者混成一个实现是 v0.2 的错误。
- 三档声明在本地直连下仍按形态分派：`async fn` 直接 await、`fn` 直接调；标了 `#[spile(blocking)]` 时**由调用方自己负责线程池**，框架不替它调度（那时框架根本不在链路上）。

---

## 9. 编译期检查与报错

| 检查 | 时机 | 报错要求 |
|---|---|---|
| `id` 缺失 / 不是 `u16` 字面量 | 展开 | 指出「必须写 `id = N`」，并给一行正确写法 |
| `async fn` + `#[spile(blocking)]` | 展开 | 「`blocking` 只能标在同步 `fn` 上」；v0.19 起由 `tests/compile-fail/blocking_on_async.rs` 冻结文本 |
| 未知方法级属性（如 `cache`） | 展开 | 「只接受 `blocking` 与 `zero_copy`」；由 `unknown_method_attr.rs` 冻结 |
| `async fn` / `#[spile(blocking)] fn` + `zero_copy` | 展开 | 分别说明「跨 await 会攥住槽位」「池线程时长不可预期」；由 `zero_copy_on_async.rs` / `zero_copy_on_blocking.rs` 冻结 |
| 接收者不是 `&self`、返回不是 `Result`、参数是模式解构 | 展开 | 定位到具体方法，说明展开需要「简单标识符参数 + `Result` 返回」 |
| 方法名重复或哈希相撞 | 展开 | 常量断言式检查；报出两个方法名 |
| `async fn` + `blocking` | 展开 | 「同步方法才需要 `blocking`」 |
| 注册撞号 | 运行期（注册时） | `Err`，**不覆盖**已有服务 |
| 改方法签名 | 编译期 | 在用户侧编译失败，错误信息必须定位到具体方法 |

测试手段：`compile-fail` 用例（`trybuild` 作为 **dev-dependency**，不进默认依赖集）。截至 v0.31 共六条：`blocking_on_async`、`missing_id`、`unknown_method_attr`、`zero_copy_on_async`、`zero_copy_on_blocking`、`rpc_error_in_contract`（契约里写 `-> Result<T, RpcError>` 必须展开期报错；v0.25 的 `error_type_mismatch` 随 `error = E` 一并删除）。任何一条的 `.stderr` 都是冻结文本，改文案就要同步改它。

---

## 10. 服务端装配与连接建立（v0.10 新增）

v0.9 的四份文档把「宏产出什么」写得很细，却**没有一处说清这些产物怎么装成一个能跑的服务**：`Server::bind` 长什么样、accept 循环在哪、每连接怎么建 `Session`、`ServiceRegistry` 怎么共享给所有连接、客户端到底用泛型还是 `AnyTransport`。照着写的人到这一步会自己发明一套，然后与 L1/L3 的签名对不上。本节把它定死。

### 10.1 `Server` 与 `bind`

```rust
pub struct Server<T: Transport> {
    transport: T,                 // bind 的产物（listener，不持有每连接状态）
    registry: ServiceRegistry,    // 按值持有，`serve` 时才包 Arc（v0.18 修）
    transport_opts: TransportOptions,
    session_opts: SessionOptions, // 含 max_message_len（v0.10 从 TransportOptions 迁来）
}

impl<T: Transport + 'static> Server<T> {
    pub async fn bind(ep: &Endpoint, transport_opts: TransportOptions,
                      session_opts: SessionOptions) -> io::Result<Self>;
    pub fn registry(&mut self) -> &mut ServiceRegistry;         // 给宏生成的 register_xxx 用
    pub fn transport(&self) -> &T;                               // 端口写 0 时靠它拿真地址
    pub fn register(&mut self, id: u16, svc: Arc<dyn Service>) -> Result<(), RpcError>;
    pub async fn serve(self) -> io::Result<()>;                 // accept 循环，永不返回（除错误）
    pub async fn serve_with<F: FnMut(Arc<Session<T::Source, T::Sink>>)>(self, on_conn: F) -> io::Result<()>;
}
```

注册表**按值**持有而不是 `Arc<ServiceRegistry>` 一开始就共享：注册只该发生在 `bind` 之后、`serve` 之前，把 `Arc` 提前交出去等于允许「运行中改路由」，那要么加锁、要么出现「一半连接用旧表」。`serve` 内部再 `Arc::new`，每连接 `clone` 一次——一个进程一次的事，不在热路径上。

规则：

- **`bind` 是异步的**（`L1-C4`，v0.13 起）：底层的 `tokio::net::TcpListener::bind().await` 会把 fd 注册进当前 runtime，这一步只能在 executor 里做。做成异步后这个前置条件由类型承担，调用方漏了 runtime 是编译不过的，而不是拿到一个「看起来像绑定失败」的构造期 panic。用法就是 `Server::bind(ep, transport_opts, session_opts).await?`，天然落在 `#[tokio::main]` 的异步段里。
- `register` 与宏生成的 `register_calc(registry, svc)` 是同一件事的两个入口：前者收已装箱的 `Arc<dyn Service>`，后者收具体类型并在内部包一层 `CalcService<S>`。**注册发生在 `bind` 之后、`serve` 之前**，撞号返回 `Err`、**不覆盖**已有服务（`INV-14`），这也是 `registry()` 只给 `&mut` 的理由——`serve` 一调用就再没有可变入口了。
- 三个配置各归各位，不许合并成一个 `Options`：`TransportOptions` 给 L1（`connect_timeout` / `spin_hint`），`SessionOptions` 给 L3（`max_message_len` / 配额 / `payload_mode` / 超时）。`design.md` 2.7 已经因为「塞在一起」返工过一次。

### 10.2 accept 循环与每连接的会话

```rust
let registry = Arc::new(self.registry);
loop {
    let (source, sink) = match self.transport.accept(&self.transport_opts).await {
        Ok(pair) => pair,
        Err(err) if is_per_connection(&err) => { continue; }   // 这个连接坏了，listener 还好
        Err(err) => return Err(err),                           // listener 级失败
    };

    let (session, _handle) = Session::new(source, sink, Arc::clone(&registry),
                                          self.session_opts.clone())?;
    tokio::spawn(async move {
        session.wait_closed().await;      // 唯一的「连接结束」信号（v0.18 新增）
        let _ = session.close().await;    // 已 Closed 时返回 Err(Closed)，忽略即可
    });
}
```

- **`ServiceRegistry` 是 `Arc` 共享给所有连接的**：每连接复制一份注册表会让「同一进程内路由不一致」变成可能，而且白白多 N 份 map。`serve` 里包一次 `Arc`，每连接 `clone` 给 `Session`。
- **每连接一个 `Session`，两个 task（reader/writer）**；`serve` 本身只做 accept，不参与收发。这样 N 条连接 = N×2 个 task，与「每连接两个半身」的结构一致。
- `SessionHandle`（发起端）在纯服务端场景用不上，但**不能随手丢掉**：定死 `SessionHandle::drop` **不关连接**（它只是个请求入口），真正关闭由 `Arc<Session>::close()` 或 drop 最后一个 `Arc<Session>` 决定。这一条不写，示例里 `let (session, _handle) = ...` 会被人当成「丢了就会关」。
- 服务端驱动的收尾入口有两个，区别只在「等不等」：`Session::close(&self)`（`03` 7.3 六步，等在飞回完）与 `Session::abort(&self)`（发 bit2 急停，立刻弃掉在飞；见 `03` 6.4）。`abort` **不挂到 `SessionHandle`** 上：句柄的语义是「发起请求」，单方面给整条连接判死不该是它的能力。「等对端 `GoAway` / EOF 再收尾」的等待逻辑属于 `Session` 内部，不要在示例代码里再拼一套状态机。**「连接结束了没有」也只有一个观测点**：`Session::wait_closed()`（监听 `closed: Arc<Notify>`）；别用 `is_closed()` 轮询，那会在每条连接上留一个 1 ms 定时器。
- accept 循环的错误分两类：**单连接 `accept` 失败**（对端提前断开）记 `debug` 后继续；**listener 级别失败**（`EMFILE`、listener 被关）直接返回 `Err`。不要用 `while let Ok(..)` 把 listener 错误静默吞成「永远 accept 不到新连接」。

### 10.3 客户端：泛型还是 `AnyTransport`

| 场景 | 用什么 | 理由 |
|---|---|---|
| 生产 / 基准 | `connect::<TcpTransport>`（或 `Local`） | 泛型单态化，零门面开销；`INV-18` 禁止基准用门面 |
| 示例的 `--transport` 开关 / 测试矩阵 | `connect_any` + `AnyTransport` | 运行时选择，接受一次 box |
| 进程内自测 | `Memory::pair` + `CalcClient::new` | 没有端点、没有 `Transport`，仍走同一条 L2/L3 代码 |

**`CalcClient` 的类型参数是传输半身，不是 transport（v0.18 修）**。v0.10 起写的是 `CalcClient<T: Transport>`，看起来最自然，实际是错的：`Memory::pair` 明确不实现 `Transport`（它没有 `bind`/`connect`/`accept` 可对应），于是 `local_calc` 这条最重要的「一条命令跑通」路径反而用不上生成的 stub。改成两个类型参数之后，stub 只依赖 `SessionHandle<S, K>`，与「连接是怎么建立的」完全解耦：

```rust
pub struct CalcClient<S, K>
where S: MessageSource + 'static, K: MessageSink + 'static
{
    handle: SessionHandle<S, K>,
    deadline_ms: u32,          // 0 = 不设期限
}

impl<S, K> CalcClient<S, K> {
    pub fn new(handle: SessionHandle<S, K>) -> Self;      // deadline_ms = 0
    pub fn with_deadline(self, ms: u32) -> Self;          // 换值返回新客户端
    pub fn deadline_ms(&self) -> u32;
    // 业务方法：encode → call(.., self.deadline_ms) → decode，全部 async
}

// 连接是独立的一步，按传输类型单态化
pub async fn connect<T: Transport>(ep: &Endpoint, opts: ConnectOptions)
    -> Result<SessionHandle<T::Source, T::Sink>, RpcError>;
```

于是「换传输」只换 `connect::<T>` 这一处，`CalcClient` 与业务代码一行不动；`ConnectOptions` 把 `TransportOptions` 与 `SessionOptions` 打包，避免 connect 出现两套参数顺序。`connect` 还会等握手到 `Ready`：`codec_id` / 版本不符在这里就变成 `Err`，不留到第一次业务调用超时。

**期限为什么挂在客户端上，而不是每个方法多一个参数（v0.24）**。`deadline_ms` 的线格式 v0.20 就落了、`CallGuard` 也在等它，但 stub 一直硬传 `0`——用户想要一次带超时的调用，只能绕到 `SessionHandle::call` 自己 encode，等于类型化 API 缺了一个最有用的旋钮。两个候选：

- 每个方法多一个 `deadline: u32` 参数：签名被噪声污染，且 `add(a, b, 100)` 这种第一个参数是业务值的调用点读起来完全不自解释；
- 客户端上挂一个字段、`with_deadline` 换值（**采用**）：期限本来就是「这条连接上这一批调用的预算」，重试与预算都在上层，一个客户端一份期限正好对应一个调用点。

用 `self` 换值而不是 `&mut self`：`SessionHandle` 是 `Clone` 的、客户端可以随手复制给并发任务，可变期限会让两条并发调用互相影响——这种 bug 只在压力下出现，代价远高于多写一次 `CalcClient::new`。

**这条改判的代价**：用户多写一行 `CalcClient::new(connect::<T>(ep, opts).await?)`。换来的是「进程内配对、UDS、TCP、shm 共用同一份客户端代码」，这也是 `tests/concurrency.rs` 能在两条传输上跑同一套断言的前提。
```

- `connect` 内部顺序固定：`transport.connect(ep, &transport_opts)` → `Session::new(source, sink, empty_registry, session_opts)` → 取 `SessionHandle` 包进客户端。
- **客户端也传非空 `registry` 的情形**：4.4 的双向对等允许服务端回调客户端，若确实要暴露回调服务，就注册到自己的 registry；否则传 `ServiceRegistry::empty()`（**不是 `None`**，`Session::new` 的签名如此）。

**`SessionHandle` 的三个发起入口（v0.20 补齐）**：

```rust
impl<S, K> SessionHandle<S, K> {
    pub async fn call(&self, service_id: u16, method: u64, payload: Bytes, deadline_ms: u32)
        -> Result<Bytes, RpcError>;         // 有回包；deadline_ms = 0 表示不设期限
    pub async fn notify(&self, service_id: u16, method: u64, payload: Bytes)
        -> Result<(), RpcError>;            // 无回包；`Ok(())` 只代表「发出去了」
    pub async fn close(&self) -> Result<(), RpcError>;
}
```

- `notify` 与 `call` 共用同一条 `tx` 队列、同样的背压，只是**不插 pending 表**——因此它不占出站配额，也不会有超时或 `Cancel`。代价是失败不可观测：服务端 handler 报错时只写 `tracing::warn!`，调用方永远看到 `Ok(())`。要确认结果就改用 `call`。
- 服务端侧的 `Notification` 与 `Request` 走同一个 `dispatch`（查服务 → 扣额度 → spawn），差别只有 `want_reply = false`：**未注册服务、额度不足都只记日志，绝不回 `Response`**。给通知回一个 `Response` 是协议噪声：对端没有对应 pending，收到只会丢掉。
- `deadline_ms` 的客户端语义是「本端愿意等多久」，因此它同时决定头里的剩余预算。放弃（超时或 future 被 drop）时 `CallGuard` 摘表并触发 `Cancel`，见 `03` 6.3。

**客户端 stub 顺手带一个 span（v0.28）**：生成的异步方法先建 `spile.call`（`service_id` / `method` / `method_name` / `deadline_ms`，`request_id` 留 `Empty`），再用 `spile::__private::instrument` 把整次调用包进去。两处细节值得写下来：

1. **`method_name` 在客户端这一侧也是常量**——宏知道方法名，没必要等回包才知道；
2. **`request_id` 由 L3 回填**：分配它的地方是 `Session::call`，那里做 `Span::current().record("request_id", id)`；span 上没有这个字段时（用户直接调 `handle.call`）`record` 是 no-op，不会报错。

不在 stub 里自己生成 `request_id`：那会把「id 分配」这件事复制成两份，迟早不一致。

### 10.4 完整的装配清单（照抄这一条就不会漏）

| 步骤 | 服务端 | 客户端 |
|---|---|---|
| 1 | `Server::bind(ep, transport_opts, session_opts).await?` | — |
| 2 | `server.register::<Calc>(7, Arc::new(CalcImpl))?`（或 `register_calc(&mut registry, svc)`） | — |
| 3 | — | `CalcClient::connect(ep, opts).await?` |
| 4 | `server.serve().await?` | 调方法 |
| 5 | 收到 `GoAway` / 本地 `close()` → 按 `03` 7.3 六步收尾 | 收到 `GoAway` → 新请求 `Closed`，等自己的 pending 清零 |

---

## 11. 示例与验收

| 示例 | 形态 | 阶段 | 关键点 |
|---|---|---|---|
| `local_calc` | `memory`，单进程 | M2 | 最小集全链路：stub → session → dispatch → codec → 返回；一条命令可跑 |
| `local_calc`（补齐） | `memory` | M3 | 七种形态：`async fn` / `fn` / blocking / `Notification` / 超时 / `GoAway` / 错误体；v0.27 起多两行同步客户端（第二条会话，服务端在独立线程上） |
| `calc_local` | `local`，两进程 | M3（v0.23 已落地） | 换 transport 不改业务代码（架构验收）；同一二进制 `server` / `client` 两角色 |
| `examples/calc-split`（`calc-api` / `calc-server` / `calc-client`） | `tcp`，两进程，**三个 crate** | M3（v0.29 新增） | 真实发布形态：契约 / 服务端 / 客户端各自独立编译；客户端依赖树里没有服务端（判据见 `design.md` 10.7） |

**示例的接口与实现必须分文件（v0.28.1）**：`calc_service/api.rs` 只放 `Calc` trait 与打印/构造错误的小工具，`calc_service/imp.rs` 放 `Calculator` 与业务逻辑。v0.30 删掉 `calc_server` / `calc_client` 后，`spile/examples/` 里剩下的 `calc_local` 与 `local_calc` 都同时扮演两端，两半都得引——所以这条规则在**本 package 内**已经没有反例可打，真正守住它的判据移到 `examples/calc-split/`（契约是独立 crate，客户端 crate 的依赖树里没有服务端）。

判据不是「看着分开了」，而是客户端二进制里没有那个类型：在 `examples/calc-split` 下 `nm target/debug/calc-client | grep -c Calculator` = 0（服务端同一命令是 37）。v0.29 之前这条判据打在 `examples/calc_client` 上，v0.30 删掉它之后只剩跨 crate 这一处可验——恰好也是更强的形态（依赖树里连 `calc-server` 都没有，`cargo tree -p calc-client | grep calc-server` 为空）。这件事在 RPC 项目里不是洁癖——宏产物只有 `Service::call` 的 `match` 分支体依赖实现类型，把实现编进客户端等于**把业务逻辑分发出去**。

`local_calc` 的两条预期输出是验收硬指标（不能随手改文案）：

- `[server] listening on memory (in-process pair, no endpoint)`
- `Err(Rejected(DivideByZero))`

M2 的范围必须写清：**只支持 `async fn` 声明**，不含 `Notification` / 超时 / `GoAway` / 同步 handler。把七种形态全压给 M2 是 v0.6 的错误，按现文不可能通过。

---

## 12. 明确不做

- **不给 `fn` 声明自动生成 `fn` 客户端方法**：那会把「handler 形态」和「调用形态」重新绑死，正好绕回 5.2 想拆开的错误。要同步客户端必须是显式宏参数。
- 不默认生成同步客户端：只由宏参数（`#[spile::service(id = N, blocking)]`）开启，代价是「每个会话多 3 条线程 + 一套 runtime 生命周期」；也不提供「多 client 复用一个 runtime」的入口（共享粒度是会话，见 §7.3）。
- handler 内反向调用对端（需 `&ServiceContext` 首参）：会改动所有 handler 签名，M3 之后再说。
- 协作式取消（handler 内轮询 `is_cancelled()`）：同上。
- 用 `HashMap<u64, Box<dyn Fn>>` 代替 `match`：调度决策在运行期只剩一个函数指针，无法分流；且 method id 重复无法在编译期发现。
- 引入 `async-trait`：Rust 1.75 已有 AFIT；只有 trait object 场景才需要显式 `BoxFuture`，已经处理。
