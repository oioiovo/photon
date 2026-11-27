# calc-split —— 「三 crate」的真实形态

spile 自己的 `examples/` 是**单个 package 的 example target**，只能演示 API 形状；真实项目里
接口、服务端、客户端是三件不同的事，编译边界也不一样。这份 demo 就是那种形态：

```
calc-split/
├── api/      # 契约 crate（lib）：trait + DTO + 错误描述，客户端与服务端都依赖它
├── server/   # 服务端程序（bin）：impl + main，只有它知道业务逻辑
└── client/   # 客户端程序（bin）：只调 stub，完全看不到实现
```

## 跑起来

```text
cd examples/calc-split
cargo run -p calc-server            # 默认 127.0.0.1:7001
cargo run -p calc-client
```

## 为什么这么拆

1. **api 只承载 ABI**：trait 形状、DTO 字段顺序（postcard 按位置编码，改字段顺序等于改线格式）、
   业务错误码段、`service_id`。它不依赖任何一方的业务库——一旦反过来依赖，拆分就白做了。
2. **server 才引实现**：`impl Calc for Calculator` 与 `main`。client 的依赖树里没有 `calc-server`，
   也就没有业务逻辑、没有数据库连接池之类的重型依赖。
3. **client 只引 api**：`CalcClient` 由宏在 api crate 里展开，client 拿到的只是 stub。

判据不是「看着分开了」，而是：

```text
cargo tree -p calc-client | grep calc-server      # 必须为空
nm target/debug/calc-client | grep -c Calculator  # 必须为 0
```

## 一个已知的空壳

api crate 里除 stub 之外还会有 `CalcServer` 包装类型与 `register_calc`（宏一次展开、两产物）。
它们只是泛型占位、不含任何业务逻辑，client 编进来也不会执行；要彻底剔除得把宏拆成
`service_client` / `service_server` 两半，收益只是少一个空壳类型，代价是「契约只写一份」没了，
所以明确不做。见 `docs/design.md` 10 节。
