//! 两进程本地 IPC：同一个二进制，`server` / `client` 两个角色。
//!
//! ```text
//! cargo run --example calc_local -- server
//! cargo run --example calc_local -- client
//! ```
//!
//! 第二个位置参数可以覆盖地址。Unix 上是 socket 文件路径（默认 `/tmp/spile-calc.sock`，
//! 因为 `sun_path` 只有约 104 字节，路径必须浅）；Windows 上它只是 pipe 名的**种子**
//! （默认 `spile-calc`），两端给同一个字符串就落到同一条 pipe。
//!
//! 与 `examples/calc-split` 的 `calc-server` 业务代码逐行相同，只换了 `Endpoint` 与
//! `Transport` 类型——这就是「换 transport 不改业务代码」的可执行证据。

// 同一个二进制既是服务端又是客户端，所以两半都引。分成两个程序时，客户端那一半
// 只该引 `api.rs`——那个形态见 `examples/calc-split/`（客户端是独立 crate，依赖树里
// 连服务端都看不到）。
#[path = "calc_service/api.rs"]
mod calc;
#[path = "calc_service/imp.rs"]
mod calc_imp;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use calc::{CalcClient, describe_call, register_calc};
use calc_imp::Calculator;
use spile::{
    ConnectOptions, Endpoint, Server, SessionOptions, connect,
    transport::local::{LocalSink, LocalSource, LocalTransport},
    transport::{LocalAddr, TransportOptions},
};

/// 默认地址。Unix 的 `sun_path` 上限是 104 字节，所以路径必须浅；Windows 上这只是
/// pipe 名的种子，短一点更省事（最终名字由后端哈希出来，见下）。
#[cfg(unix)]
const DEFAULT_PATH: &str = "/tmp/spile-calc.sock";
#[cfg(windows)]
const DEFAULT_PATH: &str = "spile-calc";

const CONNECT_BUDGET: Duration = Duration::from_secs(2);
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let role = std::env::args().nth(1).unwrap_or_else(|| "server".into());
    let path = PathBuf::from(
        std::env::args()
            .nth(2)
            .unwrap_or_else(|| DEFAULT_PATH.into()),
    );

    match role.as_str() {
        "server" => serve(path).await,
        "client" => run_client(path).await,
        other => Err(format!("未知角色 {other:?}，只接受 server / client").into()),
    }
}

async fn serve(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let ep = Endpoint::Local(LocalAddr::new(&path));

    // 两个后端的共同规则（见 `transport/local/mod.rs`）：「同一个地址只能有一个活的
    // 监听者」。Unix 上这还包括「清掉上次崩溃留下的 socket 文件，且绝不碰用户文件」；
    // Windows 上名字随句柄消失，所以没有残留问题，只有「已有人在听」这一种拒绝。
    let mut server =
        Server::<LocalTransport>::bind(&ep, TransportOptions::default(), SessionOptions::default())
            .await?;
    register_calc(server.registry(), Arc::new(Calculator))?;

    println!("[server] listening on local://{}", path.display());
    server.serve().await?;
    Ok(())
}

async fn run_client(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let ep = Endpoint::Local(LocalAddr::new(&path));
    let client = connect_with_retry(&ep).await?;
    println!("[client] connected to local://{}", path.display());

    let sum = client.add(2, 3).await?;
    println!("[client] add(2, 3) = {sum}");

    match client.div(1, 0).await {
        Ok(value) => println!("[client] div(1, 0) = {value}"),
        Err(err) => println!("[client] div(1, 0) -> {}", describe_call(&err)),
    }

    client.handle().close().await?;
    Ok(())
}

async fn connect_with_retry(
    ep: &Endpoint,
) -> Result<CalcClient<LocalSource, LocalSink>, Box<dyn std::error::Error>> {
    // 与 `examples/calc-split` 的 `calc-client` 同样按「重试 + 预算」处理时序：服务端可能晚起几百毫秒。
    let deadline = Instant::now() + CONNECT_BUDGET;
    loop {
        match connect::<LocalTransport>(ep, ConnectOptions::default()).await {
            Ok(handle) => return Ok(CalcClient::new(handle)),
            Err(err) if Instant::now() < deadline => {
                eprintln!("[client] 连接未就绪（{err}），{RETRY_INTERVAL:?} 后重试");
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
