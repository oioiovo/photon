//! 负路径全览：**框架拒绝一件事时，调用方到底拿到什么**。
//!
//! ```text
//! cargo run --example conformance
//! ```
//!
//! 三个 happy-path 示例（`local_calc` / `calc_local` / `examples/calc-split`）演的是
//! 「一路顺风长什么样」。真实项目里更常见的问题恰恰在反方向：调错 `service_id`、
//! 方法号对不上、发的东西超过对端上限、对端根本没在听。这一份把每一种都走一遍，
//! 并断言**具体形态**——`PeerRejected{code}` 还是 `InvalidInput`，连接是死是活。
//!
//! 三个传输各管一段（同一份用例在 L3 上跑，只换 L1）：
//!
//! - `memory`：无端点的进程内对，验「服务/方法不存在、通知、关闭之后」；
//! - `local`：真后端（Unix socket / Windows pipe），验「对端上限、监听者独占、无人监听」；
//! - `tcp`：真 socket，验「对端上限」与「端口无人监听」。
//!
//! 失败会打印「期望 / 实际」并以退出码 1 收尾（见 `support/check.rs`）。

#[path = "calc_service/api.rs"]
mod calc;
#[path = "calc_service/imp.rs"]
mod calc_imp;
#[path = "support/check.rs"]
mod check;

use std::path::PathBuf;
use std::sync::Arc;

use calc::{CalcClient, describe, register_calc};
use calc_imp::Calculator;
use check::Check;
use spile::{
    Bytes, CallFailure, ConnectOptions, Endpoint, RpcError, Server, ServiceRegistry, Session,
    SessionOptions, connect,
    transport::local::LocalTransport,
    transport::memory::Memory,
    transport::{LocalAddr, TransportOptions},
};

/// 服务端只肯收 4 KiB。客户端仍是默认 64 KiB ——「对端上限」才有意义。
const SMALL_LIMIT: u32 = 4096;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut check = Check::new("conformance");

    check.section("memory：服务 / 方法不存在、通知、关闭之后");
    memory_section(&mut check).await?;

    check.section("local：对端上限、监听者独占、无人监听");
    local_section(&mut check).await?;

    check.section("tcp：对端上限、端口无人监听");
    tcp_section(&mut check).await?;

    check.finish();
    Ok(())
}

/// 进程内一对半身：不需要端点，L3 以上完全同一条路径。
async fn memory_section(check: &mut Check) -> Result<(), Box<dyn std::error::Error>> {
    let ((client_src, client_sink), (server_src, server_sink)) = Memory::pair();
    let mut registry = ServiceRegistry::empty();
    register_calc(&mut registry, Arc::new(Calculator))?;

    let (server, _server_handle) = Session::new(
        server_src,
        server_sink,
        Arc::new(registry),
        SessionOptions::default(),
    )?;
    let (client_session, handle) = Session::new(
        client_src,
        client_sink,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )?;
    client_session.wait_ready().await?;

    // ① 服务号不存在：对端**知道**该回什么（它是路由错误，不是协议错误）。
    match handle.call(999, 1, Bytes::new(), 0).await {
        Err(CallFailure::Failed(RpcError::PeerRejected { code: 2, .. })) => {
            check.expect("未注册的 service_id → PeerRejected{code=2}", true, "");
        }
        other => check.expect(
            "未注册的 service_id → PeerRejected{code=2}",
            false,
            format!("{other:?}"),
        ),
    }

    // ② 服务存在、方法号不存在：同样是框架判定，码不同（3 = unknown method）。
    match handle.call(calc::service_id(), 99, Bytes::new(), 0).await {
        Err(CallFailure::Failed(RpcError::PeerRejected { code: 3, .. })) => {
            check.expect("未知 method → PeerRejected{code=3}", true, "");
        }
        other => check.expect(
            "未知 method → PeerRejected{code=3}",
            false,
            format!("{other:?}"),
        ),
    }

    // ③ 通知没有回包通道：指向不存在的服务也只是「发出去了」，对端静默丢。
    match handle.notify(999, 1, Bytes::new()).await {
        Ok(()) => check.expect("通知指向未注册服务：发出即成功（没有回包通道）", true, ""),
        Err(err) => check.expect("通知指向未注册服务：发出即成功", false, describe(&err)),
    }

    // ④ 关闭之后：本端就能判，不必等对端。
    handle.close().await?;
    match handle.call(calc::service_id(), 1, Bytes::new(), 0).await {
        Err(CallFailure::Failed(RpcError::Closed)) => {
            check.expect("close() 之后再发请求 → Closed", true, "");
        }
        other => check.expect("close() 之后再发请求 → Closed", false, format!("{other:?}")),
    }

    // ⑤ 入站配额：只留一个名额，第二条请求拿到对端自陈的 overloaded（code=1）。
    let ((quota_client_src, quota_client_sink), (quota_server_src, quota_server_sink)) =
        Memory::pair();
    let mut quota_registry = ServiceRegistry::empty();
    register_calc(&mut quota_registry, Arc::new(Calculator))?;
    let (_quota_server, _quota_server_handle) = Session::new(
        quota_server_src,
        quota_server_sink,
        Arc::new(quota_registry),
        SessionOptions {
            max_async_in_flight: 1,
            ..SessionOptions::default()
        },
    )?;
    let (quota_session, quota_handle) = Session::new(
        quota_client_src,
        quota_client_sink,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )?;
    quota_session.wait_ready().await?;

    let in_flight = tokio::spawn({
        let client = CalcClient::new(quota_handle.clone());
        async move { client.slow_op(120).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    match CalcClient::new(quota_handle).slow_op(1).await {
        Err(RpcError::PeerRejected { code: 1, .. }) => {
            check.expect("配额满 → PeerRejected{code=1 overloaded}", true, "");
        }
        Ok(ms) => check.expect(
            "配额满 → PeerRejected{code=1}",
            false,
            format!("回了 Ok({ms})"),
        ),
        Err(err) => check.expect("配额满 → PeerRejected{code=1}", false, describe(&err)),
    }
    let _ = in_flight.await;

    server.close().await?;
    Ok(())
}

/// 本示例用的本地地址种子：同进程唯一，Unix 上是真实 socket 文件。
fn local_seed(tag: &str) -> PathBuf {
    #[cfg(unix)]
    {
        PathBuf::from(format!("/tmp/spile-conf-{tag}-{}.sock", std::process::id()))
    }
    #[cfg(windows)]
    {
        PathBuf::from(format!("spile-conf-{tag}-{}", std::process::id()))
    }
}

async fn local_section(check: &mut Check) -> Result<(), Box<dyn std::error::Error>> {
    let seed = local_seed("limit");
    #[cfg(unix)]
    let _ = std::fs::remove_file(&seed);
    let ep = Endpoint::Local(LocalAddr::new(&seed));

    let mut server = Server::<LocalTransport>::bind(
        &ep,
        TransportOptions::default(),
        // 只肯收 4 KiB：客户端随后发 8 KiB 的 payload 就必须被本端拦下。
        SessionOptions {
            max_message_len: SMALL_LIMIT,
            ..SessionOptions::default()
        },
    )
    .await?;
    register_calc(server.registry(), Arc::new(Calculator))?;
    let addr = ep.clone();
    tokio::spawn(async move {
        let _ = server.serve().await;
    });

    let client =
        CalcClient::new(connect::<LocalTransport>(&addr, ConnectOptions::default()).await?);

    // ⑥ 超过对端上限：本端判 `InvalidInput`，连接必须还能用。
    match client.fast_hash(vec![0u8; 8 * 1024]).await {
        Err(RpcError::InvalidInput(message)) => check.expect(
            &format!("发 8 KiB 给 4 KiB 的对端 → InvalidInput（{message}）"),
            true,
            "",
        ),
        Ok(value) => check.expect(
            "发 8 KiB 给 4 KiB 的对端 → InvalidInput",
            false,
            format!("居然成功：{value:#x}"),
        ),
        Err(err) => check.expect(
            "发 8 KiB 给 4 KiB 的对端 → InvalidInput",
            false,
            describe(&err),
        ),
    }
    match client.add(1, 1).await {
        Ok(2) => check.expect("超限之后同一条连接仍然可用 → add(1, 1) = 2", true, ""),
        Ok(value) => check.expect("超限之后同一条连接仍然可用", false, format!("回了 {value}")),
        Err(err) => check.expect("超限之后同一条连接仍然可用", false, describe(&err)),
    }

    // ⑦ 同一地址的第二个监听者：必须被拒（`INV-24` 的本地版本）。
    //    地址现在有人听着，所以这条 bind 一定撞上「已存在」。
    match Server::<LocalTransport>::bind(
        &addr,
        TransportOptions::default(),
        SessionOptions::default(),
    )
    .await
    {
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            check.expect("同一地址的第二个监听者 → AlreadyExists", true, "");
        }
        Err(err) => check.expect(
            "同一地址的第二个监听者 → AlreadyExists",
            false,
            format!("错误种类是 {:?}（{err}）", err.kind()),
        ),
        Ok(_) => check.expect(
            "同一地址的第二个监听者 → AlreadyExists",
            false,
            "竟然绑上了：两个进程会静默地各听一半流量",
        ),
    }

    // ⑧ 没人监听的种子：连接直接失败，不是挂住。
    let nobody = Endpoint::Local(LocalAddr::new(local_seed("nobody")));
    match connect::<LocalTransport>(&nobody, ConnectOptions::default()).await {
        Err(RpcError::Io(err)) => check.expect(
            &format!("连到没人监听的地址 → Io({:?})", err.kind()),
            err.kind() == std::io::ErrorKind::NotFound,
            format!("错误种类是 {:?}（{err}）", err.kind()),
        ),
        Err(err) => check.expect("连到没人监听的地址 → Io(NotFound)", false, describe(&err)),
        Ok(_) => check.expect("连到没人监听的地址 → Io(NotFound)", false, "竟然连上了"),
    }

    #[cfg(unix)]
    let _ = std::fs::remove_file(&seed);
    Ok(())
}

async fn tcp_section(check: &mut Check) -> Result<(), Box<dyn std::error::Error>> {
    // 端口交给内核挑：`0` 绑完再读回真实端口，避免和前一个用例撞。
    let mut server = Server::<spile::transport::tcp::TcpTransport>::bind(
        &Endpoint::Tcp("127.0.0.1:0".parse()?),
        TransportOptions::default(),
        SessionOptions {
            max_message_len: SMALL_LIMIT,
            ..SessionOptions::default()
        },
    )
    .await?;
    register_calc(server.registry(), Arc::new(Calculator))?;
    let addr = server.transport().local_addr()?;
    tokio::spawn(async move {
        let _ = server.serve().await;
    });

    let client = CalcClient::new(
        connect::<spile::transport::tcp::TcpTransport>(
            &Endpoint::Tcp(addr),
            ConnectOptions::default(),
        )
        .await?,
    );

    // ⑨ 同一条「对端上限」边界，这次过真 socket：结论必须与 local 一致。
    match client.fast_hash(vec![0u8; 8 * 1024]).await {
        Err(RpcError::InvalidInput(_)) => {
            check.expect("tcp：发 8 KiB 给 4 KiB 的对端 → InvalidInput", true, "");
        }
        Ok(value) => check.expect(
            "tcp：发 8 KiB 给 4 KiB 的对端 → InvalidInput",
            false,
            format!("居然成功：{value:#x}"),
        ),
        Err(err) => check.expect(
            "tcp：发 8 KiB 给 4 KiB 的对端 → InvalidInput",
            false,
            describe(&err),
        ),
    }
    check.eq("tcp：超限之后连接仍可用", client.add(2, 3).await?, 5);

    // ⑩ 端口没人听：`ConnectionRefused`，同样不是一个挂住的 connect。
    let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
    let closed_addr = probe.local_addr()?;
    drop(probe);
    match connect::<spile::transport::tcp::TcpTransport>(
        &Endpoint::Tcp(closed_addr),
        ConnectOptions::default(),
    )
    .await
    {
        Err(RpcError::Io(err)) => check.expect(
            &format!("tcp：连到没人监听的端口 → Io({:?})", err.kind()),
            err.kind() == std::io::ErrorKind::ConnectionRefused,
            format!("错误种类是 {:?}（{err}）", err.kind()),
        ),
        Err(err) => check.expect(
            "tcp：连到没人监听的端口 → Io(ConnectionRefused)",
            false,
            describe(&err),
        ),
        Ok(_) => check.expect(
            "tcp：连到没人监听的端口 → Io(ConnectionRefused)",
            false,
            "竟然连上了",
        ),
    }

    Ok(())
}
