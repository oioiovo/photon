//! 并发验收：同一套断言在 `memory` 与 `tcp` 上各跑一遍。
//!
//! 这两条路径的差别只该在传输层：一条是内存 channel，一条是真 socket。如果
//! 请求与应答的配对逻辑在哪一边有偏差，这里的用例会先红——它比对的是
//! **每个请求拿到的那个值**，不是「有没有回包」。

use std::sync::Arc;
use std::time::Duration;

use photon::transport::memory::Memory;
use photon::transport::tcp::TcpTransport;
use photon::{
    ConnectOptions, Endpoint, MessageSink, MessageSource, Server, ServiceRegistry, Session,
    SessionHandle, SessionOptions, TransportOptions,
};

const ID: u16 = 7;
/// 整套并发用例的硬上限：卡住要变成失败，而不是把测试二进制挂住。
const SUITE_TIMEOUT: Duration = Duration::from_secs(10);

#[photon::service(id = 7)]
trait Calc {
    async fn add(&self, a: i64, b: i64) -> i64;
}

struct Calculator;

impl Calc for Calculator {
    async fn add(&self, a: i64, b: i64) -> i64 {
        // 故意让耗时随参数抖动，制造乱序：都按参数顺序回包反而掩盖配对问题。
        tokio::time::sleep(Duration::from_millis((a as u64) % 5)).await;
        a + b
    }
}

#[tokio::test]
async fn memory_no_crosstalk() {
    let ((client_source, client_sink), (server_source, server_sink)) = Memory::pair();
    let _server = server_session(server_source, server_sink);
    let handle = client_session(client_source, client_sink).await;
    tokio::time::timeout(SUITE_TIMEOUT, concurrency_suite(handle))
        .await
        .expect("10 s 内必须跑完");
}

#[tokio::test]
async fn tcp_no_crosstalk() {
    let (addr, _serve) = tcp_server().await;
    let handle = photon::connect::<TcpTransport>(&Endpoint::Tcp(addr), ConnectOptions::default())
        .await
        .expect("连接");
    tokio::time::timeout(SUITE_TIMEOUT, concurrency_suite(handle))
        .await
        .expect("10 s 内必须跑完");
}

/// 32 个并发 + 100 个乱序：每份应答必须回到发起它的那个请求上。
async fn concurrency_suite<S, K>(handle: SessionHandle<S, K>)
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    let mut tasks = Vec::new();
    for i in 0..32i64 {
        let client = CalcClient::new(handle.clone());
        tasks.push(tokio::spawn(async move {
            let result = client.add(i, 1000).await;
            (i, result)
        }));
    }
    for task in tasks {
        let (i, result) = task.await.expect("task");
        let value = result.expect("并发调用");
        assert_eq!(value, i + 1000, "请求 {i} 拿到了别人的应答");
    }

    // 顺序也乱：一组不同大小的工作量同时压进去。
    let mut tasks = Vec::new();
    for i in 0..100i64 {
        let client = CalcClient::new(handle.clone());
        tasks.push(tokio::spawn(async move {
            let result = client.add(i, 7).await;
            (i, result)
        }));
    }
    let mut mismatched = 0;
    for task in tasks {
        let (i, result) = task.await.expect("task");
        match result {
            Ok(value) if value == i + 7 => {}
            _ => mismatched += 1,
        }
    }
    assert_eq!(mismatched, 0, "100 并发里有串包的应答");
}

fn server_session<S, K>(source: S, sink: K) -> Arc<Session<S, K>>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    let mut registry = ServiceRegistry::empty();
    register_calc(&mut registry, Arc::new(Calculator)).expect("注册");

    let (session, _handle) =
        Session::new(source, sink, Arc::new(registry), SessionOptions::default()).expect("建会话");
    session
}

async fn client_session<S, K>(source: S, sink: K) -> SessionHandle<S, K>
where
    S: MessageSource + 'static,
    K: MessageSink + 'static,
{
    let (session, handle) = Session::new(
        source,
        sink,
        Arc::new(ServiceRegistry::empty()),
        SessionOptions::default(),
    )
    .expect("建会话");
    session.wait_ready().await.expect("握手");
    handle
}

/// 起一个真 TCP 服务端，返回绑定地址与 serve 的句柄。
async fn tcp_server() -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let mut server = Server::<TcpTransport>::bind(
        &Endpoint::Tcp("127.0.0.1:0".parse().expect("地址")),
        TransportOptions::default(),
        SessionOptions::default(),
    )
    .await
    .expect("bind");

    // 走 Server 的注册入口，而不是直接塞注册表：这条路径才是示例与用户会走的。
    server
        .register(ID, Arc::new(CalcService::new(Arc::new(Calculator))))
        .expect("注册");

    let addr = server.transport().local_addr().expect("local_addr");
    let serve = tokio::spawn(async move { server.serve().await });
    (addr, serve)
}
