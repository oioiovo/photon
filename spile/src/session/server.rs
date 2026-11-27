//! 服务端装配：accept 循环 + 每连接一条会话 + 一份共享注册表。
//!
//! 注册表按值持有、`serve` 时再包成 `Arc`：注册发生在 `serve` 之前，把
//! 「运行中还能改注册表」这件事从类型上排除掉，比事后加锁便宜。

use std::io;
use std::sync::Arc;

use super::{ServiceRegistry, Session, SessionOptions};
use crate::Service;
use crate::error::RpcError;
use crate::transport::{Endpoint, Transport, TransportOptions};

/// 绑好但还没开始 accept 的服务端。
pub struct Server<T>
where
    T: Transport,
{
    transport: T,
    registry: ServiceRegistry,
    transport_opts: TransportOptions,
    session_opts: SessionOptions,
}

impl<T> Server<T>
where
    T: Transport + 'static,
    T::Source: 'static,
    T::Sink: 'static,
{
    /// 绑定监听端。`bind` 是异步的：实现通常要把 fd 注册进当前 runtime。
    pub async fn bind(
        ep: &Endpoint,
        transport_opts: TransportOptions,
        session_opts: SessionOptions,
    ) -> io::Result<Self> {
        let transport = T::bind(ep, &transport_opts).await?;
        Ok(Self {
            transport,
            registry: ServiceRegistry::empty(),
            transport_opts,
            session_opts,
        })
    }

    /// 底层 listener。端口传 `0` 建监听后，测试要靠它拿真正绑到的地址。
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// 拿注册表去挂服务。宏生成的 `register_xxx` 正是接在这个参数上：
    /// `register_calc(server.registry(), Arc::new(Calc))`。
    pub fn registry(&mut self) -> &mut ServiceRegistry {
        &mut self.registry
    }

    /// 直接挂一个已装箱的服务。撞号返回 `Err`，不覆盖已有路由。
    pub fn register(&mut self, service_id: u16, service: Arc<dyn Service>) -> Result<(), RpcError> {
        self.registry.insert(service_id, service)
    }

    /// 一直 accept 下去。除了 listener 级错误不会返回。
    pub async fn serve(self) -> io::Result<()> {
        self.serve_with(|_| {}).await
    }

    /// 同上，但每条新连接会先回调一次——想按连接挂 handler 或做统计时用。
    ///
    /// 回调拿到的是**已经能收发**的会话（首帧握手已在队列里）；返回后连接照常运行。
    pub async fn serve_with<F>(self, mut on_conn: F) -> io::Result<()>
    where
        F: FnMut(Arc<Session<T::Source, T::Sink>>),
    {
        let registry = Arc::new(self.registry);

        loop {
            let (source, sink) = match self.transport.accept(&self.transport_opts).await {
                Ok(pair) => pair,
                // 单连接层面的失败不该打死 listener：客户端半路 abort、连接被
                // 重置都属于常态。listener 自己出问题（fd 坏了）才返回 Err。
                Err(err) if is_per_connection(&err) => {
                    eprintln!("spile: accept 单连接失败，继续: {err}");
                    continue;
                }
                Err(err) => return Err(err),
            };

            let (session, _handle) = match Session::new(
                source,
                sink,
                Arc::clone(&registry),
                self.session_opts.clone(),
            ) {
                Ok(pair) => pair,
                Err(err) => {
                    eprintln!("spile: 建会话失败，继续: {err}");
                    continue;
                }
            };

            on_conn(Arc::clone(&session));

            // reader / writer 是会话内部 spawn 的，外面唯一能观测到的终点是
            // `wait_closed`。这条 task 只负责「连接活着时把 Session 按住」。
            tokio::spawn(async move {
                session.wait_closed().await;
                let _ = session.close().await;
            });
        }
    }
}

/// 这些错误是「这一个连接坏了」，不是「listener 坏了」。
fn is_per_connection(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
    )
}
