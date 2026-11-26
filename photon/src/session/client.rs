//! 客户端装配：把 transport 的 `connect` 结果接成一条能发请求的会话。
//!
//! 这里不生成任何按服务定制的东西——`CalcClient` 那类类型由宏产出，它们只是
//! 在 `SessionHandle` 上加一层「encode 参数 → call → decode 返回值」。

use std::sync::Arc;

use super::{ServiceRegistry, Session, SessionHandle, SessionOptions};
use crate::error::RpcError;
use crate::transport::{Endpoint, Transport, TransportOptions};

/// 建一条客户端连接需要的全部参数。
///
/// 把 L1 与 L3 的选项打成一包：客户端的调用方不该关心「哪个参数属于哪一层」，
/// 而两个都漏写又会得到「上限 64 KiB / 握手 1 s」这类看不出问题的默认值。
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    pub transport: TransportOptions,
    pub session: SessionOptions,
}

/// 连上对端并等到握手完成，返回可直接发请求的句柄。
///
/// 握手在这一步等到 `Ready`：`codec_id` 不匹配、版本不符这类 fail-fast 应该在
/// `connect` 上就变成 `Err`，而不是让第一次业务调用莫名其妙地超时。
pub async fn connect<T>(
    ep: &Endpoint,
    opts: ConnectOptions,
) -> Result<SessionHandle<T::Source, T::Sink>, RpcError>
where
    T: Transport + 'static,
    T::Source: 'static,
    T::Sink: 'static,
{
    let (source, sink) = T::connect(ep, &opts.transport).await?;

    // 客户端不响应入站请求，注册表必须是空的而不是 `None`：L3 的 dispatch 只在
    // 「收到 Request」时才查表，空表让这条路径回 `Remote{2}` 而不是 panic。
    let (session, handle) = Session::new(
        source,
        sink,
        Arc::new(ServiceRegistry::empty()),
        opts.session,
    )?;

    session.wait_ready().await?;
    Ok(handle)
}
