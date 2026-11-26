//! TCP 传输：M0 的主力通道，跨平台、零平台差异。

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::transport::framing::{FramedSink, FramedSource};
use crate::transport::{Endpoint, Transport, TransportOptions, invalid_input};

/// TCP 的读半：底层是 `OwnedReadHalf`。
pub type TcpSource = FramedSource<OwnedReadHalf>;
/// TCP 的写半：底层是 `OwnedWriteHalf`。
pub type TcpSink = FramedSink<OwnedWriteHalf>;

/// 绑定后的监听端。
///
/// 只持有 listener，不持有任何每连接状态：`accept` 每调一次产出一对新半身，
/// 连接的生命周期从这一刻起与它无关。
#[derive(Debug)]
pub struct TcpTransport {
    listener: TcpListener,
}

impl TcpTransport {
    /// 实际绑定到的地址。
    ///
    /// 传 `127.0.0.1:0` 建监听再从这里取端口，是测试与示例里最常用的形态。
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

/// 端点必须是已解析的 `Tcp` 地址：DNS 解析不属于 L1。
fn tcp_addr(ep: &Endpoint) -> io::Result<SocketAddr> {
    match ep {
        Endpoint::Tcp(addr) => Ok(*addr),
        other => Err(invalid_input(format!(
            "TcpTransport 不接受该端点：{other:?}"
        ))),
    }
}

/// 把已连上的流拆成两个半身。
///
/// `connect` 出来的流和服务端 `accept` 出来的流是两条独立 socket，两侧都要
/// `set_nodelay`：只关一侧的话，另一个方向仍会被 Nagle 攒包，loopback 上这笔
/// 延迟纯属白付。
fn into_halves(stream: TcpStream) -> io::Result<(TcpSource, TcpSink)> {
    stream.set_nodelay(true)?;
    let (read, write) = stream.into_split();
    Ok((FramedSource::new(read), FramedSink::new(write)))
}

impl Transport for TcpTransport {
    type Source = TcpSource;
    type Sink = TcpSink;

    async fn bind(ep: &Endpoint, _opts: &TransportOptions) -> io::Result<Self> {
        let addr = tcp_addr(ep)?;
        // 直接走 tokio 的 async bind：它内部把 fd 注册进当前 runtime。
        // 不要换成 std::net::TcpListener + from_std，那条路径要求调用线程恰好
        // 处在 runtime 上下文里，漏了会在构造期 panic。
        Ok(Self {
            listener: TcpListener::bind(addr).await?,
        })
    }

    async fn connect(
        ep: &Endpoint,
        opts: &TransportOptions,
    ) -> io::Result<(Self::Source, Self::Sink)> {
        let addr = tcp_addr(ep)?;
        let stream =
            match tokio::time::timeout(opts.connect_timeout, TcpStream::connect(addr)).await {
                Ok(stream) => stream?,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("连接 {addr} 超过 {:?}", opts.connect_timeout),
                    ));
                }
            };
        into_halves(stream)
    }

    async fn accept(&self, _opts: &TransportOptions) -> io::Result<(Self::Source, Self::Sink)> {
        let (stream, _peer) = self.listener.accept().await?;
        into_halves(stream)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::transport::{MemoryAddr, MessageSink, MessageSource};

    fn loopback() -> Endpoint {
        Endpoint::Tcp("127.0.0.1:0".parse().expect("字面地址合法"))
    }

    fn opts() -> TransportOptions {
        TransportOptions::default()
    }

    #[tokio::test]
    async fn roundtrip_and_half_close() {
        let server = TcpTransport::bind(&loopback(), &opts())
            .await
            .expect("bind");
        let addr = server.local_addr().expect("取回端口");

        // 服务端先收一条，再确认客户端 drop 写半后能读到 EOF。
        let acceptor = tokio::spawn(async move {
            let (mut src, _sink) = server.accept(&opts()).await.expect("accept");
            let first = src.recv().await.expect("收第一条");
            assert_eq!(&first[..], &[0xA5u8; 24][..]);

            let eof = src
                .recv()
                .await
                .expect_err("写半 drop 后应当读到 EOF，而不是空消息");
            assert_eq!(eof.kind(), io::ErrorKind::UnexpectedEof);
        });

        let (_src, mut sink) = TcpTransport::connect(&Endpoint::Tcp(addr), &opts())
            .await
            .expect("connect");
        sink.send(Bytes::from_static(&[0xA5u8; 24]))
            .await
            .expect("send");
        drop(sink); // TCP 上 drop 写半会发 FIN，对端因此能读到 EOF

        acceptor.await.expect("服务端任务");
    }

    #[tokio::test]
    async fn rejects_foreign_endpoint() {
        let ep = Endpoint::Memory(MemoryAddr::new("不是 tcp"));
        let err = TcpTransport::bind(&ep, &opts())
            .await
            .expect_err("端点种类不符应当拒绝");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn connect_to_closed_port_reports_error() {
        // 绑一个端口拿到地址，随即释放，再往它 connect。
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("占端口");
        let addr = probe.local_addr().expect("取地址");
        drop(probe);

        let err = TcpTransport::connect(&Endpoint::Tcp(addr), &opts())
            .await
            .expect_err("端口已关闭，连接必须失败");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }
}
