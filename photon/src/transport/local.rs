//! 本地 IPC：Unix domain socket。
//!
//! 手搓而不是套 `interprocess`，是为了保住半关闭：UDS 的写半在 drop 时就是
//! `shutdown(SHUT_WR)`，对端读侧会拿到 EOF。`local` 因此与 `tcp` 的关闭语义一致，
//! 上层不需要为它写例外。
//!
//! Windows 上暂无实现，开启 `local` feature 会直接编译失败（见 `transport.rs` 的
//! `compile_error!`）：named pipe 没有半关闭，且服务端监听需要 per-connection 新建、
//! 用 `first_pipe_instance` 规避 `ERROR_PIPE_BUSY` 竞态——没有测试环境时手搓，
//! 交付的是一段没人验证过的代码。

use std::fmt;
use std::io;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};

use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};

use crate::transport::framing::{FramedSink, FramedSource};
use crate::transport::{Endpoint, Transport, TransportOptions, invalid_input};

/// `sockaddr_un.sun_path` 的上限：Linux 108、macOS/BSD 104，取较小者。
///
/// 超长路径的报错是 `ENAMETOOLONG`，只说「名字太长」而不说是哪个名字，
/// 排查一次要半个小时，所以自己先查一遍。
const SUN_PATH_LIMIT: usize = 104;

pub type LocalSource = FramedSource<OwnedReadHalf>;
pub type LocalSink = FramedSink<OwnedWriteHalf>;

/// Unix 上的本地监听端。
///
/// 自己记住路径，是因为 `tokio::net::UnixListener` 没有 `local_addr()`
/// （UDS 的对端地址无意义，内核也不提供查询）。
pub struct LocalTransport {
    listener: UnixListener,
    path: PathBuf,
}

impl fmt::Debug for LocalTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalTransport")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl LocalTransport {
    /// 监听路径。用来打印实际绑在哪，或测试结束时清理。
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn endpoint_path(ep: &Endpoint) -> io::Result<PathBuf> {
    match ep {
        Endpoint::Local(addr) => {
            let path = addr.as_path();
            let len = path.as_os_str().len();
            if len > SUN_PATH_LIMIT - 1 {
                return Err(invalid_input(format!(
                    "UDS 路径 {len} 字节，超过上限约 {}：{path:?}；\
                     把 socket 放在更浅的目录里",
                    SUN_PATH_LIMIT - 1
                )));
            }
            Ok(path.to_path_buf())
        }
        other => Err(invalid_input(format!(
            "LocalTransport 不接受该端点：{other:?}"
        ))),
    }
}

/// 绑定前的路径准备：清掉上次进程留下的 socket，但绝不碰活着的监听者或用户文件。
async fn prepare_path(path: &Path) -> io::Result<()> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    // 普通文件、目录、符号链接都可能是用户自己的东西。删掉它们换来的「bind 成功」
    // 不值得，直接拒绝并让人自己处理。
    if !meta.file_type().is_socket() {
        return Err(invalid_input(format!(
            "{path:?} 已存在且不是 socket，拒绝覆盖"
        )));
    }

    // 是 socket，还要确认没人在听：删掉路径不会杀掉监听者，只会让它再也接不到
    // 新连接，而它自己毫无察觉——这种「看起来还活着」的状态最难查。
    match UnixStream::connect(path).await {
        Ok(_) => Err(invalid_input(format!("{path:?} 已有活跃监听者，拒绝覆盖"))),
        // ECONNREFUSED：文件在但没人 listen，是上次崩溃的残留。
        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
            std::fs::remove_file(path)?;
            Ok(())
        }
        // EACCES 之类不能当成「残留」处理，否则会把权限问题伪装成清理成功。
        Err(e) => Err(e),
    }
}

fn into_halves(stream: UnixStream) -> (LocalSource, LocalSink) {
    // `into_split` 的两半各自持有底层 fd 的引用计数；`OwnedWriteHalf` 在 drop 时
    // 并且是最后一个引用时 `shutdown(SHUT_WR)`，这就是 `local` 的半关闭来源。
    let (read, write) = stream.into_split();
    (FramedSource::new(read), FramedSink::new(write))
}

impl Transport for LocalTransport {
    type Source = LocalSource;
    type Sink = LocalSink;

    async fn bind(ep: &Endpoint, _opts: &TransportOptions) -> io::Result<Self> {
        let path = endpoint_path(ep)?;
        prepare_path(&path).await?;
        let listener = UnixListener::bind(&path)?;
        Ok(Self { listener, path })
    }

    async fn connect(
        ep: &Endpoint,
        opts: &TransportOptions,
    ) -> io::Result<(Self::Source, Self::Sink)> {
        let path = endpoint_path(ep)?;
        // UDS 的 connect 几乎只有「立刻成功」和「立刻失败」两种，超时基本用不上；
        // 但同上限旋钮一样，留一个口子比事后加参数便宜。
        let stream =
            match tokio::time::timeout(opts.connect_timeout, UnixStream::connect(&path)).await {
                Ok(stream) => stream?,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("连接 {path:?} 超过 {:?}", opts.connect_timeout),
                    ));
                }
            };
        Ok(into_halves(stream))
    }

    async fn accept(&self, _opts: &TransportOptions) -> io::Result<(Self::Source, Self::Sink)> {
        let (stream, _peer) = self.listener.accept().await?;
        Ok(into_halves(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{LocalAddr, MessageSink, MessageSource};

    const HEADER_LEN: usize = 24;

    fn sock_path(tag: &str) -> PathBuf {
        // 用 /tmp 而不是 temp_dir()：macOS 的临时目录路径本身就有五十多个字符，
        // 再拼上用例名容易撞 sun_path 上限。
        PathBuf::from(format!("/tmp/photon-{}-{tag}.sock", std::process::id()))
    }

    fn frame(marker: u8) -> bytes::Bytes {
        let mut buf = Vec::with_capacity(HEADER_LEN);
        buf.resize(HEADER_LEN, marker);
        bytes::Bytes::from(buf)
    }

    fn local_ep(path: &Path) -> Endpoint {
        Endpoint::Local(LocalAddr::new(path))
    }

    #[tokio::test]
    async fn roundtrip_and_half_close() {
        let path = sock_path("half");
        let _ = std::fs::remove_file(&path);
        let ep = local_ep(&path);

        let listener = LocalTransport::bind(&ep, &TransportOptions::default())
            .await
            .expect("bind 失败");
        let addr = ep.clone();
        let accept = tokio::spawn(async move {
            listener
                .accept(&TransportOptions::default())
                .await
                .expect("accept 失败")
        });

        let (mut client_src, mut client_sink) =
            LocalTransport::connect(&addr, &TransportOptions::default())
                .await
                .expect("connect 失败");
        let (mut server_src, mut server_sink) = accept.await.expect("accept task panic");

        client_sink.send(frame(0x11)).await.expect("send 失败");
        assert_eq!(server_src.recv().await.expect("recv 失败"), frame(0x11));

        // drop 写半只关写方向：对端读到 EOF，读方向仍然可用。
        drop(client_sink);
        let eof = server_src
            .recv()
            .await
            .expect_err("drop 写半后对端应读到 EOF");
        assert_eq!(eof.kind(), io::ErrorKind::UnexpectedEof);

        // 反过来仍然能写，证明这是半关闭而不是整连接关闭。
        server_sink
            .send(frame(0x22))
            .await
            .expect("反方向 send 失败");
        assert_eq!(
            client_src.recv().await.expect("反方向 recv 失败"),
            frame(0x22)
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn refuses_existing_regular_file() {
        let path = sock_path("regular");
        std::fs::write(&path, b"not a socket").expect("写测试文件失败");

        let err = LocalTransport::bind(&local_ep(&path), &TransportOptions::default())
            .await
            .expect_err("普通文件不该被覆盖");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(std::fs::read(&path).expect("文件应仍存在"), b"not a socket");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn refuses_live_listener() {
        let path = sock_path("live");
        let _ = std::fs::remove_file(&path);
        let first = LocalTransport::bind(&local_ep(&path), &TransportOptions::default())
            .await
            .expect("首次 bind 失败");

        let err = LocalTransport::bind(&local_ep(&path), &TransportOptions::default())
            .await
            .expect_err("第二个监听者不该抢走路径");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        drop(first);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn clears_stale_socket() {
        let path = sock_path("stale");
        let _ = std::fs::remove_file(&path);
        // drop 监听端不会删除 socket 文件，正好模拟进程崩溃后的残留。
        drop(
            LocalTransport::bind(&local_ep(&path), &TransportOptions::default())
                .await
                .expect("首次 bind 失败"),
        );
        assert!(path.exists(), "残留的 socket 文件应还在");

        LocalTransport::bind(&local_ep(&path), &TransportOptions::default())
            .await
            .expect("残留 socket 应该被清掉后重新 bind 成功");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn rejects_path_too_long() {
        let path = PathBuf::from(format!("/tmp/{}.sock", "x".repeat(120)));
        let err = LocalTransport::bind(&local_ep(&path), &TransportOptions::default())
            .await
            .expect_err("超长路径应被提前拒绝");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
