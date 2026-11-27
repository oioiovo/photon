//! 本地 IPC：Windows named pipe。
//!
//! 与 Unix 后端的差别全部收在这一层，上层看到的是同一个 `LocalTransport`：
//!
//! - **没有半关闭**：读写是同一个句柄，只有两半都被 drop、句柄引用计数归零，连接才
//!   真的关掉。所以只 drop 写半，对端收不到 EOF。
//! - **监听是「一连接一实例」**：Windows 上没有一个能反复 `accept` 的 listener，只有
//!   「当前挂着的一个待连接实例」。连接成功后必须**立刻**再造一个，否则新客户端会撞
//!   `ERROR_FILE_NOT_FOUND`——那两个错误看起来完全不同，其实是同一件事的两种时机。
//! - **名字空间是机器全局的**：`LocalAddr` 里的路径只是**种子**，经稳定哈希映射成
//!   `\\.\pipe\spile-<可读前缀>-<16 位十六进制>`。相对路径不会被解析，别把 CWD 当成
//!   约定：两个进程必须给出**相同的字符串**才落到同一条 pipe。已经写成
//!   `\\.\pipe\...` 的完整名字则原样使用。
//!
//! 已知的平台毛刺：客户端 `CreateFile` 撞上「所有实例都被占满」时会在内核里等一个空闲
//! 实例，这个等待**不可被 `connect_timeout` 打断**。这是 Windows 的行为，不是本实现
//! 的取舍；正常运行时服务端总挂着一个待连接实例，所以只在连接数达到上限时才会遇到。

use std::fmt;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, PipeMode, ServerOptions,
};
use tokio::sync::Mutex;

use crate::transport::framing::{FramedSink, FramedSource};
use crate::transport::{Endpoint, Transport, TransportOptions, invalid_input};

/// 名字总长上限：内核限制 256 字符，留一截余量，免得可读前缀把名字顶到边界。
const NAME_LIMIT: usize = 200;

/// pipe 命名空间前缀。带 `\\.\pipe\` 的地址被当成完整名字，不再哈希。
const PIPE_NS: &str = r"\\.\pipe\";

/// 并发实例上限。`ServerOptions::max_instances` 断言 `< 255`，255 本身就是系统默认
/// 上限（`PIPE_UNLIMITED_INSTANCES`），所以这里写 254：一个常量、一个来源。
const INSTANCE_LIMIT: usize = 254;

/// 可读前缀最长保留多少字符。够辨认是谁，又不至于让名字超长。
const PREFIX_LIMIT: usize = 24;

/// 客户端等「空出来的实例」的退让间隔。
const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);

/// 首实例已被别人占着时，`CreateNamedPipe` 报的错误码。
const ERROR_ACCESS_DENIED: i32 = 5;

/// 「名字在，但没有空闲实例」时 `CreateNamedPipe` / `CreateFile` 报的错误码。
const ERROR_PIPE_BUSY: i32 = 231;

pub type LocalSource = FramedSource<ReadHalf<PipeStream>>;
pub type LocalSink = FramedSink<WriteHalf<PipeStream>>;

/// 服务端与客户端是两种类型（`NamedPipeServer` / `NamedPipeClient`），但上层不该为此
/// 分叉。这里合成一个值，读写两半都从它切出来。
///
/// 公开是因为它出现在 [`LocalSource`] / [`LocalSink`] 的类型参数里；不要直接依赖它。
#[derive(Debug)]
pub enum PipeStream {
    Server(NamedPipeServer),
    Client(NamedPipeClient),
}

impl AsyncRead for PipeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_read(cx, buf),
            Self::Client(pipe) => Pin::new(pipe).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PipeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_write(cx, buf),
            Self::Client(pipe) => Pin::new(pipe).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_flush(cx),
            Self::Client(pipe) => Pin::new(pipe).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // named pipe 的 shutdown 只是 flush：没有半关闭可做。
        match self.get_mut() {
            Self::Server(pipe) => Pin::new(pipe).poll_shutdown(cx),
            Self::Client(pipe) => Pin::new(pipe).poll_shutdown(cx),
        }
    }
}

/// Windows 上的本地监听端。
///
/// 只持有「下一个待连接的实例」。没有监听队列可查、也没有地址可回读：pipe 名由种子推导，
/// 见 [`LocalTransport::pipe_name`]。
pub struct LocalTransport {
    name: String,
    /// 用 `tokio::sync::Mutex` 而不是 `std` 的：要**跨 `connect().await` 持锁**。
    /// 「一次只 accept 一个」不是保守，而是这台平台的语义——同一时刻只可能有一个实例
    /// 在等连接，并发 accept 拿到的必然是同一个实例。
    ///
    /// 注意 `accept` 因此不是取消安全的：在 `connect` 期间 drop 掉它，待连接的实例会一起
    /// 消失，监听端随之失效（会话层只在整个公式关停时才会 drop 它）。
    pending: Mutex<Option<NamedPipeServer>>,
}

impl fmt::Debug for LocalTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalTransport")
            .field("pipe", &self.name)
            .finish_non_exhaustive()
    }
}

impl LocalTransport {
    /// 实际使用的 pipe 名。用来打印「到底建在哪」，或排查名字映射。
    pub fn pipe_name(&self) -> &str {
        &self.name
    }
}

/// 把 [`LocalAddr`](crate::transport::LocalAddr) 的路径映射成 pipe 名。
///
/// 已经是完整 `\\.\pipe\...` 名字的直接用；否则「可读前缀 + 路径哈希」。哈希是为了让
/// 两条不同的路径不去抢同一个名字，可读前缀是为了让人一眼认出这是谁的 pipe。
pub(crate) fn pipe_name(path: &Path) -> io::Result<String> {
    let raw = path.to_string_lossy();

    if raw.starts_with(PIPE_NS) {
        // 用户自己写了完整名字：原样用，只查长度。
        let chars = raw.chars().count();
        if chars > NAME_LIMIT {
            return Err(invalid_input(format!(
                "pipe 名 {chars} 字符，超过上限 {NAME_LIMIT}：{raw}"
            )));
        }
        return Ok(raw.into_owned());
    }

    let mut prefix: String = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(PREFIX_LIMIT)
        .collect();
    if prefix.is_empty() {
        // `C:\` 这种没有文件名的路径也得有个能看的前缀。
        prefix.push_str("ipc");
    }

    let name = format!("{PIPE_NS}spile-{prefix}-{:016x}", path_hash(path));
    // 前缀已被截断，长度天然有界；这里只是把「有界」这件事显式断言出来，
    // 免得以后有人把 PREFIX_LIMIT 调大而没人发现。
    debug_assert!(
        name.chars().count() <= NAME_LIMIT,
        "名字长度应当有界：{name}"
    );
    Ok(name)
}

/// FNV-1a 64。一个乘法一个异或，不需要额外依赖；这里防的是「两条路径不小心撞到同一条
/// pipe」，不是攻击，所以不追求抗碰撞。
fn path_hash(path: &Path) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    path.as_os_str()
        .as_encoded_bytes()
        .iter()
        .fold(OFFSET, |hash, &byte| {
            (hash ^ u64::from(byte)).wrapping_mul(PRIME)
        })
}

pub(crate) fn endpoint_name(ep: &Endpoint) -> io::Result<String> {
    match ep {
        Endpoint::Local(addr) => pipe_name(addr.as_path()),
        other => Err(invalid_input(format!(
            "LocalTransport 不接受该端点：{other:?}"
        ))),
    }
}

/// 一个实例的创建选项。除了「是不是首实例」，其余全部写死：两个平台后端里也只有这一
/// 份配置，没有第二个来源。
fn server_options(first: bool) -> ServerOptions {
    let mut opts = ServerOptions::new();
    // 显式写 Byte：消息模式会把 4 字节长度前缀当成一条独立消息，读到的永远是「半条」，
    // 而且症状是 `ERROR_MORE_DATA`，与「对端卡住」长得一模一样。默认值就是 Byte，
    // 但这条一旦失守极难定位，所以不靠默认。
    opts.pipe_mode(PipeMode::Byte);
    // 首实例独占名字：第二个监听得 `ERROR_ACCESS_DENIED`，这就是契约 2。
    opts.first_pipe_instance(first);
    // 本机 IPC 不该接受远程连接（默认已是 true，显式写是为了守住契约 7）。
    opts.reject_remote_clients(true);
    // 实例上限见 `INSTANCE_LIMIT`；超了 `CreateNamedPipe` 报 `ERROR_PIPE_BUSY`，
    // 由 accept 侧的退让重试兜住。
    opts.max_instances(INSTANCE_LIMIT);
    opts
}

fn create_instance(name: &str, first: bool) -> io::Result<NamedPipeServer> {
    server_options(first).create(name)
}

fn into_halves(stream: PipeStream) -> (LocalSource, LocalSink) {
    // `tokio::io::split` 把两半各自包进 `Arc<Mutex<..>>`：named pipe 的读写本来就是
    // 同一个句柄，半关闭不存在，这里要的只是「两个能分别 move 进不同 task 的值」。
    // 锁只在 poll 的一瞬间持有，不跨 await，所以不会把读写互锁成死结。
    let (read, write) = tokio::io::split(stream);
    (FramedSource::new(read), FramedSink::new(write))
}

impl Transport for LocalTransport {
    type Source = LocalSource;
    type Sink = LocalSink;

    async fn bind(ep: &Endpoint, _opts: &TransportOptions) -> io::Result<Self> {
        let name = endpoint_name(ep)?;
        let first = match create_instance(&name, true) {
            Ok(instance) => instance,
            // Windows 把「名字已被首实例独占」报成 ACCESS_DENIED，归一成 AlreadyExists，
            // 与 Unix 的 EADDRINUSE 同义（契约 2 / 3）。
            Err(e) if e.raw_os_error() == Some(ERROR_ACCESS_DENIED) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{name} 已有活跃监听者，拒绝抢占"),
                ));
            }
            Err(e) => return Err(e),
        };

        Ok(Self {
            name,
            pending: Mutex::new(Some(first)),
        })
    }

    async fn connect(
        ep: &Endpoint,
        opts: &TransportOptions,
    ) -> io::Result<(Self::Source, Self::Sink)> {
        let name = endpoint_name(ep)?;

        // 名字在、但没有空闲实例（服务端正在 accept，或实例已用满）不是失败，是「稍等」。
        // 没有监听者才是 NotFound，直接返回——要不要重试由调用方决定（示例里那么做）。
        let open = async {
            loop {
                match ClientOptions::new().open(&name) {
                    Ok(client) => return Ok(client),
                    Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                        tokio::time::sleep(RETRY_INTERVAL).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        };

        let client = match tokio::time::timeout(opts.connect_timeout, open).await {
            Ok(res) => res?,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("连接 {name} 超过 {:?}", opts.connect_timeout),
                ));
            }
        };

        Ok(into_halves(PipeStream::Client(client)))
    }

    async fn accept(&self, _opts: &TransportOptions) -> io::Result<(Self::Source, Self::Sink)> {
        let mut slot = self.pending.lock().await;
        let Some(server) = slot.take() else {
            return Err(io::Error::other(format!(
                "{}：没有待连接的实例，监听端已失效",
                self.name
            )));
        };

        if let Err(e) = server.connect().await {
            // 实例状态不可信，丢掉；顺手补一个新的，让监听端还能继续服务下一个人。
            *slot = self.create_next().await.ok();
            return Err(e);
        }

        // 先把下一个实例挂上，再把这条连接交出去：中间的空档越长，新客户端撞
        // `NotFound` 的机会越大。
        *slot = Some(self.create_next().await?);
        Ok(into_halves(PipeStream::Server(server)))
    }
}

impl LocalTransport {
    /// 造下一个待连接的实例。
    ///
    /// 实例用满时 `CreateNamedPipe` 报 `ERROR_PIPE_BUSY`：这不是错误，是「等别人关掉一条」。
    /// `accept` 按契约不设超时，所以这里退让重试——被取消时 `sleep` 会立刻返回，
    /// 不会吊着调用方。
    async fn create_next(&self) -> io::Result<NamedPipeServer> {
        loop {
            match create_instance(&self.name, false) {
                Ok(instance) => return Ok(instance),
                Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{LocalAddr, MessageSink, MessageSource};
    use std::path::PathBuf;
    use std::time::Duration;

    const HEADER_LEN: usize = 24;

    /// 种子用进程内自增序号分开：用例并发跑，撞名会 bind 到别人的 pipe。
    fn seed(tag: &str) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        PathBuf::from(format!("spile-w{}-{seq}-{tag}.sock", std::process::id()))
    }

    fn local_ep(path: &Path) -> Endpoint {
        Endpoint::Local(LocalAddr::new(path))
    }

    fn frame(marker: u8) -> bytes::Bytes {
        let mut buf = Vec::with_capacity(HEADER_LEN);
        buf.resize(HEADER_LEN, marker);
        bytes::Bytes::from(buf)
    }

    #[test]
    fn pipe_name_is_stable_and_distinct() {
        let a = PathBuf::from("alpha.sock");
        let b = PathBuf::from("beta.sock");

        let first = pipe_name(&a).expect("映射");
        let again = pipe_name(&a).expect("映射");
        assert_eq!(first, again, "同一个地址必须映射到同一个名字");
        assert_ne!(first, pipe_name(&b).expect("映射"), "不同地址不该撞名");
        // 前缀只保留 `[A-Za-z0-9_-]`（种子里的 `.` 等字符一律丢掉，免得任意路径文本
        // 直接漏进机器全局的 pipe 命名空间），所以 `alpha.sock` 的可读前缀是 `alphasock`。
        assert!(
            first.starts_with(r"\\.\pipe\spile-alphasock-"),
            "前缀应可辨认：{first}"
        );
        assert!(first.chars().count() <= NAME_LIMIT);
    }

    #[test]
    fn full_pipe_name_is_used_verbatim() {
        let raw = PathBuf::from(r"\\.\pipe\spile-explicit");
        assert_eq!(pipe_name(&raw).expect("映射"), r"\\.\pipe\spile-explicit");

        let too_long = PathBuf::from(format!(r"\\.\pipe\{}", "x".repeat(NAME_LIMIT)));
        let err = pipe_name(&too_long).expect_err("超长名字应当被拒");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn roundtrip_then_close_yields_eof() {
        let ep = local_ep(&seed("roundtrip"));
        let opts = TransportOptions::default();

        let listener = LocalTransport::bind(&ep, &opts).await.expect("bind 失败");
        let addr = ep.clone();
        let accept_opts = opts.clone();
        let accepting =
            tokio::spawn(async move { listener.accept(&accept_opts).await.expect("accept") });

        let (mut client_src, mut client_sink) = LocalTransport::connect(&addr, &opts)
            .await
            .expect("connect 失败");
        let (mut server_src, mut server_sink) = accepting.await.expect("accept task panic");

        client_sink.send(frame(0x11)).await.expect("send 失败");
        assert_eq!(server_src.recv().await.expect("recv 失败"), frame(0x11));

        server_sink.send(frame(0x22)).await.expect("回包失败");
        assert_eq!(client_src.recv().await.expect("回包"), frame(0x22));

        // 两半一起关 = 整条连接关：对端立刻读到 EOF。
        drop(client_sink);
        drop(client_src);
        let eof = server_src
            .recv()
            .await
            .expect_err("整条连接关闭后对端应读到 EOF");
        assert_eq!(eof.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn sink_drop_alone_keeps_connection_open() {
        let ep = local_ep(&seed("no-half-close"));
        let opts = TransportOptions::default();

        let listener = LocalTransport::bind(&ep, &opts).await.expect("bind 失败");
        let addr = ep.clone();
        let accept_opts = opts.clone();
        let accepting =
            tokio::spawn(async move { listener.accept(&accept_opts).await.expect("accept") });

        let (mut client_src, client_sink) = LocalTransport::connect(&addr, &opts)
            .await
            .expect("connect 失败");
        let (mut server_src, mut server_sink) = accepting.await.expect("accept task panic");

        drop(client_sink);
        // 读半还在，连接就没关：对端既收不到消息，也收不到结束信号。
        assert!(
            tokio::time::timeout(Duration::from_millis(100), server_src.recv())
                .await
                .is_err(),
            "named pipe 没有半关闭，只 drop 写半不该让对端读到 EOF"
        );

        // 反方向仍然通：证明连接确实还活着，而不是被当成关闭处理了。
        server_sink.send(frame(0x33)).await.expect("回包失败");
        assert_eq!(client_src.recv().await.expect("回包"), frame(0x33));
    }

    #[tokio::test]
    async fn second_listener_is_refused() {
        let ep = local_ep(&seed("live"));
        let opts = TransportOptions::default();

        let first = LocalTransport::bind(&ep, &opts)
            .await
            .expect("首次 bind 失败");
        let err = LocalTransport::bind(&ep, &opts)
            .await
            .expect_err("第二个监听者不该抢走名字");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        drop(first);
    }

    #[tokio::test]
    async fn connect_to_unknown_name_is_not_found() {
        let ep = local_ep(&seed("nobody"));
        let err = LocalTransport::connect(&ep, &TransportOptions::default())
            .await
            .expect_err("没有监听者时不应连上");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn listener_serves_clients_one_after_another() {
        let ep = local_ep(&seed("rotation"));
        let opts = TransportOptions::default();
        let listener = LocalTransport::bind(&ep, &opts).await.expect("bind 失败");

        // 第二个客户端能连上，说明 accept 之后确实补了一个新实例；
        // 只 accept 一次就断掉的实现会在这里卡在 NotFound 或 BUSY。
        for round in 0..2u8 {
            let client = LocalTransport::connect(&ep, &opts)
                .await
                .expect("connect 失败");
            let server = listener.accept(&opts).await.expect("accept 失败");
            let ((mut c_src, mut c_sink), (mut s_src, mut s_sink)) = (client, server);

            c_sink.send(frame(round)).await.expect("发送失败");
            assert_eq!(s_src.recv().await.expect("接收失败"), frame(round));
            s_sink.send(frame(round + 10)).await.expect("回包失败");
            assert_eq!(c_src.recv().await.expect("回包"), frame(round + 10));

            drop((c_src, c_sink, s_src, s_sink));
        }
    }
}
