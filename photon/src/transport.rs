//! L1 传输层：把字节流或通道抽象成「完整消息」的收发。
//!
//! 这里只有契约与公共数据结构，不放任何平台实现。读半与写半是两类独立的
//! 值，便于分别 move 进 reader / writer task；各实现的差异（有没有帧边界、
//! 能不能半关闭）由实现自己承担，不上浮到契约里。

use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use bytes::Bytes;

pub mod framing;

#[cfg(all(feature = "local", not(unix)))]
compile_error!(
    "`local` 传输目前只在 Unix 上实现。Windows 需要 named pipe 适配，     见 docs/detail/01-l1-transport.md §6.2；手搓之前先确认有 Windows 测试环境。"
);

#[cfg(all(feature = "local", unix))]
pub mod local;

#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "tcp")]
pub mod tcp;

/// 握手前两侧固定的单帧上限，也是两个旋钮的初值。
pub const DEFAULT_MAX_MESSAGE_LEN: u32 = 64 * 1024;

/// 一条消息的来源：一次取出一条**完整**消息。
///
/// `Send` 是硬性要求：半身最终会被 move 进 `tokio::spawn` 的 task。
pub trait MessageSource: Send {
    /// 取下一条完整消息。
    ///
    /// 实现必须凑满整条才返回，返半条会让上层解析到半截头。也只允许在连接
    /// 关闭这类终态取消时被打断——中途打断会丢掉落进内部缓冲的字节，那条
    /// 连接只能废弃，所以不能拿单次超时去包它。
    fn recv(&mut self) -> impl Future<Output = io::Result<Bytes>> + Send;

    /// 本端**接收**上限的旋钮，初值 [`DEFAULT_MAX_MESSAGE_LEN`]。
    ///
    /// 要在半身 move 进 task **之前**取走：move 之后就只剩 task 里的私有值，
    /// `&self` 再也拿不到。L1 只比较这个数字的大小，不知道它是协商结果。
    fn recv_limit(&self) -> Arc<AtomicU32>;
}

/// 一条消息的出口：一次写出一条完整消息。
pub trait MessageSink: Send {
    /// 写出一条完整消息，`Ok` 表示已交给内核或通道。
    ///
    /// 超上限时应在写任何字节之前失败，这样连接仍然可用；若在中途被打断，
    /// 流里会留下半帧，对端的长度前缀永远读不满，症状是「对端无端卡住」。
    fn send(&mut self, msg: Bytes) -> impl Future<Output = io::Result<()>> + Send;

    /// 本端**发送**上限的旋钮，语义上是对端声明的值，初值 [`DEFAULT_MAX_MESSAGE_LEN`]。
    ///
    /// 与 `recv_limit` 一样要在 move 之前取走；读侧用本端值、写侧用对端值，
    /// 这两个方向不能共用同一个数字。
    fn send_limit(&self) -> Arc<AtomicU32>;
}

/// 一个可按地址绑定 / 连接的传输实现。
///
/// 用关联类型而不是泛型参数：上层拿到的是具体类型，生产路径不需要动态分发。
/// 需要擦除「transport × handler」的组合时再走枚举门面，那是另一层的事。
pub trait Transport: Sized {
    type Source: MessageSource;
    type Sink: MessageSink;

    /// 绑定监听端，产出一个可 `accept` 的实例。
    ///
    /// 做成异步不是为了「等 IO」——绑定本身没有等待——而是让「必须在某个 executor
    /// 上被 poll」成为类型上的前置条件：实现通常要把 fd 注册进当前 runtime，而
    /// 这种依赖只该由 `async` 表达。写成同步签名的话，漏了 runtime 的调用方会在
    /// 构造期 panic，而不是拿到一个 `Err`。
    fn bind(
        ep: &Endpoint,
        opts: &TransportOptions,
    ) -> impl Future<Output = io::Result<Self>> + Send;

    /// 作为客户端连接，产出一对半身。
    ///
    /// 关联函数而不是方法：此时还没有实例。DNS 解析不在这一层做，
    /// [`Endpoint::Tcp`] 里的地址必须是已解析值。
    fn connect(
        ep: &Endpoint,
        opts: &TransportOptions,
    ) -> impl Future<Output = io::Result<(Self::Source, Self::Sink)>> + Send;

    /// 接受一个入站连接，产出一对半身。
    ///
    /// 不设超时：握手阶段的超时归上层会话逻辑管，L1 只负责「有人来了就给一对半身」。
    fn accept(
        &self,
        opts: &TransportOptions,
    ) -> impl Future<Output = io::Result<(Self::Source, Self::Sink)>> + Send;
}

/// 传输地址，只携带寻址信息。
///
/// 地址表达「往哪连」，选项表达「怎么连」：超时、轮询策略这类行为开关一律
/// 放 [`TransportOptions`]。
///
/// 本地 IPC 与共享内存的变体在引入对应实现时再加——现在写占位变体，只会让
/// 调用方以为它已经可用。
#[derive(Debug, Clone)]
pub enum Endpoint {
    /// 已解析的 socket 地址。
    Tcp(SocketAddr),
    /// 本地 IPC：Unix 上是 socket 文件路径。
    ///
    /// 路径本身不裁剪 feature——它是纯数据；能不能 bind 取决于 `local` 实现是否存在。
    Local(LocalAddr),
    /// 进程内传输。地址不参与查找，只用于日志与 `Debug`。
    Memory(MemoryAddr),
}

/// 本地 IPC 的地址。
///
/// Unix 上是 socket 文件路径；Windows 将来是 pipe 名，届时再扩载荷类型，
/// 现在只表达「一个文件系统路径」。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAddr(PathBuf);

impl LocalAddr {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    /// 路径形态。日志、错误信息与「长度上限检查」都要用它。
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl fmt::Display for LocalAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "local://{}", self.0.display())
    }
}

/// 进程内传输的名字，仅用于日志：真正的配对由 `Memory::pair` 直接给出。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryAddr(Arc<str>);

impl MemoryAddr {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self(name.into())
    }
}

impl fmt::Display for MemoryAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "memory://{}", self.0)
    }
}

/// 单条连接建立时的选项。
///
/// 只放 L1 自己消费的项。上限、在飞配额、语义编码都属于会话层：放进来会出现
/// 同一份值的两个来源，而 L1 在握手前也用不上它们。
#[derive(Debug, Clone)]
pub struct TransportOptions {
    /// `connect` 的超时；`accept` 不受它约束。注意 `0` 表示「立即超时」而不是
    /// 「不限」——要长等待就设一个大值，别用 0 表达无限。
    pub connect_timeout: Duration,
    /// 共享内存实现是否忙轮询。对流式实现无意义。
    pub spin_hint: bool,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            spin_hint: false,
        }
    }
}

/// 写侧超限的固定文本前缀。
///
/// L1 与上层之间只能靠「`InvalidInput` + 这段前缀」区分「我方拒发」与系统层
/// 的 `EINVAL`，改名等于改约定。
pub(crate) const MSG_EXCEEDS_PEER_LIMIT: &str = "message exceeds peer max_message_len";

/// 读侧错误的固定构造：收到它的一方应当断连。
///
/// 帧长越界、帧格式不合法都属于这一类——此时流里的位置已经不可信，
/// 继续读只会得到更离谱的结果。
pub(crate) fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// 写侧「我方拒发」的固定构造，连接保持可用。
pub(crate) fn invalid_input(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}
