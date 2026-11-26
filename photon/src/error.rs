//! 错误模型：**本端语言**、**服务端失败面**、**客户端失败面**三件，互不复用。
//!
//! 一条分界线：业务错误（用户 handler 的 `E`）从不进入这里。它是用户自己的类型，
//! 线上以不透明 tail 承载（`docs/PROTOCOL.md` 3.8），框架既不解释也不分类。
//! 这里出现的每个变体都是**框架自己产生**的判定，因此不存在「handler 伪造
//! `RpcError`」这种可能——它手上根本没有这个类型。

use bytes::Bytes;
use thiserror::Error;

/// 框架保留的错误码上限（含）。`0..=7` 是框架对调用方说的话，语言中立；
/// 业务错误没有码，它的身份由 `flags` 的 bit3 表达（见 `PROTOCOL.md` 3.8）。
pub const FRAMEWORK_CODE_MAX: u8 = 7;

/// **仅日志用**：业务拒绝在 span 的 `code` 字段里取这个值。
///
/// 业务错误在线上没有码（身份由 `flags` bit3 表达），但日志里总得能一眼分开
/// 「成功 / 框架判定 / 业务拒绝」三种结局，否则按 `code` 过滤就会把业务拒绝
/// 和成功混在一起。
pub const BUSINESS_CODE_MARK: u32 = 0x1_0000;

/// RPC 调用或协议处理中可能出现的本端错误。
///
/// 除 [`RpcError::PeerRejected`] 外，其余变体都只由**本端**产生；`PeerRejected`
/// 是对端框架（不是对端业务）的判定。两类截然不同的失败能一眼分开，是因为
/// 「谁该为这次失败负责」决定了能不能重试、该不该报给用户。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RpcError {
    /// 协议违规。`detail` 指明哪个字段、实际值是多少。
    ///
    /// 刻意不带校验条目号：编号是文档的人为编号、不上线路，固化进代码只会
    /// 多一层「文档重排就要跟着改代码」的耦合，而「Kind.field + 实际值」是
    /// 更直接的定位信息。
    ///
    /// 这类错误**不向对端回错误体**，而是直接断连：能发出违规帧的实现，
    /// 已经无法保证它会正确解析任何回包。
    #[error("protocol violation: {detail}")]
    Protocol {
        /// 人类可读的定位信息，如「Response.service_id 必须为 0，实际 7」。
        detail: String,
    },

    /// 调用超出 deadline，或等待握手/关闭超时。
    #[error("timeout")]
    Timeout,

    /// 本端配额已满，请求未被接纳。
    #[error("overloaded")]
    Overloaded,

    /// 连接已关闭或正在关闭，无法再发起新请求。
    #[error("connection closed")]
    Closed,

    /// 传输层 IO 失败。
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 承载 handler 的任务丢失（panic 或 `JoinError`），结果不可恢复。
    #[error("handler task dropped")]
    RuntimeDropped,

    /// 本端入参或载荷不合法（例如 payload 超过对端声明上限、编解码失败）。
    ///
    /// 客户端最常见的一种是**解码失败**：对端回的字节与契约形状不一致
    /// （DTO 改过、`service_id` 撞了、两侧版本不同步）。它不是网络问题，
    /// 重试不会让它变好。
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// 框架内部的不可恢复错误。
    #[error("internal error: {0}")]
    Internal(String),

    /// **对端框架**的判定（错误帧里 `flags` bit3 = 0，即框架自陈的错误）。
    ///
    /// 这里只会出现 `0..=7` 的框架码；对端的业务错误不经过这个变体，它是
    /// [`CallError::Rejected`] 里的用户类型。
    #[error("peer rejected: code={code}, {message}")]
    PeerRejected {
        /// 框架码（`0..=7`）。
        code: u8,
        /// 对端给的排障文本，可能为空。
        message: String,
    },
}

impl RpcError {
    /// 这次失败是否值得**换一条新会话**重试（同会话内重试由调用方的策略决定）。
    ///
    /// 判定只读框架自己的语义：本端配额已满、连接已关、IO 失败、对端说过载/正在
    /// 关停都属于「环境暂时不行」；协议违规、解码失败、未知方法属于「再试也还是错」。
    ///
    /// [`RpcError::Timeout`] 判 `false`：期限已经过去了，重试需要**新的期限**，
    /// 那是调用方的策略（还要考虑幂等），框架不替它决定。
    pub fn retryable(&self) -> bool {
        match self {
            RpcError::Overloaded | RpcError::Closed | RpcError::Io(_) => true,
            RpcError::PeerRejected { code, .. } => matches!(code, 1 | 7),
            _ => false,
        }
    }

    /// 本端错误 → 框架错误帧的 `(code, message)`。
    ///
    /// `Io` / `Protocol` / `RuntimeDropped` 在回包路径上不该凭空消失：编成 `0`
    /// 并把自身文本带上，总比让调用方干等到期限好。
    pub(crate) fn wire(&self) -> (u8, String) {
        match self {
            RpcError::Timeout => (6, "deadline exceeded".into()),
            RpcError::Overloaded => (1, "overloaded".into()),
            RpcError::Closed => (7, "shutting down".into()),
            RpcError::InvalidInput(m) => (4, format!("invalid args: {m}")),
            RpcError::Internal(m) => (0, m.clone()),
            RpcError::PeerRejected { code, message } => (*code, message.clone()),
            RpcError::RuntimeDropped => (0, "handler task dropped".into()),
            RpcError::Io(e) => (0, format!("io error: {e}")),
            RpcError::Protocol { detail } => (0, format!("protocol violation: {detail}")),
        }
    }
}

/// 服务端 handler 的失败面：**已经决定回哪种帧**，不再有第三种可能。
///
/// 由宏生成的 dispatch 产出；`Service::call` 的返回类型就是它。
#[derive(Debug)]
#[non_exhaustive]
pub enum HandlerError {
    /// 框架自己的判定，`code` 是上线的框架码（`0..=7`）。
    Framework {
        /// 框架码。
        code: u8,
        /// 排障文本。
        message: String,
    },
    /// 业务拒绝：tail 已经按契约编码好，框架一个字节都不解释。
    Business(Bytes),
}

impl From<RpcError> for HandlerError {
    fn from(err: RpcError) -> Self {
        let (code, message) = err.wire();
        HandlerError::Framework { code, message }
    }
}

/// 会话层交出的原始失败，**只给宏产物用**（`#[doc(hidden)]`）。
///
/// 它和 [`CallError`] 的区别只有一处：业务错误的 tail 还是字节，因为解码需要
/// 契约里的 `E`，而会话层（读任务）不知道任何用户类型。
#[doc(hidden)]
#[derive(Debug)]
pub enum CallFailure {
    /// 本端失败，或对端框架码。
    Failed(RpcError),
    /// 对端业务拒绝：tail 未解码。
    Rejected(Bytes),
}

impl From<RpcError> for CallFailure {
    fn from(err: RpcError) -> Self {
        CallFailure::Failed(err)
    }
}

/// 客户端 stub 的失败面：业务错误**保留了类型**，链路失败另占一臂。
///
/// 两臂分开是结构性的，不是风格：`Rejected` 表示「对端懂了并拒绝了」，
/// `Failed` 表示「这次对话本身没完成」。把两者压进一个 `Err` 看起来省事，
/// 但调用方就再也无法在类型层面决定「要不要重连」。
#[derive(Debug)]
#[non_exhaustive]
pub enum CallError<E> {
    /// 对端业务拒绝，`E` 由契约 crate 定义。
    Rejected(E),
    /// 本端失败或对端框架判定。
    Failed(RpcError),
}

impl<E> CallError<E> {
    /// 换新会话重试是否值得。业务拒绝一律 `false`——能不能重试只有业务知道。
    pub fn retryable(&self) -> bool {
        match self {
            CallError::Rejected(_) => false,
            CallError::Failed(err) => err.retryable(),
        }
    }

    /// 取本端/对端框架失败，业务拒绝返回 `None`。
    pub fn failed(&self) -> Option<&RpcError> {
        match self {
            CallError::Rejected(_) => None,
            CallError::Failed(err) => Some(err),
        }
    }
}

impl<E> From<RpcError> for CallError<E> {
    fn from(err: RpcError) -> Self {
        CallError::Failed(err)
    }
}
