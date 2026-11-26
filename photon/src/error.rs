//! 统一错误类型。
//!
//! 本地失败只有这一个枚举，不按层各造一种，免得每层都要 `map_err` 翻译；
//! 对端错误收敛成 [`RpcError::Remote`]，两者永不互相转换——`Remote{1}` 不能在
//! 客户端变成 `Overloaded`，否则「谁该为这次放弃负责」就糊了。
//!
//! [`RpcError::Protocol`] 必须带 `detail`：一条连接上有几十个可能违规的位置，
//! 没有字段名的「协议错误」在日志里等于没有信息。

use thiserror::Error;

/// 对端返回的错误，唯一「对端陈述它自己状态」的变体。
///
/// `code` 的保留段由框架定义，8..0xFFFF 原样透传，≥ 0x1_0000 由用户定义、
/// 框架不解释。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("remote error: code={code}, retryable={retryable}, message={message}")]
pub struct RemoteError {
    /// 错误码。
    pub code: u32,
    /// 对端建议是否可重试。线上是 `u8` 且只认 0/1；其它值按「非 0 即可重试」
    /// 归一化为 `true`，**不因此报协议错误**——错误体是收尾路径，在这里断连
    /// 只会更难排障。
    pub retryable: bool,
    /// 对端给的排障文本，可能为空。非法 UTF-8 在解析时已 lossy 处理。
    pub message: String,
}

impl RemoteError {
    /// 回包侧构造 `Remote` 的入口，只给三个码用：`2` UnknownService、
    /// `3` UnknownMethod、`7` ShuttingDown。
    ///
    /// L3 内部回包走的是 `send_error`（直接写错误体，不必绕成 `RpcError`），
    /// 这个构造器只给「手上必须交出一个 `RpcError`」的地方用——目前是宏生成的
    /// unknown-method 分支。发起侧不许碰它：那边只能从错误体解出 `Remote`，
    /// 自己造等于凭空伪造对端状态。
    pub fn new(code: u32) -> Self {
        Self {
            code,
            retryable: false,
            message: match code {
                2 => "unknown service".into(),
                3 => "unknown method".into(),
                7 => "shutting down".into(),
                other => format!("remote error {other}"),
            },
        }
    }
}

/// RPC 调用或协议处理中可能出现的全部错误。
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
    ///
    /// 与 [`RpcError::Remote`] 里的 `code = 1` 是两件事：这里是「我这边排不上队」，
    /// 那里是「对端说它排不上队」。
    #[error("overloaded")]
    Overloaded,

    /// 连接已关闭或正在关闭，无法再发起新请求。
    #[error("connection closed")]
    Closed,

    /// 传输层 IO 失败。
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 承载 handler 的任务丢失（panic 或 `JoinError`），结果不可恢复。
    ///
    /// 与 `Internal` 的区别：这里是「任务没了」，那里是「任务给了错误的结果」。
    #[error("handler task dropped")]
    RuntimeDropped,

    /// 本端入参不合法，且不是协议层问题（例如 payload 超过对端声明上限）。
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// 框架内部的不可恢复错误。
    #[error("internal error: {0}")]
    Internal(String),

    /// 对端返回的错误体。唯一会被原样透传给调用方的变体。
    #[error(transparent)]
    Remote(#[from] RemoteError),
}
