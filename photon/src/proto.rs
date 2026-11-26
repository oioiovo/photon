//! L2 协议层：24 字节语义头、逐 kind 校验。
//!
//! 线格式唯一权威是 `docs/PROTOCOL.md`，这里只是它的实现。本文件放「形状」：
//! 常量、枚举、结构体与 `parse` 入口；偏移读写和校验分支在 `header` 子模块。

use bytes::Bytes;

use crate::{error::RpcError, proto::header::validate};

pub mod handshake;
pub mod header;

/// 语义头长度。任何消息的 payload 都从偏移 24 开始。
pub const HEADER_LEN: usize = 24;

/// 帧长上限。M1 起改由握手声明，M3 之后来自 `SessionOptions`。
pub const MAX_MESSAGE_LEN: usize = 64 * 1024;

// flags 的已定义位。常量只说明「这个位是什么」，不说明「何时允许置位」——
// 合法性随 kind 变化，由 `validate` 判定。`Header::flags` 因此保持 `u8`：
// 保留位置位的输入要能被读出原值再报错，而不是在解码层就被拒。

/// payload 是错误体，仅 `Response` 允许。
pub const FLAG_ERR: u8 = 0x01;

/// 流尾标志。已定义但当前禁止：只为 `StreamChunk` 存在，M0 不实现流式。
pub const FLAG_STREAM_LAST: u8 = 0x02;

/// `GoAway` 急停，仅 `GoAway` 允许。
pub const FLAG_GOAWAY_FORCE: u8 = 0x04;

// bit3–7 保留，故意不给常量：给没有合法用途的位起名字，会让人以为它能被写入。

/// 消息类型，取值即线上的 `kind` 字节。
///
/// `#[repr(u8)]` 让编码直接用 `kind as u8`；解码必须手写 `TryFrom`，
/// 因为 7..=255 是非法输入，不是「另一个变体」。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// 一问一答的请求，必定有对应的 `Response`。
    Request = 0,
    /// 应答，bit0 表示是否携带错误体。
    Response = 1,
    /// 单向消息，不产生应答。
    Notification = 2,
    /// 按 `request_id` 取消请求，自身不带 payload。
    Cancel = 3,
    /// 流式分片，预留不实现：解码允许，校验必拒。
    StreamChunk = 4,
    /// 优雅关闭，bit2 表示急停。
    GoAway = 5,
    /// 握手首帧，payload 恰 12 字节。
    Handshake = 6,
}

impl TryFrom<u8> for Kind {
    type Error = RpcError;

    /// 不用 `From<u8>`：`From` 意味着「任何输入都能成功」，等于宣布 7..=255
    /// 合法，而协议要的正是「未定义即拒绝」。
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Kind::Request),
            1 => Ok(Kind::Response),
            2 => Ok(Kind::Notification),
            3 => Ok(Kind::Cancel),
            // 保留变体：拒绝交给 validate。
            4 => Ok(Kind::StreamChunk),
            5 => Ok(Kind::GoAway),
            6 => Ok(Kind::Handshake),
            _ => Err(RpcError::Protocol {
                detail: format!("kind={value} 未定义（有效值 0..=6）"),
            }),
        }
    }
}

/// 24 字节语义头。
///
/// 存的是已解开的值，不是线上字节：字节序只在 `decode` / `encode_into`
/// 两个边界上出现，中间层不需要记得有字节序这回事。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// 消息类型，决定后续哪些字段必须为 0。
    pub kind: Kind,
    /// bit0 = Err（仅 Response）、bit1 = 流尾、bit2 = GoAway 急停、bit3–7 保留。
    pub flags: u8,
    /// 服务标识。Response / Cancel / GoAway / Handshake 必须为 0。
    pub service_id: u16,
    /// 请求关联号，非 0。GoAway / Handshake 必须为 0。
    pub request_id: u64,
    /// 方法号，非 0。Response / Cancel / GoAway / Handshake 必须为 0。
    pub method: u64,
    /// 剩余时间预算（毫秒）。Response / Cancel / GoAway / Handshake 必须为 0。
    pub deadline_ms: u32,
}

/// 解析结果：头 + 剩余 payload。
///
/// payload 与原始 `Bytes` 共享内存（`slice` 只加引用计数），没有拷贝，
/// 且仍是 `'static`，可以直接 move 进 `tokio::spawn`。
#[derive(Debug, Clone)]
pub struct Decoded {
    pub header: Header,
    pub payload: Bytes,
}

/// 把一条消息拆成「头 + payload」，是 L2 的唯一入口。
///
/// 顺序固定：长度 → 解码 → 切片 → 校验。长度检查必须在解码与切片之前，
/// 否则一条超大异常帧会白走一遍逐字段校验。
pub fn parse(msg: Bytes) -> Result<Decoded, RpcError> {
    check_len(msg.len())?;

    let header = Header::decode(&msg[..HEADER_LEN])?;
    // slice 而非 to_vec：只增引用计数，不复制字节。
    let payload = msg.slice(HEADER_LEN..);
    validate(&header, &payload)?;

    Ok(Decoded { header, payload })
}

/// 长度检查：不足 24 字节或超上限即拒绝。抽出来是为了让 `parse` 只做编排。
fn check_len(len: usize) -> Result<(), RpcError> {
    if len < HEADER_LEN {
        return Err(RpcError::Protocol {
            detail: format!("消息长度 {len} < {HEADER_LEN}，头不完整"),
        });
    }

    if len > MAX_MESSAGE_LEN {
        return Err(RpcError::Protocol {
            detail: format!("消息长度 {len} 超过上限 {MAX_MESSAGE_LEN}"),
        });
    }

    Ok(())
}
