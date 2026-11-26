//! 错误体：`flags` bit0 = 1 的 `Response` 里装的东西。
//!
//! 8 字节定长前缀 + 变长 `message`，message 长度由「payload 长度 − 8」得出，
//! 没有第二个长度字段——L1 的边界已经是现成的。

use bytes::{Bytes, BytesMut};

use crate::{
    error::{RemoteError, RpcError},
    proto::HEADER_LEN,
};

/// 前缀宽度。少于它就连 `code` 都读不出来。
pub const ERROR_BODY_PREFIX: usize = 8;

/// 编一个错误体。
pub fn encode(code: u32, retryable: bool, message: &str) -> Bytes {
    let mut buf = BytesMut::with_capacity(ERROR_BODY_PREFIX + message.len());
    buf.extend_from_slice(&code.to_le_bytes());
    buf.extend_from_slice(&[u8::from(retryable), 0, 0, 0]);
    buf.extend_from_slice(message.as_bytes());
    buf.freeze()
}

/// 解一个错误体。
///
/// `retryable` 非 0 即视为可重试：错误体是收尾通道，在这里报协议错误只会更难排障。
pub fn decode(payload: &[u8]) -> Result<RemoteError, RpcError> {
    if payload.len() < ERROR_BODY_PREFIX {
        return Err(RpcError::Protocol {
            detail: format!(
                "错误体只有 {} 字节，至少要 {ERROR_BODY_PREFIX}",
                payload.len()
            ),
        });
    }

    let code = u32::from_le_bytes(
        payload[..4]
            .try_into()
            .expect("长度已由 ERROR_BODY_PREFIX 保证"),
    );
    let retryable = payload[4] != 0;
    let message = String::from_utf8_lossy(&payload[ERROR_BODY_PREFIX..]).into_owned();

    Ok(RemoteError {
        code,
        retryable,
        message,
    })
}

/// 把本端错误映射成能上线的 `(code, retryable, message)`。
///
/// 只有「对端陈述自己状态」的语义才该出现在这里：`Remote` 原样透传，
/// 其余本地变体按协议 code 表挑一个最接近的，绝不把本地变体直接搬上线。
pub fn code_for(err: &RpcError) -> (u32, bool, String) {
    match err {
        RpcError::Remote(e) => (e.code, e.retryable, e.message.clone()),
        RpcError::Timeout => (6, false, "deadline exceeded".into()),
        RpcError::Overloaded => (1, true, "overloaded".into()),
        RpcError::Closed => (7, false, "shutting down".into()),
        RpcError::Protocol { detail } => (0, false, format!("protocol violation: {detail}")),
        RpcError::Io(e) => (0, false, format!("io error: {e}")),
        RpcError::RuntimeDropped => (0, false, "handler task dropped".into()),
        RpcError::InvalidInput(m) => (4, false, format!("invalid args: {m}")),
        RpcError::Internal(m) => (0, false, m.clone()),
    }
}

/// 错误体最小长度必须 ≥ 8，`HEADER_LEN` 只是用来提醒「payload 不含头」。
const _: () = assert!(ERROR_BODY_PREFIX < HEADER_LEN);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let body = encode(2, false, "unknown service");
        let e = decode(&body).expect("解自己的错误体");
        assert_eq!(e.code, 2);
        assert!(!e.retryable);
        assert_eq!(e.message, "unknown service");
    }

    #[test]
    fn accepts_non_canonical_retryable() {
        let mut body = encode(1, false, "").to_vec();
        body[4] = 7;
        let e = decode(&body).expect("retryable 非 0/1 不算协议错误");
        assert!(e.retryable);
    }

    #[test]
    fn rejects_short_prefix() {
        assert!(decode(&[0u8; 7]).is_err());
    }

    #[test]
    fn maps_local_variants_to_wire_codes() {
        assert_eq!(code_for(&RpcError::Timeout).0, 6);
        assert_eq!(code_for(&RpcError::Overloaded).0, 1);
        let remote = RemoteError {
            code: 9,
            retryable: true,
            message: "x".into(),
        };
        assert_eq!(code_for(&RpcError::Remote(remote)).0, 9);
    }
}
