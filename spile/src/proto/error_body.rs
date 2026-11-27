//! **框架**错误帧的负载：`code u8 | reserved u8 | message`。
//!
//! 业务错误帧不经过这里——它的 payload 就是契约里 `E` 的编码字节，框架一个字节
//! 都不解释（`docs/PROTOCOL.md` 3.8）。两种帧靠 `flags` bit3 区分，不靠前缀。
//!
//! message 长度由「payload 长度 − 2」得出，没有第二个长度字段：L1 的边界已经是
//! 现成的。

use bytes::{Bytes, BytesMut};

use crate::error::{FRAMEWORK_CODE_MAX, RpcError};

/// 框架错误帧的前缀宽度：`code` + 保留位。少于它连码都读不出来。
pub const FRAMEWORK_PREFIX: usize = 2;

/// 编一个框架错误帧的 payload。
pub fn encode(code: u8, message: &str) -> Bytes {
    let mut buf = BytesMut::with_capacity(FRAMEWORK_PREFIX + message.len());
    buf.extend_from_slice(&[code, 0]);
    buf.extend_from_slice(message.as_bytes());
    buf.freeze()
}

/// 解一个框架错误帧的 payload。
///
/// 码段校验在这里做：`8..=255` 未定义。业务错误不走这条路径，所以「出现 8 以上
/// 的码」一定是实现或中间件写错了，报协议错误比交给上层猜更诚实。
pub fn decode(payload: &[u8]) -> Result<(u8, String), RpcError> {
    let Some((&code, tail)) = payload.split_first() else {
        return Err(RpcError::Protocol {
            detail: "框架错误帧的 payload 为空，至少要有 2 字节前缀".into(),
        });
    };
    if payload.len() < FRAMEWORK_PREFIX {
        return Err(RpcError::Protocol {
            detail: format!(
                "框架错误帧只有 {} 字节，至少要 {FRAMEWORK_PREFIX}",
                payload.len()
            ),
        });
    }
    if code > FRAMEWORK_CODE_MAX {
        return Err(RpcError::Protocol {
            detail: format!("框架错误码 {code} 未定义（有效值 0..={FRAMEWORK_CODE_MAX}）"),
        });
    }
    let _reserved = payload[1];
    let message = String::from_utf8_lossy(&tail[1..]).into_owned();
    Ok((code, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let body = encode(2, "unknown service");
        let (code, message) = decode(&body).expect("解自己的错误帧");
        assert_eq!(code, 2);
        assert_eq!(message, "unknown service");
    }

    #[test]
    fn empty_message_is_fine() {
        let body = encode(1, "");
        let (code, message) = decode(&body).expect("空 message 合法");
        assert_eq!(code, 1);
        assert!(message.is_empty());
    }

    #[test]
    fn rejects_short_prefix() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[7]).is_err());
    }

    #[test]
    fn rejects_undefined_code() {
        let body = encode(8, "x");
        assert!(decode(&body).is_err(), "8 以上不是框架码");
    }

    #[test]
    fn lossy_message_survives() {
        let mut raw = encode(3, "").to_vec();
        raw.extend_from_slice(&[0xFF, 0xFE]);
        let (code, message) = decode(&raw).expect("非法 UTF-8 不该让整帧失败");
        assert_eq!(code, 3);
        assert!(!message.is_empty());
    }
}
