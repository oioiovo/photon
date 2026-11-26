//! `Handshake` 的 12 字节 payload。
//!
//! 控制帧手写定长、永不经过 `Codec`：握手本身要交代「用哪个 codec」，
//! 让它先解自己一遍是循环依赖。
//!
//! 本文件只管 payload 的字段与语义比对；24 字节头由 `proto::Header` 负责。
//! 帧级校验（头字段全零、payload 恰 12 字节）在 `header::validate` 里已经做过，
//! 这里的长度检查是给「单独喂 payload 进来」的调用方兜底。

use bytes::{Bytes, BytesMut};

use crate::{
    error::RpcError,
    proto::{Decoded, HEADER_LEN, Header, Kind, MAX_MESSAGE_LEN},
};

/// payload 长度。握手帧整帧是 `24 + 12 = 36` 字节。
pub const HANDSHAKE_LEN: usize = 12;

/// 当前协议版本。不等即断连，没有「协商到共同版本」这种柔性逻辑。
pub const PROTOCOL_VERSION: u16 = 1;

/// 默认 codec：postcard。
pub const CODEC_POSTCARD: u16 = 0;

/// 零拷贝 codec，与 `shm` 配套，M5 才启用。
pub const CODEC_RKYV: u16 = 1;

const OFF_VERSION: usize = 0;
const OFF_FLAGS: usize = 2;
const OFF_MAX_LEN: usize = 4;
const OFF_CODEC: usize = 8;
const OFF_RESERVED: usize = 10;

/// 握手里交换的三个值。
///
/// `max_message_len` 是**本端愿意接收**的上限（含 24 字节头），不是「双方取小」：
/// 两个方向各用各的——本端 reader 用本端声明的值，本端 writer 用对端声明的值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    pub protocol_version: u16,
    /// 本端愿意接收的单帧上限（含头）。
    pub max_message_len: u32,
    pub codec_id: u16,
}

impl Default for Handshake {
    /// 版本 1、postcard、64 KiB：刚好是「还没协商」时的 L1 默认上限。
    fn default() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            max_message_len: MAX_MESSAGE_LEN as u32,
            codec_id: CODEC_POSTCARD,
        }
    }
}

impl Handshake {
    pub fn new(max_message_len: u32, codec_id: u16) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            max_message_len,
            codec_id,
        }
    }

    /// 只编码 12 字节 payload，不含头。
    ///
    /// 非法上限（< 24）是编程错误而不是协议错误：类型上没有约束，
    /// 所以用 `debug_assert` 在测试里拦住，release 下不 panic——
    /// 真发出去了，对端也会在 `decode` 时拒掉，不会造成误解码。
    pub fn encode(&self) -> BytesMut {
        debug_assert!(
            self.max_message_len >= HEADER_LEN as u32,
            "max_message_len={} < {HEADER_LEN}，连空头都装不下",
            self.max_message_len
        );

        let mut dst = BytesMut::with_capacity(HANDSHAKE_LEN);
        dst.resize(HANDSHAKE_LEN, 0);
        dst[OFF_VERSION..OFF_FLAGS].copy_from_slice(&self.protocol_version.to_le_bytes());
        // flags 与 reserved 是保留字段，恒 0：没有对应成员，也就没有「写错」的入口。
        dst[OFF_MAX_LEN..OFF_CODEC].copy_from_slice(&self.max_message_len.to_le_bytes());
        dst[OFF_CODEC..OFF_RESERVED].copy_from_slice(&self.codec_id.to_le_bytes());
        dst
    }

    /// 解 12 字节 payload，并做字段级校验。
    ///
    /// 长度必须恰好 12：多一个字节说明对端和我们理解的不是同一个格式，
    /// 这时候「读前 12 字节凑合用」只会把错误推到后面更难查的地方。
    pub fn decode(payload: &[u8]) -> Result<Self, RpcError> {
        if payload.len() != HANDSHAKE_LEN {
            return Err(RpcError::Protocol {
                detail: format!(
                    "Handshake payload 必须恰 {HANDSHAKE_LEN} 字节，实际 {} 字节",
                    payload.len()
                ),
            });
        }

        if payload[OFF_FLAGS] != 0 || payload[OFF_FLAGS + 1] != 0 {
            return Err(RpcError::Protocol {
                detail: format!(
                    "Handshake.flags 保留位必须为 0，实际 0x{:02X}{:02X}",
                    payload[OFF_FLAGS + 1],
                    payload[OFF_FLAGS]
                ),
            });
        }

        if payload[OFF_RESERVED] != 0 || payload[OFF_RESERVED + 1] != 0 {
            return Err(RpcError::Protocol {
                detail: format!(
                    "Handshake.reserved 必须为 0，实际 0x{:02X}{:02X}",
                    payload[OFF_RESERVED + 1],
                    payload[OFF_RESERVED]
                ),
            });
        }

        let hs = Self {
            protocol_version: u16::from_le_bytes(
                payload[OFF_VERSION..OFF_FLAGS]
                    .try_into()
                    .expect("长度已由 HANDSHAKE_LEN 保证"),
            ),
            max_message_len: u32::from_le_bytes(
                payload[OFF_MAX_LEN..OFF_CODEC]
                    .try_into()
                    .expect("长度已由 HANDSHAKE_LEN 保证"),
            ),
            codec_id: u16::from_le_bytes(
                payload[OFF_CODEC..OFF_RESERVED]
                    .try_into()
                    .expect("长度已由 HANDSHAKE_LEN 保证"),
            ),
        };

        if hs.max_message_len < HEADER_LEN as u32 {
            return Err(RpcError::Protocol {
                detail: format!(
                    "Handshake.max_message_len={} < {HEADER_LEN}，连空 payload 的帧都装不下",
                    hs.max_message_len
                ),
            });
        }

        Ok(hs)
    }

    /// 拼出完整的 36 字节握手帧（头 + payload）。
    ///
    /// 头的其余字段必须为 0，这是协议规定，也由 `header::validate` 复查。
    pub fn encode_frame(&self) -> Bytes {
        let header = Header {
            kind: Kind::Handshake,
            flags: 0,
            service_id: 0,
            request_id: 0,
            method: 0,
            deadline_ms: 0,
        };

        let mut buf = BytesMut::with_capacity(HEADER_LEN + HANDSHAKE_LEN);
        buf.extend_from_slice(&header.encode());
        buf.extend_from_slice(&self.encode());
        buf.freeze()
    }

    /// 从 `proto::parse` 的结果取出握手，顺带确认 kind 真的是 `Handshake`。
    ///
    /// payload 长度与头字段的校验已经由 `validate` 做完，这里只解内容。
    pub fn from_decoded(decoded: &Decoded) -> Result<Self, RpcError> {
        if decoded.header.kind != Kind::Handshake {
            return Err(RpcError::Protocol {
                detail: format!("期望首帧为 Handshake，实际 {:?}", decoded.header.kind),
            });
        }
        Self::decode(&decoded.payload)
    }

    /// 与对端声明的值比对，全部 fail-fast。
    ///
    /// 不比较 `max_message_len` 的大小：它是每个方向的独立声明，不是协商结果。
    /// 这里只确认对端给的值本身可用——它可能来自一个绕过 `decode` 直接
    /// 构造的结构体，所以不假设对方已经校验过。
    pub fn check_against(&self, peer: &Self) -> Result<(), RpcError> {
        if self.protocol_version != peer.protocol_version {
            return Err(RpcError::Protocol {
                detail: format!(
                    "协议版本不一致：本端 {}，对端 {}",
                    self.protocol_version, peer.protocol_version
                ),
            });
        }

        if self.codec_id != peer.codec_id {
            return Err(RpcError::Protocol {
                detail: format!(
                    "codec 不一致：本端 {}，对端 {}；双方各自只编译进一种 codec，无法降级",
                    self.codec_id, peer.codec_id
                ),
            });
        }

        if peer.max_message_len < HEADER_LEN as u32 {
            return Err(RpcError::Protocol {
                detail: format!(
                    "对端声明的 max_message_len={} < {HEADER_LEN}，不可用",
                    peer.max_message_len
                ),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload_with(version: u16, max_len: u32, codec: u16) -> Vec<u8> {
        let mut v = vec![0u8; HANDSHAKE_LEN];
        v[OFF_VERSION..OFF_FLAGS].copy_from_slice(&version.to_le_bytes());
        v[OFF_MAX_LEN..OFF_CODEC].copy_from_slice(&max_len.to_le_bytes());
        v[OFF_CODEC..OFF_RESERVED].copy_from_slice(&codec.to_le_bytes());
        v
    }

    #[test]
    fn roundtrip() {
        let hs = Handshake {
            protocol_version: PROTOCOL_VERSION,
            max_message_len: 1024 * 1024,
            codec_id: CODEC_RKYV,
        };
        assert_eq!(Handshake::decode(&hs.encode()).expect("解自己编的"), hs);
    }

    #[test]
    fn rejects_wrong_length() {
        for len in [0, 11, 13, 24] {
            let err = Handshake::decode(&vec![0u8; len]).expect_err("长度不对应当拒绝");
            assert!(matches!(err, RpcError::Protocol { .. }), "len={len}");
        }
    }

    #[test]
    fn rejects_nonzero_flags_and_reserved() {
        let mut flags = payload_with(PROTOCOL_VERSION, 64 * 1024, CODEC_POSTCARD);
        flags[OFF_FLAGS] = 1;
        assert!(Handshake::decode(&flags).is_err(), "flags 非 0 应拒绝");

        let mut reserved = payload_with(PROTOCOL_VERSION, 64 * 1024, CODEC_POSTCARD);
        reserved[OFF_RESERVED + 1] = 0x01;
        assert!(
            Handshake::decode(&reserved).is_err(),
            "reserved 非 0 应拒绝"
        );
    }

    #[test]
    fn rejects_len_below_header() {
        assert!(
            Handshake::decode(&payload_with(PROTOCOL_VERSION, 23, CODEC_POSTCARD)).is_err(),
            "23 < 24 应拒绝"
        );
        assert!(
            Handshake::decode(&payload_with(PROTOCOL_VERSION, 24, CODEC_POSTCARD)).is_ok(),
            "24 是边界，应放行"
        );
    }

    #[test]
    fn check_against_rejects_version_and_codec_mismatch() {
        let local = Handshake::default();

        let mut other_version = local;
        other_version.protocol_version = 2;
        assert!(local.check_against(&other_version).is_err());

        let mut other_codec = local;
        other_codec.codec_id = CODEC_RKYV;
        assert!(local.check_against(&other_codec).is_err());

        // 上限不同不算冲突：两个方向各用各的。
        let mut smaller = local;
        smaller.max_message_len = 1024;
        assert!(local.check_against(&smaller).is_ok());

        // 绕过 decode 直接构造出非法值，也要在这一层拦住。
        let broken = Handshake::new(8, CODEC_POSTCARD);
        assert!(local.check_against(&broken).is_err());
    }

    #[test]
    fn from_decoded_requires_handshake_kind() {
        let hs = Handshake::default();
        let ok = crate::proto::parse(hs.encode_frame()).expect("自己的帧应当合法");
        assert_eq!(Handshake::from_decoded(&ok).expect("是握手"), hs);

        let request = Header {
            kind: Kind::Request,
            flags: 0,
            service_id: 1,
            request_id: 1,
            method: 1,
            deadline_ms: 0,
        };
        let mut buf = request.encode();
        buf.extend_from_slice(&[0u8; 4]);
        let decoded = crate::proto::parse(buf.freeze()).expect("请求帧合法");
        assert!(
            Handshake::from_decoded(&decoded).is_err(),
            "Request 不是握手"
        );
    }

    #[test]
    fn golden_frame_is_36_bytes() {
        // 与 PROTOCOL 8.7 的冻结字节逐字节对齐：全零头 + 12 字节 payload。
        let expected: [u8; 36] = [
            0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 头
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // payload
        ];
        assert_eq!(&Handshake::default().encode_frame()[..], &expected);
    }
}
