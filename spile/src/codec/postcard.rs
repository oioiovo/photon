//! postcard 实现：紧凑、无 schema，`codec_id = 0`。
//!
//! 取舍写在设计文档里：没有 schema 演进能力、反序列化必然产出 owned 结构。
//! 换 rkyv（M5）是**另一个编译产物**，不与它共存于同一组 handler。

use bytes::{BufMut, Bytes};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Codec;
use crate::error::RpcError;

/// postcard 的 codec。无状态，本身即类型参数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Postcard;

impl Codec for Postcard {
    const ID: u16 = 0;

    fn encode_into<T, B>(value: &T, dst: &mut B) -> Result<(), RpcError>
    where
        T: Serialize + ?Sized,
        B: BufMut + ?Sized,
    {
        // postcard 只能往 `Extend<u8>` / io::Write 里写，没有「直接写 BufMut」的口子；
        // 这个适配器把 `extend` 转成 `put_u8`，避免再摊一次临时 Vec。
        struct ToBuf<'a, B: ?Sized>(&'a mut B);

        impl<B: BufMut + ?Sized> Extend<u8> for ToBuf<'_, B> {
            fn extend<I: IntoIterator<Item = u8>>(&mut self, iter: I) {
                for byte in iter {
                    self.0.put_u8(byte);
                }
            }
        }

        ::postcard::to_extend(value, ToBuf(dst))
            .map(|_| ())
            .map_err(|e| encode_error(&e))
    }

    fn decode<T: DeserializeOwned>(src: &Bytes) -> Result<T, RpcError> {
        ::postcard::from_bytes(src.as_ref()).map_err(|e| decode_error(&e))
    }
}

/// 编码失败说明**本端**给了 codec 一个编不出来的值（例如长度超出 postcard 的
/// 定长上限），属于调用方参数问题，不是对端的事。
fn encode_error(err: &::postcard::Error) -> RpcError {
    RpcError::InvalidInput(format!("postcard 编码失败：{err}"))
}

/// 解码失败一律 `InvalidInput`（线上 `code = 4`）：帧格式没问题，是这条请求的
/// 参数不对，不该因此断连。
fn decode_error(err: &::postcard::Error) -> RpcError {
    RpcError::InvalidInput(format!("postcard 解码失败：{err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Args {
        a: i64,
        b: i64,
    }

    #[test]
    fn roundtrip_tuple() {
        let args = (7i64, -3i64);
        let bytes = Postcard::encode(&args).expect("编码");
        let back: (i64, i64) = Postcard::decode(&bytes).expect("解码");
        assert_eq!(back, args);
    }

    #[test]
    fn encode_into_keeps_headroom() {
        let args = Args { a: 12, b: 34 };
        let whole = Postcard::encode(&args).expect("encode");

        // 预置 24 字节头：`encode_into` 只往后追加，头的位置原样保留。
        let mut dst = vec![0xAAu8; 24];
        Postcard::encode_into(&args, &mut dst).expect("encode_into");
        assert_eq!(&dst[..24], &[0xAAu8; 24]);
        assert_eq!(&dst[24..], &whole[..]);
    }

    #[test]
    fn decode_truncated_is_invalid_input() {
        let err = Postcard::decode::<(i64, i64)>(&Bytes::from_static(&[0x01]))
            .expect_err("截断的 payload 必须报错");
        assert!(matches!(err, RpcError::InvalidInput(_)), "实际 {err:?}");
    }

    #[test]
    fn codec_id_is_protocol_constant() {
        assert_eq!(Postcard::ID, 0, "postcard = 0，值不许改");
    }
}
