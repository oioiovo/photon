//! payload 编解码：编译期选定一种实现，没有 `Box<dyn Codec>`。
//!
//! `Codec` 的方法带泛型参数又不带 `&self`（codec 本身无状态），因此不是 dyn-safe；
//! `Default` 是类型别名而不是 trait 对象。握手里报的 `codec_id` 直接取
//! `Default::ID`，全项目只维护这一份映射。

use bytes::{BufMut, Bytes};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::RpcError;

pub trait Codec {
    /// 线上 `codec_id`。属于协议的一部分，值不许改。
    const ID: u16;

    /// 就地写进目标缓冲。`dst` 应已留好 24 字节头的位置。
    fn encode_into<T, B>(value: &T, dst: &mut B) -> Result<(), RpcError>
    where
        T: Serialize + ?Sized,
        B: BufMut + ?Sized;

    fn decode<T: DeserializeOwned>(src: &Bytes) -> Result<T, RpcError>;

    /// 便利包装：不需要预留头的场景（例如客户端现造 payload）直接拿 `Bytes`。
    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, RpcError> {
        let mut dst = Vec::new();
        Self::encode_into(value, &mut dst)?;
        Ok(Bytes::from(dst))
    }
}

#[cfg(feature = "codec-postcard")]
pub mod postcard;

/// 当前构建使用的 codec。用法与类型名一致：`Default::encode(&args)`。
#[cfg(feature = "codec-postcard")]
pub use postcard::Postcard as Default;
