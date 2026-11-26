//! 24 字节头的编解码与逐 kind 校验。
//!
//! 只做两件事：按固定偏移读写 6 个字段、判断头与 payload 是否自洽。
//! 不做 IO、不碰状态机、不分配。
//!
//! 之所以手写而不用序列化框架：头是定长协议，postcard 的 varint 与 rkyv
//! 的对齐要求都会造出与线格式并列的第二套事实。也不用 `#[repr(C)]` +
//! `transmute`——`repr(C)` 受对齐规则支配（`u64` 会被插到 8 的倍数处），
//! 与协议的紧致排布并不等价。

use bytes::BytesMut;

use crate::{
    error::RpcError,
    proto::{FLAG_ERR, FLAG_GOAWAY_FORCE, HEADER_LEN, Header, Kind},
};

/// 各字段在 24 字节头中的起始偏移。
///
/// 用常量而非硬编码下标：编码、解码、校验都引用同一组名字，布局一旦变动
/// 会一起暴露。
const OFF_KIND: usize = 0;
const OFF_FLAGS: usize = 1;
const OFF_SERVICE_ID: usize = 2;
const OFF_REQUEST_ID: usize = 4;
const OFF_METHOD: usize = 12;
const OFF_DEADLINE_MS: usize = 20;

impl Header {
    /// 把语义头写入 `dst` 的前 24 字节，全小端。不做语义校验——
    /// 非法 kind 在 `Kind` 类型上就构造不出来。
    ///
    /// # Panics
    ///
    /// `dst` 不足 24 字节时 panic：那是调用方给少了缓冲区，属于编程错误，
    /// 不是协议错误。
    pub fn encode_into(&self, dst: &mut [u8]) {
        assert!(
            dst.len() >= HEADER_LEN,
            "header buffer too small: {} < {}",
            dst.len(),
            HEADER_LEN
        );

        dst[OFF_KIND] = self.kind as u8;
        dst[OFF_FLAGS] = self.flags;
        dst[OFF_SERVICE_ID..OFF_REQUEST_ID].copy_from_slice(&self.service_id.to_le_bytes());
        dst[OFF_REQUEST_ID..OFF_METHOD].copy_from_slice(&self.request_id.to_le_bytes());
        dst[OFF_METHOD..OFF_DEADLINE_MS].copy_from_slice(&self.method.to_le_bytes());
        dst[OFF_DEADLINE_MS..HEADER_LEN].copy_from_slice(&self.deadline_ms.to_le_bytes());
    }

    /// 分配恰好 24 字节并写入头。
    ///
    /// `with_capacity` 只给容量、长度为 0，必须先 `resize` 再写，
    /// 否则 `encode_into` 拿到空切片会直接触发 assert。
    pub fn encode(&self) -> BytesMut {
        let mut dst = BytesMut::with_capacity(HEADER_LEN);
        dst.resize(HEADER_LEN, 0);
        self.encode_into(&mut dst[..]);
        dst
    }

    /// 从至少 24 字节的切片读出语义头。
    ///
    /// 只读不判：字段是否允许非零、payload 长度是否匹配，都留给 [`validate`]。
    /// 拆开的理由是失败定位——能区分「读错了」（偏移、字节序）与
    /// 「读对了但不合法」（kind 与字段冲突）。未定义的 kind 在这一步就失败，
    /// 因为它连「这是哪种消息」都不成立。
    ///
    /// 用关联函数而不是 `impl TryFrom<&[u8]>`：后者回答不了「传进来 30 字节
    /// 怎么办」，而显式写 `&msg[..HEADER_LEN]` 把「我只喂头」写进了调用点。
    pub fn decode(h: &[u8]) -> Result<Self, RpcError> {
        if h.len() < HEADER_LEN {
            return Err(RpcError::Protocol {
                detail: format!("传入切片长度 {} < {HEADER_LEN}，头不完整", h.len()),
            });
        }

        Ok(Self {
            kind: Kind::try_from(h[OFF_KIND])?,
            flags: h[OFF_FLAGS],
            service_id: u16::from_le_bytes(
                h[OFF_SERVICE_ID..OFF_REQUEST_ID]
                    .try_into()
                    .expect("slice length is fixed by HEADER_LEN"),
            ),
            request_id: u64::from_le_bytes(
                h[OFF_REQUEST_ID..OFF_METHOD]
                    .try_into()
                    .expect("slice length is fixed by HEADER_LEN"),
            ),
            method: u64::from_le_bytes(
                h[OFF_METHOD..OFF_DEADLINE_MS]
                    .try_into()
                    .expect("slice length is fixed by HEADER_LEN"),
            ),
            // 上界必须写 HEADER_LEN 而不是留空：调用方可以传整条消息进来，
            // 开放区间会切出 24 字节之外的尾巴，`try_into` 随即 panic。
            deadline_ms: u32::from_le_bytes(
                h[OFF_DEADLINE_MS..HEADER_LEN]
                    .try_into()
                    .expect("slice length is fixed by HEADER_LEN"),
            ),
        })
    }
}

/// 按 kind 校验头与 payload 是否自洽。
///
/// 必须在 [`Header::decode`] 成功之后调用。任何一条不过都算协议违规，
/// 由上层直接断连、不回错误体——此时对端的实现已经不可信，任何回包
/// 都可能被它误读。
///
/// 覆盖范围：flags 白名单、`StreamChunk` 拒绝、「必须为 0」字段矩阵、
/// id 非 0、控制帧定长。帧长上限、握手版本与依赖会话状态的检查不在这里。
pub fn validate(h: &Header, payload: &[u8]) -> Result<(), RpcError> {
    check_flags(h)?;

    match h.kind {
        // 预留编号，M0 不实现流式，整帧拒绝。
        Kind::StreamChunk => Err(RpcError::Protocol {
            detail: format!("{:?} 是预留编号，M0 不实现流式，整帧拒绝", h.kind),
        }),

        // service_id 与 deadline_ms 只要求「有效」，不要求非 0。
        Kind::Request | Kind::Notification => {
            check_nonzero(h.request_id, h.kind, "request_id")?;
            check_nonzero(h.method, h.kind, "method")?;
            Ok(())
        }

        // Err 时错误体至少要 8 字节，否则读不出 code。
        Kind::Response => {
            check_zero(h.service_id, h.kind, "service_id")?;
            check_zero(h.method, h.kind, "method")?;
            check_zero(h.deadline_ms, h.kind, "deadline_ms")?;
            check_nonzero(h.request_id, h.kind, "request_id")?;
            if h.flags & FLAG_ERR != 0 && payload.len() < 8 {
                return Err(RpcError::Protocol {
                    detail: format!(
                        "{:?} 携带错误体，但 payload 只有 {} 字节，至少需要 8（否则读不出 code）",
                        h.kind,
                        payload.len()
                    ),
                });
            }
            Ok(())
        }

        // 只带 request_id，且必须空 payload。
        Kind::Cancel => {
            check_zero(h.service_id, h.kind, "service_id")?;
            check_zero(h.method, h.kind, "method")?;
            check_zero(h.deadline_ms, h.kind, "deadline_ms")?;
            check_nonzero(h.request_id, h.kind, "request_id")?;
            check_empty(payload, h.kind)?;
            Ok(())
        }

        // 全零头 + 空 payload，关闭语义完全由 flags bit2 表达。
        Kind::GoAway => {
            check_zero(h.service_id, h.kind, "service_id")?;
            check_zero(h.request_id, h.kind, "request_id")?;
            check_zero(h.method, h.kind, "method")?;
            check_zero(h.deadline_ms, h.kind, "deadline_ms")?;
            check_empty(payload, h.kind)?;
            Ok(())
        }

        // 全零头 + 恰 12 字节。这里只查长度不解析内容：版本与 codec 的比对
        // 要等握手状态机，只查一半会让人以为握手已经校验过了。
        Kind::Handshake => {
            check_zero(h.service_id, h.kind, "service_id")?;
            check_zero(h.request_id, h.kind, "request_id")?;
            check_zero(h.method, h.kind, "method")?;
            check_zero(h.deadline_ms, h.kind, "deadline_ms")?;
            if payload.len() != 12 {
                return Err(RpcError::Protocol {
                    detail: format!(
                        "{:?} 的 payload 必须恰 12 字节，实际 {} 字节",
                        h.kind,
                        payload.len()
                    ),
                });
            }
            Ok(())
        }
    }
}

/// flags 白名单。
///
/// 用白名单而不是逐条判非法位：`bit0 + bit2` 同时置位这种组合，在两条
/// 独立的黑名单里各被拒一半，很容易误以为已覆盖。
fn check_flags(h: &Header) -> Result<(), RpcError> {
    let legal = match h.kind {
        Kind::Response => matches!(h.flags, 0 | FLAG_ERR),
        Kind::GoAway => matches!(h.flags, 0 | FLAG_GOAWAY_FORCE),
        _ => h.flags == 0,
    };
    if legal {
        return Ok(());
    }

    Err(RpcError::Protocol {
        detail: format!("{:?} 的 flags=0x{:02X} 非法", h.kind, h.flags),
    })
}

/// 断言某个字段必须为 0。
fn check_zero<V: Into<u64>>(value: V, kind: Kind, field: &'static str) -> Result<(), RpcError> {
    let actual = value.into();
    if actual == 0 {
        Ok(())
    } else {
        Err(RpcError::Protocol {
            detail: format!("{kind:?}.{field} 必须为 0，实际 {actual}"),
        })
    }
}

/// 断言某个字段必须非 0（0 在各 id 空间里都是保留值）。
fn check_nonzero<V: Into<u64>>(value: V, kind: Kind, field: &'static str) -> Result<(), RpcError> {
    if value.into() == 0 {
        Err(RpcError::Protocol {
            detail: format!("{kind:?}.{field} 必须非 0（0 是保留值）"),
        })
    } else {
        Ok(())
    }
}

/// 断言 payload 必须为空：控制帧是定长的，带内容即说明编码错位。
fn check_empty(payload: &[u8], kind: Kind) -> Result<(), RpcError> {
    if payload.is_empty() {
        Ok(())
    } else {
        Err(RpcError::Protocol {
            detail: format!("{kind:?} 的 payload 必须为空，实际 {} 字节", payload.len()),
        })
    }
}
