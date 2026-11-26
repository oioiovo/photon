//! L2 协议层测试：24 字节头编解码 + 逐 kind 校验。
//!
//! 只覆盖头与消息长度边界，不含握手状态机、错误体内容、payload 解码。
//! 每项都经由公开入口 `parse` 或 `Header::decode`，不碰私有实现。
//! 反例的意义：没有反例，硬约束就等于一条都没测。

use bytes::Bytes;

use photon::error::RpcError;
use photon::proto::{
    FLAG_BUSINESS, FLAG_ERR, FLAG_GOAWAY_FORCE, FLAG_STREAM_LAST, HEADER_LEN, Header, Kind,
    MAX_MESSAGE_LEN, parse,
};

/// 12 字节握手 payload 的真实值：版本 2、flags 0、上限 64 KiB、codec 0、reserved 0。
/// M0 不校验内容，用真实值是为了将来接上握手校验时不必改测试。
const HANDSHAKE_PAYLOAD: [u8; 12] = [2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0];

/// 每个 kind 的「语义合法」头，payload 由调用方另给。
///
/// `StreamChunk` 也给一份：它的字段值不参与校验（整帧先被拒），
/// 这份只是用来测「decode 能读、validate 必拒」这条分界。
fn legal(kind: Kind) -> Header {
    match kind {
        Kind::Request | Kind::Notification => Header {
            kind,
            flags: 0,
            service_id: 1,
            request_id: 1,
            method: 1,
            deadline_ms: 0,
        },
        Kind::Response | Kind::Cancel => Header {
            kind,
            flags: 0,
            service_id: 0,
            request_id: 1,
            method: 0,
            deadline_ms: 0,
        },
        Kind::GoAway | Kind::Handshake | Kind::StreamChunk => Header {
            kind,
            flags: 0,
            service_id: 0,
            request_id: 0,
            method: 0,
            deadline_ms: 0,
        },
    }
}

/// 拼一条完整消息：24 字节头 + payload。
fn frame(h: &Header, payload: &[u8]) -> Bytes {
    let mut buf = vec![0u8; HEADER_LEN];
    h.encode_into(&mut buf);
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

/// 断言结果是 `RpcError::Protocol`。
///
/// 不用 `assert_eq!`：`RpcError` 刻意没有 `PartialEq`（它带 `message` 的变体
/// 在 M2 才出现，那个字段不该参与等价判断），用 `matches!` 避免为测试反向改 API。
fn assert_protocol<T: std::fmt::Debug>(r: Result<T, RpcError>) {
    match r {
        // 只断言「是协议错误」，不锁条目号：反例可能被另一条更早的规则挡住，
        // 锁死编号会让「改一条规则的归类」波及一堆测试。
        // 「字段名与实际值是否真的被填进去」由 `violation_carries_field_and_value` 单独验证。
        Err(RpcError::Protocol { .. }) => {}
        Err(other) => panic!("期望 Protocol，实际是 {other:?}"),
        Ok(value) => panic!("期望 Protocol，实际通过：Ok({value:?})"),
    }
}

// ---------------------------------------------------------------- 组 1：边界

#[test]
fn boundary_message_lengths() {
    // 24 字节恰是空 payload 的 GoAway，是最短的合法帧。
    let goaway = frame(&legal(Kind::GoAway), &[]);
    assert_eq!(goaway.len(), HEADER_LEN);
    assert!(parse(goaway.clone()).is_ok(), "24 字节的合法控制帧应当通过");

    // 23 字节：头都不完整。
    assert_protocol(parse(goaway.slice(..HEADER_LEN - 1)));

    // 0 字节：空输入不是「空消息」，而是缺头。
    assert_protocol(parse(Bytes::new()));

    // 上限边界：恰好等于 `MAX_MESSAGE_LEN` 必须放行，多一个字节必须拒。
    // 这两个用例字段全合法，唯一的差别就是长度，因此失败只可能来自帧长检查。
    let at_limit = frame(
        &legal(Kind::Request),
        &vec![0u8; MAX_MESSAGE_LEN - HEADER_LEN],
    );
    assert_eq!(at_limit.len(), MAX_MESSAGE_LEN);
    assert!(parse(at_limit).is_ok(), "等于上限的帧应当通过");

    let over_limit = frame(
        &legal(Kind::Request),
        &vec![0u8; MAX_MESSAGE_LEN - HEADER_LEN + 1],
    );
    assert_protocol(parse(over_limit));
}

// -------------------------------------------------- 组 2：七种 kind 的往返

#[test]
fn roundtrip_all_kinds() {
    let kinds = [
        Kind::Request,
        Kind::Response,
        Kind::Notification,
        Kind::Cancel,
        Kind::StreamChunk,
        Kind::GoAway,
        Kind::Handshake,
    ];

    for kind in kinds {
        let h = legal(kind);
        let bytes = frame(&h, &[]);
        // 这一组只走 decode，不走 parse：StreamChunk 在语义层必被拒，
        // 但编解码本身必须对它是对称的，否则连拒绝都无从谈起。
        let decoded = Header::decode(&bytes[..]).expect("decode 应当成功");
        assert_eq!(decoded, h, "kind {kind:?} 往返后字段不一致");
    }
}

// ------------------------------------------ 组 3：逐 kind「必须为 0」矩阵反例

#[test]
fn zero_matrix_violations() {
    // Response 的 service_id / method / deadline_ms 必须为 0
    for h in [
        Header {
            service_id: 1,
            ..legal(Kind::Response)
        },
        Header {
            method: 1,
            ..legal(Kind::Response)
        },
        Header {
            deadline_ms: 1,
            ..legal(Kind::Response)
        },
    ] {
        assert_protocol(parse(frame(&h, &[])));
    }

    // Response 的 request_id 必须非 0
    assert_protocol(parse(frame(
        &Header {
            request_id: 0,
            ..legal(Kind::Response)
        },
        &[],
    )));

    // Cancel：service_id / method / deadline_ms 为 0，request_id 非 0
    assert_protocol(parse(frame(
        &Header {
            service_id: 1,
            ..legal(Kind::Cancel)
        },
        &[],
    )));
    assert_protocol(parse(frame(
        &Header {
            request_id: 0,
            ..legal(Kind::Cancel)
        },
        &[],
    )));

    // GoAway 的 request_id 必须为 0
    assert_protocol(parse(frame(
        &Header {
            request_id: 1,
            ..legal(Kind::GoAway)
        },
        &[],
    )));

    // Handshake 的 service_id 必须为 0
    assert_protocol(parse(frame(
        &Header {
            service_id: 1,
            ..legal(Kind::Handshake)
        },
        &HANDSHAKE_PAYLOAD,
    )));

    // Request 的 request_id / method 必须非 0
    assert_protocol(parse(frame(
        &Header {
            request_id: 0,
            ..legal(Kind::Request)
        },
        &[],
    )));
    assert_protocol(parse(frame(
        &Header {
            method: 0,
            ..legal(Kind::Request)
        },
        &[],
    )));

    // Notification 的 method 必须非 0
    assert_protocol(parse(frame(
        &Header {
            method: 0,
            ..legal(Kind::Notification)
        },
        &[],
    )));
}

// ----------------------------------------------- 组 4：未定义与保留 kind

#[test]
fn undefined_and_reserved_kinds() {
    // 7..=255 未定义：解码期就该拒，不必等到语义校验。
    for raw in [7u8, 255] {
        let mut bytes = frame(&legal(Kind::GoAway), &[]).to_vec();
        bytes[0] = raw;
        assert_protocol(parse(Bytes::from(bytes)));
    }

    // StreamChunk（4）是已定义但未实现的编号占位：能解码，但整帧拒绝。
    // 这两步必须分开断言，否则「未定义」和「已定义但未实现」被混成同一个失败。
    let bytes = frame(&legal(Kind::StreamChunk), &[]);
    assert!(
        Header::decode(&bytes[..]).is_ok(),
        "kind=4 应当在 decode 阶段可读"
    );
    assert_protocol(parse(bytes));
}

// --------------------------------------------- 组 5：flags 的非法组合

#[test]
fn illegal_flag_combinations() {
    // bit0 只允许 Response
    assert_protocol(parse(frame(
        &Header {
            flags: FLAG_ERR,
            ..legal(Kind::Request)
        },
        &[],
    )));

    // bit2 只允许 GoAway
    assert_protocol(parse(frame(
        &Header {
            flags: FLAG_GOAWAY_FORCE,
            ..legal(Kind::Request)
        },
        &[],
    )));

    // bit1 当前在任何 kind 上都非法（只对未实现的 StreamChunk 有意义）
    assert_protocol(parse(frame(
        &Header {
            flags: FLAG_STREAM_LAST,
            ..legal(Kind::Response)
        },
        &[],
    )));

    // bit0 与 bit2 同时置位
    assert_protocol(parse(frame(
        &Header {
            flags: FLAG_ERR | FLAG_GOAWAY_FORCE,
            ..legal(Kind::Response)
        },
        &[],
    )));

    // 保留位 bit3
    assert_protocol(parse(frame(
        &Header {
            flags: 0x08,
            ..legal(Kind::Request)
        },
        &[],
    )));

    // GoAway 只允许 0x00 / 0x04，bit0 非法
    assert_protocol(parse(frame(
        &Header {
            flags: FLAG_ERR,
            ..legal(Kind::GoAway)
        },
        &[],
    )));

    // 两个合法组合仍需通过，防止白名单写成了「一律拒绝」
    assert!(
        parse(frame(
            &Header {
                flags: FLAG_ERR,
                ..legal(Kind::Response)
            },
            &[0u8; 8],
        ))
        .is_ok()
    );
    assert!(
        parse(frame(
            &Header {
                flags: FLAG_GOAWAY_FORCE,
                ..legal(Kind::GoAway)
            },
            &[],
        ))
        .is_ok()
    );
}

// ------------------------------------------------ 组 6：定长与空 payload

#[test]
fn fixed_length_payloads() {
    // Handshake 必须恰 12 字节
    let hs = legal(Kind::Handshake);
    assert_protocol(parse(frame(&hs, &HANDSHAKE_PAYLOAD[..11])));
    assert_protocol(parse(frame(&hs, &[0u8; 13])));
    assert!(parse(frame(&hs, &HANDSHAKE_PAYLOAD)).is_ok());

    // 框架错误帧至少有 2 字节前缀（`code u8 | reserved u8`）
    let err_resp = Header {
        flags: FLAG_ERR,
        ..legal(Kind::Response)
    };
    assert_protocol(parse(frame(&err_resp, &[0u8; 1])));
    assert!(parse(frame(&err_resp, &[0u8, 0])).is_ok());

    // 业务错误帧的 payload 归契约：0 字节也合法，框架不设下限
    let biz_resp = Header {
        flags: FLAG_ERR | FLAG_BUSINESS,
        ..legal(Kind::Response)
    };
    assert!(parse(frame(&biz_resp, &[])).is_ok());

    // Cancel / GoAway 必须无 payload
    assert_protocol(parse(frame(&legal(Kind::Cancel), &[0u8; 1])));
    assert_protocol(parse(frame(&legal(Kind::GoAway), &[0u8; 1])));
}

// ---------------------------------------------------- 组 7：零拷贝切片

#[test]
fn payload_is_zero_copy_slice() {
    let payload = b"hello";
    let msg = frame(&legal(Kind::Request), payload);
    let base = msg.as_ptr();

    let decoded = parse(msg).expect("合法帧应当通过");

    // payload 必须落在消息起点 + 24 的位置，且共享同一块内存：
    // `Bytes::slice` 只增引用计数，不复制字节。
    assert_eq!(
        decoded.payload.as_ptr(),
        base.wrapping_add(HEADER_LEN),
        "payload 起点应当正好是消息偏移 24"
    );
    assert_eq!(&decoded.payload[..], payload, "payload 内容应当原样保留");
}

// ------------------------------------ 组 8：逐字节黄金样例

/// 断言一条帧的编码结果与冻结字面量完全一致，且能原样解析回来。
///
/// 唯一能抓住「偏移写错、字节序写反，但 encode/decode 仍对称」的测试：
/// 往返只证明自洽，不证明符合文档，`from_be_bytes` 那类 bug 在往返里永远绿。
///
/// 样例内联而非 `.bin` + `include_bytes!`：二进制在 diff 里不可见，
/// 内联字面量逐行可审、可逐段注释，冻结效果等价。
fn assert_golden(name: &str, h: &Header, payload: &[u8], expected: &[u8]) {
    let bytes = frame(h, payload);
    assert_eq!(
        bytes.as_ref(),
        expected,
        "{name} 与冻结样例不一致：偏移或字节序偏离了文档"
    );

    let decoded = parse(bytes).unwrap_or_else(|e| panic!("{name} 应当通过全部校验，实际：{e}"));
    assert_eq!(decoded.header, *h, "{name} 的头字段回读不一致");
    assert_eq!(&decoded.payload[..], payload, "{name} 的 payload 不一致");
}

#[test]
fn golden_samples_match_protocol_section_8() {
    // Request（26 字节）：id 字段均非 0，payload 从偏移 24 开始
    let request = Header {
        kind: Kind::Request,
        flags: 0,
        service_id: 7,
        request_id: 1,
        method: 0xC0DE_0000_0000_0001,
        deadline_ms: 500,
    };
    assert_golden(
        "Request",
        &request,
        &[0xAA, 0xBB],
        &[
            0x00, 0x00, // kind = Request(0)，flags = 0
            0x07, 0x00, // service_id = 7（小端：低字节在前）
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request_id = 1
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xC0, // method = 0xC0DE_0000_0000_0001
            0xF4, 0x01, 0x00, 0x00, // deadline_ms = 500
            0xAA, 0xBB, // payload
        ],
    );

    // Response Ok（25 字节）：service_id / method / deadline_ms 必须全 0
    assert_golden(
        "Response Ok",
        &legal(Kind::Response),
        &[0xCC],
        &[
            0x01, 0x00, // Response(1)，flags = 0（Ok）
            0x00, 0x00, // service_id 必须 0
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request_id = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // method 必须 0
            0x00, 0x00, 0x00, 0x00, // deadline_ms 必须 0
            0xCC, // payload
        ],
    );

    // Response Err（34 字节）：2 字节错误体前缀（code + reserved）+ "bad args"
    let response_err = Header {
        flags: FLAG_ERR,
        ..legal(Kind::Response)
    };
    assert_golden(
        "Response Err",
        &response_err,
        &[
            0x04, // code = 4（InvalidArgs）
            0x00, // reserved 必须 0
            0x62, 0x61, 0x64, 0x20, 0x61, 0x72, 0x67, 0x73, // "bad args"
        ],
        &[
            0x01, 0x01, // Response(1)，flags bit0 = 1（Err）
            0x00, 0x00, // service_id 必须 0
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request_id = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // method 必须 0
            0x00, 0x00, 0x00, 0x00, // deadline_ms 必须 0
            0x04, // code = 4
            0x00, // reserved
            0x62, 0x61, 0x64, 0x20, 0x61, 0x72, 0x67, 0x73, // "bad args"
        ],
    );

    // Notification（25 字节）：不产生 Response，id 仍必须非 0
    let notification = Header {
        kind: Kind::Notification,
        flags: 0,
        service_id: 7,
        request_id: 2,
        method: 0xC0DE_0000_0000_0002,
        deadline_ms: 0,
    };
    assert_golden(
        "Notification",
        &notification,
        &[0xDD],
        &[
            0x02, 0x00, // Notification(2)，flags = 0
            0x07, 0x00, // service_id = 7
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request_id = 2
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xC0, // method = 0xC0DE_0000_0000_0002
            0x00, 0x00, 0x00, 0x00, // deadline_ms = 0
            0xDD, // payload
        ],
    );

    // Cancel（24 字节）：全零头（除 request_id）+ 空 payload
    assert_golden(
        "Cancel",
        &legal(Kind::Cancel),
        &[],
        &[
            0x03, 0x00, // Cancel(3)，flags = 0
            0x00, 0x00, // service_id 必须 0
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request_id = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // method 必须 0
            0x00, 0x00, 0x00, 0x00, // deadline_ms 必须 0
        ],
    );

    // GoAway（24 字节）：优雅关闭，全零头 + 空 payload
    let goaway_graceful: [u8; 24] = [
        0x05, 0x00, // GoAway(5)，flags = 0
        0x00, 0x00, // service_id 必须 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request_id 必须 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // method 必须 0
        0x00, 0x00, 0x00, 0x00, // deadline_ms 必须 0
    ];
    assert_golden("GoAway", &legal(Kind::GoAway), &[], &goaway_graceful);

    // 急停变体：与优雅版只差 flags 的第 2 位
    let mut goaway_force = goaway_graceful;
    goaway_force[1] = FLAG_GOAWAY_FORCE;
    assert_golden(
        "GoAway 急停",
        &Header {
            flags: FLAG_GOAWAY_FORCE,
            ..legal(Kind::GoAway)
        },
        &[],
        &goaway_force,
    );

    // Handshake（36 字节）：全零头 + 12 字节定长 payload
    assert_golden(
        "Handshake",
        &legal(Kind::Handshake),
        &HANDSHAKE_PAYLOAD,
        &[
            0x06, 0x00, // Handshake(6)，flags = 0
            0x00, 0x00, // service_id 必须 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request_id 必须 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // method 必须 0
            0x00, 0x00, 0x00, 0x00, // deadline_ms 必须 0
            0x02, 0x00, // protocol_version = 2
            0x00, 0x00, // flags 保留，必须 0
            0x00, 0x00, 0x01, 0x00, // max_message_len = 0x0001_0000（64 KiB）
            0x00, 0x00, // codec_id = 0（postcard）
            0x00, 0x00, // reserved 必须 0
        ],
    );
}

// ----------------------------------------- 组 9：错误信息必须能定位

#[test]
fn violation_carries_field_and_value() {
    // 这是给「排障能力」本身的测试：协议错误不能是一个没有内容的 `Protocol`。
    // 一条连接上有几十个可能违规的位置，只有「Kind.field + 实际值」才能定位。
    let h = Header {
        service_id: 7,
        ..legal(Kind::Response)
    };

    match parse(frame(&h, &[])) {
        Err(RpcError::Protocol { detail }) => {
            assert!(
                detail.contains("Response.service_id"),
                "detail 应当指明出错字段，实际：{detail}"
            );
            assert!(
                detail.contains('7'),
                "detail 应当带上实际值，实际：{detail}"
            );
        }
        other => panic!("期望协议错误，实际：{other:?}"),
    }

    // flags 报错同样要带 kind 与实际位模式，而不是只说「flags 非法」。
    let h = Header {
        flags: FLAG_STREAM_LAST,
        ..legal(Kind::Request)
    };
    match parse(frame(&h, &[])) {
        Err(RpcError::Protocol { detail }) => {
            assert!(
                detail.contains("Request"),
                "detail 应当指明 kind，实际：{detail}"
            );
            assert!(
                detail.contains("0x02"),
                "detail 应当带上实际 flags，实际：{detail}"
            );
        }
        other => panic!("期望协议错误，实际：{other:?}"),
    }
}
