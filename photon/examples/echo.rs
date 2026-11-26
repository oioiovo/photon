//! M0 示例：一条命令跑通「24 字节头 + 长度前缀 + 消息边界」。
//!
//! 不碰 Session（M1 才有）：这里只验证 L1 的收发分帧与 L2 的头编解码。
//!
//! 运行：`cargo run --example echo`

use std::io;

use bytes::Bytes;

use photon::proto::{Header, Kind, parse};
use photon::transport::tcp::TcpTransport;
use photon::transport::{Endpoint, MessageSink, MessageSource, Transport, TransportOptions};

/// 连发几条，用来证明「多条消息不会粘在一起」。
const FRAMES: u64 = 3;
/// payload 填充长度，让每条消息明显长于 24 字节的头。
const FILLER: usize = 16;

/// 拼一条**合法**的 Request：真实头 + 填充 payload。
///
/// 用 L2 的 `Header::encode` 而不是手写字节，是为了让示例同时演示头的编解码。
fn request(request_id: u64) -> Bytes {
    let header = Header {
        kind: Kind::Request,
        flags: 0,
        service_id: 7,
        request_id,
        method: 1,
        deadline_ms: 1000,
    };
    let mut msg = header.encode();
    msg.extend_from_slice(&[0xAA; FILLER]);
    msg.freeze()
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let opts = TransportOptions::default();

    // 端口写 0：让内核分配空闲端口，避免与本机其它进程撞车。
    let listener = TcpTransport::bind(
        &Endpoint::Tcp("127.0.0.1:0".parse().expect("字面地址合法")),
        &opts,
    )
    .await?;
    let addr = listener.local_addr()?;
    println!("[echo] listening on tcp://{addr}");

    let server_opts = opts.clone();
    let server = tokio::spawn(async move {
        let (mut src, mut sink) = listener.accept(&server_opts).await?;
        let mut n = 0u64;
        loop {
            match src.recv().await {
                Ok(msg) => {
                    // 原样回发。L1 不认识头，但字节必须一字节不差地过去，
                    // 这条 echo 就是「分帧有没有把消息切开」的最直接证据。
                    sink.send(msg).await?;
                    n += 1;
                }
                // 对端 drop 写半 → 读到 EOF，正常收尾。
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        Ok::<u64, io::Error>(n)
    });

    let (mut src, mut sink) = TcpTransport::connect(&Endpoint::Tcp(addr), &opts).await?;

    // 先连发 N 条，再逐条收：如果长度前缀或边界算错，这里会读到两条粘在一起的
    // 消息，或者第二条永远等不到。
    let sent: Vec<Bytes> = (1..=FRAMES).map(request).collect();
    for m in &sent {
        sink.send(m.clone()).await?;
    }

    for (i, expected) in sent.iter().enumerate() {
        let echoed = src.recv().await?;
        assert_eq!(
            &echoed[..],
            &expected[..],
            "第 {} 帧回包必须逐字节相同",
            i + 1
        );

        let decoded = parse(echoed).map_err(|e| io::Error::other(e.to_string()))?;
        println!(
            "[echo] frame {}: {} bytes, kind={:?} service_id={} request_id={} payload={}",
            i + 1,
            expected.len(),
            decoded.header.kind,
            decoded.header.service_id,
            decoded.header.request_id,
            decoded.payload.len(),
        );
    }

    // TCP 上 drop 写半会发 FIN，服务端因此能读到 EOF 并退出。
    drop(sink);
    let echoed = server.await.expect("服务端任务未 panic")?;
    println!("[echo] server closed after echoing {echoed} frames");

    assert_eq!(echoed, FRAMES, "服务端应当收到全部帧");
    println!("[echo] ok");
    Ok(())
}
