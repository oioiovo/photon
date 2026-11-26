//! 进程内传输：两条单向 `mpsc`，充当 L1 抽象的一致性测试替身。
//!
//! 它不碰 syscall、没有平台差异、也不存在半关闭：这些特性正好让「同一套用例
//! 在多个实现上跑」这件事变得干净。如果连进程内 channel 都塞不进 `Bytes`
//! 零拷贝，共享内存一定白做。

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::transport::{
    DEFAULT_MAX_MESSAGE_LEN, MSG_EXCEEDS_PEER_LIMIT, MessageSink, MessageSource, invalid_data,
    invalid_input,
};

/// 单条队列的默认容量。
///
/// 实现默认值，不是协议约定：它只决定配对之后能积压多少条消息，不进线格式。
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// 进程内传输的读半。
#[derive(Debug)]
pub struct MemorySource {
    rx: mpsc::Receiver<Bytes>,
    limit: Arc<AtomicU32>,
}

/// 进程内传输的写半。
#[derive(Debug)]
pub struct MemorySink {
    tx: mpsc::Sender<Bytes>,
    limit: Arc<AtomicU32>,
}

impl MemorySource {
    fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            limit: Arc::new(AtomicU32::new(DEFAULT_MAX_MESSAGE_LEN)),
        }
    }
}

impl MemorySink {
    fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self {
            tx,
            limit: Arc::new(AtomicU32::new(DEFAULT_MAX_MESSAGE_LEN)),
        }
    }
}

impl MessageSource for MemorySource {
    async fn recv(&mut self) -> io::Result<Bytes> {
        // `None` 只代表对端写半被 drop。返回空消息会让 L2 报「头不完整」，
        // 把「连接结束」误诊成「收到一条坏帧」。
        let msg = self
            .rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "对端写半已 drop"))?;

        // 消息式传输没有前缀错位风险，但上限仍要查：同一套验收用例要求
        // 「超本端上限」在 memory 上也表现为 InvalidData。
        let limit = self.limit.load(Ordering::Relaxed) as usize;
        if msg.len() > limit {
            return Err(invalid_data(format!(
                "消息 {} 字节超过接收上限 {limit}",
                msg.len()
            )));
        }
        Ok(msg)
    }

    fn recv_limit(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.limit)
    }
}

impl MessageSink for MemorySink {
    async fn send(&mut self, msg: Bytes) -> io::Result<()> {
        // 与流式实现同一口径：超对端上限在写之前就失败，这条连接仍可用。
        let limit = self.limit.load(Ordering::Relaxed) as usize;
        if msg.len() > limit {
            return Err(invalid_input(format!(
                "{MSG_EXCEEDS_PEER_LIMIT}: {} > {limit}",
                msg.len()
            )));
        }

        // 队列满就等，这是背压而不是错误。
        self.tx
            .send(msg)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "对端读半已 drop"))
    }

    fn send_limit(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.limit)
    }
}

/// 进程内传输的构造入口。
///
/// 不实现 [`Transport`](crate::transport::Transport)：地址在这里不参与查找，
/// 两端由 `pair` 一次性配好，`bind` / `connect` / `accept` 没有可对应的动作。
pub struct Memory;

impl Memory {
    /// 造一对已经接好的双向连接，返回 A、B 两侧的完整半身。
    ///
    /// 两条队列的方向：`a2b` 承载 A 写 → B 读，`b2a` 承载 B 写 → A 读。
    pub fn pair() -> ((MemorySource, MemorySink), (MemorySource, MemorySink)) {
        let (tx_a2b, rx_a2b) = mpsc::channel(DEFAULT_QUEUE_CAPACITY);
        let (tx_b2a, rx_b2a) = mpsc::channel(DEFAULT_QUEUE_CAPACITY);

        let a = (MemorySource::new(rx_b2a), MemorySink::new(tx_a2b));
        let b = (MemorySource::new(rx_a2b), MemorySink::new(tx_b2a));
        (a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip_then_eof() {
        let ((_a_src, mut a_sink), (mut b_src, _b_sink)) = Memory::pair();

        a_sink
            .send(Bytes::from_static(&[0x11u8; 24]))
            .await
            .expect("送一条");
        assert_eq!(&b_src.recv().await.expect("收一条")[..], &[0x11u8; 24][..]);

        drop(a_sink);
        let eof = b_src.recv().await.expect_err("对端写半消失后必须报 EOF");
        assert_eq!(eof.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn send_over_limit_is_invalid_input_and_connection_survives() {
        let ((_a_src, mut a_sink), (_b_src, _b_sink)) = Memory::pair();
        a_sink.send_limit().store(1024, Ordering::Relaxed);

        let err = a_sink
            .send(Bytes::from(vec![0u8; 2048]))
            .await
            .expect_err("超上限应当拒发");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().starts_with(MSG_EXCEEDS_PEER_LIMIT),
            "标记文本是 L1 与 L3 之间的契约"
        );

        // 拒发不消耗任何队列容量，连接仍然可用。
        a_sink
            .send(Bytes::from_static(&[1u8; 24]))
            .await
            .expect("小消息仍应发得出去");
    }

    #[tokio::test]
    async fn handoff_between_task_is_zero_copy() {
        let ((_a_src, mut a_sink), (mut b_src, _b_sink)) = Memory::pair();

        // `Bytes::from_owner` 把一块外部内存交给 `Bytes` 管，正是「顺手 copy 一份」
        // 最容易被写坏的路径；用指针相等来证明它只是换了主人，字节没动过。
        let owned = vec![0x5Au8; 512];
        let ptr = owned.as_ptr();
        let msg = Bytes::from_owner(owned);
        assert_eq!(msg.as_ptr(), ptr, "from_owner 不应复制");

        a_sink.send(msg).await.expect("发送");
        let received = b_src.recv().await.expect("接收");
        assert_eq!(
            received.as_ptr(),
            ptr,
            "memory 传输在收发之间复制了字节；进程内都做不到零拷贝，shm 就没有意义"
        );
        assert_eq!(received.len(), 512);
    }

    #[tokio::test]
    async fn recv_over_limit_is_invalid_data() {
        let ((_a_src, mut a_sink), (mut b_src, _b_sink)) = Memory::pair();
        b_src.recv_limit().store(1024, Ordering::Relaxed);

        a_sink
            .send(Bytes::from(vec![2u8; 2048]))
            .await
            .expect("写侧上限仍是 64 KiB，发得出去");

        let err = b_src.recv().await.expect_err("超接收上限应当拒绝");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
