//! 流式传输的分帧：把「字节流」适配成「一次一条完整消息」。
//!
//! 字节流没有消息边界，所以每条消息前面加 4 字节小端长度前缀，前缀只算
//! 消息体、不含自身。`recv` 负责读到前缀、读满消息体；`send` 负责先写前缀
//! 再写消息体。`tcp` 与本地 IPC 共用这一份，避免各写一遍边界逻辑。
//!
//! 这一层只认长度，不解析消息内容；上限的比较对象是一个外部写入的数字，
//! 它从哪来与本层无关。

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::transport::{
    DEFAULT_MAX_MESSAGE_LEN, MSG_EXCEEDS_PEER_LIMIT, MessageSink, MessageSource, invalid_data,
    invalid_input,
};

/// 长度前缀自身的字节数。
const LEN_PREFIX: usize = 4;

/// 一条消息的最小长度。
///
/// 与语义头长度同值，但这里只把它当「一条消息不可能这么短」的下界——
/// 本层不承认「头」这个概念，拒绝的是明显截断或错位的帧。
const MIN_MESSAGE_LEN: usize = 24;

/// 把底层读半包成消息源。
#[derive(Debug)]
pub struct FramedSource<S> {
    inner: S,
    /// 跨消息复用：容量按上一条消息增长并保留，避免每帧重新分配。
    buf: BytesMut,
    limit: Arc<AtomicU32>,
}

impl<S> FramedSource<S> {
    /// 装配一个读半，接收上限从 [`DEFAULT_MAX_MESSAGE_LEN`] 开始。
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            buf: BytesMut::new(),
            limit: Arc::new(AtomicU32::new(DEFAULT_MAX_MESSAGE_LEN)),
        }
    }

    /// 取回底层流。半关闭或需要在关闭前要回流句柄时用。
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// 缓冲里还剩多少字节。只给测试断言「读完一条消息后没有残留」。
    #[cfg(test)]
    fn buffered(&self) -> usize {
        self.buf.len()
    }
}

impl<S: AsyncRead + Unpin + Send> MessageSource for FramedSource<S> {
    async fn recv(&mut self) -> io::Result<Bytes> {
        // 1. 凑满前缀。EOF 出现在任何未满处都不是「空消息」，而是连接断了。
        while self.buf.len() < LEN_PREFIX {
            if self.inner.read_buf(&mut self.buf).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("连接在帧前缀读完前关闭（已缓冲 {} 字节）", self.buf.len()),
                ));
            }
        }

        // 2. 解析长度。前缀本身不算长度。
        let len = u32::from_le_bytes(
            self.buf[..LEN_PREFIX]
                .try_into()
                .expect("前缀长度固定为 LEN_PREFIX"),
        ) as usize;

        // 3. 立刻消费前缀。漏掉这一步，后面切出来的就是「前缀 + 前 len-4
        //    字节」，字段全部错位，而且不 panic、只静默读出乱码。
        self.buf.advance(LEN_PREFIX);

        // 4. 两个上界都在分配缓冲之前判定，所以最坏分配被夹在上限以内。
        if len < MIN_MESSAGE_LEN {
            return Err(invalid_data(format!(
                "帧长 {len} 小于最小 {MIN_MESSAGE_LEN}，帧已截断或错位"
            )));
        }
        let limit = self.limit.load(Ordering::Relaxed) as usize;
        if len > limit {
            return Err(invalid_data(format!("帧长 {len} 超过接收上限 {limit}")));
        }

        // 5. 读满。中途 EOF 一律报错，绝不返回半条。
        self.buf.reserve(len);
        while self.buf.len() < len {
            if self.inner.read_buf(&mut self.buf).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("帧体未读完连接就关闭（{}/{len} 字节）", self.buf.len()),
                ));
            }
        }

        // 6. split_to 而不是 split：前者取走头部、把尾巴留在缓冲里，
        //    后者方向相反，第二帧就永远读不到。
        Ok(self.buf.split_to(len).freeze())
    }

    fn recv_limit(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.limit)
    }
}

/// 把底层写半包成消息出口。
#[derive(Debug)]
pub struct FramedSink<S> {
    inner: S,
    limit: Arc<AtomicU32>,
}

impl<S> FramedSink<S> {
    /// 装配一个写半，发送上限从 [`DEFAULT_MAX_MESSAGE_LEN`] 开始。
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            limit: Arc::new(AtomicU32::new(DEFAULT_MAX_MESSAGE_LEN)),
        }
    }

    /// 取回底层流。
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncWrite + Unpin + Send> MessageSink for FramedSink<S> {
    async fn send(&mut self, msg: Bytes) -> io::Result<()> {
        // 上限必须在写第一个字节之前查：这里失败连接仍然可用，
        // 而写到一半再发现就只能在流里留下半帧。
        let limit = self.limit.load(Ordering::Relaxed) as usize;
        if msg.len() > limit {
            return Err(invalid_input(format!(
                "{MSG_EXCEEDS_PEER_LIMIT}: {} > {limit}",
                msg.len()
            )));
        }

        // 上限是 u32，`msg.len()` 已被它夹住，这里的紧缩转换不会溢出。
        self.inner
            .write_all(&(msg.len() as u32).to_le_bytes())
            .await?;
        self.inner.write_all(&msg).await?;
        Ok(())
    }

    fn send_limit(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.limit)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use tokio::io::duplex;
    use tokio::time::timeout;

    use super::*;

    /// 拼一条完整帧：4 字节小端前缀 + 消息体。
    fn frame(body: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(LEN_PREFIX + body.len());
        v.extend_from_slice(&(body.len() as u32).to_le_bytes());
        v.extend_from_slice(body);
        v
    }

    /// 只写前缀、不带消息体。
    fn prefix_only(len: u32) -> Vec<u8> {
        len.to_le_bytes().to_vec()
    }

    /// 记下所有写入字节的假写半，用来断言前缀的形状。
    #[derive(Default)]
    struct RecordingSink {
        written: Vec<u8>,
    }

    impl AsyncWrite for RecordingSink {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn single_frame_roundtrip() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        let body: Vec<u8> = (0..24).collect();
        peer.write_all(&frame(&body)).await.expect("写帧");

        let got = src.recv().await.expect("读帧");
        assert_eq!(&got[..], &body[..], "消息体应当逐字节一致");
        assert_eq!(src.buffered(), 0, "读完一条消息后缓冲不应残留");
    }

    #[tokio::test]
    async fn partial_frame_is_not_returned_early() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        let body = vec![0x11u8; 32];
        let full = frame(&body);
        let split = LEN_PREFIX + 10;
        peer.write_all(&full[..split]).await.expect("写半帧");

        // 凑不满整条时必须挂起，而不是把半条交出去。
        //
        // 注意这里故意不复用这个 `src`：取消 `recv` 时前缀可能已被消费，
        // 下次调用会把帧体当成新前缀。这也是「只允许终态取消」的原因。
        assert!(
            timeout(Duration::from_millis(20), src.recv())
                .await
                .is_err(),
            "只到一半的帧不能被返回"
        );
    }

    #[tokio::test]
    async fn frame_completes_after_remainder_arrives() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        let body = vec![0x11u8; 32];
        let full = frame(&body);
        let split = LEN_PREFIX + 10;
        peer.write_all(&full[..split]).await.expect("写半帧");

        // 剩余字节延迟到达，让 recv 真的进入「等读满」的循环。
        let rest = full[split..].to_vec();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            peer.write_all(&rest).await.expect("写剩余");
        });

        let got = timeout(Duration::from_secs(1), src.recv())
            .await
            .expect("补写后应当完成")
            .expect("读帧");
        assert_eq!(&got[..], &body[..]);
        assert_eq!(src.buffered(), 0);
        writer.await.expect("写任务结束");
    }

    #[tokio::test]
    async fn eof_before_prefix_is_unexpected_eof() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        peer.write_all(&[0u8, 0, 0]).await.expect("写 3 字节");
        drop(peer);

        let err = src.recv().await.expect_err("前缀不足应当报错");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn eof_mid_body_is_unexpected_eof() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        let body = vec![0x22u8; 64];
        peer.write_all(&frame(&body)[..LEN_PREFIX + 10])
            .await
            .expect("写半帧");
        drop(peer);

        let err = src.recv().await.expect_err("半条消息不能返回");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn rejects_frame_shorter_than_min() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        peer.write_all(&prefix_only(8)).await.expect("写前缀");

        // 这里必须包超时：如果长度检查缺失，recv 会进入「等读满」而不是返回错误，
        // 直接 await 会让测试挂住，看不见问题。
        let err = timeout(Duration::from_millis(200), src.recv())
            .await
            .expect("长度检查缺失：recv 不应进入等待读满")
            .expect_err("过短的帧应当被拒");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn rejects_frame_over_limit() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        peer.write_all(&prefix_only(DEFAULT_MAX_MESSAGE_LEN + 1))
            .await
            .expect("写前缀");

        let err = timeout(Duration::from_millis(200), src.recv())
            .await
            .expect("上限检查缺失：recv 不应试图分配并读满这个长度的缓冲")
            .expect_err("超上限应当被拒");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // 此时前缀已被消费，缓冲里不应还留着它。
        assert_eq!(src.buffered(), 0);
    }

    #[tokio::test]
    async fn hundred_frames_in_order() {
        let (mut peer, me) = duplex(64);
        let mut src = FramedSource::new(me);

        // 一写一读，保证每次读之前流里只有一帧：顺序与「无残留」才能同时断言。
        for i in 0..100u8 {
            let mut body = vec![0xA5u8; 24];
            body[0] = i;
            peer.write_all(&frame(&body)).await.expect("写帧");

            let got = src
                .recv()
                .await
                .unwrap_or_else(|e| panic!("第 {i} 帧失败: {e}"));
            assert_eq!(&got[..], &body[..], "第 {i} 帧内容不符");
            assert_eq!(src.buffered(), 0, "第 {i} 帧读完缓冲应当为空");
        }
    }

    #[tokio::test]
    async fn buffered_frames_are_split_one_by_one() {
        let (mut peer, me) = duplex(256);
        let mut src = FramedSource::new(me);

        // 一次写三帧，读一侧会预读进同一个缓冲，这是特性不是问题；
        // 关键是每条消息被单独切出，且最后一帧读完不留残余。
        let bodies = [vec![0x01u8; 24], vec![0x02u8; 24], vec![0x03u8; 24]];
        for b in &bodies {
            peer.write_all(&frame(b)).await.expect("写帧");
        }

        for (i, expected) in bodies.iter().enumerate() {
            let got = src.recv().await.expect("读帧");
            assert_eq!(&got[..], &expected[..], "第 {i} 帧内容不符");
        }
        assert_eq!(src.buffered(), 0, "三帧读完后缓冲应当为空");
    }

    #[tokio::test]
    async fn send_writes_prefix_then_body() {
        let mut sink = FramedSink::new(RecordingSink::default());
        let body = Bytes::from_static(&[0xEEu8; 24]);

        sink.send(body.clone()).await.expect("写消息");

        let inner = sink.into_inner();
        let mut expected = Vec::with_capacity(LEN_PREFIX + 24);
        expected.extend_from_slice(&(24u32).to_le_bytes());
        expected.extend_from_slice(&body);
        assert_eq!(
            inner.written, expected,
            "输出应当是 4 字节小端前缀 + 消息体"
        );
    }

    #[tokio::test]
    async fn send_over_limit_keeps_connection_usable() {
        let mut sink = FramedSink::new(RecordingSink::default());

        let oversize = Bytes::from(vec![0u8; DEFAULT_MAX_MESSAGE_LEN as usize + 1]);
        let err = sink.send(oversize).await.expect_err("超上限应当本地拒发");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().starts_with(MSG_EXCEEDS_PEER_LIMIT),
            "错误文本是上层识别「我方拒发」的唯一依据，实际：{err}"
        );

        // 拒发不写任何字节，连接继续可用。
        sink.send(Bytes::from_static(&[1u8; 24]))
            .await
            .expect("小消息应当成功");
        let inner = sink.into_inner();
        assert_eq!(
            inner.written.len(),
            LEN_PREFIX + 24,
            "过大的消息不应写进流里"
        );
    }

    #[tokio::test]
    async fn limit_knob_affects_both_directions() {
        const BIG: usize = 500 * 1024;

        // 读侧：把旋钮提到 1 MiB 才能收 500 KiB，调回默认后必须拒。
        let (mut peer, me) = duplex(BIG + 1024);
        let mut src = FramedSource::new(me);
        let knob = src.recv_limit();

        knob.store(1024 * 1024, Ordering::Relaxed);
        let body = vec![0x33u8; BIG];
        peer.write_all(&frame(&body)).await.expect("写大帧");
        let got = src.recv().await.expect("放宽后应当收得下");
        assert_eq!(got.len(), BIG);

        knob.store(DEFAULT_MAX_MESSAGE_LEN, Ordering::Relaxed);
        peer.write_all(&frame(&body)).await.expect("写大帧");
        let err = src.recv().await.expect_err("调回默认后应当拒收");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // 写侧同理。
        let mut sink = FramedSink::new(RecordingSink::default());
        let send_knob = sink.send_limit();
        send_knob.store(1024 * 1024, Ordering::Relaxed);
        sink.send(Bytes::from(body.clone()))
            .await
            .expect("放宽后应当发得出去");
        send_knob.store(DEFAULT_MAX_MESSAGE_LEN, Ordering::Relaxed);
        let err = sink
            .send(Bytes::from(body))
            .await
            .expect_err("调回默认后应当拒发");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
