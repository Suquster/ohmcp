//! 软总线（DSoftBus）传输适配 PoC：证明协议栈可无改动移植到
//! OpenHarmony 分布式软总线 Session 之上。
//!
//! DSoftBus Session 是消息 / 字节导向 API（`SendBytes` / `OnBytesReceived`），
//! 与流式套接字不同。本模块提供：
//!
//! - [`SessionEndpoint`]：软总线 Session 的最小抽象（收发字节块），
//!   真实设备上由 `SendBytes` / `OnBytesReceived` 回调实现；
//! - [`SimSession`]：进程内模拟实现（保留消息边界），用于 PoC 与测试；
//! - [`SessionStream`]：把任意 [`SessionEndpoint`] 适配为
//!   `AsyncRead + AsyncWrite`，使 [`FrameReader`](crate::FrameReader) /
//!   [`FrameWriter`](crate::FrameWriter)、压缩、AEAD 加密等上层组件
//!   **零改动**运行在软总线之上——帧化协议自带长度前缀，天然容忍
//!   消息边界与流边界的差异。
//!
//! 跨设备时不启用共享内存通道（自动回退常规加密帧），与设计文档 §7.1 一致。

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

/// 软总线 Session 最小抽象：面向字节块的可靠有序双向通道。
pub trait SessionEndpoint: Send {
    /// 发送一个字节块（对应 DSoftBus `SendBytes`）。
    fn send_bytes(&mut self, data: &[u8]) -> io::Result<()>;
    /// 轮询接收下一个字节块（对应 `OnBytesReceived` 回调投递）。
    fn poll_recv_bytes(&mut self, cx: &mut Context<'_>) -> Poll<Option<Vec<u8>>>;
}

/// 进程内模拟 Session：mpsc 通道保留消息边界，模拟软总线可靠有序传输。
pub struct SimSession {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl SimSession {
    /// 创建一对互联的模拟 Session（设备 A 端 / 设备 B 端）。
    pub fn pair() -> (SimSession, SimSession) {
        let (ta, ra) = mpsc::unbounded_channel();
        let (tb, rb) = mpsc::unbounded_channel();
        (SimSession { tx: ta, rx: rb }, SimSession { tx: tb, rx: ra })
    }
}

impl SessionEndpoint for SimSession {
    fn send_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.tx
            .send(data.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "peer session closed"))
    }

    fn poll_recv_bytes(&mut self, cx: &mut Context<'_>) -> Poll<Option<Vec<u8>>> {
        self.rx.poll_recv(cx)
    }
}

/// 将 SessionEndpoint 适配为 AsyncRead + AsyncWrite：
/// 写侧每次 flush 作为一个 SendBytes 块发出；读侧缓存跨块残余字节。
pub struct SessionStream<S> {
    session: S,
    pending: Vec<u8>,
    pos: usize,
}

impl<S: SessionEndpoint> SessionStream<S> {
    pub fn new(session: S) -> SessionStream<S> {
        SessionStream {
            session,
            pending: Vec::new(),
            pos: 0,
        }
    }
}

impl<S: SessionEndpoint + Unpin> AsyncRead for SessionStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.pos >= this.pending.len() {
            match this.session.poll_recv_bytes(cx) {
                Poll::Ready(Some(chunk)) => {
                    this.pending = chunk;
                    this.pos = 0;
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF
                Poll::Pending => return Poll::Pending,
            }
        }
        let n = (this.pending.len() - this.pos).min(buf.remaining());
        buf.put_slice(&this.pending[this.pos..this.pos + n]);
        this.pos += n;
        Poll::Ready(Ok(()))
    }
}

impl<S: SessionEndpoint + Unpin> AsyncWrite for SessionStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.session.send_bytes(buf)?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FrameReader, FrameWriter};
    use bytes::Bytes;
    use ohmcp_core::{Frame, MsgType};

    #[tokio::test]
    async fn frames_over_simulated_softbus_session() {
        let (a, b) = SimSession::pair();
        let mut w = FrameWriter::new(SessionStream::new(a));
        let mut r = FrameReader::new(SessionStream::new(b));

        // 批量帧写出：单个 SendBytes 块内含多帧（帧自带长度前缀）。
        for i in 0..64u64 {
            w.queue(&Frame::new(MsgType::Ping, i, Bytes::from(vec![7u8; 100])));
        }
        w.flush().await.unwrap();
        for i in 0..64u64 {
            let f = r.next_frame().await.unwrap().unwrap();
            assert_eq!(f.header.request_id, i);
            assert_eq!(f.payload.len(), 100);
        }
    }

    #[tokio::test]
    async fn large_frame_spans_message_boundaries() {
        // 帧化协议必须容忍单帧跨越多个消息块（真实软总线 SendBytes 有块大小上限）。
        let (mut a, b) = SimSession::pair();
        let frame = Frame::new(MsgType::CallToolResult, 42, Bytes::from(vec![9u8; 300_000]));
        let mut encoded = bytes::BytesMut::new();
        frame.encode(&mut encoded);
        for chunk in encoded.chunks(4096) {
            a.send_bytes(chunk).unwrap();
        }
        drop(a);
        let mut r = FrameReader::new(SessionStream::new(b));
        let f = r.next_frame().await.unwrap().unwrap();
        assert_eq!(f.header.request_id, 42);
        assert_eq!(f.payload.len(), 300_000);
        assert!(r.next_frame().await.unwrap().is_none(), "clean EOF");
    }

    #[tokio::test]
    async fn closed_session_is_clean_eof() {
        let (a, b) = SimSession::pair();
        drop(a);
        let mut r = FrameReader::new(SessionStream::new(b));
        assert!(r.next_frame().await.unwrap().is_none());
    }
}
