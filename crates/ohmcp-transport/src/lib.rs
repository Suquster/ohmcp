//! ohmcp-transport: Unix Domain Socket 帧化传输。
//!
//! 特点：
//! - 单连接全双工多路复用：请求以 request_id 关联响应，客户端可
//!   流水线式并发发出任意数量请求，无需队头等待；
//! - 写路径批量聚合（encode 到同一 BytesMut 后单次 write），减少
//!   syscall 次数；
//! - 读路径增量解码，零额外拷贝地切分 payload（bytes::BytesMut）。

pub mod shm;
pub mod softbus;

use bytes::BytesMut;
use ohmcp_core::{CoreError, Frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const DEFAULT_SOCKET_PATH: &str = "/tmp/ohmcpd.sock";
const READ_BUF_SIZE: usize = 256 * 1024;

/// 帧化读端：包装任意 AsyncRead。
pub struct FrameReader<R> {
    inner: R,
    buf: BytesMut,
    bytes_read: u64,
}

impl<R: AsyncReadExt + Unpin> FrameReader<R> {
    pub fn new(inner: R) -> FrameReader<R> {
        FrameReader {
            inner,
            buf: BytesMut::with_capacity(READ_BUF_SIZE),
            bytes_read: 0,
        }
    }

    /// 累计从底层读入的字节数（线上字节）。
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// 仅从已缓冲字节解码下一帧，不触发 socket 读。
    /// 用于批量处理：缓冲中还有整帧时可先处理完再冲刷写端。
    pub fn next_buffered(&mut self) -> Result<Option<Frame>, CoreError> {
        Frame::decode(&mut self.buf)
    }

    /// 读取下一帧；连接关闭时返回 Ok(None)。
    pub async fn next_frame(&mut self) -> Result<Option<Frame>, CoreError> {
        loop {
            if let Some(frame) = Frame::decode(&mut self.buf)? {
                return Ok(Some(frame));
            }
            let n = self
                .inner
                .read_buf(&mut self.buf)
                .await
                .map_err(|e| CoreError::Codec(e.to_string()))?;
            self.bytes_read += n as u64;
            if n == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(CoreError::Incomplete);
            }
        }
    }
}

/// 帧化写端：包装任意 AsyncWrite，支持批量帧写出。
pub struct FrameWriter<W> {
    inner: W,
    buf: BytesMut,
    bytes_written: u64,
}

impl<W: AsyncWriteExt + Unpin> FrameWriter<W> {
    pub fn new(inner: W) -> FrameWriter<W> {
        FrameWriter {
            inner,
            buf: BytesMut::with_capacity(READ_BUF_SIZE),
            bytes_written: 0,
        }
    }

    /// 累计写出的字节数（线上字节）。
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// 将帧写入内部缓冲（不触发 syscall）。
    pub fn queue(&mut self, frame: &Frame) {
        frame.encode(&mut self.buf);
    }

    /// 单次 write 冲刷所有排队帧。
    pub async fn flush(&mut self) -> Result<(), CoreError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.inner
            .write_all(&self.buf)
            .await
            .map_err(|e| CoreError::Codec(e.to_string()))?;
        self.bytes_written += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
    }

    /// queue + flush 的便捷组合。
    pub async fn send(&mut self, frame: &Frame) -> Result<(), CoreError> {
        self.queue(frame);
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ohmcp_core::MsgType;

    #[tokio::test]
    async fn duplex_roundtrip() {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (ar, _aw) = tokio::io::split(a);
        let (_br, bw) = tokio::io::split(b);
        let mut w = FrameWriter::new(bw);
        let mut r = FrameReader::new(ar);
        for i in 0..100u64 {
            w.queue(&Frame::new(MsgType::Ping, i, Bytes::new()));
        }
        w.flush().await.unwrap();
        for i in 0..100u64 {
            let f = r.next_frame().await.unwrap().unwrap();
            assert_eq!(f.header.request_id, i);
        }
    }
}
