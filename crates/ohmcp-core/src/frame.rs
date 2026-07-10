//! OHMF (OpenHarmony MCP Frame) 二进制帧格式。
//!
//! 帧布局（小端序）：
//! ```text
//! +--------+---------+--------+---------+------------+------------+
//! | magic  | version | flags  | msgtype | request id | payload len|
//! | u16    | u8      | u8     | u8      | u64        | u32        |
//! +--------+---------+--------+---------+------------+------------+
//! | payload (payload len bytes)                                   |
//! +---------------------------------------------------------------+
//! ```
//! 头部固定 17 字节。相比 JSON-RPC 文本协议，帧头允许在不解析
//! payload 的情况下完成路由、并发分发与缓存查找。

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{CoreError, Result};

pub const FRAME_MAGIC: u16 = 0x4F4D; // "OM"
pub const PROTOCOL_VERSION: u8 = 1;
pub const HEADER_LEN: usize = 17;
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// 帧标志位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameFlags(pub u8);

impl FrameFlags {
    pub const NONE: FrameFlags = FrameFlags(0);
    /// payload 使用 LZ4 压缩。
    pub const COMPRESSED: u8 = 0b0000_0001;
    /// payload 使用 ChaCha20-Poly1305 加密。
    pub const ENCRYPTED: u8 = 0b0000_0010;
    /// payload 为缓存引用（内容哈希），而非完整数据。
    pub const CACHE_REF: u8 = 0b0000_0100;
    /// 结果可缓存（服务端提示，客户端据此收录本地缓存）。
    pub const CACHEABLE: u8 = 0b0000_1000;
    /// payload 为共享内存环形缓冲区引用（offset u64 + len u32），
    /// 实际数据位于通过 SCM_RIGHTS 传递的 memfd 中。
    pub const SHM_REF: u8 = 0b0001_0000;

    pub fn compressed(self) -> bool {
        self.0 & Self::COMPRESSED != 0
    }
    pub fn encrypted(self) -> bool {
        self.0 & Self::ENCRYPTED != 0
    }
    pub fn cache_ref(self) -> bool {
        self.0 & Self::CACHE_REF != 0
    }
    pub fn cacheable(self) -> bool {
        self.0 & Self::CACHEABLE != 0
    }
    pub fn shm_ref(self) -> bool {
        self.0 & Self::SHM_REF != 0
    }
}

/// 消息类型，对应 MCP 核心方法。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    Initialize = 0x01,
    InitializeResult = 0x02,
    ListTools = 0x03,
    ListToolsResult = 0x04,
    CallTool = 0x05,
    CallToolResult = 0x06,
    Ping = 0x07,
    Pong = 0x08,
    Auth = 0x09,
    AuthResult = 0x0A,
    ShmSetup = 0x0B,
    ShmSetupResult = 0x0C,
    ListResources = 0x0D,
    ListResourcesResult = 0x0E,
    ReadResource = 0x0F,
    ReadResourceResult = 0x10,
    ListPrompts = 0x11,
    ListPromptsResult = 0x12,
    GetPrompt = 0x13,
    GetPromptResult = 0x14,
    /// 取消通知（客户端 → 服务端，无响应）。
    Cancel = 0x15,
    /// 进度通知（服务端 → 客户端，无响应）。
    Progress = 0x16,
    SubscribeResource = 0x17,
    SubscribeResourceResult = 0x18,
    UnsubscribeResource = 0x19,
    /// 资源更新通知（服务端 → 已订阅客户端，无响应）。
    ResourceUpdated = 0x1A,
    /// 资源列表变更通知（服务端 → 客户端，无响应）。
    ResourceListChanged = 0x1B,
    Error = 0x7F,
}

impl MsgType {
    pub fn from_u8(v: u8) -> Result<MsgType> {
        Ok(match v {
            0x01 => MsgType::Initialize,
            0x02 => MsgType::InitializeResult,
            0x03 => MsgType::ListTools,
            0x04 => MsgType::ListToolsResult,
            0x05 => MsgType::CallTool,
            0x06 => MsgType::CallToolResult,
            0x07 => MsgType::Ping,
            0x08 => MsgType::Pong,
            0x09 => MsgType::Auth,
            0x0A => MsgType::AuthResult,
            0x0B => MsgType::ShmSetup,
            0x0C => MsgType::ShmSetupResult,
            0x0D => MsgType::ListResources,
            0x0E => MsgType::ListResourcesResult,
            0x0F => MsgType::ReadResource,
            0x10 => MsgType::ReadResourceResult,
            0x11 => MsgType::ListPrompts,
            0x12 => MsgType::ListPromptsResult,
            0x13 => MsgType::GetPrompt,
            0x14 => MsgType::GetPromptResult,
            0x15 => MsgType::Cancel,
            0x16 => MsgType::Progress,
            0x17 => MsgType::SubscribeResource,
            0x18 => MsgType::SubscribeResourceResult,
            0x19 => MsgType::UnsubscribeResource,
            0x1A => MsgType::ResourceUpdated,
            0x1B => MsgType::ResourceListChanged,
            0x7F => MsgType::Error,
            other => return Err(CoreError::Codec(format!("unknown msg type {other:#x}"))),
        })
    }
}

#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub flags: FrameFlags,
    pub msg_type: MsgType,
    pub request_id: u64,
    pub payload_len: u32,
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(msg_type: MsgType, request_id: u64, payload: Bytes) -> Frame {
        Frame {
            header: FrameHeader {
                flags: FrameFlags::NONE,
                msg_type,
                request_id,
                payload_len: payload.len() as u32,
            },
            payload,
        }
    }

    /// 编码到输出缓冲区。
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.reserve(HEADER_LEN + self.payload.len());
        buf.put_u16_le(FRAME_MAGIC);
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(self.header.flags.0);
        buf.put_u8(self.header.msg_type as u8);
        buf.put_u64_le(self.header.request_id);
        buf.put_u32_le(self.payload.len() as u32);
        buf.put_slice(&self.payload);
    }

    /// 尝试从缓冲区解码一个完整帧；数据不足时返回 `Ok(None)`。
    pub fn decode(buf: &mut BytesMut) -> Result<Option<Frame>> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let magic = u16::from_le_bytes([buf[0], buf[1]]);
        if magic != FRAME_MAGIC {
            return Err(CoreError::BadMagic);
        }
        let version = buf[2];
        if version != PROTOCOL_VERSION {
            return Err(CoreError::BadVersion(version));
        }
        let payload_len = u32::from_le_bytes([buf[13], buf[14], buf[15], buf[16]]);
        if payload_len > MAX_FRAME_LEN {
            return Err(CoreError::FrameTooLarge(payload_len));
        }
        let total = HEADER_LEN + payload_len as usize;
        if buf.len() < total {
            return Ok(None);
        }
        let flags = FrameFlags(buf[3]);
        let msg_type = MsgType::from_u8(buf[4])?;
        let request_id = u64::from_le_bytes([
            buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12],
        ]);
        buf.advance(HEADER_LEN);
        let payload = buf.split_to(payload_len as usize).freeze();
        Ok(Some(Frame {
            header: FrameHeader {
                flags,
                msg_type,
                request_id,
                payload_len,
            },
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let f = Frame::new(MsgType::CallTool, 42, Bytes::from_static(b"hello"));
        let mut buf = BytesMut::new();
        f.encode(&mut buf);
        let g = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(g.header.request_id, 42);
        assert_eq!(g.header.msg_type, MsgType::CallTool);
        assert_eq!(&g.payload[..], b"hello");
        assert!(buf.is_empty());
    }

    #[test]
    fn incomplete_returns_none() {
        let f = Frame::new(MsgType::Ping, 1, Bytes::from_static(b"abcdef"));
        let mut buf = BytesMut::new();
        f.encode(&mut buf);
        let mut partial = BytesMut::from(&buf[..buf.len() - 3]);
        assert!(Frame::decode(&mut partial).unwrap().is_none());
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = BytesMut::from(&[0u8; HEADER_LEN][..]);
        assert!(matches!(Frame::decode(&mut buf), Err(CoreError::BadMagic)));
    }

    #[test]
    fn version_mismatch_rejected() {
        let f = Frame::new(MsgType::Ping, 1, Bytes::new());
        let mut buf = BytesMut::new();
        f.encode(&mut buf);
        buf[2] = PROTOCOL_VERSION + 1;
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(CoreError::BadVersion(_))
        ));
    }

    #[test]
    fn oversized_frame_rejected() {
        let f = Frame::new(MsgType::Ping, 1, Bytes::new());
        let mut buf = BytesMut::new();
        f.encode(&mut buf);
        buf[13..17].copy_from_slice(&(MAX_FRAME_LEN + 1).to_le_bytes());
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(CoreError::FrameTooLarge(_))
        ));
    }

    #[test]
    fn unknown_msg_type_rejected() {
        let f = Frame::new(MsgType::Ping, 1, Bytes::new());
        let mut buf = BytesMut::new();
        f.encode(&mut buf);
        buf[4] = 0x66;
        assert!(matches!(Frame::decode(&mut buf), Err(CoreError::Codec(_))));
    }

    #[test]
    fn fuzz_random_bytes_never_panic() {
        // 任意字节流喂入解码器不得 panic，只能返回 Ok(None)/Err。
        let mut seed = 0x9e3779b97f4a7c15u64;
        for len in 0..256usize {
            let mut data = Vec::with_capacity(len);
            for _ in 0..len {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                data.push((seed >> 33) as u8);
            }
            let mut buf = BytesMut::from(&data[..]);
            let _ = Frame::decode(&mut buf);
        }
    }
}
