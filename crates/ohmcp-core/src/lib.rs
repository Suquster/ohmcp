//! ohmcp-core: OpenHarmony 原生 MCP 协议栈核心层。
//!
//! 定义 OHMF (OpenHarmony MCP Frame) 二进制帧格式与 MCP 消息模型。
//! 与标准 MCP (JSON-RPC 2.0) 语义完全兼容，可无损互转，
//! 但在系统内部传输时使用紧凑二进制帧，避免逐消息 JSON 解析开销。

pub mod frame;
pub mod message;
pub mod rpc;

pub use frame::{Frame, FrameFlags, FrameHeader, MsgType, FRAME_MAGIC, PROTOCOL_VERSION};
pub use message::*;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid frame magic")]
    BadMagic,
    #[error("unsupported protocol version {0}")]
    BadVersion(u8),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(u32),
    #[error("incomplete frame")]
    Incomplete,
    #[error("codec error: {0}")]
    Codec(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, CoreError>;
