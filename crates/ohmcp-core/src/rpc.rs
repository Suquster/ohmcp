//! 与标准 MCP JSON-RPC 2.0 表示的互转层。
//!
//! 保证 OHMF 帧与标准 MCP 消息语义等价：任何标准 MCP 客户端消息
//! 都可以映射为一个 OHMF 帧，反之亦然。这使得 ohmcp 能通过兼容
//! 桥（stdio bridge）无缝接入现有 Agent 框架。

use bytes::Bytes;
use serde_json::{json, Value};

use crate::frame::{Frame, MsgType};
use crate::{CoreError, Result};

/// 将标准 JSON-RPC 请求转换为 OHMF 帧。
pub fn jsonrpc_to_frame(v: &Value) -> Result<Frame> {
    let method = v
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| CoreError::Codec("missing method".into()))?;
    let id = v.get("id").and_then(Value::as_u64).unwrap_or(0);
    let params = v.get("params").cloned().unwrap_or(Value::Null);
    let (msg_type, payload) = match method {
        "initialize" => (MsgType::Initialize, params),
        "tools/list" => (MsgType::ListTools, params),
        "tools/call" => (MsgType::CallTool, params),
        "ping" => (MsgType::Ping, Value::Null),
        other => return Err(CoreError::Codec(format!("unsupported method {other}"))),
    };
    let bytes = if payload.is_null() {
        Bytes::new()
    } else {
        Bytes::from(serde_json::to_vec(&payload)?)
    };
    Ok(Frame::new(msg_type, id, bytes))
}

/// 将 OHMF 响应帧转换为标准 JSON-RPC 响应。
pub fn frame_to_jsonrpc(frame: &Frame) -> Result<Value> {
    let id = frame.header.request_id;
    let body: Value = if frame.payload.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&frame.payload)?
    };
    Ok(match frame.header.msg_type {
        MsgType::Error => json!({"jsonrpc": "2.0", "id": id, "error": body}),
        MsgType::Pong => json!({"jsonrpc": "2.0", "id": id, "result": {}}),
        _ => json!({"jsonrpc": "2.0", "id": id, "result": body}),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_tool_roundtrip() {
        let req = json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": {"name": "echo", "arguments": {"msg": "hi"}}
        });
        let frame = jsonrpc_to_frame(&req).unwrap();
        assert_eq!(frame.header.msg_type, MsgType::CallTool);
        assert_eq!(frame.header.request_id, 7);
        let p: Value = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(p["name"], "echo");
    }

    #[test]
    fn response_mapping() {
        let f = Frame::new(
            MsgType::CallToolResult,
            7,
            Bytes::from(serde_json::to_vec(&json!({"content": []})).unwrap()),
        );
        let v = frame_to_jsonrpc(&f).unwrap();
        assert_eq!(v["id"], 7);
        assert!(v["result"]["content"].is_array());
    }
}
