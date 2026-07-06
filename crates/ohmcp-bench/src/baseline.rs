//! 基线实现：标准 MCP 风格的 JSON-RPC 2.0 + 换行分隔文本传输（UDS）。
//!
//! 对齐官方 MCP SDK（Python/TypeScript）的 stdio/socket 传输语义：
//! 每条消息为一行 JSON，逐消息完整解析；无压缩、无结果缓存、无二进制帧。
//! 工具执行逻辑与 ohmcpd 完全相同（复用同一 ToolRegistry），
//! 保证对比只体现协议栈差异。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use ohmcpd::tools::ToolRegistry;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// 启动基线 JSON-RPC 服务端。
pub async fn run_server(socket_path: &str, registry: Arc<ToolRegistry>) -> Result<()> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    loop {
        let (stream, _) = listener.accept().await?;
        let registry = registry.clone();
        tokio::spawn(async move {
            let _ = handle(stream, registry).await;
        });
    }
}

async fn handle(stream: UnixStream, registry: Arc<ToolRegistry>) -> Result<()> {
    let (rh, mut wh) = stream.into_split();
    let mut lines = BufReader::new(rh).lines();
    while let Some(line) = lines.next_line().await? {
        let req: Value = serde_json::from_str(&line)?;
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "initialize" => json!({
                "protocolVersion": "2025-06-18",
                "serverInfo": {"name": "baseline", "version": "0.1.0"},
                "capabilities": {"tools": {}}
            }),
            "tools/list" => json!({"tools": registry.tools()}),
            "tools/call" => {
                let params = req.get("params").cloned().unwrap_or(Value::Null);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let args = params.get("arguments").cloned().unwrap_or(Value::Null);
                match registry.call(name, &args) {
                    Some(r) => serde_json::to_value(r)?,
                    None => {
                        let resp = json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"unknown tool"}});
                        wh.write_all(format!("{resp}\n").as_bytes()).await?;
                        continue;
                    }
                }
            }
            "ping" => json!({}),
            _ => json!({}),
        };
        let resp = json!({"jsonrpc": "2.0", "id": id, "result": result});
        wh.write_all(format!("{resp}\n").as_bytes()).await?;
    }
    Ok(())
}

/// 基线客户端：换行分隔 JSON-RPC，后台任务按 id 分发响应
/// （对齐官方 SDK 的并发请求能力）。统计线上字节数。
pub struct BaselineClient {
    write: tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>,
    pending: Arc<std::sync::Mutex<std::collections::HashMap<u64, tokio::sync::oneshot::Sender<Value>>>>,
    next_id: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_recv: Arc<AtomicU64>,
}

impl BaselineClient {
    pub async fn connect(socket_path: &str) -> Result<BaselineClient> {
        let stream = UnixStream::connect(socket_path).await?;
        let (rh, wh) = stream.into_split();
        let pending: Arc<std::sync::Mutex<std::collections::HashMap<u64, tokio::sync::oneshot::Sender<Value>>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let bytes_recv = Arc::new(AtomicU64::new(0));
        {
            let pending = pending.clone();
            let bytes_recv = bytes_recv.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(rh).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    bytes_recv.fetch_add(line.len() as u64 + 1, Ordering::Relaxed);
                    let Ok(resp) = serde_json::from_str::<Value>(&line) else { break };
                    let id = resp.get("id").and_then(Value::as_u64).unwrap_or(0);
                    let tx = pending.lock().unwrap().remove(&id);
                    if let Some(tx) = tx {
                        let _ = tx.send(resp);
                    }
                }
            });
        }
        let c = BaselineClient {
            write: tokio::sync::Mutex::new(wh),
            pending,
            next_id: AtomicU64::new(1),
            bytes_sent: AtomicU64::new(0),
            bytes_recv,
        };
        c.request("initialize", json!({"protocolVersion":"2025-06-18","clientInfo":{"name":"bench","version":"0"},"capabilities":{}})).await?;
        Ok(c)
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let line = format!("{req}\n");
        self.bytes_sent.fetch_add(line.len() as u64, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        {
            let mut w = self.write.lock().await;
            w.write_all(line.as_bytes()).await?;
        }
        let resp = rx.await.map_err(|_| anyhow!("closed"))?;
        if let Some(err) = resp.get("error") {
            if !err.is_null() {
                return Err(anyhow!("server error: {err}"));
            }
        }
        Ok(resp["result"].clone())
    }

    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<Value> {
        self.request("tools/call", json!({"name": name, "arguments": arguments}))
            .await
    }
}
