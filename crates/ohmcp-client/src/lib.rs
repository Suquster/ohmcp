//! ohmcp-client: Agent 侧客户端库。
//!
//! 提供与 ohmcpd 的多路复用连接，自动处理认证、加解密、压缩与
//! 客户端结果缓存。API 与标准 MCP 客户端语义一致。

pub mod pipeline;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use ohmcp_cache::{CacheKey, ResultCache};
use ohmcp_core::{
    AuthParams, AuthResult, CallToolResult, Frame, InitializeParams,
    InitializeResult, Implementation, ListToolsResult, MsgType,
};
use ohmcp_security::{derive_session_key, hmac_response};
use ohmcp_transport::{FrameReader, FrameWriter};
use rand::RngCore;
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::UnixStream;
use tokio::sync::{oneshot, Mutex};

use pipeline::PayloadPipeline;

/// 多路复用 MCP 客户端连接。
///
/// 响应分发采用“机会主义内联读”（connection combiner）：
/// 同一时刻由持有读锁的请求方直接从 socket 读帧并分发，
/// 顺序调用时零任务切换开销，并发调用时自动多路复用。
pub struct OhmcpClient {
    writer: Mutex<FrameWriter<OwnedWriteHalf>>,
    reader: Mutex<FrameReader<tokio::net::unix::OwnedReadHalf>>,
    pending: std::sync::Mutex<HashMap<u64, oneshot::Sender<Frame>>>,
    next_id: AtomicU64,
    pipeline: PayloadPipeline,
}

impl OhmcpClient {
    /// 连接并完成初始化 + 可选认证握手。
    pub async fn connect(
        socket_path: &str,
        agent_id: &str,
        token: Option<&[u8]>,
    ) -> Result<Arc<OhmcpClient>> {
        let stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("connect {socket_path}"))?;
        let (rh, wh) = stream.into_split();
        let mut reader = FrameReader::new(rh);
        let mut writer = FrameWriter::new(wh);

        let mut pipeline = PayloadPipeline::new();

        // 认证握手（同步阶段，未开启后台分发前进行）。
        if let Some(token) = token {
            let mut nonce = [0u8; 16];
            rand::thread_rng().fill_bytes(&mut nonce);
            let nonce_hex = hex(&nonce);
            let response = hmac_response(token, nonce_hex.as_bytes());
            let params = AuthParams {
                agent_id: agent_id.to_string(),
                token: hex(&response),
                nonce: Some(nonce_hex.clone()),
            };
            let frame = Frame::new(MsgType::Auth, 0, Bytes::from(serde_json::to_vec(&params)?));
            writer.send(&frame).await.map_err(|e| anyhow!("{e}"))?;
            let resp = reader
                .next_frame()
                .await
                .map_err(|e| anyhow!("{e}"))?
                .ok_or_else(|| anyhow!("connection closed during auth"))?;
            let result: AuthResult = serde_json::from_slice(&resp.payload)?;
            if !result.ok {
                return Err(anyhow!("auth failed: {:?}", result.reason));
            }
            let key = derive_session_key(token, nonce_hex.as_bytes());
            pipeline.enable_encryption(key);
        }

        let client = Arc::new(OhmcpClient {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            pending: std::sync::Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            pipeline,
        });

        // initialize 握手。
        let init = InitializeParams {
            protocol_version: "2025-06-18".to_string(),
            client_info: Implementation {
                name: agent_id.to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            capabilities: serde_json::json!({}),
        };
        let payload = Bytes::from(serde_json::to_vec(&init)?);
        let resp = client.request(MsgType::Initialize, payload).await?;
        let _: InitializeResult = serde_json::from_slice(&resp)?;
        Ok(client)
    }

    async fn request(&self, msg_type: MsgType, payload: Bytes) -> Result<Bytes> {
        self.request_with_cache(msg_type, payload, None).await
    }

    async fn request_with_cache(
        &self,
        msg_type: MsgType,
        payload: Bytes,
        cache_key: Option<CacheKey>,
    ) -> Result<Bytes> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (frame, _) = self.pipeline.wrap(msg_type, id, payload);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        {
            let mut w = self.writer.lock().await;
            w.send(&frame).await.map_err(|e| anyhow!("{e}"))?;
        }
        let resp = tokio::time::timeout(Duration::from_secs(30), self.recv_response(id, rx))
            .await
            .context("request timeout")??;
        let body = self.pipeline.unwrap(&resp).map_err(|e| anyhow!("{e}"))?;
        if resp.header.msg_type == MsgType::Error {
            return Err(anyhow!("server error: {}", String::from_utf8_lossy(&body)));
        }
        if let Some(key) = cache_key {
            // 仅收录服务端标记为可缓存的完整结果，避免不可缓存工具污染本地缓存。
            if resp.header.flags.cacheable() && !resp.header.flags.cache_ref() {
                self.pipeline.remember(key, body.clone());
            }
        }
        Ok(body)
    }

    pub async fn list_tools(&self) -> Result<ListToolsResult> {
        let body = self.request(MsgType::ListTools, Bytes::new()).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn call_tool(&self, name: &str, arguments: serde_json::Value) -> Result<CallToolResult> {
        // 参数仅序列化一次：既作缓存键规范形式，又直接拼接进 payload。
        let canonical = serde_json::to_vec(&arguments)?;
        let key = CacheKey::compute(name, &canonical);
        let mut payload = Vec::with_capacity(canonical.len() + name.len() + 24);
        payload.extend_from_slice(b"{\"name\":");
        serde_json::to_writer(&mut payload, name)?;
        payload.extend_from_slice(b",\"arguments\":");
        payload.extend_from_slice(&canonical);
        payload.push(b'}');
        let body = self
            .request_with_cache(MsgType::CallTool, Bytes::from(payload), Some(key))
            .await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn ping(&self) -> Result<()> {
        self.request(MsgType::Ping, Bytes::new()).await?;
        Ok(())
    }

    /// 客户端缓存统计（命中数, 未命中数）。
    pub fn cache_stats(&self) -> (u64, u64) {
        self.pipeline.cache_stats()
    }

    /// 线上字节总数（发送 + 接收）。
    pub async fn wire_bytes(&self) -> u64 {
        self.writer.lock().await.bytes_written() + self.reader.lock().await.bytes_read()
    }
}

impl OhmcpClient {
    /// 等待本请求响应：若无其他持锁者则内联读 socket 并代为分发。
    async fn recv_response(&self, id: u64, mut rx: oneshot::Receiver<Frame>) -> Result<Frame> {
        loop {
            tokio::select! {
                biased;
                r = &mut rx => return r.map_err(|_| anyhow!("connection dropped")),
                mut guard = self.reader.lock() => {
                    // 先检查是否已被前一个持锁者分发。
                    match rx.try_recv() {
                        Ok(frame) => return Ok(frame),
                        Err(oneshot::error::TryRecvError::Closed) => {
                            return Err(anyhow!("connection dropped"))
                        }
                        Err(oneshot::error::TryRecvError::Empty) => {}
                    }
                    // 持锁内联读，直到读到自己的响应。
                    loop {
                        let frame = guard
                            .next_frame()
                            .await
                            .map_err(|e| anyhow!("{e}"))?
                            .ok_or_else(|| anyhow!("connection closed"))?;
                        if frame.header.request_id == id {
                            self.pending.lock().unwrap().remove(&id);
                            return Ok(frame);
                        }
                        let tx = self.pending.lock().unwrap().remove(&frame.header.request_id);
                        if let Some(tx) = tx {
                            let _ = tx.send(frame);
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

// 供 pipeline 使用的共享缓存类型别名。
pub type SharedCache = Arc<std::sync::Mutex<ResultCache>>;
