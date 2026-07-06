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
    AuthParams, AuthResult, CallToolResult, Frame, Implementation, InitializeParams,
    InitializeResult, ListToolsResult, MsgType,
};
use ohmcp_security::{derive_session_key, hmac_response};
use ohmcp_transport::shm::{decode_shm_ref, recv_fd_blocking, ShmRing, MAX_SHM_CAP};
use ohmcp_transport::{FrameReader, FrameWriter};
use rand::RngCore;
use std::os::fd::AsRawFd;
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
    /// 共享内存大 payload 通道（connect_shm 协商成功后启用）。
    shm: Option<ShmRing>,
}

impl OhmcpClient {
    /// 连接并完成初始化 + 可选认证握手。
    pub async fn connect(
        socket_path: &str,
        agent_id: &str,
        token: Option<&[u8]>,
    ) -> Result<Arc<OhmcpClient>> {
        Self::connect_inner(socket_path, agent_id, token, false).await
    }

    /// 同 connect，但额外协商同设备共享内存大 payload 通道：
    /// 服务端经 SCM_RIGHTS 下发 memfd，超阈值结果零套接字拷贝直达。
    pub async fn connect_shm(
        socket_path: &str,
        agent_id: &str,
        token: Option<&[u8]>,
    ) -> Result<Arc<OhmcpClient>> {
        Self::connect_inner(socket_path, agent_id, token, true).await
    }

    async fn connect_inner(
        socket_path: &str,
        agent_id: &str,
        token: Option<&[u8]>,
        use_shm: bool,
    ) -> Result<Arc<OhmcpClient>> {
        let stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("connect {socket_path}"))?;
        let raw_fd = stream.as_raw_fd();
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

        // 共享内存通道协商（严格顺序阶段，帧读缓冲此时为空，
        // 先收 fd 载荷字节再读结果帧，避免 SCM_RIGHTS 被普通读丢弃）。
        let mut shm = None;
        if use_shm {
            writer
                .send(&Frame::new(MsgType::ShmSetup, 0, Bytes::new()))
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let fd = tokio::task::spawn_blocking(move || recv_fd_blocking(raw_fd)).await??;
            let resp = reader
                .next_frame()
                .await
                .map_err(|e| anyhow!("{e}"))?
                .ok_or_else(|| anyhow!("connection closed during shm setup"))?;
            let body = pipeline.unwrap(&resp).map_err(|e| anyhow!("{e}"))?;
            let v: serde_json::Value = serde_json::from_slice(&body)?;
            let ok = v.get("ok").and_then(serde_json::Value::as_bool) == Some(true);
            let size = v
                .get("size")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0) as usize;
            if let (true, Some(fd)) = (ok && size > 0 && size <= MAX_SHM_CAP, fd) {
                shm = Some(ShmRing::from_fd(fd, size)?);
            }
        }

        let client = Arc::new(OhmcpClient {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            pending: std::sync::Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            pipeline,
            shm,
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

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> Result<CallToolResult> {
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

    /// 共享内存通道是否已启用。
    pub fn shm_enabled(&self) -> bool {
        self.shm.is_some()
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
        #[allow(clippy::never_loop)]
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
                    // SHM 引用必须在此处（帧到达顺序 = 写入顺序）立即
                    // 取出并释放，保证 SPSC 环 FIFO 消费语义。
                    loop {
                        let frame = guard
                            .next_frame()
                            .await
                            .map_err(|e| anyhow!("{e}"))?
                            .ok_or_else(|| anyhow!("connection closed"))?;
                        let frame = self.resolve_shm(frame)?;
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

impl OhmcpClient {
    /// 将 SHM_REF 帧的 12 字节引用替换为环中实际数据并释放空间。
    fn resolve_shm(&self, mut frame: Frame) -> Result<Frame> {
        if !frame.header.flags.shm_ref() {
            return Ok(frame);
        }
        let ring = self
            .shm
            .as_ref()
            .ok_or_else(|| anyhow!("SHM_REF frame but shm channel not negotiated"))?;
        let (offset, len) =
            decode_shm_ref(&frame.payload).ok_or_else(|| anyhow!("bad shm ref payload"))?;
        let data = ring
            .read_release(offset, len as usize)
            .ok_or_else(|| anyhow!("invalid shm ref (offset={offset}, len={len})"))?;
        frame.payload = Bytes::from(data);
        frame.header.payload_len = len;
        frame.header.flags.0 &= !ohmcp_core::FrameFlags::SHM_REF;
        Ok(frame)
    }
}

pub(crate) fn hex(data: &[u8]) -> String {
    use std::fmt::Write;
    data.iter()
        .fold(String::with_capacity(data.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

// 供 pipeline 使用的共享缓存类型别名。
pub type SharedCache = Arc<std::sync::Mutex<ResultCache>>;
