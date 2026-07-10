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
use ohmcp_security::{derive_session_key, hmac_response, EphemeralKeyPair};
use ohmcp_transport::shm::{
    decode_shm_ref, encode_shm_ref, recv_fd_blocking, ShmRing, MAX_SHM_CAP, SHM_THRESHOLD,
};
use ohmcp_transport::{FrameReader, FrameWriter};
use rand::RngCore;
use std::os::fd::AsRawFd;
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::UnixStream;
use tokio::sync::{oneshot, Mutex};

use pipeline::PayloadPipeline;

/// 进度通知回调。
type ProgressCallback = Box<dyn Fn(ohmcp_core::ProgressParams) + Send + Sync>;

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
    /// 共享内存大 payload 通道（connect_shm 协商成功后启用）：
    /// 下行环（服务端写 → 本端读）与上行环（本端写 → 服务端读）。
    shm: Option<ShmRing>,
    shm_up: Option<ShmRing>,
    /// 按请求 id 登记的进度回调（服务端 Progress 通知分发）。
    progress: std::sync::Mutex<HashMap<u64, ProgressCallback>>,
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
            let eph = EphemeralKeyPair::generate();
            let params = AuthParams {
                agent_id: agent_id.to_string(),
                token: hex(&response),
                nonce: Some(nonce_hex.clone()),
                eph_pub: Some(hex(&eph.public_bytes())),
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
            // 前向保密：服务端回传临时公钥时掺入 ECDH 共享秘密；
            // 旧版服务端未回传时回退到仅基于令牌的派生。
            let key = match result.eph_pub.as_deref().and_then(unhex32) {
                Some(server_pub) => {
                    eph.derive_fs_session_key(&server_pub, token, nonce_hex.as_bytes())
                }
                None => derive_session_key(token, nonce_hex.as_bytes()),
            };
            pipeline.enable_encryption(key);
        }

        // 共享内存通道协商（严格顺序阶段，帧读缓冲此时为空，
        // 先收 fd 载荷字节再读结果帧，避免 SCM_RIGHTS 被普通读丢弃）。
        let mut shm = None;
        let mut shm_up = None;
        if use_shm {
            writer
                .send(&Frame::new(MsgType::ShmSetup, 0, Bytes::new()))
                .await
                .map_err(|e| anyhow!("{e}"))?;
            // 服务端依次下发下行环与上行环两个 fd（拒绝时仅一个拒绝字节）。
            let down_fd = tokio::task::spawn_blocking(move || recv_fd_blocking(raw_fd)).await??;
            let up_fd = if down_fd.is_some() {
                tokio::task::spawn_blocking(move || recv_fd_blocking(raw_fd)).await??
            } else {
                None
            };
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
            let up_size = v
                .get("up_size")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0) as usize;
            if let (true, Some(fd)) = (ok && size > 0 && size <= MAX_SHM_CAP, down_fd) {
                shm = Some(ShmRing::from_fd(fd, size)?);
            }
            if let (true, Some(fd)) = (
                shm.is_some() && up_size > 0 && up_size <= MAX_SHM_CAP,
                up_fd,
            ) {
                shm_up = Some(ShmRing::from_fd(fd, up_size)?);
            }
        }

        let client = Arc::new(OhmcpClient {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            pending: std::sync::Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            pipeline,
            shm,
            shm_up,
            progress: std::sync::Mutex::new(HashMap::new()),
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
        self.request_with_id(msg_type, payload, cache_key, id).await
    }

    async fn request_with_id(
        &self,
        msg_type: MsgType,
        payload: Bytes,
        cache_key: Option<CacheKey>,
        id: u64,
    ) -> Result<Bytes> {
        // 超阈值请求优先走上行共享内存环（帧内仅 12 字节引用），
        // 空间不足时回退到常规压缩 + 加密帧。
        let frame = match &self.shm_up {
            Some(ring) if payload.len() >= SHM_THRESHOLD => match ring.try_write(&payload) {
                Some(offset) => {
                    let body =
                        Bytes::copy_from_slice(&encode_shm_ref(offset, payload.len() as u32));
                    let mut f = Frame::new(msg_type, id, body);
                    f.header.flags.0 |= ohmcp_core::FrameFlags::SHM_REF;
                    f
                }
                None => self.pipeline.wrap(msg_type, id, payload).0,
            },
            _ => self.pipeline.wrap(msg_type, id, payload).0,
        };
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        // 机会主义写聚合：存在其他在飞请求时先排队再让出一次调度，
        // 并发请求在此窗口内排入同一写缓冲，由首个到达冲刷点的任务
        // 单次 syscall 写出（缓冲已被他人冲刷时 flush 为空操作）；
        // 无并发时直接写出，不引入额外调度延迟。
        let contended = self.pending.lock().unwrap().len() > 1;
        if contended {
            {
                let mut w = self.writer.lock().await;
                w.queue(&frame);
            }
            tokio::task::yield_now().await;
            let mut w = self.writer.lock().await;
            w.flush().await.map_err(|e| anyhow!("{e}"))?;
        } else {
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
        // 大参数调用几乎不可能幂等复用，跳过缓存键哈希开销。
        let key = (canonical.len() < SHM_THRESHOLD).then(|| CacheKey::compute(name, &canonical));
        let mut payload = Vec::with_capacity(canonical.len() + name.len() + 24);
        payload.extend_from_slice(b"{\"name\":");
        serde_json::to_writer(&mut payload, name)?;
        payload.extend_from_slice(b",\"arguments\":");
        payload.extend_from_slice(&canonical);
        payload.push(b'}');
        let body = self
            .request_with_cache(MsgType::CallTool, Bytes::from(payload), key)
            .await?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// 带进度通知的工具调用：服务端执行期间的 Progress 通知
    /// 逐条回调 `on_progress`，完成后返回最终结果。
    pub async fn call_tool_with_progress(
        &self,
        name: &str,
        arguments: serde_json::Value,
        on_progress: impl Fn(ohmcp_core::ProgressParams) + Send + Sync + 'static,
    ) -> Result<CallToolResult> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let payload = serde_json::to_vec(&serde_json::json!({
            "name": name,
            "arguments": arguments,
            "_meta": {"progress": true},
        }))?;
        self.progress
            .lock()
            .unwrap()
            .insert(id, Box::new(on_progress));
        let r = self
            .request_with_id(MsgType::CallTool, Bytes::from(payload), None, id)
            .await;
        self.progress.lock().unwrap().remove(&id);
        Ok(serde_json::from_slice(&r?)?)
    }

    /// 发送取消通知（尽力而为；若服务端尚未处理到该请求则跳过执行
    /// 并回 -32800，已完成则忽略）。
    pub async fn cancel(&self, request_id: u64, reason: Option<&str>) -> Result<()> {
        let params = ohmcp_core::CancelParams {
            request_id,
            reason: reason.map(str::to_string),
        };
        let (frame, _) = self.pipeline.wrap(
            MsgType::Cancel,
            request_id,
            Bytes::from(serde_json::to_vec(&params)?),
        );
        self.writer
            .lock()
            .await
            .send(&frame)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        Ok(())
    }

    pub async fn list_resources(&self) -> Result<ohmcp_core::ListResourcesResult> {
        let body = self.request(MsgType::ListResources, Bytes::new()).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn read_resource(&self, uri: &str) -> Result<ohmcp_core::ReadResourceResult> {
        let params = serde_json::to_vec(&serde_json::json!({ "uri": uri }))?;
        let body = self
            .request(MsgType::ReadResource, Bytes::from(params))
            .await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn list_prompts(&self) -> Result<ohmcp_core::ListPromptsResult> {
        let body = self.request(MsgType::ListPrompts, Bytes::new()).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn get_prompt(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> Result<ohmcp_core::GetPromptResult> {
        let params = serde_json::to_vec(&serde_json::json!({
            "name": name,
            "arguments": arguments,
        }))?;
        let body = self
            .request(MsgType::GetPrompt, Bytes::from(params))
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
                        // 进度通知不是响应：分发给登记的回调后继续读。
                        if frame.header.msg_type == MsgType::Progress {
                            if let Ok(body) = self.pipeline.unwrap(&frame) {
                                if let Ok(p) =
                                    serde_json::from_slice::<ohmcp_core::ProgressParams>(&body)
                                {
                                    let cbs = self.progress.lock().unwrap();
                                    if let Some(cb) = cbs.get(&p.request_id) {
                                        cb(p);
                                    }
                                }
                            }
                            continue;
                        }
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

/// 解码 32 字节 hex 公钥；长度或字符非法时返回 None。
fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in out.iter_mut().enumerate() {
        *chunk = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

// 供 pipeline 使用的共享缓存类型别名。
pub type SharedCache = Arc<std::sync::Mutex<ResultCache>>;
