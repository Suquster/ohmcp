//! UDS 服务端：每连接一个异步任务，共享工具注册表与服务端结果缓存。

use std::collections::HashSet;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use ohmcp_cache::{CacheKey, ResultCache};
use ohmcp_client::pipeline::PayloadPipeline;
use ohmcp_core::FrameFlags;
use ohmcp_core::{
    AuthParams, AuthResult, CallToolParams, ErrorBody, Frame, Implementation, InitializeResult,
    ListToolsResult, MsgType,
};
use ohmcp_security::{derive_session_key, verify_response, EphemeralKeyPair, ToolAcl};
use ohmcp_transport::shm::{
    decode_shm_ref, encode_shm_ref, send_fd_blocking, send_fd_decline, ShmRing, DEFAULT_SHM_CAP,
    SHM_THRESHOLD,
};
use ohmcp_transport::{FrameReader, FrameWriter};
use serde_json::json;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::tools::ToolRegistry;

pub struct Shared {
    pub registry: ToolRegistry,
    pub cache: std::sync::Mutex<ResultCache>,
    pub acl: std::sync::Mutex<ToolAcl>,
    pub token: Option<Vec<u8>>,
}

/// 服务端运维配置：连接数上限与优雅停机宽限期。
#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// 并发连接数上限；超出后新连接收到 -32005 错误帧并被关闭。
    pub max_connections: usize,
    /// 收到停机信号后等待在飞连接收尾的宽限期。
    pub shutdown_grace: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 256,
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

pub async fn run(socket_path: &str, token: Option<Vec<u8>>, registry: ToolRegistry) -> Result<()> {
    run_with(
        socket_path,
        token,
        registry,
        ServerConfig::default(),
        std::future::pending::<()>(),
    )
    .await
}

/// 带运维配置与停机信号的服务主循环：`shutdown` 完成后停止接受
/// 新连接、通知在飞会话关闭，并在宽限期内等待其收尾。
pub async fn run_with(
    socket_path: &str,
    token: Option<Vec<u8>>,
    registry: ToolRegistry,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    info!(
        socket = socket_path,
        auth = token.is_some(),
        max_conns = config.max_connections,
        "ohmcpd listening"
    );
    let mut acl = ToolAcl::new();
    // 默认策略：匿名模式（未启用认证）授予全部工具；启用认证时，
    // agent 在认证通过后获得授权。细粒度按工具授权可经配置扩展。
    acl.grant_all("anonymous");
    let shared = Arc::new(Shared {
        registry,
        cache: std::sync::Mutex::new(ResultCache::new(65536, Duration::from_secs(300))),
        acl: std::sync::Mutex::new(acl),
        token,
    });
    let limiter = Arc::new(Semaphore::new(config.max_connections));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut conns: JoinSet<()> = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let permit = match limiter.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!(max = config.max_connections, "connection limit reached, rejecting");
                        tokio::spawn(reject_busy(stream));
                        continue;
                    }
                };
                let shared = shared.clone();
                let rx = shutdown_rx.clone();
                conns.spawn(async move {
                    if let Err(e) = handle_conn(stream, shared, rx, permit).await {
                        warn!("connection error: {e}");
                    }
                });
            }
        }
    }
    // 优雅停机：停止监听、通知在飞会话，宽限期内等待收尾。
    drop(listener);
    let _ = std::fs::remove_file(socket_path);
    let _ = shutdown_tx.send(true);
    let drained = tokio::time::timeout(config.shutdown_grace, async {
        while conns.join_next().await.is_some() {}
    })
    .await
    .is_ok();
    info!(drained, "ohmcpd shut down");
    Ok(())
}

/// 向超出连接上限的客户端发送 -32005 错误帧后关闭连接。
async fn reject_busy(stream: UnixStream) {
    let err = ErrorBody {
        code: -32005,
        message: "server busy: connection limit reached".into(),
        data: None,
    };
    if let Ok(body) = serde_json::to_vec(&err) {
        let mut w = FrameWriter::new(stream);
        let _ = w
            .send(&Frame::new(MsgType::Error, 0, Bytes::from(body)))
            .await;
    }
}

async fn handle_conn(
    stream: UnixStream,
    shared: Arc<Shared>,
    mut shutdown_rx: watch::Receiver<bool>,
    _permit: OwnedSemaphorePermit,
) -> Result<()> {
    let raw_fd = stream.as_raw_fd();
    let (rh, wh) = stream.into_split();
    let mut reader = FrameReader::new(rh);
    let writer = Arc::new(Mutex::new(FrameWriter::new(wh)));
    let mut pipeline = PayloadPipeline::new();
    let mut authenticated = shared.token.is_none();
    let mut agent_id = String::from("anonymous");
    // 已向该客户端完整下发过的缓存键（可安全发送 CACHE_REF）。
    let mut delivered: HashSet<CacheKey> = HashSet::new();
    // 会话级共享内存大 payload 通道（客户端显式协商后启用）：
    // 下行环（服务端写 → 客户端读）与上行环（客户端写 → 服务端读）。
    let mut shm: Option<Arc<ShmRing>> = None;
    let mut shm_up: Option<Arc<ShmRing>> = None;
    // 已收到取消通知但尚未处理到的请求 id（尽力而为取消语义）。
    let mut cancelled: HashSet<u64> = HashSet::new();
    // 本会话已订阅的资源 uri；资源更新事件命中时推送 ResourceUpdated。
    let mut subscribed: HashSet<String> = HashSet::new();
    let mut resource_events = shared.registry.subscribe_events();

    'session: loop {
        // 批量处理：读缓冲中还有整帧时只排队响应不冲刷；缓冲耗尽
        // 才单次 syscall 冲刷全部排队响应，再阻塞等待下一批。
        let frame = match reader.next_buffered().map_err(|e| anyhow::anyhow!("{e}"))? {
            Some(f) => f,
            None => {
                writer
                    .lock()
                    .await
                    .flush()
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                'wait: loop {
                    tokio::select! {
                        r = reader.next_frame() => match r.map_err(|e| anyhow::anyhow!("{e}"))? {
                            Some(f) => break 'wait f,
                            None => break 'session,
                        },
                        _ = shutdown_rx.changed() => {
                            info!(agent = %agent_id, "closing session for shutdown");
                            break 'session;
                        }
                        evt = resource_events.recv() => {
                            if let Ok(uri) = evt {
                                if subscribed.contains(&uri) {
                                    let params = ohmcp_core::ResourceUpdatedParams { uri };
                                    let (f, _) = pipeline.wrap(
                                        MsgType::ResourceUpdated,
                                        0,
                                        Bytes::from(serde_json::to_vec(&params)?),
                                    );
                                    let mut w = writer.lock().await;
                                    w.queue(&f);
                                    w.flush().await.map_err(|e| anyhow::anyhow!("{e}"))?;
                                }
                            }
                        }
                    }
                }
            }
        };
        let id = frame.header.request_id;
        // 上行 SHM 引用：将 12 字节引用替换为上行环中的实际数据。
        let frame = if frame.header.flags.shm_ref() {
            let ring = shm_up
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("SHM_REF request but uplink not negotiated"))?;
            let (offset, len) = decode_shm_ref(&frame.payload)
                .ok_or_else(|| anyhow::anyhow!("bad uplink shm ref payload"))?;
            let data = ring
                .read_release(offset, len as usize)
                .ok_or_else(|| anyhow::anyhow!("invalid uplink shm ref"))?;
            let mut f = frame;
            f.payload = Bytes::from(data);
            f.header.payload_len = len;
            f.header.flags.0 &= !FrameFlags::SHM_REF;
            f
        } else {
            frame
        };
        match frame.header.msg_type {
            MsgType::Auth => {
                let params: AuthParams = serde_json::from_slice(&frame.payload)?;
                let ok = match (&shared.token, &params.nonce) {
                    (Some(token), Some(nonce)) => {
                        let resp = unhex(&params.token).unwrap_or_default();
                        verify_response(token, nonce.as_bytes(), &resp)
                    }
                    _ => false,
                };
                // 前向保密：客户端携带临时公钥时生成服务端临时密钥对，
                // 会话密钥掺入 ECDH 共享秘密；无公钥时回退令牌派生。
                let client_pub = params.eph_pub.as_deref().and_then(unhex32);
                let eph = if ok && client_pub.is_some() {
                    Some(EphemeralKeyPair::generate())
                } else {
                    None
                };
                let result = AuthResult {
                    ok,
                    session_key_b64: None,
                    reason: if ok {
                        None
                    } else {
                        Some("invalid token".into())
                    },
                    eph_pub: eph.as_ref().map(|e| hex(&e.public_bytes())),
                };
                let payload = Bytes::from(serde_json::to_vec(&result)?);
                writer
                    .lock()
                    .await
                    .queue(&Frame::new(MsgType::AuthResult, id, payload));
                if ok {
                    authenticated = true;
                    agent_id = params.agent_id.clone();
                    let token = shared.token.as_ref().unwrap();
                    let nonce = params.nonce.as_ref().unwrap().as_bytes();
                    let key = match (eph, client_pub) {
                        (Some(eph), Some(peer)) => eph.derive_fs_session_key(&peer, token, nonce),
                        _ => derive_session_key(token, nonce),
                    };
                    pipeline.enable_encryption(key);
                    shared.acl.lock().unwrap().grant_all(&agent_id);
                    info!(agent = %agent_id, "agent authenticated");
                } else {
                    warn!(agent = %params.agent_id, "auth rejected");
                    return Ok(());
                }
            }
            _ if !authenticated => {
                let err = ErrorBody {
                    code: -32001,
                    message: "authentication required".into(),
                    data: None,
                };
                let (f, _) =
                    pipeline.wrap(MsgType::Error, id, Bytes::from(serde_json::to_vec(&err)?));
                writer.lock().await.queue(&f);
            }
            MsgType::ShmSetup => {
                // 顺序：先冲刷排队响应（fd 字节经原始 sendmsg 绕过写缓冲，
                // 必须保持字节流顺序），再经辅助数据传 fd（下行环 + 上行环，
                // 或拒绝字节），最后发结果帧。
                writer
                    .lock()
                    .await
                    .flush()
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let ring = ShmRing::create(DEFAULT_SHM_CAP).ok().map(Arc::new);
                let up_ring = ShmRing::create(DEFAULT_SHM_CAP).ok().map(Arc::new);
                let fds = ring
                    .as_ref()
                    .zip(up_ring.as_ref())
                    .map(|(d, u)| (d.raw_fd(), u.raw_fd()));
                let sock_dup = unsafe { libc::dup(raw_fd) };
                if sock_dup < 0 {
                    anyhow::bail!("dup failed during shm setup");
                }
                let sent = tokio::task::spawn_blocking(move || {
                    let r = match fds {
                        Some((down_fd, up_fd)) => send_fd_blocking(sock_dup, down_fd)
                            .and_then(|()| send_fd_blocking(sock_dup, up_fd)),
                        None => send_fd_decline(sock_dup),
                    };
                    unsafe { libc::close(sock_dup) };
                    r
                })
                .await?;
                let ok = sent.is_ok() && ring.is_some() && up_ring.is_some();
                let result = json!({"ok": ok, "size": DEFAULT_SHM_CAP, "up_size": DEFAULT_SHM_CAP});
                let (f, _) = pipeline.wrap(
                    MsgType::ShmSetupResult,
                    id,
                    Bytes::from(serde_json::to_vec(&result)?),
                );
                writer.lock().await.queue(&f);
                if ok {
                    shm = ring;
                    shm_up = up_ring;
                    info!(agent = %agent_id, cap = DEFAULT_SHM_CAP, "bidirectional shm channel enabled");
                }
            }
            MsgType::Initialize => {
                let result = InitializeResult {
                    protocol_version: "2025-06-18".to_string(),
                    server_info: Implementation {
                        name: "ohmcpd".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                    },
                    capabilities: json!({"tools": {}, "resources": {}, "prompts": {}}),
                };
                let (f, _) = pipeline.wrap(
                    MsgType::InitializeResult,
                    id,
                    Bytes::from(serde_json::to_vec(&result)?),
                );
                writer.lock().await.queue(&f);
            }
            MsgType::ListTools => {
                let result = ListToolsResult {
                    tools: shared.registry.tools().to_vec(),
                };
                let (f, _) = pipeline.wrap(
                    MsgType::ListToolsResult,
                    id,
                    Bytes::from(serde_json::to_vec(&result)?),
                );
                writer.lock().await.queue(&f);
            }
            MsgType::ListResources => {
                let result = ohmcp_core::ListResourcesResult {
                    resources: shared.registry.resources().to_vec(),
                };
                let (f, _) = pipeline.wrap(
                    MsgType::ListResourcesResult,
                    id,
                    Bytes::from(serde_json::to_vec(&result)?),
                );
                writer.lock().await.queue(&f);
            }
            MsgType::ReadResource => {
                let body = pipeline
                    .unwrap(&frame)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let params: ohmcp_core::ReadResourceParams = serde_json::from_slice(&body)?;
                let f = match shared.registry.read_resource(&params.uri) {
                    Some(contents) => {
                        let result = ohmcp_core::ReadResourceResult {
                            contents: vec![contents],
                        };
                        let (f, _) = pipeline.wrap(
                            MsgType::ReadResourceResult,
                            id,
                            Bytes::from(serde_json::to_vec(&result)?),
                        );
                        f
                    }
                    None => {
                        let err = ErrorBody {
                            code: -32002,
                            message: format!("resource not found: {}", params.uri),
                            data: None,
                        };
                        let (f, _) = pipeline.wrap(
                            MsgType::Error,
                            id,
                            Bytes::from(serde_json::to_vec(&err)?),
                        );
                        f
                    }
                };
                writer.lock().await.queue(&f);
            }
            MsgType::ListPrompts => {
                let result = ohmcp_core::ListPromptsResult {
                    prompts: shared.registry.prompts().to_vec(),
                };
                let (f, _) = pipeline.wrap(
                    MsgType::ListPromptsResult,
                    id,
                    Bytes::from(serde_json::to_vec(&result)?),
                );
                writer.lock().await.queue(&f);
            }
            MsgType::GetPrompt => {
                let body = pipeline
                    .unwrap(&frame)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let params: ohmcp_core::GetPromptParams = serde_json::from_slice(&body)?;
                let f = match shared.registry.get_prompt(&params.name, &params.arguments) {
                    Some(result) => {
                        let (f, _) = pipeline.wrap(
                            MsgType::GetPromptResult,
                            id,
                            Bytes::from(serde_json::to_vec(&result)?),
                        );
                        f
                    }
                    None => {
                        let err = ErrorBody {
                            code: -32602,
                            message: format!("unknown prompt: {}", params.name),
                            data: None,
                        };
                        let (f, _) = pipeline.wrap(
                            MsgType::Error,
                            id,
                            Bytes::from(serde_json::to_vec(&err)?),
                        );
                        f
                    }
                };
                writer.lock().await.queue(&f);
            }
            MsgType::Ping => {
                let (f, _) = pipeline.wrap(MsgType::Pong, id, Bytes::from_static(b"{}"));
                writer.lock().await.queue(&f);
            }
            MsgType::SubscribeResource | MsgType::UnsubscribeResource => {
                let is_sub = frame.header.msg_type == MsgType::SubscribeResource;
                let body = pipeline
                    .unwrap(&frame)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let params: ohmcp_core::SubscribeResourceParams = serde_json::from_slice(&body)?;
                let f = if is_sub && !shared.registry.has_resource(&params.uri) {
                    let err = ErrorBody {
                        code: -32002,
                        message: format!("resource not found: {}", params.uri),
                        data: None,
                    };
                    let (f, _) =
                        pipeline.wrap(MsgType::Error, id, Bytes::from(serde_json::to_vec(&err)?));
                    f
                } else {
                    if is_sub {
                        subscribed.insert(params.uri);
                    } else {
                        subscribed.remove(&params.uri);
                    }
                    let (f, _) = pipeline.wrap(
                        MsgType::SubscribeResourceResult,
                        id,
                        Bytes::from_static(b"{}"),
                    );
                    f
                };
                writer.lock().await.queue(&f);
            }
            MsgType::Cancel => {
                // 取消为通知，无响应；登记后在处理到该请求时跳过执行。
                let body = pipeline
                    .unwrap(&frame)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                if let Ok(params) = serde_json::from_slice::<ohmcp_core::CancelParams>(&body) {
                    cancelled.insert(params.request_id);
                }
            }
            MsgType::CallTool => {
                if cancelled.remove(&id) {
                    let err = ErrorBody {
                        code: -32800,
                        message: "request cancelled".into(),
                        data: None,
                    };
                    let (f, _) =
                        pipeline.wrap(MsgType::Error, id, Bytes::from(serde_json::to_vec(&err)?));
                    writer.lock().await.queue(&f);
                    continue;
                }
                let body = pipeline
                    .unwrap(&frame)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let params: CallToolParams = serde_json::from_slice(&body)?;
                let wants_progress = params
                    .meta
                    .as_ref()
                    .and_then(|m| m.get("progress"))
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                if wants_progress {
                    let p = ohmcp_core::ProgressParams {
                        request_id: id,
                        progress: 0,
                        total: Some(1),
                        message: Some(format!("executing {}", params.name)),
                    };
                    let (f, _) =
                        pipeline.wrap(MsgType::Progress, id, Bytes::from(serde_json::to_vec(&p)?));
                    writer.lock().await.queue(&f);
                }
                if shared
                    .acl
                    .lock()
                    .unwrap()
                    .check(&agent_id, &params.name)
                    .is_err()
                {
                    let err = ErrorBody {
                        code: -32002,
                        message: format!("access denied: {}", params.name),
                        data: None,
                    };
                    let (f, _) =
                        pipeline.wrap(MsgType::Error, id, Bytes::from(serde_json::to_vec(&err)?));
                    writer.lock().await.queue(&f);
                    continue;
                }
                let cacheable = shared.registry.is_cacheable(&params.name);
                // 缓存键（规范化参数的 SHA-256）仅对可缓存工具计算，
                // 非幂等大参数调用不付哈希开销。
                let key = if cacheable {
                    Some(CacheKey::compute(
                        &params.name,
                        &serde_json::to_vec(&params.arguments)?,
                    ))
                } else {
                    None
                };

                if let Some(key) = key {
                    let cached = shared.cache.lock().unwrap().get(&key);
                    if let Some(result_bytes) = cached {
                        let f = if delivered.contains(&key) {
                            // 客户端已有完整副本：只发 32 字节引用。
                            pipeline.wrap_cache_ref(MsgType::CallToolResult, id, &key)
                        } else {
                            delivered.insert(key);
                            let mut f = wrap_maybe_shm(&pipeline, &shm, id, result_bytes);
                            f.header.flags.0 |= FrameFlags::CACHEABLE;
                            f
                        };
                        writer.lock().await.queue(&f);
                        continue;
                    }
                }

                match shared.registry.call(&params.name, &params.arguments) {
                    Some(result) => {
                        let result_bytes = Bytes::from(serde_json::to_vec(&result)?);
                        if let Some(key) = key {
                            shared.cache.lock().unwrap().put(key, result_bytes.clone());
                            delivered.insert(key);
                        }
                        let mut f = wrap_maybe_shm(&pipeline, &shm, id, result_bytes);
                        if key.is_some() {
                            f.header.flags.0 |= FrameFlags::CACHEABLE;
                        }
                        writer.lock().await.queue(&f);
                    }
                    None => {
                        let err = ErrorBody {
                            code: -32601,
                            message: format!("unknown tool: {}", params.name),
                            data: None,
                        };
                        let (f, _) = pipeline.wrap(
                            MsgType::Error,
                            id,
                            Bytes::from(serde_json::to_vec(&err)?),
                        );
                        writer.lock().await.queue(&f);
                    }
                }
            }
            other => {
                warn!("unexpected msg type {other:?}");
            }
        }
    }
    // 会话结束（对端关闭或停机）前冲刷仍在排队的响应。
    let _ = writer.lock().await.flush().await;
    Ok(())
}

/// 大 payload 优先走共享内存通道（帧内仅 12 字节引用），
/// 空间不足或未启用时回退到常规压缩 + 加密帧。
fn wrap_maybe_shm(
    pipeline: &PayloadPipeline,
    shm: &Option<Arc<ShmRing>>,
    id: u64,
    payload: Bytes,
) -> Frame {
    if let Some(ring) = shm {
        if payload.len() >= SHM_THRESHOLD {
            if let Some(offset) = ring.try_write(&payload) {
                let body = Bytes::copy_from_slice(&encode_shm_ref(offset, payload.len() as u32));
                let mut f = Frame::new(MsgType::CallToolResult, id, body);
                f.header.flags.0 |= FrameFlags::SHM_REF;
                return f;
            }
        }
    }
    let (f, _) = pipeline.wrap(MsgType::CallToolResult, id, payload);
    f
}

fn hex(data: &[u8]) -> String {
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

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() & 1 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
