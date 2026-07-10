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
use ohmcp_security::{derive_session_key, verify_response, ToolAcl};
use ohmcp_transport::shm::{
    encode_shm_ref, send_fd_blocking, send_fd_decline, ShmRing, DEFAULT_SHM_CAP,
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

/// 走共享内存通道的最小 payload（小于此值帧内传输更划算）。
const SHM_THRESHOLD: usize = 16 * 1024;

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
    // 会话级共享内存大 payload 通道（客户端显式协商后启用）。
    let mut shm: Option<Arc<ShmRing>> = None;

    loop {
        let frame = tokio::select! {
            r = reader.next_frame() => match r.map_err(|e| anyhow::anyhow!("{e}"))? {
                Some(f) => f,
                None => break,
            },
            _ = shutdown_rx.changed() => {
                info!(agent = %agent_id, "closing session for shutdown");
                break;
            }
        };
        let id = frame.header.request_id;
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
                let result = AuthResult {
                    ok,
                    session_key_b64: None,
                    reason: if ok {
                        None
                    } else {
                        Some("invalid token".into())
                    },
                };
                let payload = Bytes::from(serde_json::to_vec(&result)?);
                writer
                    .lock()
                    .await
                    .send(&Frame::new(MsgType::AuthResult, id, payload))
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                if ok {
                    authenticated = true;
                    agent_id = params.agent_id.clone();
                    let key = derive_session_key(
                        shared.token.as_ref().unwrap(),
                        params.nonce.as_ref().unwrap().as_bytes(),
                    );
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
                writer
                    .lock()
                    .await
                    .send(&f)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            MsgType::ShmSetup => {
                // 顺序：先经辅助数据传 fd（或拒绝字节），再发结果帧，
                // 保证客户端帧读缓冲不会吞掉携带 SCM_RIGHTS 的字节。
                let ring = ShmRing::create(DEFAULT_SHM_CAP).ok().map(Arc::new);
                let ring_fd = ring.as_ref().map(|r| r.raw_fd());
                let sock_dup = unsafe { libc::dup(raw_fd) };
                if sock_dup < 0 {
                    anyhow::bail!("dup failed during shm setup");
                }
                let sent = tokio::task::spawn_blocking(move || {
                    let r = match ring_fd {
                        Some(fd) => send_fd_blocking(sock_dup, fd),
                        None => send_fd_decline(sock_dup),
                    };
                    unsafe { libc::close(sock_dup) };
                    r
                })
                .await?;
                let ok = sent.is_ok() && ring.is_some();
                let result = json!({"ok": ok, "size": DEFAULT_SHM_CAP});
                let (f, _) = pipeline.wrap(
                    MsgType::ShmSetupResult,
                    id,
                    Bytes::from(serde_json::to_vec(&result)?),
                );
                writer
                    .lock()
                    .await
                    .send(&f)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                if ok {
                    shm = ring;
                    info!(agent = %agent_id, cap = DEFAULT_SHM_CAP, "shm channel enabled");
                }
            }
            MsgType::Initialize => {
                let result = InitializeResult {
                    protocol_version: "2025-06-18".to_string(),
                    server_info: Implementation {
                        name: "ohmcpd".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                    },
                    capabilities: json!({"tools": {}}),
                };
                let (f, _) = pipeline.wrap(
                    MsgType::InitializeResult,
                    id,
                    Bytes::from(serde_json::to_vec(&result)?),
                );
                writer
                    .lock()
                    .await
                    .send(&f)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
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
                writer
                    .lock()
                    .await
                    .send(&f)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            MsgType::Ping => {
                let (f, _) = pipeline.wrap(MsgType::Pong, id, Bytes::from_static(b"{}"));
                writer
                    .lock()
                    .await
                    .send(&f)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            MsgType::CallTool => {
                let body = pipeline
                    .unwrap(&frame)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let params: CallToolParams = serde_json::from_slice(&body)?;
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
                    writer
                        .lock()
                        .await
                        .send(&f)
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    continue;
                }
                let canonical = serde_json::to_vec(&params.arguments)?;
                let key = CacheKey::compute(&params.name, &canonical);
                let cacheable = shared.registry.is_cacheable(&params.name);

                if cacheable {
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
                        writer
                            .lock()
                            .await
                            .send(&f)
                            .await
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                        continue;
                    }
                }

                match shared.registry.call(&params.name, &params.arguments) {
                    Some(result) => {
                        let result_bytes = Bytes::from(serde_json::to_vec(&result)?);
                        if cacheable {
                            shared.cache.lock().unwrap().put(key, result_bytes.clone());
                            delivered.insert(key);
                        }
                        let mut f = wrap_maybe_shm(&pipeline, &shm, id, result_bytes);
                        if cacheable {
                            f.header.flags.0 |= FrameFlags::CACHEABLE;
                        }
                        writer
                            .lock()
                            .await
                            .send(&f)
                            .await
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
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
                        writer
                            .lock()
                            .await
                            .send(&f)
                            .await
                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                    }
                }
            }
            other => {
                warn!("unexpected msg type {other:?}");
            }
        }
    }
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

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() & 1 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
