//! UDS 服务端：每连接一个异步任务，共享工具注册表与服务端结果缓存。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use ohmcp_cache::{CacheKey, ResultCache};
use ohmcp_client::pipeline::PayloadPipeline;
use ohmcp_core::{
    AuthParams, AuthResult, CallToolParams, ErrorBody, Frame, InitializeResult, Implementation,
    ListToolsResult, MsgType,
};
use ohmcp_security::{derive_session_key, verify_response, ToolAcl};
use ohmcp_transport::{FrameReader, FrameWriter};
use serde_json::json;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::tools::ToolRegistry;

pub struct Shared {
    pub registry: ToolRegistry,
    pub cache: std::sync::Mutex<ResultCache>,
    pub acl: std::sync::Mutex<ToolAcl>,
    pub token: Option<Vec<u8>>,
}

pub async fn run(socket_path: &str, token: Option<Vec<u8>>, registry: ToolRegistry) -> Result<()> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    info!(socket = socket_path, auth = token.is_some(), "ohmcpd listening");
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
    loop {
        let (stream, _) = listener.accept().await?;
        let shared = shared.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, shared).await {
                warn!("connection error: {e}");
            }
        });
    }
}

async fn handle_conn(stream: UnixStream, shared: Arc<Shared>) -> Result<()> {
    let (rh, wh) = stream.into_split();
    let mut reader = FrameReader::new(rh);
    let writer = Arc::new(Mutex::new(FrameWriter::new(wh)));
    let mut pipeline = PayloadPipeline::new();
    let mut authenticated = shared.token.is_none();
    let mut agent_id = String::from("anonymous");
    // 已向该客户端完整下发过的缓存键（可安全发送 CACHE_REF）。
    let mut delivered: HashSet<CacheKey> = HashSet::new();

    while let Some(frame) = reader.next_frame().await.map_err(|e| anyhow::anyhow!("{e}"))? {
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
                    reason: if ok { None } else { Some("invalid token".into()) },
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
                let (f, _) = pipeline.wrap(MsgType::Error, id, Bytes::from(serde_json::to_vec(&err)?));
                writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
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
                writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            MsgType::ListTools => {
                let result = ListToolsResult {
                    tools: shared.registry.tools().to_vec(),
                };
                let (f, _) = pipeline.wrap(MsgType::ListToolsResult, id, Bytes::from(serde_json::to_vec(&result)?));
                writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            MsgType::Ping => {
                let (f, _) = pipeline.wrap(MsgType::Pong, id, Bytes::from_static(b"{}"));
                writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            MsgType::CallTool => {
                let body = pipeline.unwrap(&frame).map_err(|e| anyhow::anyhow!("{e}"))?;
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
                    let (f, _) = pipeline.wrap(MsgType::Error, id, Bytes::from(serde_json::to_vec(&err)?));
                    writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
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
                            let (mut f, _) = pipeline.wrap(MsgType::CallToolResult, id, result_bytes);
                            f.header.flags.0 |= ohmcp_core::FrameFlags::CACHEABLE;
                            f
                        };
                        writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
                        continue;
                    }
                }

                match shared.registry.call(&params.name, &params.arguments) {
                    Some(result) => {
                        let result_bytes = Bytes::from(serde_json::to_vec(&result)?);
                        if cacheable {
                            shared
                                .cache
                                .lock()
                                .unwrap()
                                .put(key, result_bytes.clone());
                            delivered.insert(key);
                        }
                        let (mut f, _) = pipeline.wrap(MsgType::CallToolResult, id, result_bytes);
                        if cacheable {
                            f.header.flags.0 |= ohmcp_core::FrameFlags::CACHEABLE;
                        }
                        writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
                    }
                    None => {
                        let err = ErrorBody {
                            code: -32601,
                            message: format!("unknown tool: {}", params.name),
                            data: None,
                        };
                        let (f, _) = pipeline.wrap(MsgType::Error, id, Bytes::from(serde_json::to_vec(&err)?));
                        writer.lock().await.send(&f).await.map_err(|e| anyhow::anyhow!("{e}"))?;
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

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
