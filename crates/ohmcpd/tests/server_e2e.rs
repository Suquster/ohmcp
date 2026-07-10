//! 端到端集成测试：服务端协议错误路径与缓存语义。
//!
//! 覆盖：
//! - 错误令牌认证被拒绝（握手即失败，会话关闭）；
//! - 未认证直连开启认证的服务端被 -32001 拒绝；
//! - 未知工具返回服务端错误（-32601）；
//! - 可缓存工具重复调用命中客户端缓存（CACHE_REF 语义）；
//! - list_tools / ping 基本协议路径。

use ohmcp_client::OhmcpClient;
use serde_json::json;

fn sock_path(tag: &str) -> String {
    format!("/tmp/ohmcp-srv-e2e-{}-{}.sock", std::process::id(), tag)
}

async fn spawn_server(sock: &str, token: Option<Vec<u8>>) {
    let sock = sock.to_string();
    let _ = std::fs::remove_file(&sock);
    tokio::spawn(async move {
        ohmcpd::server::run(&sock, token, ohmcpd::tools::builtin_registry()).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
}

#[tokio::test]
async fn wrong_token_is_rejected() {
    let sock = sock_path("badtoken");
    spawn_server(&sock, Some(b"correct-token".to_vec())).await;

    let r = OhmcpClient::connect(&sock, "evil-agent", Some(b"wrong-token")).await;
    assert!(r.is_err(), "wrong token must fail the handshake");
}

#[tokio::test]
async fn unauthenticated_requests_are_denied() {
    let sock = sock_path("noauth");
    spawn_server(&sock, Some(b"secret".to_vec())).await;

    // 不带令牌连接开启认证的服务端：initialize 应收到 -32001 错误。
    let r = OhmcpClient::connect(&sock, "anon-agent", None).await;
    let err = format!("{:#}", r.err().expect("must be denied"));
    assert!(
        err.contains("-32001") || err.contains("authentication"),
        "{err}"
    );
}

#[tokio::test]
async fn unknown_tool_returns_server_error() {
    let sock = sock_path("unknown");
    spawn_server(&sock, None).await;

    let c = OhmcpClient::connect(&sock, "agent", None).await.unwrap();
    let err = match c.call_tool("no.such.tool", json!({})).await {
        Ok(_) => panic!("unknown tool must error"),
        Err(e) => e,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("-32601") || msg.contains("server error"),
        "{msg}"
    );

    // 会话必须仍然可用（错误不拆连接）。
    c.ping().await.expect("session survives tool error");
}

#[tokio::test]
async fn cacheable_repeat_hits_client_cache() {
    let sock = sock_path("cache");
    spawn_server(&sock, None).await;

    let c = OhmcpClient::connect(&sock, "agent", None).await.unwrap();
    let args = json!({"query": "软总线", "top_k": 3});
    let first = c.call_tool("kb.search", args.clone()).await.unwrap();
    let second = c.call_tool("kb.search", args).await.unwrap();
    assert_eq!(
        serde_json::to_string(&first).unwrap(),
        serde_json::to_string(&second).unwrap()
    );
    // 客户端仅在收到 CACHE_REF 时查本地缓存：第二次调用必须命中。
    let (hits, _) = c.cache_stats();
    assert!(hits >= 1, "second call must hit local cache (hits={hits})");

    // 非幂等工具不得进入缓存。
    let _ = c
        .call_tool("math.sum", json!({"values": [1, 2]}))
        .await
        .unwrap();
    let _ = c
        .call_tool("math.sum", json!({"values": [1, 2]}))
        .await
        .unwrap();
    let (hits2, _) = c.cache_stats();
    assert_eq!(hits2, hits, "non-cacheable tool must not hit cache");
}

#[tokio::test]
async fn progress_notifications_delivered_before_result() {
    let sock = sock_path("progress");
    spawn_server(&sock, None).await;

    let c = OhmcpClient::connect(&sock, "agent", None).await.unwrap();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let r = c
        .call_tool_with_progress("echo", json!({"msg": "hi"}), move |p| {
            seen2.lock().unwrap().push(p);
        })
        .await
        .unwrap();
    assert!(!r.is_error);
    let seen = seen.lock().unwrap();
    assert!(
        !seen.is_empty(),
        "must receive at least one progress update"
    );
    assert_eq!(seen[0].progress, 0);
    assert_eq!(seen[0].total, Some(1));
}

#[tokio::test]
async fn cancelled_request_is_skipped_and_session_survives() {
    use bytes::Bytes;
    use ohmcp_core::{Frame, MsgType};
    use ohmcp_transport::{FrameReader, FrameWriter};

    let sock = sock_path("cancel");
    spawn_server(&sock, None).await;

    // 原始帧客户端：先发 id=7 的取消通知，再发同 id 的 CallTool，
    // 服务端应跳过执行并回 -32800；随后 id=8 正常执行，会话存活。
    let stream = tokio::net::UnixStream::connect(&sock).await.unwrap();
    let (rh, wh) = stream.into_split();
    let mut r = FrameReader::new(rh);
    let mut w = FrameWriter::new(wh);

    let cancel = serde_json::to_vec(&json!({"requestId": 7})).unwrap();
    w.send(&Frame::new(MsgType::Cancel, 7, Bytes::from(cancel)))
        .await
        .unwrap();
    let call = serde_json::to_vec(&json!({"name": "echo", "arguments": {"msg": "x"}})).unwrap();
    w.send(&Frame::new(MsgType::CallTool, 7, Bytes::from(call.clone())))
        .await
        .unwrap();
    let resp = r.next_frame().await.unwrap().unwrap();
    assert_eq!(resp.header.request_id, 7);
    assert_eq!(resp.header.msg_type, MsgType::Error);
    assert!(String::from_utf8_lossy(&resp.payload).contains("-32800"));

    w.send(&Frame::new(MsgType::CallTool, 8, Bytes::from(call)))
        .await
        .unwrap();
    let resp = r.next_frame().await.unwrap().unwrap();
    assert_eq!(resp.header.request_id, 8);
    assert_eq!(resp.header.msg_type, MsgType::CallToolResult);
}

#[tokio::test]
async fn resources_and_prompts_roundtrip() {
    let sock = sock_path("resprompt");
    spawn_server(&sock, None).await;

    let c = OhmcpClient::connect(&sock, "agent", None).await.unwrap();

    let rs = c.list_resources().await.unwrap();
    let uris: Vec<_> = rs.resources.iter().map(|r| r.uri.as_str()).collect();
    assert!(uris.contains(&"ohmcp://docs/protocol"), "{uris:?}");

    let rd = c.read_resource("ohmcp://docs/protocol").await.unwrap();
    assert_eq!(rd.contents.len(), 1);
    assert!(rd.contents[0].text.as_deref().unwrap().contains("ohmcp"));

    // 不存在的资源必须返回错误且会话存活。
    assert!(c.read_resource("ohmcp://no/such").await.is_err());
    c.ping().await.unwrap();

    let ps = c.list_prompts().await.unwrap();
    assert!(ps.prompts.iter().any(|p| p.name == "summarize"));

    let g = c
        .get_prompt("summarize", json!({"text": "分布式软总线"}))
        .await
        .unwrap();
    assert_eq!(g.messages.len(), 1);
    match &g.messages[0].content {
        ohmcp_core::ContentBlock::Text { text } => assert!(text.contains("分布式软总线")),
        other => panic!("unexpected content: {other:?}"),
    }

    // 未知提示模板返回错误且会话存活。
    assert!(c.get_prompt("no.such", json!({})).await.is_err());
    c.ping().await.unwrap();
}

#[tokio::test]
async fn resource_subscription_delivers_updates() {
    let sock = sock_path("subscribe");
    let _ = std::fs::remove_file(&sock);
    let registry = ohmcpd::tools::builtin_registry();
    let updater = registry.updater();
    {
        let sock = sock.clone();
        tokio::spawn(async move { ohmcpd::server::run(&sock, None, registry).await });
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let c = OhmcpClient::connect(&sock, "agent", None).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    c.on_resource_updated(move |uri| {
        let _ = tx.send(uri);
    });

    // 订阅不存在的资源必须返回错误且会话存活。
    assert!(c.subscribe_resource("ohmcp://no/such").await.is_err());
    c.ping().await.unwrap();

    c.subscribe_resource("ohmcp://docs/protocol").await.unwrap();
    assert!(updater.update("ohmcp://docs/protocol", "更新后的协议文档 v2"));

    // 通知在会话空闲等待期推送；发一个 ping 驱动客户端读循环。
    let uri = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Ok(u) = rx.try_recv() {
                break u;
            }
            c.ping().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("subscribed update must be delivered");
    assert_eq!(uri, "ohmcp://docs/protocol");

    // 读取到的内容为更新后的文本。
    let rd = c.read_resource("ohmcp://docs/protocol").await.unwrap();
    assert_eq!(rd.contents[0].text.as_deref(), Some("更新后的协议文档 v2"));

    // 退订后更新不再推送。
    c.unsubscribe_resource("ohmcp://docs/protocol")
        .await
        .unwrap();
    while rx.try_recv().is_ok() {}
    assert!(updater.update("ohmcp://docs/protocol", "v3"));
    for _ in 0..10 {
        c.ping().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        rx.try_recv().is_err(),
        "no notification after unsubscribing"
    );
}

#[tokio::test]
async fn forward_secret_session_encrypts_and_works() {
    let sock = sock_path("fs");
    spawn_server(&sock, Some(b"fs-token".to_vec())).await;

    // 认证握手交换 X25519 临时公钥；若双方派生密钥不一致，
    // 后续加密帧将解密失败，调用不可能成功。
    let c = OhmcpClient::connect(&sock, "agent", Some(b"fs-token"))
        .await
        .unwrap();
    let r = c.call_tool("echo", json!({"msg": "pfs"})).await.unwrap();
    assert!(serde_json::to_string(&r).unwrap().contains("pfs"));
    c.ping().await.unwrap();
}

#[tokio::test]
async fn connection_limit_rejects_excess_clients() {
    let sock = sock_path("connlimit");
    let _ = std::fs::remove_file(&sock);
    let cfg = ohmcpd::server::ServerConfig {
        max_connections: 2,
        ..Default::default()
    };
    let s = sock.clone();
    tokio::spawn(async move {
        ohmcpd::server::run_with(
            &s,
            None,
            ohmcpd::tools::builtin_registry(),
            cfg,
            std::future::pending::<()>(),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let c1 = OhmcpClient::connect(&sock, "a1", None).await.unwrap();
    let c2 = OhmcpClient::connect(&sock, "a2", None).await.unwrap();
    // 第三个连接超出上限：握手必须失败（收到 -32005 busy 或连接被关闭）。
    let r3 = OhmcpClient::connect(&sock, "a3", None).await;
    assert!(r3.is_err(), "third connection must be rejected");
    // 既有会话不受影响。
    c1.ping().await.unwrap();
    c2.ping().await.unwrap();
    // 释放一个槽位后可再次接入。
    drop(c1);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let c4 = OhmcpClient::connect(&sock, "a4", None).await.unwrap();
    c4.ping().await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown_drains_and_removes_socket() {
    let sock = sock_path("shutdown");
    let _ = std::fs::remove_file(&sock);
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let s = sock.clone();
    let server = tokio::spawn(async move {
        ohmcpd::server::run_with(
            &s,
            None,
            ohmcpd::tools::builtin_registry(),
            ohmcpd::server::ServerConfig::default(),
            async {
                let _ = rx.await;
            },
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let c = OhmcpClient::connect(&sock, "agent", None).await.unwrap();
    c.ping().await.unwrap();

    tx.send(()).unwrap();
    let r = tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .expect("server must stop within grace period")
        .unwrap();
    assert!(r.is_ok(), "graceful shutdown returns Ok: {r:?}");
    assert!(
        !std::path::Path::new(&sock).exists(),
        "socket file must be removed on shutdown"
    );
    // 停机后无法再建立新连接。
    assert!(OhmcpClient::connect(&sock, "late", None).await.is_err());
}

#[tokio::test]
async fn list_tools_and_ping() {
    let sock = sock_path("list");
    spawn_server(&sock, None).await;

    let c = OhmcpClient::connect(&sock, "agent", None).await.unwrap();
    let tools = c.list_tools().await.unwrap();
    let names: Vec<_> = tools.tools.iter().map(|t| t.name.as_str()).collect();
    for expected in [
        "echo",
        "kb.search",
        "kb.dump",
        "kb.blob",
        "device.status",
        "math.sum",
    ] {
        assert!(names.contains(&expected), "missing tool {expected}");
    }
    c.ping().await.unwrap();
}
