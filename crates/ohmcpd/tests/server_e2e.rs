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
