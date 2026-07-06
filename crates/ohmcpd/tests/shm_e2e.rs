//! 端到端集成测试：共享内存大 payload 通道在多路复用下的正确性与回退语义。
//!
//! 覆盖：
//! - connect_shm 协商成功、shm_enabled 为真；
//! - 单连接并发发起多个大结果调用（kb.dump ~64KB），全部内容正确
//!   （验证 SPSC 环按写入顺序消费、SHM_REF 解析无错位）；
//! - 大小混合负载交错调用，SHM 路径与帧内路径共存无干扰；
//! - 常规 connect（未协商 SHM）功能等价，结果一致。

use std::sync::Arc;

use ohmcp_client::OhmcpClient;
use serde_json::json;

fn sock_path(tag: &str) -> String {
    format!("/tmp/ohmcp-shm-e2e-{}-{}.sock", std::process::id(), tag)
}

async fn spawn_server(sock: &str, token: Option<Vec<u8>>) {
    let sock = sock.to_string();
    let _ = std::fs::remove_file(&sock);
    tokio::spawn(async move {
        ohmcpd::server::run(&sock, token, ohmcpd::tools::builtin_registry()).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
}

fn text_of(r: &ohmcp_core::CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|b| match b {
            ohmcp_core::ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn shm_concurrent_large_payloads_are_correct() {
    let sock = sock_path("concurrent");
    let token = b"e2e-token".to_vec();
    spawn_server(&sock, Some(token.clone())).await;

    let client = OhmcpClient::connect_shm(&sock, "shm-agent", Some(&token))
        .await
        .expect("connect_shm");
    assert!(client.shm_enabled(), "shm channel must be negotiated");

    // 单连接并发发起 32 个大结果调用，全部经共享内存通道返回。
    let mut handles = Vec::new();
    for i in 0..32u32 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let r = c
                .call_tool("kb.dump", json!({ "doc_id": format!("doc-{i}") }))
                .await
                .expect("call_tool");
            let t = text_of(&r);
            // 内容必须对应各自 doc_id，且长度约 64KB。
            assert!(t.contains(&format!("[doc-{i}#0]")), "wrong doc for {i}");
            assert!(t.len() >= 64 * 1024, "payload too small: {}", t.len());
            i
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
}

#[tokio::test]
async fn shm_mixed_sizes_and_fallback_coexist() {
    let sock = sock_path("mixed");
    spawn_server(&sock, None).await;

    let client = OhmcpClient::connect_shm(&sock, "mix-agent", None)
        .await
        .expect("connect_shm");
    assert!(client.shm_enabled());

    // 交错：小负载（帧内）与大负载（SHM），验证两条路径互不干扰。
    for i in 0..20u32 {
        let small = client
            .call_tool("echo", json!({ "msg": format!("m-{i}") }))
            .await
            .unwrap();
        assert_eq!(text_of(&small), format!("m-{i}"));

        let big = client
            .call_tool("kb.blob", json!({ "size_kb": 256, "seed": i }))
            .await
            .unwrap();
        let t = text_of(&big);
        assert!(t.starts_with(&format!("[blob#{i}:0]")));
        // 截断至字符边界，长度为 256KiB 上取整到下一个字符边界。
        assert!(
            (256 * 1024..256 * 1024 + 4).contains(&t.len()),
            "len {}",
            t.len()
        );
    }
}

#[tokio::test]
async fn plain_connect_matches_shm_result() {
    let sock = sock_path("parity");
    spawn_server(&sock, None).await;

    let plain = OhmcpClient::connect(&sock, "plain", None).await.unwrap();
    let shm = OhmcpClient::connect_shm(&sock, "shm", None).await.unwrap();
    assert!(!plain.shm_enabled());
    assert!(shm.shm_enabled());

    let args = json!({ "doc_id": "parity" });
    let a = plain.call_tool("kb.dump", args.clone()).await.unwrap();
    let b = shm.call_tool("kb.dump", args).await.unwrap();
    assert_eq!(text_of(&a), text_of(&b), "SHM and frame paths must agree");
}

// 让 Arc<OhmcpClient> 在并发任务间传递（connect_shm 返回 Arc）。
fn _assert_send_sync<T: Send + Sync>() {}
#[allow(dead_code)]
fn _bounds() {
    _assert_send_sync::<Arc<OhmcpClient>>();
}
