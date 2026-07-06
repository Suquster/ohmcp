//! 端到端多 Agent 演示：单个 ohmcpd 守护进程 + 三个并发 Agent。
//!
//! 展示：认证握手（令牌不过网）、单守护进程多 Agent 复用、工具调用、
//! 内容寻址缓存命中（CACHE_REF）、加密信道、错误处理。
//!
//! 运行：`cargo run --release -p ohmcp-bench --bin demo`

use std::time::Instant;

use anyhow::Result;
use ohmcp_client::OhmcpClient;
use serde_json::json;

const SOCK: &str = "/tmp/ohmcp-demo.sock";
const TOKEN: &[u8] = b"demo-shared-token";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    tokio::spawn(async move {
        ohmcpd::server::run(
            SOCK,
            Some(TOKEN.to_vec()),
            ohmcpd::tools::builtin_registry(),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    println!("=== ohmcp 多 Agent 演示（认证 + 加密开启） ===\n");

    // Agent 1：语音助手 —— 知识库检索（演示缓存命中路径）。
    // Agent 2：系统调度 —— 设备状态查询。
    // Agent 3：计算智能体 —— 数值聚合。
    let (a, b, c) = tokio::try_join!(
        OhmcpClient::connect(SOCK, "voice-assistant", Some(TOKEN)),
        OhmcpClient::connect(SOCK, "system-scheduler", Some(TOKEN)),
        OhmcpClient::connect(SOCK, "calc-agent", Some(TOKEN)),
    )?;

    let tools = a.list_tools().await?;
    println!(
        "[voice-assistant] 可用工具: {:?}\n",
        tools.tools.iter().map(|t| &t.name).collect::<Vec<_>>()
    );

    // 首次调用（缓存未命中，回传完整结果并置 CACHEABLE）。
    let t0 = Instant::now();
    let r1 = a
        .call_tool(
            "kb.search",
            json!({"query": "鸿蒙分布式软总线", "top_k": 3}),
        )
        .await?;
    let r1 = serde_json::to_string(&r1)?;
    println!(
        "[voice-assistant] kb.search 首次调用: {} 字节, {:?}",
        r1.len(),
        t0.elapsed()
    );

    // 重复调用（服务端缓存命中，仅回传 32 字节 CACHE_REF，客户端本地还原）。
    let t1 = Instant::now();
    let r2 = a
        .call_tool(
            "kb.search",
            json!({"query": "鸿蒙分布式软总线", "top_k": 3}),
        )
        .await?;
    let r2 = serde_json::to_string(&r2)?;
    println!(
        "[voice-assistant] kb.search 重复调用（线上仅 32 字节 CACHE_REF）: {:?}",
        t1.elapsed()
    );
    assert_eq!(r1, r2);
    let (hits, misses) = a.cache_stats();
    println!("[voice-assistant] 本地缓存: {hits} 命中 / {misses} 未命中\n");

    let st = b
        .call_tool("device.status", json!({"device_id": "watch-01"}))
        .await?;
    println!(
        "[system-scheduler] device.status: {}",
        serde_json::to_string(&st)?
    );

    let sum = c
        .call_tool("math.sum", json!({"values": [1.5, 2.5, 96.0]}))
        .await?;
    println!("[calc-agent] math.sum: {}\n", serde_json::to_string(&sum)?);

    // 错误处理：调用不存在的工具。
    match c.call_tool("fs.delete_all", json!({})).await {
        Err(e) => println!("[calc-agent] 越界调用被拒: {e}"),
        Ok(_) => unreachable!(),
    }

    // 错误令牌认证失败演示。
    match OhmcpClient::connect(SOCK, "rogue-agent", Some(b"wrong-token")).await {
        Err(e) => println!("[rogue-agent] 错误令牌被拒: {e}"),
        Ok(_) => unreachable!(),
    }

    println!("\n=== 演示完成：多 Agent 复用单守护进程，全程加密，缓存生效 ===");
    Ok(())
}
