//! ohmcpd: OpenHarmony 原生 MCP 协议栈守护进程。
//!
//! 作为用户态系统服务运行，通过 Unix Domain Socket 向本机 Agent
//! 暴露 MCP 工具能力。集成认证、加密、ACL、结果缓存与压缩。
//!
//! 用法：
//! ```text
//! ohmcpd --socket /tmp/ohmcpd.sock [--token <hex-or-utf8-token>] [--max-conns N]
//! ```

use anyhow::Result;
use ohmcpd::{server, tools};

// mimalloc 全局分配器：协议栈热路径（帧/压缩/加密缓冲）分配密集，
// 线程本地堆显著降低多核下的分配器争用。
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn parse_arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let args: Vec<String> = std::env::args().collect();
    let socket = parse_arg(&args, "--socket")
        .unwrap_or_else(|| ohmcp_transport::DEFAULT_SOCKET_PATH.to_string());
    let token = parse_arg(&args, "--token");
    let mut config = server::ServerConfig::default();
    if let Some(n) = parse_arg(&args, "--max-conns").and_then(|v| v.parse().ok()) {
        config.max_connections = n;
    }
    let registry = tools::builtin_registry();
    server::run_with(
        &socket,
        token.map(|t| t.into_bytes()),
        registry,
        config,
        shutdown_signal(),
    )
    .await
}

/// 等待 SIGTERM 或 Ctrl-C，触发优雅停机。
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
