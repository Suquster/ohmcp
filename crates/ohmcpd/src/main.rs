//! ohmcpd: OpenHarmony 原生 MCP 协议栈守护进程。
//!
//! 作为用户态系统服务运行，通过 Unix Domain Socket 向本机 Agent
//! 暴露 MCP 工具能力。集成认证、加密、ACL、结果缓存与压缩。
//!
//! 用法：
//! ```text
//! ohmcpd --socket /tmp/ohmcpd.sock [--token <hex-or-utf8-token>]
//! ```

use anyhow::Result;
use ohmcpd::{server, tools};

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
    let registry = tools::builtin_registry();
    server::run(&socket, token.map(|t| t.into_bytes()), registry).await
}
