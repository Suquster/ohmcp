//! ohmcp-bench: ohmcp 协议栈 vs 基线 JSON-RPC(官方 SDK 风格) 对比压测。
//!
//! 场景：
//! 1. latency  — 单连接顺序小消息往返延迟（p50/p99）
//! 2. bulk     — 大结果工具调用（kb.search）吞吐与线上字节数
//! 3. repeat   — 重复幂等调用（上下文缓存收益）
//! 4. concur   — 多 Agent 并发（16 连接 × 顺序调用）吞吐
//!
//! 输出 Markdown 表格与 JSON（--json <path>）。

mod baseline;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use hdrhistogram::Histogram;
use serde_json::{json, Value};

use baseline::BaselineClient;
use ohmcp_client::OhmcpClient;

const BASE_SOCK: &str = "/tmp/ohmcp-bench-baseline.sock";
const OHMCP_SOCK: &str = "/tmp/ohmcp-bench-ohmcp.sock";
const OHMCP_PLAIN_SOCK: &str = "/tmp/ohmcp-bench-ohmcp-plain.sock";
const TOKEN: &str = "bench-secret-token";

#[derive(Debug, serde::Serialize)]
struct Metrics {
    scenario: String,
    stack: String,
    ops: u64,
    elapsed_ms: f64,
    ops_per_sec: f64,
    p50_us: f64,
    p99_us: f64,
    bytes_on_wire: u64,
}

fn record(h: &mut Histogram<u64>, start: Instant) {
    h.record(start.elapsed().as_micros() as u64).ok();
}

fn metrics(
    scenario: &str,
    stack: &str,
    h: &Histogram<u64>,
    elapsed_ms: f64,
    bytes: u64,
) -> Metrics {
    Metrics {
        scenario: scenario.into(),
        stack: stack.into(),
        ops: h.len(),
        elapsed_ms,
        ops_per_sec: h.len() as f64 / (elapsed_ms / 1000.0),
        p50_us: h.value_at_quantile(0.5) as f64,
        p99_us: h.value_at_quantile(0.99) as f64,
        bytes_on_wire: bytes,
    }
}

async fn bench_baseline(
    scenario: &str,
    n: usize,
    f: impl Fn(usize) -> (String, Value),
) -> Result<Metrics> {
    let c = BaselineClient::connect(BASE_SOCK).await?;
    let mut h = Histogram::<u64>::new(3)?;
    let t0 = Instant::now();
    for i in 0..n {
        let (tool, args) = f(i);
        let s = Instant::now();
        c.call_tool(&tool, args).await?;
        record(&mut h, s);
    }
    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
    let bytes = c.bytes_sent.load(Ordering::Relaxed) + c.bytes_recv.load(Ordering::Relaxed);
    Ok(metrics(scenario, "baseline", &h, elapsed, bytes))
}

async fn bench_ohmcp(
    scenario: &str,
    n: usize,
    f: impl Fn(usize) -> (String, Value),
) -> Result<Metrics> {
    bench_ohmcp_inner(scenario, "ohmcp", OHMCP_SOCK, Some(TOKEN.as_bytes()), n, f).await
}

async fn bench_ohmcp_plain(
    scenario: &str,
    n: usize,
    f: impl Fn(usize) -> (String, Value),
) -> Result<Metrics> {
    bench_ohmcp_inner(scenario, "ohmcp-plain", OHMCP_PLAIN_SOCK, None, n, f).await
}

/// 共享内存大 payload 通道（memfd + SCM_RIGHTS）：超阈值结果零套接字拷贝。
async fn bench_ohmcp_shm(
    scenario: &str,
    n: usize,
    f: impl Fn(usize) -> (String, Value),
) -> Result<Metrics> {
    let c = OhmcpClient::connect_shm(OHMCP_SOCK, "bench-agent", Some(TOKEN.as_bytes())).await?;
    anyhow::ensure!(c.shm_enabled(), "shm negotiation failed");
    let mut h = Histogram::<u64>::new(3)?;
    let t0 = Instant::now();
    for i in 0..n {
        let (tool, args) = f(i);
        let s = Instant::now();
        c.call_tool(&tool, args).await?;
        record(&mut h, s);
    }
    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
    let bytes = c.wire_bytes().await;
    Ok(metrics(scenario, "ohmcp-shm", &h, elapsed, bytes))
}

async fn bench_ohmcp_inner(
    scenario: &str,
    stack: &str,
    sock: &str,
    token: Option<&[u8]>,
    n: usize,
    f: impl Fn(usize) -> (String, Value),
) -> Result<Metrics> {
    let c = OhmcpClient::connect(sock, "bench-agent", token).await?;
    let mut h = Histogram::<u64>::new(3)?;
    let t0 = Instant::now();
    for i in 0..n {
        let (tool, args) = f(i);
        let s = Instant::now();
        c.call_tool(&tool, args).await?;
        record(&mut h, s);
    }
    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
    let bytes = c.wire_bytes().await;
    Ok(metrics(scenario, stack, &h, elapsed, bytes))
}

async fn bench_concurrent<Fut, C>(
    scenario: &str,
    stack: &str,
    clients: usize,
    per_client: usize,
    connect: impl Fn() -> Fut,
) -> Result<Metrics>
where
    Fut: std::future::Future<Output = Result<C>> + Send,
    C: CallTool + Send + Sync + 'static,
{
    let mut conns = Vec::new();
    for _ in 0..clients {
        conns.push(Arc::new(connect().await?));
    }
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for (ci, c) in conns.into_iter().enumerate() {
        handles.push(tokio::spawn(async move {
            let mut h = Histogram::<u64>::new(3).unwrap();
            for i in 0..per_client {
                let s = Instant::now();
                c.call(
                    "kb.search",
                    json!({"query": format!("q-{ci}-{i}"), "top_k": 3}),
                )
                .await
                .unwrap();
                record(&mut h, s);
            }
            h
        }));
    }
    let mut total = Histogram::<u64>::new(3)?;
    for hd in handles {
        total.add(hd.await?).ok();
    }
    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
    Ok(metrics(scenario, stack, &total, elapsed, 0))
}

/// 单连接多路复用：64 个并发 worker 共享一条连接。
async fn bench_pipelined<C>(
    scenario: &str,
    stack: &str,
    client: Arc<C>,
    workers: usize,
    per_worker: usize,
) -> Result<Metrics>
where
    C: CallTool + Send + Sync + 'static,
{
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for w in 0..workers {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let mut h = Histogram::<u64>::new(3).unwrap();
            for i in 0..per_worker {
                let s = Instant::now();
                c.call("echo", json!({"msg": format!("p-{w}-{i}")}))
                    .await
                    .unwrap();
                record(&mut h, s);
            }
            h
        }));
    }
    let mut total = Histogram::<u64>::new(3)?;
    for hd in handles {
        total.add(hd.await?).ok();
    }
    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
    Ok(metrics(scenario, stack, &total, elapsed, 0))
}

trait CallTool {
    fn call(
        &self,
        name: &str,
        args: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;
}

impl CallTool for BaselineClient {
    fn call(
        &self,
        name: &str,
        args: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            self.call_tool(&name, args).await?;
            Ok(())
        })
    }
}

impl CallTool for Arc<OhmcpClient> {
    fn call(
        &self,
        name: &str,
        args: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            self.call_tool(&name, args).await?;
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1).cloned());

    // 进程内启动两套服务端。
    let registry = Arc::new(ohmcpd::tools::builtin_registry());
    let reg2 = registry.clone();
    tokio::spawn(async move { baseline::run_server(BASE_SOCK, reg2).await });
    tokio::spawn(async move {
        ohmcpd::server::run(
            OHMCP_SOCK,
            Some(TOKEN.as_bytes().to_vec()),
            ohmcpd::tools::builtin_registry(),
        )
        .await
    });
    tokio::spawn(async move {
        ohmcpd::server::run(OHMCP_PLAIN_SOCK, None, ohmcpd::tools::builtin_registry()).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut all: Vec<Metrics> = Vec::new();
    const N: usize = 5000;

    // 每场景运行 3 次取吞吐中位数，消除运行间抖动。
    macro_rules! med3 {
        ($e:expr) => {{
            let mut v = vec![$e.await?, $e.await?, $e.await?];
            v.sort_by(|a, b| a.ops_per_sec.partial_cmp(&b.ops_per_sec).unwrap());
            v.swap_remove(1)
        }};
    }

    // 1. latency：小消息 echo。
    all.push(med3!(bench_baseline("latency-echo", N, |i| {
        ("echo".into(), json!({"msg": format!("m{i}")}))
    })));
    all.push(med3!(bench_ohmcp("latency-echo", N, |i| {
        ("echo".into(), json!({"msg": format!("m{i}")}))
    })));
    all.push(med3!(bench_ohmcp_plain("latency-echo", N, |i| {
        ("echo".into(), json!({"msg": format!("m{i}")}))
    })));

    // 2. bulk：大结果 kb.search（每次不同 query，无缓存收益，考验压缩+帧解析）。
    all.push(med3!(bench_baseline("bulk-kb-search", N, |i| {
        (
            "kb.search".into(),
            json!({"query": format!("unique-{i}"), "top_k": 10}),
        )
    })));
    all.push(med3!(bench_ohmcp("bulk-kb-search", N, |i| {
        (
            "kb.search".into(),
            json!({"query": format!("unique-{i}"), "top_k": 10}),
        )
    })));
    all.push(med3!(bench_ohmcp_plain("bulk-kb-search", N, |i| {
        (
            "kb.search".into(),
            json!({"query": format!("unique-{i}"), "top_k": 10}),
        )
    })));

    // 3. repeat：重复幂等调用（10 个不同 query 轮转，缓存命中率 ~99.8%）。
    all.push(med3!(bench_baseline("repeat-cached", N, |i| {
        (
            "kb.search".into(),
            json!({"query": format!("hot-{}", i % 10), "top_k": 10}),
        )
    })));
    all.push(med3!(bench_ohmcp("repeat-cached", N, |i| {
        (
            "kb.search".into(),
            json!({"query": format!("hot-{}", i % 10), "top_k": 10}),
        )
    })));

    // 3b. bulk-64k：整文档拉取（~64KB 结果，端侧 RAG 全文注入负载）。
    const M: usize = 500;
    all.push(med3!(bench_baseline("bulk-doc-64k", M, |i| {
        ("kb.dump".into(), json!({"doc_id": format!("d{i}")}))
    })));
    all.push(med3!(bench_ohmcp("bulk-doc-64k", M, |i| {
        ("kb.dump".into(), json!({"doc_id": format!("d{i}")}))
    })));
    all.push(med3!(bench_ohmcp_shm("bulk-doc-64k", M, |i| {
        ("kb.dump".into(), json!({"doc_id": format!("d{i}")}))
    })));

    // 4. pipeline：单连接 64 路并发多路复用（队头阻塞考验）。
    {
        let bc = Arc::new(BaselineClient::connect(BASE_SOCK).await?);
        all.push(med3!(bench_pipelined(
            "pipeline-64",
            "baseline",
            bc.clone(),
            64,
            80
        )));
        let oc = OhmcpClient::connect(OHMCP_SOCK, "bench-agent", Some(TOKEN.as_bytes())).await?;
        let oc = Arc::new(oc);
        all.push(med3!(bench_pipelined(
            "pipeline-64",
            "ohmcp",
            oc.clone(),
            64,
            80
        )));
    }

    // 5. concur：16 并发 Agent。
    all.push(med3!(bench_concurrent(
        "concurrent-16",
        "baseline",
        16,
        500,
        || async { BaselineClient::connect(BASE_SOCK).await }
    )));
    all.push(med3!(bench_concurrent(
        "concurrent-16",
        "ohmcp",
        16,
        500,
        || async { OhmcpClient::connect(OHMCP_SOCK, "bench-agent", Some(TOKEN.as_bytes())).await }
    )));

    // 输出。
    println!("| scenario | stack | ops | elapsed(ms) | ops/s | p50(us) | p99(us) | bytes |");
    println!("|---|---|---|---|---|---|---|---|");
    for m in &all {
        println!(
            "| {} | {} | {} | {:.1} | {:.0} | {:.0} | {:.0} | {} |",
            m.scenario,
            m.stack,
            m.ops,
            m.elapsed_ms,
            m.ops_per_sec,
            m.p50_us,
            m.p99_us,
            m.bytes_on_wire
        );
    }
    // 提升摘要。
    println!();
    for sc in [
        "latency-echo",
        "bulk-kb-search",
        "bulk-doc-64k",
        "repeat-cached",
        "pipeline-64",
        "concurrent-16",
    ] {
        let b = all
            .iter()
            .find(|m| m.scenario == sc && m.stack == "baseline")
            .unwrap();
        let o = all
            .iter()
            .find(|m| m.scenario == sc && m.stack == "ohmcp")
            .unwrap();
        let bytes = if b.bytes_on_wire > 0 && o.bytes_on_wire > 0 {
            format!(
                "  wire bytes {:.1}% fewer",
                (1.0 - o.bytes_on_wire as f64 / b.bytes_on_wire as f64) * 100.0
            )
        } else {
            String::new()
        };
        println!(
            "{sc}: throughput {:+.1}%  p50 latency {:.1}% lower{bytes}",
            (o.ops_per_sec / b.ops_per_sec - 1.0) * 100.0,
            (1.0 - o.p50_us / b.p50_us) * 100.0
        );
    }

    if let Some(path) = json_out {
        std::fs::write(&path, serde_json::to_string_pretty(&all)?)?;
        println!("\nJSON written to {path}");
    }
    Ok(())
}
