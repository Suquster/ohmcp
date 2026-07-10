# 演示视频分镜脚本（约 3 分钟）

> 录制建议：终端字号调大（≥18pt）、深色主题；每步命令先停顿 1 秒再回车。

## 镜头 1（0:00–0:20）：开场与仓库

- 画面：GitLink 仓库首页 https://www.gitlink.org.cn/Taoyouce/ohmcp
- 旁白：ohmcp 是面向 OpenHarmony 的原生 MCP 协议栈——保持 MCP 语义不变，
  用二进制帧、多路复用、内容寻址缓存、默认加密和共享内存快速通道
  重造传输底盘。线上字节最高降低 99.8%，吞吐最高提升 84%。

## 镜头 2（0:20–0:50）：一键构建与全量测试

```bash
git clone https://www.gitlink.org.cn/Taoyouce/ohmcp.git && cd ohmcp
cargo test --workspace        # 39 单元 + 8 端到端集成，47 项全绿
```

- 旁白：全部 Rust 原创实现，47 项测试覆盖帧编解码、对抗性解码（篡改/截断/
  fuzz 不 panic）、加解密、缓存、共享内存通道并发正确性、软总线 Session 适配。

## 镜头 3（0:50–1:30）：多 Agent 端到端演示

```bash
cargo run --release -p ohmcp-bench --bin demo
```

- 画面重点（放大指出三行输出）：
  1. `kb.search 重复调用（线上仅 32 字节 CACHE_REF）`——内容寻址缓存；
  2. `[doc-agent] kb.dump 经共享内存通道: 6.5 万字节结果（套接字仅 12 字节引用）`；
  3. 多 Agent（voice-assistant / system-scheduler / doc-agent）共用一个守护进程。
- 旁白：认证与加密全程开启；三类 Agent 复用同一 ohmcpd 守护进程。

## 镜头 4（1:30–2:20）：基准对比官方 SDK 语义基线

```bash
cargo run --release -p ohmcp-bench
```

- 画面重点：结果表滚动后停在汇总行；切到 README 中的吞吐对比图
  （docs/benchmark-chart.svg）。
- 旁白：七大场景每场景 3 次取中位数，吞吐全部为正：
  pipeline-64 +84%、repeat-cached 字节 −94%、bulk-doc-64k 经共享内存
  通道套接字字节 −99.8%；扩展性扫描显示 1MiB 负载下 p99 −39%。

## 镜头 5（2:20–2:50）：文档与 CI

- 画面：依次快速展示 docs/design.md §4.1（共享内存通道架构图/说明）、
  docs/test-report.md 扩展性表格、GitHub Actions 绿色对勾。
- 旁白：设计文档、测试报告、答辩提纲齐备，数字全文一致；
  CI 强制 clippy 零警告 + 全量测试 + 基准烟雾。

## 镜头 6（2:50–3:00）：收尾

- 画面：slides.md 最后一页（软总线原生化展望）。
- 旁白：传输层抽象已就绪，可直接替换为 DSoftBus Session——
  目标是成为 OpenHarmony 多 Agent 生态的默认 MCP 底座。
