---
marp: true
paginate: true
theme: default
---

<!-- 答辩幻灯片（Marp 格式）。渲染：npx @marp-team/marp-cli docs/slides.md -o slides.pdf -->

# ohmcp：泛在 OS 原生 MCP 协议栈

**保持 MCP 语义不变，为 OpenHarmony 重造传输底盘**

二进制帧 · 多路复用 · 内容寻址缓存 · 默认加密 · 共享内存快速通道

线上字节最高 **−99.8%** ｜ 吞吐最高 **+84%** ｜ p99 最高 **−67%**

---

## 问题：MCP 的传输层不适合端侧

- 官方 SDK：JSON-RPC 文本 + 逐消息解析，每字节都是能耗；
- 端侧多 Agent：一个 Agent 一个进程/连接，无复用、无缓存、无加密默认值；
- 大结果（RAG 全文、传感器批量数据）在同设备也要走完整套接字拷贝。

**赛题要求 ≥10% 通信效率提升 —— 我们做到了 −81% ~ −99.8%。**

---

## 架构：7 crate 模块化 Rust workspace

| 层 | crate | 要点 |
|---|---|---|
| 核心 | ohmcp-core | 17 字节帧头 OHMF 二进制帧，版本协商 |
| 传输 | ohmcp-transport | UDS + 可选 memfd 共享内存大 payload 通道 |
| 安全 | ohmcp-security | HMAC 挑战应答 + ChaCha20-Poly1305（原地加密） |
| 缓存 | ohmcp-cache | LZ4 压缩 + CACHE_REF 内容寻址（SHA-256） |
| 服务 | ohmcpd | 多 Agent 守护进程、工具注册表 |
| 客户端 | ohmcp-client | 单连接 64 路多路复用、机会主义内联读 |
| 基准 | ohmcp-bench | 六场景 + 扩展性扫描，每场景 3 次取中位数 |

---

## 创新点 1：CACHE_REF 内容寻址缓存

- 服务端/客户端对称缓存，键 = SHA-256(工具名 + 规范化参数)；
- 热点重复调用线上仅传 **32 字节哈希引用**；
- 服务端 CACHEABLE 提示 + O(1) 采样 LRU 淘汰。

**repeat-cached：字节 −94.1%，吞吐 +37% ~ +59%** —— MCP 生态未见同类机制。

---

## 创新点 2：机会主义内联读

- 顺序调用：请求方自己在当前任务内联读响应，**零任务切换**；
- 并发调用：自动“变身” combiner 批量收发，无队头阻塞。

**pipeline-64：吞吐 +66% ~ +84%（23.1 万 ops/s），p99 816µs → 302µs**

---

## 创新点 3：默认安全且开销可量化

- HMAC 挑战应答认证（令牌不过网）；
- 每会话 ChaCha20-Poly1305，单调计数 nonce，帧头入 AAD 防篡改；
- 原地加密：热路径单分配单拷贝；
- **全部基准数字均在加密开启下测得** —— 安全不是选配。

---

## 创新点 4：共享内存大 payload 通道

- memfd 密封（SHRINK/GROW/SEAL）环形缓冲 + SCM_RIGHTS fd 传递；
- 超阈值（16KB）结果零套接字拷贝，帧内仅 **12 字节引用**；
- 无锁 SPSC 环，空间不足自动回退常规帧 —— 语义不变；
- 对齐软总线“同设备快速通道”理念。

**bulk-doc-64k：吞吐 +52%，p99 −67%，套接字字节 −99.8%**
**扩展性：1 MiB 负载下吞吐 +16%、p99 −39%，套接字字节恒定 ~130B**

---

## 核心数字（vs 官方 SDK 语义基线，全程加密）

![六场景吞吐对比](benchmark-chart.svg)

六大场景吞吐全部为正 ｜ 39 项测试全绿 ｜ CI 零警告

---

## 工程质量与开源合规

- 36 单元 + 3 端到端集成测试（含对抗性：篡改/截断/超限帧/fuzz 不 panic）；
- CI：fmt + clippy(-D warnings) + 全量测试 + 基准烟雾；
- Apache-2.0，0% 代码拷贝，语义化提交历史；
- 设计文档 / 测试报告 / 答辩提纲齐备，数字全文一致。

---

## 展望：软总线原生化

- 传输层抽象已就绪，UDS 可直接替换为 DSoftBus Session（设计文档 §7.1）；
- 跨设备：常规加密帧；同设备：共享内存快速通道 —— 与软总线分层一致；
- 目标：成为 OpenHarmony 多 Agent 生态的默认 MCP 底座。

**仓库：https://www.gitlink.org.cn/Taoyouce/ohmcp**
