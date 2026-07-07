# ohmcp 特性设计文档

## 1. 背景与目标

MCP（Model Context Protocol）已成为 LLM Agent 与工具交互的事实标准，但官方
Python/TypeScript SDK 面向云端进程间 stdio 场景设计：JSON-RPC 2.0 文本信封、
逐消息完整解析、无压缩、无结果缓存、无内建加密。在 OpenHarmony 泛在终端上，
多 Agent 高频调用本地工具时，这些开销直接转化为时延、功耗与内存压力。

ohmcp 的目标：为 OpenHarmony 设计一套**原生 MCP 协议栈**，保持 MCP 语义
（initialize / tools/list / tools/call / ping）不变，重构其下的传输与上下文通道，
量化指标上达到：

- 通信效率（线上字节）优化 ≥ 10%（实测最高 −94%）；
- 端到端时延与吞吐显著优于官方 SDK 语义基线；
- 内建认证、加密与访问控制，默认安全。

## 2. 总体架构

```
┌──────────────┐   ┌──────────────┐        ┌──────────────┐
│ Agent A      │   │ Agent B      │  ...   │ Agent N      │
│ ohmcp-client │   │ ohmcp-client │        │ ohmcp-client │
└──────┬───────┘   └──────┬───────┘        └──────┬───────┘
       │  OHMF 二进制帧 / UDS（多路复用 + AEAD 加密）  │
┌──────┴──────────────────┴─────────────────────────┴──────┐
│                        ohmcpd 守护进程                     │
│  帧解码 → 解密 → 解压 → ACL 检查 → 结果缓存 → 工具执行      │
│           （命中缓存且客户端已有副本 ⇒ 仅回 32B CACHE_REF）  │
└──────────────────────────────────────────────────────────┘
```

七个 crate 组成 workspace：

| crate | 职责 |
|---|---|
| ohmcp-core | 帧格式、消息类型、MCP 数据结构、错误类型 |
| ohmcp-transport | UDS 帧化读写（批量写、增量解码） |
| ohmcp-cache | LZ4 压缩、内容寻址结果缓存（采样 LRU + TTL） |
| ohmcp-security | HMAC 认证、会话密钥派生、AEAD、工具 ACL |
| ohmcp-client | 多路复用客户端 + Payload 管线 |
| ohmcpd | 服务端守护进程 + 工具注册表 |
| ohmcp-bench | 基线对比基准 |

## 3. OHMF 二进制帧格式

```
0        2      3      4        5              13            17
+--------+------+------+--------+--------------+-------------+----------+
| magic  | ver  | flags| msgtype| request_id   | payload_len | payload  |
| 0x4F4D | 0x01 | u8   | u8     | u64 LE       | u32 LE      | ...      |
+--------+------+------+--------+--------------+-------------+----------+
```

- 17 字节定长头，无需扫描换行或解析 JSON 信封即可完成路由；
- `request_id` 支持单连接任意数量在途请求（多路复用，无队头阻塞）；
- flags 位：`COMPRESSED(0x01)`、`ENCRYPTED(0x02)`、`CACHE_REF(0x04)`、
  `CACHEABLE(0x08)`；
- msgtype 直接映射 MCP 方法（Initialize/ListTools/CallTool/Ping/Auth 及其结果、Error）。

对比：JSON-RPC 基线每次 echo 往返约 200 字节文本 + 两次完整 JSON 解析；
OHMF 头部开销 17 字节，payload 仅含参数本体。

## 4. 传输层

- **批量写**：`FrameWriter` 将帧编码进同一 `BytesMut`，单次 `write` 冲刷，
  流水线场景下多帧合并为一次 syscall；
- **增量解码**：`FrameReader` 以 256KB 缓冲增量读取，`Frame::decode` 基于
  `bytes` 零拷贝切分 payload；
- **机会主义内联读**（客户端）：等待响应的请求方通过 `tokio::select!`
  同时等待 oneshot 与读锁——无并发时由调用方内联读 socket（零任务切换，
  等价于同步 RPC 的时延），并发时持锁者代为分发其他请求的响应（connection
  combiner 模式）。

### 4.1 共享内存大 payload 通道（memfd + SCM_RIGHTS）

同设备大结果（默认 ≥16KB）可选绕过套接字数据拷贝：

- **协商**：客户端认证后发 `ShmSetup` 帧；服务端创建密封（SEAL_SHRINK/GROW/SEAL）
  的 memfd 环形缓冲区（默认 4MiB），先经 UDS 辅助数据（SCM_RIGHTS）传递 fd，
  再回 `ShmSetupResult`；双方 mmap 同一物理页；
- **数据面**：超阈值结果写入环中，帧内仅携带 12 字节引用（offset u64 + len u32，
  `SHM_REF` 标志位）；客户端按帧到达顺序（即写入顺序）取出并推进消费游标；
- **无锁 SPSC 环**：头部两条独立缓存行存放 head/tail 原子游标（避免伪共享），
  记录保持物理连续（尾部不足时跳到下一圈起点）；空间不足自动回退到常规
  压缩 + 加密帧，功能语义不变；
- **安全性**：memfd 匿名且密封，仅会话双方持有 fd，等价于内核强制的进程间
  访问控制；跨设备场景不走本通道（回退常规加密帧）。

实测（bulk-doc-64k）：吞吐 +52%、p99 −67%、套接字字节 −99.8%。

## 5. 上下文优化层

### 5.1 透明压缩

payload ≥ 512B 时尝试 LZ4（`compress_prepend_size`），仅在确有收益时置
`COMPRESSED` 位。知识库检索类 3KB 结果实测压缩至约 45%。

### 5.2 内容寻址结果缓存

- 键：`sha256(tool_name ‖ 0x00 ‖ canonical_args)`（32 字节）；
- 服务端与客户端各持一份对称缓存（采样近似 LRU + 300s TTL，O(1) 淘汰，
  避免全表扫描造成尾延迟尖刺）；
- 工具注册时声明是否幂等可缓存；服务端在可缓存结果帧上置 `CACHEABLE` 位，
  客户端据此收录本地副本；
- 再次命中且客户端已有完整副本时，服务端仅回传 32 字节 `CACHE_REF`（内容哈希），
  客户端本地还原——热点调用线上字节 −94%，且跳过工具执行与序列化。

## 6. 安全机制

### 6.1 认证（令牌不过网）

客户端生成随机 nonce，发送 `HMAC-SHA256(token, nonce)`；服务端以同一预共享
令牌常数时间验证。明文令牌从不出现在信道上。

### 6.2 会话加密

认证通过后双方以 `HMAC(token, "ohmcp-session" ‖ nonce)` 派生 32 字节会话密钥，
启用 ChaCha20-Poly1305 AEAD：

- nonce = 4 字节随机会话前缀 ‖ 8 字节单调计数器（会话内唯一，免除每消息 CSPRNG）；
- 帧头摘要（msgtype ‖ request_id）作为 AAD——篡改消息类型或请求号即解密失败。

### 6.3 访问控制

`ToolAcl` 维护 agent → 工具白名单，`CallTool` 逐次检查；未认证连接只允许 `Auth`。

## 7. 性能设计要点汇总

1. 二进制定长帧头替代 JSON 信封（解析 O(1)）；
2. 单连接多路复用 + 批量写（syscall 合并、消除队头阻塞）；
3. 机会主义内联读（顺序调用零调度开销）；
4. 请求热路径无粗粒度锁：加密器无锁（原子计数 nonce）、缓存短临界区、
   未压缩/未加密路径零拷贝直通；
5. 内容寻址缓存 + CACHE_REF（重复调用 −94% 字节）；
6. LZ4 透明压缩（大结果 −81% 字节）；
7. `target-cpu=native`：ChaCha20 SIMD 路径提速约 4 倍。

## 7.1 跨设备扩展：分布式软总线集成（设计 + PoC 代码）

OHMF 帧格式与传输层解耦，天然可承载于 OpenHarmony 分布式软总线（DSoftBus）：

- **会话映射（已代码化，`ohmcp-transport/src/softbus.rs`）**：DSoftBus Session
  是消息导向 API（`SendBytes`/`OnBytesReceived`），本 PoC 提供
  `SessionEndpoint` 最小抽象 + `SessionStream` 适配器，把任意 Session 适配为
  `AsyncRead + AsyncWrite`，帧协议、压缩、AEAD 加密层**零改动**运行其上；
  测试覆盖单块多帧、单帧跨多消息块（SendBytes 有块大小上限）、会话关闭
  干净 EOF 三类边界。真实设备上仅需以 `SendBytes`/回调实现该 trait；
- **设备发现**：ohmcpd 经软总线发布 `ohmcp.daemon` 能力，远端 Agent 发现后
  建立会话，形成"跨设备工具调用"——手表 Agent 调用手机侧知识库工具；
- **字节效率即能耗**：软总线底层为 BLE/P2P/WLAN，带宽与功耗受限，
  本协议 −81% ~ −95.6% 的线上字节直接转化为传输能耗与时延优势；
- **安全对齐**：软总线设备认证之上叠加本协议的 Agent 级 HMAC 认证与
  工具级 ACL，实现设备-Agent-工具三级最小权限。

## 8. 与 OpenHarmony 的结合

ohmcpd 作为用户态系统服务运行于 UDS（`/tmp/ohmcpd.sock`，产品化可迁移至
`/dev/unix/socket/` 命名空间并由 init 拉起），设备上各 Agent（语音助手、
输入法智能体、系统调度智能体等）通过 ohmcp-client 复用同一守护进程的工具生态；
工具注册表可映射到系统能力（分布式软总线、设备状态、媒体库检索等）。
Rust 实现无 GC、内存占用小，适配资源受限终端。

## 9. 开源合规

- 许可证：Apache-2.0；
- 全部源码原创；第三方代码仅以 crates.io 依赖引用（tokio、serde、bytes、
  chacha20poly1305、sha2、hmac、lz4_flex、hdrhistogram 等），无拷贝植入。
