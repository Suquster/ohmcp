//! ohmcp-cache: 上下文优化层。
//!
//! 两个互补机制共同压缩 Agent 与工具间的有效通信量：
//! 1. **透明 LZ4 压缩**：payload 超过阈值时自动压缩（帧标志位标记），
//!    对 LLM 上下文这类高冗余文本压缩比通常 2~5x。
//! 2. **内容寻址工具结果缓存**：对幂等工具调用（tools/list、只读查询）
//!    以 `sha256(tool, args)` 为键缓存结果；命中时仅回传 32 字节哈希
//!    引用（CACHE_REF 帧），客户端从本地缓存还原，端到端省去整个
//!    结果体传输与工具重执行。

pub mod compress;
pub mod result_cache;

pub use compress::{maybe_compress, maybe_decompress, COMPRESS_THRESHOLD};
pub use result_cache::{CacheKey, ResultCache};
