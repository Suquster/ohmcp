//! 内容寻址工具结果缓存（LRU + TTL）。
//!
//! 键为 `sha256(tool_name || 0x00 || canonical_args)`，值为工具结果
//! 原始字节。服务端与客户端各持有一份对称缓存：服务端命中时以
//! CACHE_REF 帧仅回传 32 字节键，客户端凭键取本地副本。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey(pub [u8; 32]);

impl CacheKey {
    pub fn compute(tool_name: &str, canonical_args: &[u8]) -> CacheKey {
        let mut h = Sha256::new();
        h.update(tool_name.as_bytes());
        h.update([0u8]);
        h.update(canonical_args);
        CacheKey(h.finalize().into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_slice(s: &[u8]) -> Option<CacheKey> {
        s.try_into().ok().map(CacheKey)
    }
}

struct Entry {
    value: Bytes,
    inserted: Instant,
    last_used: Instant,
}

/// 线程内 LRU+TTL 缓存；在守护进程中由每连接任务独占或经互斥锁共享。
pub struct ResultCache {
    map: HashMap<CacheKey, Entry>,
    capacity: usize,
    ttl: Duration,
    hits: u64,
    misses: u64,
}

impl ResultCache {
    pub fn new(capacity: usize, ttl: Duration) -> ResultCache {
        ResultCache {
            map: HashMap::with_capacity(capacity),
            capacity,
            ttl,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, key: &CacheKey) -> Option<Bytes> {
        let now = Instant::now();
        if let Some(e) = self.map.get_mut(key) {
            if now.duration_since(e.inserted) <= self.ttl {
                e.last_used = now;
                self.hits += 1;
                return Some(e.value.clone());
            }
            self.map.remove(key);
        }
        self.misses += 1;
        None
    }

    pub fn put(&mut self, key: CacheKey, value: Bytes) {
        let now = Instant::now();
        if self.map.len() >= self.capacity && !self.map.contains_key(&key) {
            self.evict_lru();
        }
        self.map.insert(
            key,
            Entry {
                value,
                inserted: now,
                last_used: now,
            },
        );
    }

    /// 采样近似 LRU 淘汰（Redis 风格）：随机取 8 个样本，淘汰其中
    /// 最久未使用者。O(1) 均摊，避免全表扫描造成尾延迟尖刺。
    fn evict_lru(&mut self) {
        if let Some(k) = self
            .map
            .iter()
            .take(8)
            .min_by_key(|(_, e)| e.last_used)
            .map(|(k, _)| *k)
        {
            self.map.remove(&k);
        }
    }

    pub fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_and_miss() {
        let mut c = ResultCache::new(4, Duration::from_secs(60));
        let k = CacheKey::compute("echo", b"{\"msg\":\"hi\"}");
        assert!(c.get(&k).is_none());
        c.put(k, Bytes::from_static(b"result"));
        assert_eq!(c.get(&k).unwrap(), Bytes::from_static(b"result"));
        assert_eq!(c.stats(), (1, 1));
    }

    #[test]
    fn ttl_expiry() {
        let mut c = ResultCache::new(4, Duration::from_millis(0));
        let k = CacheKey::compute("t", b"a");
        c.put(k, Bytes::from_static(b"v"));
        std::thread::sleep(Duration::from_millis(2));
        assert!(c.get(&k).is_none());
    }

    #[test]
    fn lru_eviction() {
        let mut c = ResultCache::new(2, Duration::from_secs(60));
        let k1 = CacheKey::compute("a", b"1");
        let k2 = CacheKey::compute("b", b"2");
        let k3 = CacheKey::compute("c", b"3");
        c.put(k1, Bytes::from_static(b"1"));
        std::thread::sleep(Duration::from_millis(2));
        c.put(k2, Bytes::from_static(b"2"));
        std::thread::sleep(Duration::from_millis(2));
        assert!(c.get(&k1).is_some()); // 触摸 k1，使 k2 成为 LRU
        c.put(k3, Bytes::from_static(b"3"));
        assert!(c.get(&k2).is_none());
        assert!(c.get(&k1).is_some());
        assert!(c.get(&k3).is_some());
    }
}
