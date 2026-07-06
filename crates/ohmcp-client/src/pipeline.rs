//! PayloadPipeline: 帧 payload 的压缩 / 加密 / 缓存引用统一处理管线。
//!
//! 出站顺序：缓存引用替换 -> LZ4 压缩 -> AEAD 加密；入站逆序。
//! 服务端与客户端共用本实现，保证标志位语义对称。
//! 所有方法为 `&self`，加密器无锁、缓存内部短临界区互斥，
//! 消除请求热路径上的粗粒度锁。

use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use ohmcp_cache::{maybe_compress, maybe_decompress, CacheKey, ResultCache};
use ohmcp_core::{Frame, FrameFlags, FrameHeader, MsgType};
use ohmcp_security::SessionCipher;

pub struct PayloadPipeline {
    cipher: Option<SessionCipher>,
    cache: Mutex<ResultCache>,
}

impl Default for PayloadPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl PayloadPipeline {
    pub fn new() -> PayloadPipeline {
        PayloadPipeline {
            cipher: None,
            cache: Mutex::new(ResultCache::new(4096, Duration::from_secs(300))),
        }
    }

    pub fn enable_encryption(&mut self, key: [u8; 32]) {
        self.cipher = Some(SessionCipher::new(&key));
    }

    pub fn encryption_enabled(&self) -> bool {
        self.cipher.is_some()
    }

    fn aad(msg_type: MsgType, request_id: u64) -> [u8; 9] {
        let mut aad = [0u8; 9];
        aad[0] = msg_type as u8;
        aad[1..9].copy_from_slice(&request_id.to_le_bytes());
        aad
    }

    /// 出站包装：压缩 + 加密，返回帧。未启用加密且无压缩收益时零拷贝。
    pub fn wrap(&self, msg_type: MsgType, request_id: u64, payload: Bytes) -> (Frame, FrameFlags) {
        let mut flags = 0u8;
        let (data, compressed) = maybe_compress(payload);
        if compressed {
            flags |= FrameFlags::COMPRESSED;
        }
        let data = if let Some(cipher) = &self.cipher {
            flags |= FrameFlags::ENCRYPTED;
            cipher.encrypt(&data, &Self::aad(msg_type, request_id))
        } else {
            data
        };
        let flags = FrameFlags(flags);
        let frame = Frame {
            header: FrameHeader {
                flags,
                msg_type,
                request_id,
                payload_len: data.len() as u32,
            },
            payload: data,
        };
        (frame, flags)
    }

    /// 出站缓存引用帧：payload 仅为 32 字节内容哈希。
    pub fn wrap_cache_ref(&self, msg_type: MsgType, request_id: u64, key: &CacheKey) -> Frame {
        let mut flags = FrameFlags::CACHE_REF;
        let data = if let Some(cipher) = &self.cipher {
            flags |= FrameFlags::ENCRYPTED;
            cipher.encrypt(key.as_bytes(), &Self::aad(msg_type, request_id))
        } else {
            Bytes::copy_from_slice(key.as_bytes())
        };
        Frame {
            header: FrameHeader {
                flags: FrameFlags(flags),
                msg_type,
                request_id,
                payload_len: data.len() as u32,
            },
            payload: data,
        }
    }

    /// 入站解包：解密 + 解压 + 缓存引用还原。
    pub fn unwrap(&self, frame: &Frame) -> Result<Bytes, String> {
        let flags = frame.header.flags;
        let data: Bytes = if flags.encrypted() {
            let cipher = self
                .cipher
                .as_ref()
                .ok_or("encrypted frame but no session key")?;
            cipher
                .decrypt(
                    &frame.payload,
                    &Self::aad(frame.header.msg_type, frame.header.request_id),
                )
                .map_err(|e| e.to_string())?
        } else {
            frame.payload.clone()
        };
        let data = maybe_decompress(data, flags.compressed())?;
        if flags.cache_ref() {
            let key = CacheKey::from_slice(&data).ok_or("bad cache ref")?;
            return self
                .cache
                .lock()
                .unwrap()
                .get(&key)
                .ok_or_else(|| "cache ref miss".to_string());
        }
        Ok(data)
    }

    /// 记录完整结果到本地缓存（用于后续 CACHE_REF 还原）。
    pub fn remember(&self, key: CacheKey, value: Bytes) {
        self.cache.lock().unwrap().put(key, value);
    }

    pub fn cache_get(&self, key: &CacheKey) -> Option<Bytes> {
        self.cache.lock().unwrap().get(key)
    }

    pub fn cache_stats(&self) -> (u64, u64) {
        self.cache.lock().unwrap().stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_unwrap_plain() {
        let p = PayloadPipeline::new();
        let payload = b"x".repeat(2048);
        let (frame, flags) = p.wrap(MsgType::CallTool, 5, Bytes::from(payload.clone()));
        assert!(flags.compressed());
        let out = p.unwrap(&frame).unwrap();
        assert_eq!(&out[..], &payload[..]);
    }

    #[test]
    fn wrap_unwrap_encrypted() {
        let mut a = PayloadPipeline::new();
        let mut b = PayloadPipeline::new();
        a.enable_encryption([9u8; 32]);
        b.enable_encryption([9u8; 32]);
        let (frame, flags) = a.wrap(
            MsgType::CallToolResult,
            8,
            Bytes::from_static(b"secret result"),
        );
        assert!(flags.encrypted());
        assert_eq!(&b.unwrap(&frame).unwrap()[..], b"secret result");
    }

    #[test]
    fn tampered_request_id_fails() {
        let mut a = PayloadPipeline::new();
        a.enable_encryption([9u8; 32]);
        let (mut frame, _) = a.wrap(MsgType::CallToolResult, 8, Bytes::from_static(b"secret"));
        frame.header.request_id = 999; // 篡改
        assert!(a.unwrap(&frame).is_err());
    }
}
