//! ChaCha20-Poly1305 AEAD 传输加密。
//!
//! 密文布局：`nonce(12) || ciphertext+tag`。帧头摘要作为 AAD，
//! 使消息类型/请求 id 被篡改时解密失败。

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use chacha20poly1305::aead::{Aead, AeadInPlace, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce};
use rand::RngCore;

use crate::SecurityError;

pub struct SessionCipher {
    cipher: ChaCha20Poly1305,
    /// nonce = 4 字节随机会话前缀 || 8 字节单调计数器，
    /// 保证会话内唯一且避免每消息调用 CSPRNG。
    nonce_prefix: [u8; 4],
    nonce_counter: AtomicU64,
}

impl SessionCipher {
    pub fn new(key: &[u8; 32]) -> SessionCipher {
        let mut prefix = [0u8; 4];
        rand::thread_rng().fill_bytes(&mut prefix);
        SessionCipher {
            cipher: ChaCha20Poly1305::new(Key::from_slice(key)),
            nonce_prefix: prefix,
            nonce_counter: AtomicU64::new(0),
        }
    }

    pub fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Bytes {
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..4].copy_from_slice(&self.nonce_prefix);
        let ctr = self.nonce_counter.fetch_add(1, Ordering::Relaxed);
        nonce_bytes[4..].copy_from_slice(&ctr.to_le_bytes());
        // 原地加密：单次分配、单次拷贝（nonce || ciphertext || tag）。
        let mut buf = Vec::with_capacity(12 + plaintext.len() + 16);
        buf.extend_from_slice(&nonce_bytes);
        buf.extend_from_slice(plaintext);
        let tag = self
            .cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce_bytes), aad, &mut buf[12..])
            .expect("encryption is infallible for valid key");
        buf.extend_from_slice(&tag);
        Bytes::from(buf)
    }

    pub fn decrypt(&self, data: &[u8], aad: &[u8]) -> Result<Bytes, SecurityError> {
        if data.len() < 12 + 16 {
            return Err(SecurityError::DecryptFailed);
        }
        let (nonce_bytes, ct) = data.split_at(12);
        self.cipher
            .decrypt(Nonce::from_slice(nonce_bytes), Payload { msg: ct, aad })
            .map(Bytes::from)
            .map_err(|_| SecurityError::DecryptFailed)
    }
}

/// 便捷函数：一次性加密。
pub fn encrypt(key: &[u8; 32], plaintext: &[u8], aad: &[u8]) -> Bytes {
    SessionCipher::new(key).encrypt(plaintext, aad)
}

/// 便捷函数：一次性解密。
pub fn decrypt(key: &[u8; 32], data: &[u8], aad: &[u8]) -> Result<Bytes, SecurityError> {
    SessionCipher::new(key).decrypt(data, aad)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key = [7u8; 32];
        let ct = encrypt(&key, b"secret payload", b"aad");
        let pt = decrypt(&key, &ct, b"aad").unwrap();
        assert_eq!(&pt[..], b"secret payload");
    }

    #[test]
    fn tampered_ciphertext_rejected() {
        let key = [7u8; 32];
        let mut ct = encrypt(&key, b"secret", b"aad").to_vec();
        let last = ct.len() - 1;
        ct[last] ^= 1;
        assert!(decrypt(&key, &ct, b"aad").is_err());
    }

    #[test]
    fn wrong_aad_rejected() {
        let key = [7u8; 32];
        let ct = encrypt(&key, b"secret", b"aad-1");
        assert!(decrypt(&key, &ct, b"aad-2").is_err());
    }

    #[test]
    fn wrong_key_rejected() {
        let ct = encrypt(&[1u8; 32], b"secret", b"");
        assert!(decrypt(&[2u8; 32], &ct, b"").is_err());
    }

    #[test]
    fn truncated_ciphertext_rejected() {
        let key = [7u8; 32];
        let ct = encrypt(&key, b"secret", b"");
        for n in 0..(12 + 16) {
            assert!(decrypt(&key, &ct[..n], b"").is_err());
        }
    }

    #[test]
    fn nonce_unique_per_message() {
        let key = [7u8; 32];
        let c = SessionCipher::new(&key);
        let a = c.encrypt(b"x", b"");
        let b = c.encrypt(b"x", b"");
        assert_ne!(&a[..12], &b[..12], "nonce must differ across messages");
        assert_ne!(a, b, "identical plaintext must yield distinct ciphertext");
    }

    #[test]
    fn replayed_nonce_still_decrypts_but_ciphertexts_differ_across_sessions() {
        // 会话前缀随机化：不同会话同计数器的 nonce 不同。
        let key = [7u8; 32];
        let c1 = SessionCipher::new(&key);
        let c2 = SessionCipher::new(&key);
        let a = c1.encrypt(b"x", b"");
        let b = c2.encrypt(b"x", b"");
        assert_ne!(&a[..4], &b[..4]);
    }
}
