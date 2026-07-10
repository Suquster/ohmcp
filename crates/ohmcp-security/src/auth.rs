//! HMAC-SHA256 挑战应答认证与会话密钥派生。
//!
//! 流程：
//! 1. 客户端发送 `Auth { agent_id, response = HMAC(token, nonce_c), nonce_c }`
//! 2. 服务端以本地登记的 token 验证 response
//! 3. 双方以 `HKDF 简化式：HMAC(token, "ohmcp-session" || nonce_c)` 派生
//!    32 字节会话密钥，用于后续 ChaCha20-Poly1305 加密。

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 计算挑战应答：HMAC(token, nonce)。
pub fn hmac_response(token: &[u8], nonce: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(token).expect("hmac accepts any key length");
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

/// 常数时间验证应答。
pub fn verify_response(token: &[u8], nonce: &[u8], response: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(token).expect("hmac accepts any key length");
    mac.update(nonce);
    mac.verify_slice(response).is_ok()
}

/// 派生每会话对称密钥。
pub fn derive_session_key(token: &[u8], nonce: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(token).expect("hmac accepts any key length");
    mac.update(b"ohmcp-session");
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

/// X25519 临时密钥对：为每个会话提供前向保密。
///
/// 双方在认证握手中交换临时公钥，会话密钥掺入 ECDH 共享秘密：
/// `HMAC(token, "ohmcp-fs" || nonce || dh_shared)`。即使预共享令牌
/// 事后泄露，历史流量也无法解密（临时私钥握手后即销毁）。
pub struct EphemeralKeyPair {
    secret: x25519_dalek::EphemeralSecret,
    public: x25519_dalek::PublicKey,
}

impl EphemeralKeyPair {
    /// 生成新的临时密钥对（私钥仅存活至 `derive` 被调用）。
    pub fn generate() -> Self {
        let secret = x25519_dalek::EphemeralSecret::random_from_rng(rand::rngs::OsRng);
        let public = x25519_dalek::PublicKey::from(&secret);
        Self { secret, public }
    }

    /// 本方临时公钥（32 字节，随认证消息明文传输）。
    pub fn public_bytes(&self) -> [u8; 32] {
        self.public.to_bytes()
    }

    /// 消费私钥，与对方临时公钥完成 ECDH 并派生前向保密会话密钥。
    pub fn derive_fs_session_key(
        self,
        peer_public: &[u8; 32],
        token: &[u8],
        nonce: &[u8],
    ) -> [u8; 32] {
        let peer = x25519_dalek::PublicKey::from(*peer_public);
        let shared = self.secret.diffie_hellman(&peer);
        let mut mac = HmacSha256::new_from_slice(token).expect("hmac accepts any key length");
        mac.update(b"ohmcp-fs");
        mac.update(nonce);
        mac.update(shared.as_bytes());
        mac.finalize().into_bytes().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_response_verifies() {
        let r = hmac_response(b"secret", b"nonce1");
        assert!(verify_response(b"secret", b"nonce1", &r));
    }

    #[test]
    fn wrong_token_rejected() {
        let r = hmac_response(b"attacker", b"nonce1");
        assert!(!verify_response(b"secret", b"nonce1", &r));
    }

    #[test]
    fn fs_key_agreement_matches_both_sides() {
        let a = EphemeralKeyPair::generate();
        let b = EphemeralKeyPair::generate();
        let a_pub = a.public_bytes();
        let b_pub = b.public_bytes();
        let ka = a.derive_fs_session_key(&b_pub, b"token", b"nonce");
        let kb = b.derive_fs_session_key(&a_pub, b"token", b"nonce");
        assert_eq!(ka, kb);
    }

    #[test]
    fn fs_keys_differ_per_session() {
        let mk = || {
            let a = EphemeralKeyPair::generate();
            let b = EphemeralKeyPair::generate();
            let b_pub = b.public_bytes();
            a.derive_fs_session_key(&b_pub, b"token", b"nonce")
        };
        assert_ne!(mk(), mk(), "ephemeral keys must yield unique session keys");
    }

    #[test]
    fn session_keys_differ_per_nonce() {
        assert_ne!(
            derive_session_key(b"secret", b"n1"),
            derive_session_key(b"secret", b"n2")
        );
    }
}
