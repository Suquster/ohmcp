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
    fn session_keys_differ_per_nonce() {
        assert_ne!(
            derive_session_key(b"secret", b"n1"),
            derive_session_key(b"secret", b"n2")
        );
    }
}
