//! ohmcp-security: 内置安全机制。
//!
//! - **身份认证**：预共享 token 的 HMAC-SHA256 挑战应答，token 明文
//!   永不过线；认证成功后派生每会话密钥。
//! - **传输加密**：ChaCha20-Poly1305 AEAD，96-bit 随机 nonce 前置，
//!   帧头（含消息类型与请求 id）作为 AAD 参与认证，防篡改与重放拼接。
//! - **能力级访问控制**：按 agent 授予工具白名单，最小权限原则。

pub mod acl;
pub mod auth;
pub mod crypto;

pub use acl::ToolAcl;
pub use auth::{derive_session_key, hmac_response, verify_response};
pub use crypto::{decrypt, encrypt, SessionCipher};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SecurityError {
    #[error("authentication failed")]
    AuthFailed,
    #[error("decryption failed")]
    DecryptFailed,
    #[error("access denied for tool {0}")]
    AccessDenied(String),
}
