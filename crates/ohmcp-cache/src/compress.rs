//! 透明 LZ4 压缩：小 payload 不压缩（避免负收益），大 payload
//! 使用 size-prepended LZ4 块格式。

use bytes::Bytes;

/// 低于该阈值的 payload 不压缩。
pub const COMPRESS_THRESHOLD: usize = 512;

/// 若 payload 达到阈值且压缩有收益则压缩，返回 (数据, 是否已压缩)。
/// 未压缩路径零拷贝直通。
pub fn maybe_compress(payload: Bytes) -> (Bytes, bool) {
    if payload.len() < COMPRESS_THRESHOLD {
        return (payload, false);
    }
    let compressed = lz4_flex::compress_prepend_size(&payload);
    if compressed.len() < payload.len() {
        (Bytes::from(compressed), true)
    } else {
        (payload, false)
    }
}

/// 按帧标志位解压；未压缩路径零拷贝直通。
pub fn maybe_decompress(payload: Bytes, compressed: bool) -> Result<Bytes, String> {
    if !compressed {
        return Ok(payload);
    }
    lz4_flex::decompress_size_prepended(&payload)
        .map(Bytes::from)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_payload_not_compressed() {
        let (out, c) = maybe_compress(Bytes::from_static(b"tiny"));
        assert!(!c);
        assert_eq!(&out[..], b"tiny");
    }

    #[test]
    fn large_payload_roundtrip() {
        let data = "context ".repeat(1000);
        let (out, c) = maybe_compress(Bytes::from(data.clone()));
        assert!(c);
        assert!(out.len() < data.len());
        let back = maybe_decompress(out, true).unwrap();
        assert_eq!(&back[..], data.as_bytes());
    }
}
