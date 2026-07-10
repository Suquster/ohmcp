//! 共享内存大 payload 通道：memfd 环形缓冲区 + SCM_RIGHTS fd 传递。
//!
//! 设计（同设备零套接字拷贝快速路径）：
//! - 服务端为每个会话创建一块 memfd（密封 SHRINK/GROW/SEAL），
//!   通过 UDS 辅助数据（SCM_RIGHTS）一次性传给客户端；
//! - 超过阈值的工具结果直接写入环形缓冲区，帧内只携带
//!   12 字节引用（offset u64 + len u32），绕过套接字数据拷贝；
//! - SPSC 环形缓冲：服务端为唯一生产者，客户端按帧到达顺序
//!   （即写入顺序）消费并推进 tail，无锁（两个 u64 原子游标）；
//! - 空间不足时自动回退到常规帧内传输，功能语义不变；
//! - 安全性：memfd 匿名且密封，仅会话双方持有 fd，等价于
//!   内核强制的进程间访问控制；跨设备场景不走本通道。

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};

/// 头部区域（两条缓存行，避免 head/tail 伪共享）。
const HDR: usize = 128;
/// 默认环容量：4 MiB。
pub const DEFAULT_SHM_CAP: usize = 4 * 1024 * 1024;
/// 环容量上限（防御性）。
pub const MAX_SHM_CAP: usize = 64 * 1024 * 1024;
/// 走共享内存通道的最小 payload（小于此值帧内传输更划算）。
pub const SHM_THRESHOLD: usize = 16 * 1024;

/// memfd 环形缓冲区。创建方与接入方共用同一实现。
pub struct ShmRing {
    fd: OwnedFd,
    base: *mut u8,
    cap: usize,
}

// base 指向 MAP_SHARED 映射，游标为原子操作，跨线程转移安全。
unsafe impl Send for ShmRing {}
unsafe impl Sync for ShmRing {}

impl ShmRing {
    /// 创建新的 memfd 环（生产者侧）。
    pub fn create(cap: usize) -> io::Result<ShmRing> {
        assert!(cap > 0 && cap <= MAX_SHM_CAP);
        let raw = unsafe {
            libc::memfd_create(
                c"ohmcp-shm".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        if unsafe { libc::ftruncate(raw, (HDR + cap) as libc::off_t) } != 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe {
            libc::fcntl(
                raw,
                libc::F_ADD_SEALS,
                libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL,
            );
        }
        Self::map(fd, cap)
    }

    /// 从收到的 fd 接入既有环（消费者侧）。
    pub fn from_fd(fd: OwnedFd, cap: usize) -> io::Result<ShmRing> {
        if cap == 0 || cap > MAX_SHM_CAP {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad shm cap"));
        }
        Self::map(fd, cap)
    }

    fn map(fd: OwnedFd, cap: usize) -> io::Result<ShmRing> {
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                HDR + cap,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(ShmRing {
            fd,
            base: base as *mut u8,
            cap,
        })
    }

    pub fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    fn head(&self) -> &AtomicU64 {
        unsafe { &*(self.base as *const AtomicU64) }
    }

    fn tail(&self) -> &AtomicU64 {
        unsafe { &*(self.base.add(64) as *const AtomicU64) }
    }

    /// 生产者：写入一段数据，返回其逻辑偏移；空间不足返回 None
    /// （调用方回退到帧内传输）。仅允许单生产者调用。
    pub fn try_write(&self, data: &[u8]) -> Option<u64> {
        let len = data.len();
        if len == 0 || len > self.cap {
            return None;
        }
        let mut pos = self.head().load(Ordering::Relaxed);
        // 记录必须物理连续：不足以放下时跳到下一圈起点。
        if (pos as usize % self.cap) + len > self.cap {
            pos += (self.cap - pos as usize % self.cap) as u64;
        }
        let tail = self.tail().load(Ordering::Acquire);
        if pos + len as u64 - tail > self.cap as u64 {
            return None;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.base.add(HDR + pos as usize % self.cap),
                len,
            );
        }
        self.head().store(pos + len as u64, Ordering::Release);
        Some(pos)
    }

    /// 消费者：按帧到达顺序读出并释放一段数据。偏移非法返回 None。
    pub fn read_release(&self, offset: u64, len: usize) -> Option<Vec<u8>> {
        if len == 0 || len > self.cap || (offset as usize % self.cap) + len > self.cap {
            return None;
        }
        let head = self.head().load(Ordering::Acquire);
        if offset + len as u64 > head {
            return None;
        }
        let mut out = vec![0u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.base.add(HDR + offset as usize % self.cap),
                out.as_mut_ptr(),
                len,
            );
        }
        self.tail().store(offset + len as u64, Ordering::Release);
        Some(out)
    }
}

impl Drop for ShmRing {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base as *mut libc::c_void, HDR + self.cap);
        }
    }
}

/// SHM_REF 帧 payload 编码：offset u64 LE + len u32 LE。
pub fn encode_shm_ref(offset: u64, len: u32) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[..8].copy_from_slice(&offset.to_le_bytes());
    b[8..].copy_from_slice(&len.to_le_bytes());
    b
}

/// 解析 SHM_REF payload。
pub fn decode_shm_ref(payload: &[u8]) -> Option<(u64, u32)> {
    if payload.len() != 12 {
        return None;
    }
    let offset = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let len = u32::from_le_bytes(payload[8..].try_into().unwrap());
    Some((offset, len))
}

/// 经 UDS 发送一个 fd（SCM_RIGHTS，携带 1 字节 0x01）。
/// 套接字可为非阻塞：EAGAIN 时 poll POLLOUT 等待。
pub fn send_fd_blocking(sock: RawFd, fd: RawFd) -> io::Result<()> {
    let byte = [1u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_ptr() as *mut libc::c_void,
        iov_len: 1,
    };
    let mut cmsg_buf = [0u8; unsafe { libc::CMSG_SPACE(4) as usize }];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_buf.len();
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(4) as usize;
        std::ptr::copy_nonoverlapping(&fd as *const RawFd as *const u8, libc::CMSG_DATA(cmsg), 4);
    }
    loop {
        let n = unsafe { libc::sendmsg(sock, &msg, 0) };
        if n >= 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            poll_wait(sock, libc::POLLOUT)?;
            continue;
        }
        return Err(err);
    }
}

/// 经 UDS 接收一个 fd。携带字节为 0x00 表示对端拒绝（无 fd）。
pub fn recv_fd_blocking(sock: RawFd) -> io::Result<Option<OwnedFd>> {
    let mut byte = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr() as *mut libc::c_void,
        iov_len: 1,
    };
    let mut cmsg_buf = [0u8; unsafe { libc::CMSG_SPACE(4) as usize }];
    loop {
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_buf.len();
        let n = unsafe { libc::recvmsg(sock, &mut msg, 0) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                poll_wait(sock, libc::POLLIN)?;
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "closed during fd exchange",
            ));
        }
        if byte[0] == 0 {
            return Ok(None);
        }
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            if cmsg.is_null()
                || (*cmsg).cmsg_level != libc::SOL_SOCKET
                || (*cmsg).cmsg_type != libc::SCM_RIGHTS
            {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "no fd in cmsg"));
            }
            let mut fd: RawFd = -1;
            std::ptr::copy_nonoverlapping(
                libc::CMSG_DATA(cmsg),
                &mut fd as *mut RawFd as *mut u8,
                4,
            );
            if fd < 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad fd"));
            }
            return Ok(Some(OwnedFd::from_raw_fd(fd)));
        }
    }
}

/// 发送“拒绝”标记（1 字节 0x00，无 fd）。
pub fn send_fd_decline(sock: RawFd) -> io::Result<()> {
    let byte = [0u8];
    loop {
        let n = unsafe { libc::send(sock, byte.as_ptr() as *const libc::c_void, 1, 0) };
        if n >= 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            poll_wait(sock, libc::POLLOUT)?;
            continue;
        }
        return Err(err);
    }
}

fn poll_wait(fd: RawFd, events: libc::c_short) -> io::Result<()> {
    let mut pfd = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    let r = unsafe { libc::poll(&mut pfd, 1, 5000) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    if r == 0 {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "fd exchange timeout",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_roundtrip() {
        let ring = ShmRing::create(4096).unwrap();
        let data = vec![7u8; 1000];
        let off = ring.try_write(&data).unwrap();
        assert_eq!(ring.read_release(off, 1000).unwrap(), data);
    }

    #[test]
    fn ring_wraparound_and_backpressure() {
        let ring = ShmRing::create(4096).unwrap();
        // 填满后无空间。
        let a = ring.try_write(&[1u8; 3000]).unwrap();
        assert!(
            ring.try_write(&[2u8; 3000]).is_none(),
            "no space until release"
        );
        // 释放后可跨圈边界写入（记录保持物理连续）。
        assert_eq!(ring.read_release(a, 3000).unwrap(), vec![1u8; 3000]);
        let b = ring.try_write(&[2u8; 3000]).unwrap();
        assert_eq!(b as usize % 4096, 0, "skipped to ring start for contiguity");
        assert_eq!(ring.read_release(b, 3000).unwrap(), vec![2u8; 3000]);
    }

    #[test]
    fn ring_rejects_bad_ref() {
        let ring = ShmRing::create(4096).unwrap();
        assert!(ring.read_release(0, 100).is_none(), "unwritten region");
        let off = ring.try_write(&[3u8; 64]).unwrap();
        assert!(ring.read_release(off, 65).is_none(), "beyond head");
        assert!(ring.read_release(off, 8192).is_none(), "over capacity");
    }

    #[test]
    fn shm_ref_codec() {
        let b = encode_shm_ref(123456789, 4242);
        assert_eq!(decode_shm_ref(&b), Some((123456789, 4242)));
        assert_eq!(decode_shm_ref(&b[..11]), None);
    }

    #[test]
    fn fd_exchange_over_socketpair() {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        let ring = ShmRing::create(8192).unwrap();
        let off = ring.try_write(b"cross-process payload").unwrap();
        send_fd_blocking(a.as_raw_fd(), ring.raw_fd()).unwrap();
        let fd = recv_fd_blocking(b.as_raw_fd()).unwrap().unwrap();
        let peer = ShmRing::from_fd(fd, 8192).unwrap();
        assert_eq!(
            peer.read_release(off, 21).unwrap(),
            b"cross-process payload"
        );
    }

    #[test]
    fn fd_exchange_decline() {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        send_fd_decline(a.as_raw_fd()).unwrap();
        assert!(recv_fd_blocking(b.as_raw_fd()).unwrap().is_none());
    }
}
