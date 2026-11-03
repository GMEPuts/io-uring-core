use std::io;
use std::mem::{size_of, zeroed};
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::fd::RawFd;

/// Blocking DNS. Only called at registration before `run`, or on the resolver thread.
/// IPv4 addresses come first (many venues and VPCs have no working IPv6 route).
pub(crate) fn resolve(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    let mut addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    addrs.sort_by_key(|a| a.is_ipv6());
    addrs.dedup();
    if addrs.is_empty() {
        return Err(io::Error::new(io::ErrorKind::NotFound, format!("no address for {host}")));
    }
    Ok(addrs)
}

pub(crate) fn to_sockaddr(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: a.port().to_be(),
                sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(a.ip().octets()) },
                sin_zero: [0; 8],
            };
            unsafe { (&mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in).write(sin) };
            size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(a) => {
            let sin6 = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: a.flowinfo(),
                sin6_addr: libc::in6_addr { s6_addr: a.ip().octets() },
                sin6_scope_id: a.scope_id(),
            };
            unsafe { (&mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in6).write(sin6) };
            size_of::<libc::sockaddr_in6>()
        }
    };
    (storage, len as libc::socklen_t)
}

pub(crate) fn tcp_socket(addr: &SocketAddr) -> io::Result<RawFd> {
    let family = if addr.is_ipv4() { libc::AF_INET } else { libc::AF_INET6 };
    let fd = cvt(unsafe { libc::socket(family, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) })?;
    let opts = [
        (libc::IPPROTO_TCP, libc::TCP_NODELAY, 1),
        (libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1),
        (libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, 10),
        (libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, 5),
        (libc::IPPROTO_TCP, libc::TCP_KEEPCNT, 3),
    ];
    for (level, name, value) in opts {
        if let Err(e) = set_opt(fd, level, name, value) {
            unsafe { libc::close(fd) };
            return Err(e);
        }
    }
    Ok(fd)
}

/// io_uring holds its own reference to the socket, so close() alone would leave the multishot
/// recv armed and the peer still streaming; shutdown() ends both.
pub(crate) fn shutdown_and_close(fd: RawFd) {
    unsafe {
        libc::shutdown(fd, libc::SHUT_RDWR);
        libc::close(fd);
    }
}

/// Non-blocking send straight to the socket. `Ok(n)` may be short (or 0 when the socket buffer
/// is full); `Err(errno)` is fatal for the connection.
#[inline]
pub(crate) fn send_now(fd: RawFd, buf: &[u8]) -> Result<usize, i32> {
    let n = unsafe { libc::send(fd, buf.as_ptr().cast(), buf.len(), libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) };
    if n >= 0 {
        return Ok(n as usize);
    }
    match io::Error::last_os_error().raw_os_error().unwrap_or(0) {
        libc::EAGAIN | libc::EINTR => Ok(0),
        e => Err(e),
    }
}

pub(crate) fn eventfd() -> io::Result<RawFd> {
    cvt(unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) })
}

fn set_opt(fd: RawFd, level: libc::c_int, name: libc::c_int, value: libc::c_int) -> io::Result<()> {
    cvt(unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &value as *const libc::c_int as *const libc::c_void,
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    })
    .map(drop)
}

fn cvt(rc: libc::c_int) -> io::Result<libc::c_int> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}
