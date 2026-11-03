use std::{fmt, io};

use crate::url::UrlError;
use crate::ws::WsError;

/// Which connection step timed out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// TCP connect.
    Connect,
    /// TLS handshake.
    Tls,
    /// WebSocket upgrade response.
    Upgrade,
}

/// Why a connection closed. The IO reconnects afterwards unless the reason is `Removed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// `socket()` failed (errno).
    SocketFailed(i32),
    /// TCP connect failed (errno).
    ConnectFailed(i32),
    /// recv failed (errno).
    RecvError(i32),
    /// send failed (errno).
    SendError(i32),
    /// The server closed the connection (FIN or a WebSocket close frame).
    PeerClosed,
    /// TLS handshake or record error.
    Tls,
    /// WebSocket protocol violation by the server.
    Ws(WsError),
    /// The upgrade got an HTTP status other than 101.
    UpgradeRejected(u16),
    /// The receive buffer filled with an incomplete frame (raise `IoBuilder::rx_buffer`).
    RxOverflow,
    /// A fragmented message grew past the receive buffer size.
    MessageTooLarge,
    /// A connection step took too long.
    Timeout(Stage),
    /// Nothing received for `stale_after`.
    Stale,
    /// A ping with an expected reply got none in time.
    PingTimeout,
    /// Rotated after `max_age`.
    MaxAge,
    /// `cx.reconnect(conn)` was called.
    Reconnect,
    /// `cx.remove(handle)` was called: closed for good.
    Removed,
}

impl CloseReason {
    /// Whether this counts as a failure (backoff grows, the next attempt uses another IP).
    pub fn is_failure(self) -> bool {
        !matches!(self, CloseReason::MaxAge | CloseReason::Reconnect | CloseReason::Removed)
    }
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Why a send didn't happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The connection (or every connection behind the handle) isn't open right now.
    NotOpen,
    /// The `ConnId`/`Handle` doesn't refer to a live registration.
    Unknown,
    /// Writing failed and the connection is being closed.
    Closed,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SendError::NotOpen => "connection not open",
            SendError::Unknown => "unknown connection",
            SendError::Closed => "connection closed",
        })
    }
}

impl std::error::Error for SendError {}

/// Registration and setup errors.
#[derive(Debug)]
pub enum Error {
    /// The URL didn't parse or its scheme isn't supported for this connection type.
    Url(UrlError),
    /// DNS lookup failed.
    Resolve(String, io::Error),
    /// A `wss://` URL but no TLS config (enable the `ring` feature or call `IoBuilder::tls_config`).
    NoTlsConfig,
    /// More connections than `IoBuilder::max_connections`.
    TooManyConnections,
    /// OS error.
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Url(e) => write!(f, "bad url: {e}"),
            Error::Resolve(host, e) => write!(f, "resolving {host}: {e}"),
            Error::NoTlsConfig => f.write_str("wss:// needs a TLS config (feature `ring`, or IoBuilder::tls_config)"),
            Error::TooManyConnections => f.write_str("too many connections (raise IoBuilder::max_connections)"),
            Error::Io(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Url(e) => Some(e),
            Error::Resolve(_, e) | Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<UrlError> for Error {
    fn from(e: UrlError) -> Self {
        Error::Url(e)
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(e) => e,
            other => io::Error::other(other),
        }
    }
}
