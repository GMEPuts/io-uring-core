//! # io_uring_core
//!
//! A connection-oriented WebSocket client runtime on io_uring, built for low-latency trading.
//!
//! You register connections (a URL, a ping if the venue needs one, a message handler) and the
//! IO owns everything else: DNS, connect, TLS, the WebSocket upgrade, subscriptions, pings,
//! stale detection, reconnects with backoff and IP failover, and the io_uring plumbing.
//!
//! ```ignore
//! use io_uring_core::{secs, Io, Ping};
//!
//! struct App { /* your state */ }
//!
//! let mut io = Io::new(App { })?;
//! io.ws("wss://fstream.binance.com/ws")
//!     .subscribe(r#"{"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":1}"#)
//!     .on_message(|app, _cx, msg| { /* msg.data: &[u8], borrowed, no copy */ })
//!     .open()?;
//! io.ws("wss://ws.okx.com:8443/ws/v5/public")
//!     .subscribe(OKX_SUB)
//!     .ping(Ping::text("ping").every(secs(25)).expect("pong"))
//!     .on_message(|app, _cx, msg| { /* ... */ })
//!     .open()?;
//! io.run()
//! ```
//!
//! The runtime is Linux-only (io_uring). The protocol pieces (URL parsing, WebSocket framing,
//! ping scheduling, timers) build and test on any OS.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

mod backoff;
pub mod clock;
mod error;
mod ping;
mod rxbuf;
mod timer;
mod tls;
mod types;
pub mod url;
mod ws;

#[cfg(target_os = "linux")]
mod rt;
#[cfg(target_os = "linux")]
pub mod sys;

use std::time::Duration;

pub use error::{CloseReason, Error, SendError, Stage};
pub use ping::Ping;
#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
pub use tls::default_tls_config;
pub use types::{ConnId, Handle, Msg, Target};
pub use ws::WsError;

#[cfg(target_os = "linux")]
pub use rt::{spawn, ConnInfo, Cx, Full, Io, IoBuilder, Sender, WaitMode, WsBuilder};

/// `Duration` of `n` milliseconds.
pub const fn millis(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// `Duration` of `n` seconds.
pub const fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// `Duration` of `n` minutes.
pub const fn mins(n: u64) -> Duration {
    Duration::from_secs(n * 60)
}

/// `Duration` of `n` hours.
pub const fn hours(n: u64) -> Duration {
    Duration::from_secs(n * 3600)
}
