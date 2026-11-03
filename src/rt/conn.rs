use std::mem::zeroed;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::time::{Duration, Instant};

use crate::error::CloseReason;
use crate::ping::PingState;
use crate::rxbuf::RxBuf;
use crate::types::ConnId;
use crate::ws::{self, OpCode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// Slot not in use (or removed, waiting for an in-flight send to drain).
    Free,
    /// Waiting for DNS (registered at runtime, or every address failed to resolve).
    Resolving,
    Idle { retry_at: Instant },
    Connecting { deadline: Instant },
    Tls { deadline: Instant },
    Upgrade { deadline: Instant },
    Open { since: Instant },
}

/// The receive side, taken out of the `Conn` while its frames are dispatched so handlers can
/// use the rest of the runtime (including sending on this same connection) freely.
#[derive(Default)]
pub(crate) struct RxSide {
    pub buf: RxBuf,
    pub frag: Vec<u8>,
    pub frag_binary: bool,
    pub frag_active: bool,
}

impl RxSide {
    pub(crate) fn new(capacity: usize) -> Self {
        RxSide { buf: RxBuf::new(capacity), ..Default::default() }
    }

    pub(crate) fn clear(&mut self) {
        self.buf.clear();
        self.frag.clear();
        self.frag_active = false;
    }
}

pub(crate) struct Conn {
    pub group: u32,
    /// Position within its registration: spreads the pool over resolved IPs and staggers rotation.
    pub index: usize,
    /// User-facing identity; bumped when the slot is reused by another registration.
    pub epoch: u32,
    /// Socket generation; bumped on every close.
    pub gen: u32,
    pub fd: RawFd,
    /// Lives here, not on the stack: the Connect SQE points at it until it is submitted.
    pub addr: libc::sockaddr_storage,
    pub addr_len: libc::socklen_t,
    pub peer: Option<SocketAddr>,
    pub state: State,
    pub tls: Option<crate::tls::Tls>,
    pub rx: RxSide,
    pub scratch: Vec<u8>,
    /// Bytes ready for the socket (ciphertext for TLS, plaintext otherwise).
    pub pending: Vec<u8>,
    /// Owned by the kernel while `send_inflight`: never touched until its completion.
    pub inflight: Vec<u8>,
    pub inflight_sent: usize,
    pub send_inflight: bool,
    pub recv_armed: bool,
    pub last_rx_us: u64,
    pub last_tx: Instant,
    pub failures: u32,
    pub max_age: Option<Duration>,
    pub ping: PingState,
    pub wake_at: Option<Instant>,
    pub wake_seq: u32,
    pub dirty: bool,
    pub closing: Option<CloseReason>,
}

impl Conn {
    pub(crate) fn new(group: u32, index: usize, rx_capacity: usize, max_age: Option<Duration>, now: Instant) -> Self {
        Conn {
            group,
            index,
            epoch: 0,
            gen: 0,
            fd: -1,
            addr: unsafe { zeroed() },
            addr_len: 0,
            peer: None,
            state: State::Idle { retry_at: now },
            tls: None,
            rx: RxSide::new(rx_capacity),
            scratch: Vec::with_capacity(4096),
            pending: Vec::with_capacity(16 * 1024),
            inflight: Vec::with_capacity(16 * 1024),
            inflight_sent: 0,
            send_inflight: false,
            recv_armed: false,
            last_rx_us: 0,
            last_tx: now,
            failures: 0,
            max_age,
            ping: PingState::default(),
            wake_at: None,
            wake_seq: 0,
            dirty: false,
            closing: None,
        }
    }

    /// Re-targets a free slot at a new registration. The socket generation keeps counting so
    /// completions from the previous occupant are still recognised as stale.
    pub(crate) fn reuse(&mut self, group: u32, index: usize, max_age: Option<Duration>, now: Instant) {
        self.group = group;
        self.index = index;
        self.epoch = self.epoch.wrapping_add(1);
        self.peer = None;
        self.state = State::Idle { retry_at: now };
        self.failures = 0;
        self.max_age = max_age;
        self.last_tx = now;
        self.last_rx_us = 0;
        self.closing = None;
        self.reset_session();
    }

    pub(crate) fn id(&self, slot: usize) -> ConnId {
        ConnId { slot: slot as u16, epoch: self.epoch }
    }

    #[inline]
    pub(crate) fn is_open(&self) -> bool {
        matches!(self.state, State::Open { .. }) && self.closing.is_none()
    }

    /// Socket connected (TLS, upgrade or open): recv should be armed.
    #[inline]
    pub(crate) fn is_connected(&self) -> bool {
        matches!(self.state, State::Tls { .. } | State::Upgrade { .. } | State::Open { .. })
    }

    /// Clears per-session state. `inflight`/`send_inflight` stay: a send may still own them.
    pub(crate) fn reset_session(&mut self) {
        self.tls = None;
        self.rx.clear();
        self.pending.clear();
        self.recv_armed = false;
        self.ping.reset();
    }

    /// Queues a WebSocket frame (masked; encrypted if TLS).
    #[inline]
    pub(crate) fn queue_ws(&mut self, opcode: OpCode, payload: &[u8]) -> Result<(), CloseReason> {
        let mask = fastrand::u32(..).to_ne_bytes();
        match &mut self.tls {
            Some(tls) => {
                self.scratch.clear();
                ws::encode_frame(opcode, payload, mask, &mut self.scratch);
                tls.write_plaintext(&self.scratch)
            }
            None => {
                ws::encode_frame(opcode, payload, mask, &mut self.pending);
                Ok(())
            }
        }
    }

    /// Queues `scratch` as raw plaintext (the upgrade request).
    pub(crate) fn queue_scratch(&mut self) -> Result<(), CloseReason> {
        match &mut self.tls {
            Some(tls) => tls.write_plaintext(&self.scratch),
            None => {
                self.pending.extend_from_slice(&self.scratch);
                Ok(())
            }
        }
    }
}
