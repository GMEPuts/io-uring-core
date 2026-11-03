use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use io_uring::{opcode, squeue, types, IoUring};
use rustls::ClientConfig;

use crate::backoff::{backoff, jitter};
use crate::clock::now_us;
use crate::error::{CloseReason, Error, SendError, Stage};
use crate::ping::{Ping, PingAction, PingKind};
use crate::timer::{TimerKey, Timers};
use crate::tls::Tls;
use crate::types::{ConnId, Handle, Msg, Target};
use crate::url::{Scheme, Url, UrlError};
use crate::ws::{self, OpCode};

use super::channel::Waker;
use super::conn::{Conn, RxSide, State};
use super::cx::Cx;
use super::resolver::Resolver;
use super::socket;
use super::token::{Op, Token};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TLS_TIMEOUT: Duration = Duration::from_secs(5);
const UPGRADE_TIMEOUT: Duration = Duration::from_secs(10);
/// After failures, re-resolve DNS at most this often (the old IPs may have been retired).
const DNS_REFRESH: Duration = Duration::from_secs(30);
/// A closed connection whose last send is still owned by the kernel retries this soon.
const SEND_DRAIN_RETRY: Duration = Duration::from_millis(10);
/// Longest rotation stagger between connections of one registration.
const MAX_ROTATE_STAGGER: Duration = Duration::from_secs(600);

pub(crate) type MsgHandler<A> = Box<dyn FnMut(&mut A, &mut Cx<'_, A>, Msg<'_>)>;
pub(crate) type ConnHook<A> = Box<dyn FnMut(&mut A, &mut Cx<'_, A>, ConnId)>;
pub(crate) type CloseHook<A> = Box<dyn FnMut(&mut A, &mut Cx<'_, A>, ConnId, CloseReason)>;
pub(crate) type TimerFn<A> = Box<dyn FnMut(&mut A, &mut Cx<'_, A>)>;
pub(crate) type OnceFn<A> = Box<dyn FnOnce(&mut A, &mut Cx<'_, A>)>;

/// What `WsBuilder::open` registers.
pub(crate) struct WsSpec {
    pub url: Url,
    pub headers: Vec<(String, String)>,
    pub subscribe: Vec<Box<[u8]>>,
    pub ping: Option<Ping>,
    pub stale_after: Option<Duration>,
    pub max_age: Option<Duration>,
    pub connections: usize,
}

/// One registration: shared settings for its identical connections.
pub(crate) struct Group {
    pub url: Url,
    pub tls: bool,
    pub host_header: String,
    pub headers: Vec<(String, String)>,
    pub subscribe: Vec<Box<[u8]>>,
    pub ping: Option<Ping>,
    pub stale_after: Option<Duration>,
    pub addrs: Vec<SocketAddr>,
    pub conns: Vec<u16>,
    pub rr: usize,
    pub removed: bool,
    pub resolving: bool,
    pub last_resolve: Option<Instant>,
}

pub(crate) struct Every<A> {
    pub period: Duration,
    pub next: Instant,
    pub f: Option<TimerFn<A>>,
}

/// Everything except the app state and the receive-buffer ring, so handlers can get `&mut A`
/// and a `Cx` (which wraps `&mut Core`) at the same time.
pub(crate) struct Core<A> {
    /// First field: dropped before `Io::bufs`, which unregisters the buffer ring before it is freed.
    pub ring: IoUring,
    pub bgid: u16,
    /// Allocated with capacity `max_conns` and never grown past it: in-flight SQEs point into it.
    pub conns: Vec<Conn>,
    pub max_conns: usize,
    pub rx_capacity: usize,
    pub groups: Vec<Group>,
    pub handlers: Vec<Option<MsgHandler<A>>>,
    pub on_open: Option<ConnHook<A>>,
    pub on_close: Option<CloseHook<A>>,
    pub on_connect_failed: Option<CloseHook<A>>,
    pub every: Vec<Every<A>>,
    pub after: Vec<Option<OnceFn<A>>>,
    pub after_free: Vec<u32>,
    pub timers: Timers,
    pub dirty: Vec<u16>,
    pub pending_close: Vec<(u16, CloseReason)>,
    pub tls_config: Option<Arc<ClientConfig>>,
    pub waker: Arc<Waker>,
    pub wake_buf: Box<[u8; 8]>,
    pub resolver: Option<Resolver>,
    /// Cached once per loop pass.
    pub now: Instant,
}

#[inline]
fn earliest(a: Option<Instant>, b: Instant) -> Option<Instant> {
    Some(a.map_or(b, |a| a.min(b)))
}

fn push_sqe(ring: &mut IoUring, sqe: &squeue::Entry) {
    loop {
        if unsafe { ring.submission().push(sqe) }.is_ok() {
            return;
        }
        ring.submit().expect("io_uring submit failed");
    }
}

impl<A: 'static> Core<A> {
    pub(crate) fn new(
        ring: IoUring,
        bgid: u16,
        waker: Arc<Waker>,
        tls_config: Option<Arc<ClientConfig>>,
        max_conns: usize,
        rx_capacity: usize,
    ) -> Self {
        assert!(max_conns > 0 && max_conns <= u16::MAX as usize, "max_connections must be in 1..=65535");
        Core {
            ring,
            bgid,
            conns: Vec::with_capacity(max_conns),
            max_conns,
            rx_capacity,
            groups: Vec::new(),
            handlers: Vec::new(),
            on_open: None,
            on_close: None,
            on_connect_failed: None,
            every: Vec::new(),
            after: Vec::new(),
            after_free: Vec::new(),
            timers: Timers::default(),
            dirty: Vec::with_capacity(max_conns),
            pending_close: Vec::new(),
            tls_config,
            waker,
            wake_buf: Box::new([0; 8]),
            resolver: None,
            now: Instant::now(),
        }
    }

    // ---------------------------------------------------------------- registration

    fn free_capacity(&self) -> usize {
        let reusable = self.conns.iter().filter(|c| c.state == State::Free && !c.send_inflight).count();
        reusable + (self.max_conns - self.conns.len())
    }

    fn alloc_slot(&mut self, group: u32, index: usize, max_age: Option<Duration>, now: Instant) -> usize {
        if let Some(i) = self.conns.iter().position(|c| c.state == State::Free && !c.send_inflight) {
            let c = &mut self.conns[i];
            c.reuse(group, index, max_age, now);
            if c.rx.buf.capacity() != self.rx_capacity {
                c.rx = RxSide::new(self.rx_capacity);
            }
            return i;
        }
        assert!(self.conns.len() < self.max_conns, "slot capacity checked by the caller");
        self.conns.push(Conn::new(group, index, self.rx_capacity, max_age, now));
        self.conns.len() - 1
    }

    pub(crate) fn register_ws(&mut self, spec: WsSpec, handler: MsgHandler<A>, sync_resolve: bool) -> Result<Handle, Error> {
        let scheme = spec.url.scheme;
        if !matches!(scheme, Scheme::Ws | Scheme::Wss) {
            return Err(Error::Url(UrlError::UnsupportedScheme(format!("{scheme:?} (expected ws:// or wss://)"))));
        }
        let tls = scheme.is_tls();
        if tls && self.tls_config.is_none() {
            return Err(Error::NoTlsConfig);
        }
        let n = spec.connections.max(1);
        if self.free_capacity() < n {
            return Err(Error::TooManyConnections);
        }
        let now = Instant::now();
        let mut addrs = if sync_resolve {
            socket::resolve(&spec.url.host, spec.url.port).map_err(|e| Error::Resolve(spec.url.host.clone(), e))?
        } else {
            Vec::new()
        };
        // DNS returns a random subset; another registration on the same host gets a different start.
        fastrand::shuffle(&mut addrs);

        let gi = self.groups.len();
        self.groups.push(Group {
            host_header: spec.url.host_header(),
            url: spec.url,
            tls,
            headers: spec.headers,
            subscribe: spec.subscribe,
            ping: spec.ping,
            stale_after: spec.stale_after,
            addrs,
            conns: Vec::with_capacity(n),
            rr: 0,
            removed: false,
            resolving: false,
            last_resolve: sync_resolve.then_some(now),
        });
        self.handlers.push(Some(handler));

        // Connections of one registration rotate at staggered times, so they never all
        // reconnect together.
        let stagger = spec.max_age.map(|a| (a / (2 * n as u32)).min(MAX_ROTATE_STAGGER)).unwrap_or_default();
        for k in 0..n {
            let age = spec.max_age.map(|a| a.saturating_sub(stagger * k as u32));
            let s = self.alloc_slot(gi as u32, k, age, now);
            self.groups[gi].conns.push(s as u16);
            self.schedule_conn(s); // Idle, retry now: connects on the next timer pass.
        }
        if !sync_resolve {
            self.request_resolve(gi);
        }
        Ok(Handle { group: gi as u32 })
    }

    pub(crate) fn remove_group(&mut self, gi: usize) {
        let Some(g) = self.groups.get_mut(gi) else { return };
        if g.removed {
            return;
        }
        g.removed = true;
        self.handlers[gi] = None;
        for k in 0..self.groups[gi].conns.len() {
            let s = self.groups[gi].conns[k] as usize;
            self.request_close(s, CloseReason::Removed);
        }
    }

    pub(crate) fn add_every(&mut self, period: Duration, f: TimerFn<A>) {
        let next = Instant::now() + period;
        let i = self.every.len() as u32;
        self.every.push(Every { period, next, f: Some(f) });
        self.timers.push(next, TimerKey::Every(i));
    }

    pub(crate) fn add_after(&mut self, delay: Duration, f: OnceFn<A>) {
        let i = match self.after_free.pop() {
            Some(i) => {
                self.after[i as usize] = Some(f);
                i
            }
            None => {
                self.after.push(Some(f));
                (self.after.len() - 1) as u32
            }
        };
        self.timers.push(Instant::now() + delay, TimerKey::After(i));
    }

    // ---------------------------------------------------------------- lookups

    #[inline]
    pub(crate) fn live_slot(&self, id: ConnId) -> Option<usize> {
        let s = id.slot as usize;
        let c = self.conns.get(s)?;
        (c.epoch == id.epoch && c.state != State::Free).then_some(s)
    }

    fn pick_open(&mut self, gi: usize) -> Option<usize> {
        let g = &mut self.groups[gi];
        let n = g.conns.len();
        for k in 0..n {
            let i = (g.rr + k) % n;
            let s = g.conns[i] as usize;
            if self.conns[s].is_open() {
                g.rr = i + 1;
                return Some(s);
            }
        }
        None
    }

    // ---------------------------------------------------------------- timers

    /// Recomputes when connection `slot` next needs attention and arms the timer for it.
    pub(crate) fn schedule_conn(&mut self, slot: usize) {
        let c = &self.conns[slot];
        if c.closing.is_some() {
            return; // apply_close reschedules
        }
        let g = &self.groups[c.group as usize];
        let want = match c.state {
            State::Free | State::Resolving => None,
            State::Idle { retry_at } => Some(retry_at),
            State::Connecting { deadline } | State::Tls { deadline } | State::Upgrade { deadline } => Some(deadline),
            State::Open { since } => {
                let mut t = c.ping.next_wake();
                if let Some(age) = c.max_age {
                    t = earliest(t, since + age);
                }
                if let Some(stale) = g.stale_after {
                    let due_us = c.last_rx_us.saturating_add(stale.as_micros() as u64);
                    t = earliest(t, Instant::now() + Duration::from_micros(due_us.saturating_sub(now_us())));
                }
                t
            }
        };
        if want == c.wake_at {
            return;
        }
        let c = &mut self.conns[slot];
        c.wake_at = want;
        c.wake_seq = c.wake_seq.wrapping_add(1);
        if let Some(at) = want {
            self.timers.push(at, TimerKey::Conn { slot: slot as u16, seq: c.wake_seq });
        }
    }

    /// A connection's timer fired: timeouts, reconnect retries, pings, stale and age checks.
    pub(crate) fn service(&mut self, slot: usize, now: Instant) {
        match self.conns[slot].state {
            State::Free | State::Resolving => {}
            State::Idle { retry_at } => {
                if now >= retry_at {
                    self.start_connect(slot, now);
                }
            }
            State::Connecting { deadline } => {
                if now >= deadline {
                    self.request_close(slot, CloseReason::Timeout(Stage::Connect));
                }
            }
            State::Tls { deadline } => {
                if now >= deadline {
                    self.request_close(slot, CloseReason::Timeout(Stage::Tls));
                }
            }
            State::Upgrade { deadline } => {
                if now >= deadline {
                    self.request_close(slot, CloseReason::Timeout(Stage::Upgrade));
                }
            }
            State::Open { since } => self.service_open(slot, since, now),
        }
        self.schedule_conn(slot);
    }

    fn service_open(&mut self, slot: usize, since: Instant, now: Instant) {
        enum Act {
            Nothing,
            Close(CloseReason),
            Ping,
        }
        let act = {
            let c = &mut self.conns[slot];
            if c.closing.is_some() {
                return;
            }
            let g = &self.groups[c.group as usize];
            if g.stale_after.is_some_and(|s| now_us().saturating_sub(c.last_rx_us) >= s.as_micros() as u64) {
                Act::Close(CloseReason::Stale)
            } else if c.max_age.is_some_and(|a| now.duration_since(since) >= a) {
                Act::Close(CloseReason::MaxAge)
            } else if let Some(p) = &g.ping {
                match c.ping.poll(p, now, c.last_tx) {
                    PingAction::Send => Act::Ping,
                    PingAction::TimedOut => Act::Close(CloseReason::PingTimeout),
                    PingAction::Wait => Act::Nothing,
                }
            } else {
                Act::Nothing
            }
        };
        match act {
            Act::Nothing => {}
            Act::Close(r) => self.request_close(slot, r),
            Act::Ping => self.send_ping(slot, now),
        }
    }

    fn send_ping(&mut self, slot: usize, now: Instant) {
        let c = &mut self.conns[slot];
        let Some(p) = &self.groups[c.group as usize].ping else { return };
        let r = match &p.kind {
            PingKind::Frame(b) => c.queue_ws(OpCode::Ping, b),
            PingKind::Text(b) => c.queue_ws(OpCode::Text, b),
            PingKind::Binary(b) => c.queue_ws(OpCode::Binary, b),
        };
        c.last_tx = now;
        match r {
            Ok(()) => self.mark_dirty(slot),
            Err(e) => self.request_close(slot, e),
        }
    }

    // ---------------------------------------------------------------- DNS

    pub(crate) fn request_resolve(&mut self, gi: usize) {
        if self.groups[gi].resolving {
            return;
        }
        if self.resolver.is_none() {
            match Resolver::spawn(self.waker.clone()) {
                Ok(r) => self.resolver = Some(r),
                Err(_) => {
                    // No resolver thread: let waiting connections retry later.
                    let now = Instant::now();
                    for k in 0..self.groups[gi].conns.len() {
                        let s = self.groups[gi].conns[k] as usize;
                        if self.conns[s].state == State::Resolving {
                            self.conns[s].state = State::Idle { retry_at: now + Duration::from_secs(1) };
                            self.schedule_conn(s);
                        }
                    }
                    return;
                }
            }
        }
        let g = &mut self.groups[gi];
        g.resolving = true;
        g.last_resolve = Some(Instant::now());
        if let Some(r) = &self.resolver {
            r.request(gi as u32, &g.url.host, g.url.port);
        }
    }

    pub(crate) fn maybe_refresh_dns(&mut self, gi: usize, now: Instant) {
        let g = &self.groups[gi];
        if !g.resolving && g.last_resolve.is_none_or(|t| now.duration_since(t) >= DNS_REFRESH) {
            self.request_resolve(gi);
        }
    }

    /// Applies finished lookups. Cheap when there are none: one atomic load.
    #[inline]
    pub(crate) fn poll_resolver(&mut self) {
        while let Some(res) = self.resolver.as_mut().and_then(|r| r.pop()) {
            let gi = res.group as usize;
            if gi >= self.groups.len() || self.groups[gi].removed {
                continue;
            }
            let g = &mut self.groups[gi];
            g.resolving = false;
            if let Ok(mut addrs) = res.result {
                if !addrs.is_empty() {
                    fastrand::shuffle(&mut addrs);
                    g.addrs = addrs;
                }
            }
            let now = Instant::now();
            for k in 0..self.groups[gi].conns.len() {
                let s = self.groups[gi].conns[k] as usize;
                if self.conns[s].state != State::Resolving {
                    continue;
                }
                if self.groups[gi].addrs.is_empty() {
                    let c = &mut self.conns[s];
                    c.failures += 1;
                    c.state = State::Idle { retry_at: now + backoff(c.failures, jitter()) };
                } else {
                    self.start_connect(s, now);
                }
                self.schedule_conn(s);
            }
        }
    }

    /// A failing IP is often a bad edge node: the next attempt goes elsewhere.
    pub(crate) fn move_to_other_ip(&mut self, slot: usize) {
        let c = &mut self.conns[slot];
        let addrs = &self.groups[c.group as usize].addrs;
        let current = c.peer;
        let others = addrs.iter().filter(|a| Some(**a) != current).count();
        if others > 0 {
            let pick = fastrand::usize(..others);
            c.peer = addrs.iter().filter(|a| Some(**a) != current).nth(pick).copied();
        }
    }

    // ---------------------------------------------------------------- connection lifecycle

    pub(crate) fn start_connect(&mut self, slot: usize, now: Instant) {
        let c = &mut self.conns[slot];
        // The previous socket's send still owns `inflight`; try again shortly.
        if c.send_inflight {
            c.state = State::Idle { retry_at: now + SEND_DRAIN_RETRY };
            return;
        }
        let gi = c.group as usize;
        let g = &self.groups[gi];
        if g.addrs.is_empty() {
            c.state = State::Resolving;
            self.request_resolve(gi);
            return;
        }
        let addr = match c.peer {
            Some(p) if g.addrs.contains(&p) => p,
            _ => g.addrs[c.index % g.addrs.len()],
        };
        c.peer = Some(addr);
        match socket::tcp_socket(&addr) {
            Ok(fd) => {
                let (storage, len) = socket::to_sockaddr(&addr);
                c.fd = fd;
                c.addr = storage;
                c.addr_len = len;
                c.reset_session();
                c.state = State::Connecting { deadline: now + CONNECT_TIMEOUT };
                let sqe = opcode::Connect::new(
                    types::Fd(fd),
                    &c.addr as *const libc::sockaddr_storage as *const libc::sockaddr,
                    c.addr_len,
                )
                .build()
                .user_data(Token::new(Op::Connect, slot as u16, c.gen).encode());
                push_sqe(&mut self.ring, &sqe);
            }
            Err(_) => {
                c.failures += 1;
                c.state = State::Idle { retry_at: now + backoff(c.failures, jitter()) };
            }
        }
    }

    pub(crate) fn on_connected(&mut self, slot: usize, res: i32) {
        if self.conns[slot].closing.is_some() {
            return;
        }
        if res < 0 {
            self.request_close(slot, CloseReason::ConnectFailed(-res));
            return;
        }
        let now = Instant::now();
        let gi = self.conns[slot].group as usize;
        let result = if self.groups[gi].tls {
            match self.tls_config.clone() {
                Some(cfg) => match Tls::new(cfg, &self.groups[gi].url.host) {
                    Ok(tls) => {
                        let c = &mut self.conns[slot];
                        c.tls = Some(tls);
                        c.state = State::Tls { deadline: now + TLS_TIMEOUT };
                        Ok(())
                    }
                    Err(r) => Err(r),
                },
                None => Err(CloseReason::Tls),
            }
        } else {
            self.send_upgrade(slot, now)
        };
        if let Err(r) = result {
            self.request_close(slot, r);
            return;
        }
        self.push_recv(slot);
        self.mark_dirty(slot); // ClientHello, or the upgrade request
        self.schedule_conn(slot);
    }

    /// Queues the HTTP upgrade request and moves to `Upgrade`. The caller marks dirty and reschedules.
    pub(crate) fn send_upgrade(&mut self, slot: usize, now: Instant) -> Result<(), CloseReason> {
        let c = &mut self.conns[slot];
        let g = &self.groups[c.group as usize];
        c.scratch.clear();
        ws::write_upgrade_request(&mut c.scratch, &g.host_header, &g.url.path, &ws::new_key(), &g.headers);
        c.queue_scratch()?;
        c.state = State::Upgrade { deadline: now + UPGRADE_TIMEOUT };
        Ok(())
    }

    /// Upgrade accepted: open, start pings, send the registration's subscriptions.
    pub(crate) fn on_ws_open(&mut self, slot: usize, now: Instant) -> Result<(), CloseReason> {
        {
            let c = &mut self.conns[slot];
            let g = &self.groups[c.group as usize];
            c.state = State::Open { since: now };
            c.failures = 0;
            c.last_rx_us = now_us();
            c.last_tx = now;
            if let Some(p) = &g.ping {
                c.ping.on_open(p, now);
            }
            for msg in &g.subscribe {
                c.queue_ws(OpCode::Text, msg)?;
            }
        }
        self.mark_dirty(slot);
        self.schedule_conn(slot);
        Ok(())
    }

    /// Closes are deferred: applied (with the app's on_close hook) once the current handler or
    /// completion is done, so no handler ever sees a connection change underneath it.
    pub(crate) fn request_close(&mut self, slot: usize, reason: CloseReason) {
        let c = &mut self.conns[slot];
        if c.closing.is_some() || c.state == State::Free {
            return;
        }
        let active = !matches!(c.state, State::Idle { .. } | State::Resolving);
        if !active && reason != CloseReason::Removed {
            return;
        }
        c.closing = Some(reason);
        self.pending_close.push((slot as u16, reason));
    }

    // ---------------------------------------------------------------- sending

    #[inline]
    pub(crate) fn send_ws(&mut self, to: Target, opcode: OpCode, data: &[u8]) -> Result<ConnId, SendError> {
        let slot = match to {
            Target::Conn(id) => {
                let s = self.live_slot(id).ok_or(SendError::Unknown)?;
                if !self.conns[s].is_open() {
                    return Err(SendError::NotOpen);
                }
                s
            }
            Target::Any(h) => {
                let gi = h.group as usize;
                if self.groups.get(gi).is_none_or(|g| g.removed) {
                    return Err(SendError::Unknown);
                }
                self.pick_open(gi).ok_or(SendError::NotOpen)?
            }
        };
        let now = self.now;
        let c = &mut self.conns[slot];
        if let Err(r) = c.queue_ws(opcode, data) {
            self.request_close(slot, r);
            return Err(SendError::Closed);
        }
        c.last_tx = now;
        let id = c.id(slot);
        self.mark_dirty(slot);
        Ok(id)
    }

    #[inline]
    pub(crate) fn mark_dirty(&mut self, slot: usize) {
        let c = &mut self.conns[slot];
        if !c.dirty {
            c.dirty = true;
            self.dirty.push(slot as u16);
        }
    }

    /// Sends everything queued since the last flush: one `send()` per connection touched.
    #[inline]
    pub(crate) fn flush_dirty(&mut self) {
        while let Some(slot) = self.dirty.pop() {
            let s = slot as usize;
            self.conns[s].dirty = false;
            if let Err(r) = self.flush(s) {
                self.request_close(s, r);
            }
        }
    }

    /// Direct non-blocking `send()` when nothing is in flight; whatever the socket doesn't take
    /// goes to the ring in a buffer the kernel owns until completion. Never two sends in flight
    /// on one socket, so byte order is preserved.
    fn flush(&mut self, slot: usize) -> Result<(), CloseReason> {
        let c = &mut self.conns[slot];
        if c.fd < 0 || c.closing.is_some() || !c.is_connected() {
            return Ok(());
        }
        if let Some(tls) = c.tls.as_mut() {
            tls.write_ciphertext(&mut c.pending)?;
        }
        if c.send_inflight || c.pending.is_empty() {
            return Ok(());
        }
        let sent = socket::send_now(c.fd, &c.pending).map_err(CloseReason::SendError)?;
        if sent == c.pending.len() {
            c.pending.clear();
            return Ok(());
        }
        debug_assert!(c.inflight.is_empty());
        mem::swap(&mut c.pending, &mut c.inflight);
        c.pending.clear();
        c.inflight_sent = sent;
        self.push_send(slot);
        Ok(())
    }

    fn push_send(&mut self, slot: usize) {
        let c = &mut self.conns[slot];
        let rest = &c.inflight[c.inflight_sent..];
        let sqe = opcode::Send::new(types::Fd(c.fd), rest.as_ptr(), rest.len() as u32)
            .flags(libc::MSG_NOSIGNAL)
            .build()
            .user_data(Token::new(Op::Send, slot as u16, c.gen).encode());
        c.send_inflight = true;
        push_sqe(&mut self.ring, &sqe);
    }

    pub(crate) fn on_send(&mut self, slot: usize, res: i32) {
        let c = &mut self.conns[slot];
        c.send_inflight = false;
        if res < 0 {
            c.inflight.clear();
            c.inflight_sent = 0;
            self.request_close(slot, CloseReason::SendError(-res));
            return;
        }
        c.inflight_sent += res as usize;
        if c.inflight_sent < c.inflight.len() {
            self.push_send(slot);
            return;
        }
        c.inflight.clear();
        c.inflight_sent = 0;
        self.mark_dirty(slot); // anything queued meanwhile
    }

    /// The send belonged to a socket that has since closed: just release the buffer.
    pub(crate) fn release_stale_send(&mut self, slot: usize) {
        let c = &mut self.conns[slot];
        c.send_inflight = false;
        c.inflight.clear();
        c.inflight_sent = 0;
    }

    pub(crate) fn push_recv(&mut self, slot: usize) {
        let c = &mut self.conns[slot];
        let sqe = opcode::RecvMulti::new(types::Fd(c.fd), self.bgid)
            .build()
            .flags(squeue::Flags::BUFFER_SELECT)
            .user_data(Token::new(Op::Recv, slot as u16, c.gen).encode());
        c.recv_armed = true;
        push_sqe(&mut self.ring, &sqe);
    }

    /// Re-arms the eventfd read that lets other threads wake a sleeping loop.
    pub(crate) fn arm_wake(&mut self) {
        let efd = self.waker.efd();
        if efd < 0 {
            return;
        }
        let sqe = opcode::Read::new(types::Fd(efd), self.wake_buf.as_mut_ptr(), 8)
            .build()
            .user_data(Token::new(Op::Wake, 0, 0).encode());
        push_sqe(&mut self.ring, &sqe);
    }
}

impl<A> Drop for Core<A> {
    fn drop(&mut self) {
        for c in &self.conns {
            if c.fd >= 0 {
                socket::shutdown_and_close(c.fd);
            }
        }
    }
}
