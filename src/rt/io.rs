use std::io;
use std::mem;
use std::str::FromStr;
use std::sync::atomic::{fence, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use io_uring::{cqueue, types, IoUring};
use rustls::ClientConfig;

use crate::backoff::{backoff, jitter};
use crate::clock::{now_ticks, now_us};
use crate::error::CloseReason;
use crate::timer::TimerKey;
use crate::types::{ConnId, Handle, Msg};
use crate::ws::{self, OpCode, Upgrade, WsError};

use super::buf_ring::BufRing;
use super::builder::WsBuilder;
use super::channel::{self, Inbox, Sender, Waker};
use super::conn::{RxSide, State};
use super::cx::Cx;
use super::engine::Core;
use super::resolver::{DnsPort, Resolver};
use super::socket;
use super::token::{Op, Token};

const BUF_GROUP: u16 = 0;
/// Bounds the work one loop pass spends on timers, so a burst of due timers can't starve recv.
const MAX_TIMERS_PER_TURN: usize = 1024;

/// How the loop waits for work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WaitMode {
    /// Busy-poll the completion queue from user space; never sleeps. Needs a dedicated core.
    #[default]
    Spin,
    /// Wait inside io_uring with NAPI busy polling: this core polls the NIC receive queues
    /// directly. Best tail latency on BinanceRelay. Needs a dedicated core.
    Napi,
    /// Sleep until a completion or deadline. Low CPU, adds wakeup latency: development boxes.
    Block,
}

impl FromStr for WaitMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "spin" => Ok(WaitMode::Spin),
            "napi" => Ok(WaitMode::Napi),
            "block" => Ok(WaitMode::Block),
            other => Err(format!("unknown wait mode {other:?} (spin|napi|block)")),
        }
    }
}

/// Runtime settings. Every field has a default; most programs only set `wait`.
pub struct IoBuilder {
    wait: WaitMode,
    napi_busy_poll_us: u32,
    ring_entries: u32,
    recv_buffers: u16,
    recv_buffer_len: usize,
    rx_buffer: usize,
    max_connections: usize,
    tls_config: Option<Arc<ClientConfig>>,
    external_dns: bool,
}

impl Default for IoBuilder {
    fn default() -> Self {
        IoBuilder {
            wait: WaitMode::Spin,
            napi_busy_poll_us: 5_000,
            ring_entries: 256,
            recv_buffers: 256,
            recv_buffer_len: 16 * 1024,
            rx_buffer: 128 * 1024,
            max_connections: 128,
            tls_config: None,
            external_dns: false,
        }
    }
}

impl IoBuilder {
    /// How the loop waits (default `Spin`).
    pub fn wait(mut self, wait: WaitMode) -> Self {
        self.wait = wait;
        self
    }

    /// NAPI busy-poll budget per wait, in µs (default 5 ms). Each wait is capped at a quarter of
    /// it, so a wait always ends while still busy polling and the thread never actually sleeps.
    ///
    /// Keep it under 10 ms: the kernel (seen on 6.18) ends an io_uring busy poll after 10 ms
    /// whatever budget is asked for, then sleeps until a completion arrives. With a 1 s budget
    /// (waits capped at 250 ms) a quiet feed left the thread asleep ~65% of the time, with
    /// packets handled by the NIC interrupt on another core.
    pub fn napi_busy_poll_us(mut self, us: u32) -> Self {
        self.napi_busy_poll_us = us;
        self
    }

    /// Most connections open at once, counting every connection of every registration
    /// (default 128).
    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = n;
        self
    }

    /// Per-connection receive buffer, which is also the largest message accepted
    /// (default 128 KiB).
    pub fn rx_buffer(mut self, bytes: usize) -> Self {
        self.rx_buffer = bytes;
        self
    }

    /// Kernel receive buffers shared by all connections: `count` (a power of two) of `len`
    /// bytes each (default 256 × 16 KiB).
    pub fn recv_buffers(mut self, count: u16, len: usize) -> Self {
        self.recv_buffers = count;
        self.recv_buffer_len = len;
        self
    }

    /// io_uring submission queue size (default 256).
    pub fn ring_entries(mut self, n: u32) -> Self {
        self.ring_entries = n;
        self
    }

    /// TLS config for `wss://` (default: webpki roots with the provider picked by the crate
    /// feature). Pass your own for native certs, a different provider, or client auth.
    pub fn tls_config(mut self, config: Arc<ClientConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Re-resolves DNS through a [`DnsPort`] rather than the crate's helper thread, for a program that
    /// must control every thread it runs. Collect the port with [`Io::take_dns_port`], and keep
    /// answering it: a connection whose lookup is never answered waits for it. `.open()` still
    /// resolves on the calling thread during setup.
    pub fn external_dns(mut self) -> Self {
        self.external_dns = true;
        self
    }

    /// Creates the runtime. Call it on the thread that will `run` it (io_uring's single-issuer
    /// mode ties the ring to its creating thread); `io_uring_core::spawn` does that for you.
    pub fn build<A: 'static>(self, app: A) -> io::Result<Io<A>> {
        let ring = match self.wait {
            // DEFER_TASKRUN runs completions only when the thread enters the kernel, which Spin never does.
            WaitMode::Spin => IoUring::builder().setup_single_issuer().build(self.ring_entries)?,
            WaitMode::Napi | WaitMode::Block => {
                IoUring::builder().setup_single_issuer().setup_defer_taskrun().build(self.ring_entries)?
            }
        };
        if self.wait == WaitMode::Napi {
            let mut napi = types::Napi::new().set_busy_poll_timeout(self.napi_busy_poll_us).set_prefer_busy_poll(true);
            ring.submitter().register_napi(&mut napi)?;
        }
        let bufs = BufRing::new(&ring.submitter(), self.recv_buffers, self.recv_buffer_len, BUF_GROUP)?;
        let efd = if self.wait == WaitMode::Spin { -1 } else { socket::eventfd()? };
        let waker = Arc::new(Waker::new(efd));
        let tls = self.tls_config.or_else(default_tls);
        let mut core = Core::new(ring, bufs.bgid(), waker, tls, self.max_connections, self.rx_buffer);
        core.arm_wake();
        let dns_port = self.external_dns.then(|| {
            let (resolver, port) = Resolver::external(core.waker.clone());
            core.resolver = Some(resolver);
            port
        });
        let max_wait = (self.wait == WaitMode::Napi)
            .then(|| Duration::from_micros(self.napi_busy_poll_us as u64 / 4).max(Duration::from_micros(10)));
        Ok(Io { core, bufs, app, channels: Vec::new(), wait: self.wait, max_wait, dns_port })
    }
}

#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
fn default_tls() -> Option<Arc<ClientConfig>> {
    Some(crate::tls::default_tls_config())
}

#[cfg(not(any(feature = "ring", feature = "aws-lc-rs")))]
fn default_tls() -> Option<Arc<ClientConfig>> {
    None
}

impl Io<()> {
    /// Runtime settings: `Io::builder().wait(WaitMode::Napi).build(app)`.
    pub fn builder() -> IoBuilder {
        IoBuilder::default()
    }
}

/// The runtime: owns your app state `A`, the connections, and the io_uring event loop.
pub struct Io<A> {
    // Field order matters: `core` (the ring) drops before `bufs`, unregistering the buffer
    // ring before its memory is freed.
    core: Core<A>,
    bufs: BufRing,
    app: A,
    channels: Vec<Box<dyn Inbox<A>>>,
    wait: WaitMode,
    /// Longest single wait. In NAPI mode it is well inside the busy-poll budget, so every wait
    /// ends (by completion or timeout) while still polling and the thread never actually sleeps.
    max_wait: Option<Duration>,
    dns_port: Option<DnsPort>,
}

impl<A: 'static> Io<A> {
    /// Runtime with default settings (spin wait).
    pub fn new(app: A) -> io::Result<Self> {
        IoBuilder::default().build(app)
    }

    /// Registers a WebSocket endpoint (`ws://` or `wss://`). DNS is resolved by `.open()`.
    pub fn ws(&mut self, url: &str) -> WsBuilder<'_, A> {
        WsBuilder::new(&mut self.core, url, true)
    }

    /// Called when any connection opens (after the upgrade, after its `.subscribe` messages
    /// are queued): dynamic subscriptions, logins, "connected" status.
    pub fn on_open(&mut self, f: impl FnMut(&mut A, &mut Cx<'_, A>, ConnId) + 'static) {
        self.core.on_open = Some(Box::new(f));
    }

    /// Called when an open connection closes. It reconnects automatically afterwards
    /// (unless the reason is `Removed`).
    pub fn on_close(&mut self, f: impl FnMut(&mut A, &mut Cx<'_, A>, ConnId, CloseReason) + 'static) {
        self.core.on_close = Some(Box::new(f));
    }

    /// Called when a connection attempt fails before opening (connect, TLS or upgrade).
    /// Useful for logging; the IO retries with backoff either way.
    pub fn on_connect_failed(&mut self, f: impl FnMut(&mut A, &mut Cx<'_, A>, ConnId, CloseReason) + 'static) {
        self.core.on_connect_failed = Some(Box::new(f));
    }

    /// Runs `f` every `period` (first run one period from now). Missed runs are skipped, not
    /// bunched up.
    pub fn every(&mut self, period: Duration, f: impl FnMut(&mut A, &mut Cx<'_, A>) + 'static) {
        assert!(!period.is_zero(), "period must be non-zero");
        self.core.add_every(period, Box::new(f));
    }

    /// A queue from another thread into the IO thread: every value sent is passed to `f` on the
    /// next loop pass. One producer per channel; make one per producing thread.
    pub fn channel<T: Send + 'static>(
        &mut self,
        capacity: usize,
        f: impl FnMut(&mut A, &mut Cx<'_, A>, T) + 'static,
    ) -> Sender<T> {
        let (tx, inbox) = channel::channel(capacity, self.core.waker.clone(), f);
        self.channels.push(inbox);
        tx
    }

    /// The port from [`IoBuilder::external_dns`], once. `None` without that setting.
    pub fn take_dns_port(&mut self) -> Option<DnsPort> {
        self.dns_port.take()
    }

    /// Your app state.
    pub fn app(&self) -> &A {
        &self.app
    }

    /// Your app state, mutably.
    pub fn app_mut(&mut self) -> &mut A {
        &mut self.app
    }

    /// Runs the event loop forever. Returns only on an io_uring error.
    pub fn run(&mut self) -> io::Result<()> {
        loop {
            self.turn()?;
        }
    }

    /// One loop pass: wait, completions, channels, DNS results, timers.
    fn turn(&mut self) -> io::Result<()> {
        self.wait_for_work()?;
        self.core.now = Instant::now();
        while let Some((user_data, res, flags)) = self.next_cqe() {
            self.on_cqe(user_data, res, flags);
            settle(&mut self.app, &mut self.core);
            // A busy feed mustn't hold up channel work, such as an order, until the batch of
            // completions ends. Costs one atomic load per channel.
            self.drain_channels();
        }
        self.drain_channels();
        self.core.poll_resolver();
        self.run_timers();
        Ok(())
    }

    #[inline]
    fn drain_channels(&mut self) {
        for ch in self.channels.iter_mut() {
            ch.drain(&mut self.app, &mut self.core);
        }
    }

    #[inline]
    fn next_cqe(&mut self) -> Option<(u64, i32, u32)> {
        self.core.ring.completion().next().map(|c| (c.user_data(), c.result(), c.flags()))
    }

    fn wait_for_work(&mut self) -> io::Result<()> {
        let core = &mut self.core;
        if self.wait == WaitMode::Spin {
            if !core.ring.submission().is_empty() {
                core.ring.submit()?;
            }
            return Ok(());
        }
        let now = Instant::now();
        let deadline = core.timers.next_deadline();
        if !core.ring.completion().is_empty() || deadline.is_some_and(|d| d <= now) {
            return ok_if_transient(core.ring.submit());
        }
        // Announce the sleep, then re-check for work published before the announcement
        // (pairs with the fence in Waker::wake).
        core.waker.sleeping.store(true, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let pending = self.channels.iter().any(|c| c.has_items()) || core.resolver.as_ref().is_some_and(|r| r.has_results());
        let timeout = match (deadline.map(|d| d - now), self.max_wait) {
            (Some(t), Some(cap)) => Some(t.min(cap)),
            (t, cap) => t.or(cap),
        };
        let res = if pending {
            core.ring.submit()
        } else if let Some(t) = timeout {
            let ts = types::Timespec::from(t);
            let args = types::SubmitArgs::new().timespec(&ts);
            core.ring.submitter().submit_with_args(1, &args)
        } else {
            core.ring.submit_and_wait(1)
        };
        core.waker.sleeping.store(false, Ordering::Relaxed);
        ok_if_transient(res)
    }

    fn on_cqe(&mut self, user_data: u64, res: i32, flags: u32) {
        let Some(tok) = Token::decode(user_data) else { return };
        let Io { core, bufs, app, .. } = self;
        if tok.op == Op::Wake {
            core.arm_wake();
            return;
        }
        let slot = tok.slot as usize;
        if slot >= core.conns.len() || core.conns[slot].gen != tok.gen {
            // From a socket that has since closed: give back what it holds, nothing else.
            match tok.op {
                Op::Recv => {
                    if let Some(bid) = cqueue::buffer_select(flags) {
                        bufs.recycle(bid);
                    }
                }
                Op::Send if slot < core.conns.len() => core.release_stale_send(slot),
                _ => {}
            }
            return;
        }
        match tok.op {
            Op::Connect => core.on_connected(slot, res),
            Op::Recv => on_recv(app, core, bufs, slot, res, flags),
            Op::Send => core.on_send(slot, res),
            Op::Wake => {}
        }
    }

    fn run_timers(&mut self) {
        let Io { core, app, .. } = self;
        let now = Instant::now();
        core.now = now;
        for _ in 0..MAX_TIMERS_PER_TURN {
            let Some(key) = core.timers.pop_due(now) else { break };
            match key {
                TimerKey::Conn { slot, seq } => {
                    let s = slot as usize;
                    if s < core.conns.len() && core.conns[s].wake_seq == seq {
                        core.conns[s].wake_at = None;
                        core.service(s, now);
                    }
                }
                TimerKey::Every(i) => {
                    let i = i as usize;
                    if let Some(mut f) = core.every[i].f.take() {
                        f(app, &mut Cx::new(core));
                        core.every[i].f = Some(f);
                    }
                    let e = &mut core.every[i];
                    e.next += e.period;
                    if e.next <= now {
                        e.next = now + e.period;
                    }
                    let next = e.next;
                    core.timers.push(next, TimerKey::Every(i as u32));
                }
                TimerKey::After(i) => {
                    if let Some(f) = core.after[i as usize].take() {
                        core.after_free.push(i);
                        f(app, &mut Cx::new(core));
                    }
                }
            }
            settle(app, core);
        }
    }
}

fn ok_if_transient(res: io::Result<usize>) -> io::Result<()> {
    match res {
        Ok(_) => Ok(()),
        Err(e) if matches!(e.raw_os_error(), Some(libc::ETIME | libc::EINTR | libc::EBUSY)) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Flushes queued sends and applies deferred closes until both are drained. Runs after every
/// unit of work (completion, channel item, timer).
pub(crate) fn settle<A: 'static>(app: &mut A, core: &mut Core<A>) {
    loop {
        core.flush_dirty();
        let Some((slot, reason)) = core.pending_close.pop() else { break };
        apply_close(app, core, slot as usize, reason);
    }
}

fn apply_close<A: 'static>(app: &mut A, core: &mut Core<A>, slot: usize, reason: CloseReason) {
    let now = Instant::now();
    let c = &mut core.conns[slot];
    c.closing = None;
    let was_open = matches!(c.state, State::Open { .. });
    let was_active = c.is_connected() || matches!(c.state, State::Connecting { .. });
    if !was_active && reason != CloseReason::Removed {
        return;
    }
    if c.fd >= 0 {
        socket::shutdown_and_close(c.fd);
        c.fd = -1;
    }
    c.gen = c.gen.wrapping_add(1);
    c.reset_session();
    let id = c.id(slot);
    let gi = c.group as usize;

    if reason == CloseReason::Removed {
        c.state = State::Free;
        core.groups[gi].conns.retain(|&s| s as usize != slot);
    } else {
        let retry_at = match reason {
            CloseReason::MaxAge => now,
            CloseReason::Reconnect => {
                core.move_to_other_ip(slot);
                now
            }
            _ => {
                let c = &mut core.conns[slot];
                c.failures += 1;
                let failures = c.failures;
                core.move_to_other_ip(slot);
                now + backoff(failures, jitter())
            }
        };
        core.conns[slot].state = State::Idle { retry_at };
        if reason.is_failure() {
            core.maybe_refresh_dns(gi, now);
        }
    }
    core.schedule_conn(slot);

    if was_open {
        if let Some(mut h) = core.on_close.take() {
            h(app, &mut Cx::new(core), id, reason);
            core.on_close.get_or_insert(h);
        }
    } else if reason != CloseReason::Removed {
        if let Some(mut h) = core.on_connect_failed.take() {
            h(app, &mut Cx::new(core), id, reason);
            core.on_connect_failed.get_or_insert(h);
        }
    }
}

/// When a receive completion was handled, on both clocks. Taken together, so they mark one moment.
#[derive(Clone, Copy)]
struct RecvTime {
    us: u64,
    ticks: u64,
}

#[inline]
fn on_recv<A: 'static>(app: &mut A, core: &mut Core<A>, bufs: &mut BufRing, slot: usize, res: i32, flags: u32) {
    let recv = RecvTime { us: now_us(), ticks: now_ticks() };
    if !cqueue::more(flags) {
        core.conns[slot].recv_armed = false;
    }
    if res > 0 {
        let Some(bid) = cqueue::buffer_select(flags) else {
            core.request_close(slot, CloseReason::RecvError(0));
            return;
        };
        if core.conns[slot].closing.is_some() {
            bufs.recycle(bid);
            return;
        }
        let bytes = bufs.get(bid, res as usize);
        let mut rx = mem::take(&mut core.conns[slot].rx);
        let result = process_bytes(app, core, slot, &mut rx, bytes, recv);
        core.conns[slot].rx = rx;
        bufs.recycle(bid);
        core.conns[slot].last_rx_us = recv.us;
        if let Err(reason) = result {
            core.request_close(slot, reason);
            return;
        }
        // The handshake (or a key update) may have produced TLS records to send.
        if core.conns[slot].tls.as_ref().is_some_and(|t| t.wants_write()) {
            core.mark_dirty(slot);
        }
    } else if res == 0 {
        core.request_close(slot, CloseReason::PeerClosed);
        return;
    } else if res != -libc::ENOBUFS {
        core.request_close(slot, CloseReason::RecvError(-res));
        return;
    }
    // ENOBUFS (buffer ring momentarily empty) or the multishot ended: re-arm.
    let c = &core.conns[slot];
    if !c.recv_armed && c.closing.is_none() && c.is_connected() {
        core.push_recv(slot);
    }
}

/// Feeds one recv completion through TLS (chunk by chunk) and the protocol, so the first
/// message reaches its handler before the rest of the batch is decrypted.
#[inline]
fn process_bytes<A: 'static>(
    app: &mut A,
    core: &mut Core<A>,
    slot: usize,
    rx: &mut RxSide,
    mut bytes: &[u8],
    recv: RecvTime,
) -> Result<(), CloseReason> {
    if core.conns[slot].tls.is_some() {
        while !bytes.is_empty() {
            let tls = core.conns[slot].tls.as_mut().ok_or(CloseReason::Tls)?;
            tls.read_chunk(&mut bytes, &mut rx.buf)?;
            advance(app, core, slot, rx, recv)?;
            if core.conns[slot].closing.is_some() {
                return Ok(());
            }
        }
    } else {
        while !bytes.is_empty() {
            if !rx.buf.ensure_spare() {
                return Err(CloseReason::RxOverflow);
            }
            let spare = rx.buf.spare_mut();
            let n = spare.len().min(bytes.len());
            spare[..n].copy_from_slice(&bytes[..n]);
            rx.buf.commit(n);
            bytes = &bytes[n..];
            advance(app, core, slot, rx, recv)?;
            if core.conns[slot].closing.is_some() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Moves the connection forward with whatever plaintext is buffered: TLS done → send upgrade,
/// upgrade response → open, then WebSocket frames → handlers.
#[inline]
fn advance<A: 'static>(app: &mut A, core: &mut Core<A>, slot: usize, rx: &mut RxSide, recv: RecvTime) -> Result<(), CloseReason> {
    if let State::Tls { .. } = core.conns[slot].state {
        if core.conns[slot].tls.as_ref().is_some_and(|t| t.is_handshaking()) {
            return Ok(());
        }
        core.send_upgrade(slot, Instant::now())?;
        core.mark_dirty(slot);
        core.schedule_conn(slot);
    }

    if let State::Upgrade { .. } = core.conns[slot].state {
        match ws::parse_upgrade_response(rx.buf.data()) {
            Upgrade::Incomplete => return Ok(()),
            Upgrade::Rejected { status } => return Err(CloseReason::UpgradeRejected(status)),
            Upgrade::Accepted { header_len } => {
                rx.buf.consume(header_len);
                core.on_ws_open(slot, Instant::now())?;
                fire_open(app, core, slot);
                if core.conns[slot].closing.is_some() {
                    return Ok(());
                }
            }
        }
    }

    if !matches!(core.conns[slot].state, State::Open { .. }) {
        return Ok(());
    }

    let max_msg = rx.buf.capacity();
    let RxSide { buf, frag, frag_binary, frag_active } = rx;
    loop {
        let frame = match ws::parse_frame(buf.data()) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => return Err(CloseReason::Ws(e)),
        };
        let len = frame.len;
        match frame.opcode {
            OpCode::Text | OpCode::Binary => {
                if *frag_active {
                    return Err(CloseReason::Ws(WsError::BadFragment));
                }
                let binary = frame.opcode == OpCode::Binary;
                if frame.fin {
                    dispatch(app, core, slot, frame.payload, binary, recv);
                } else {
                    frag.clear();
                    frag.extend_from_slice(frame.payload);
                    *frag_binary = binary;
                    *frag_active = true;
                }
            }
            OpCode::Continuation => {
                if !*frag_active {
                    return Err(CloseReason::Ws(WsError::BadFragment));
                }
                if frag.len() + frame.payload.len() > max_msg {
                    return Err(CloseReason::MessageTooLarge);
                }
                frag.extend_from_slice(frame.payload);
                if frame.fin {
                    *frag_active = false;
                    dispatch(app, core, slot, frag, *frag_binary, recv);
                    frag.clear();
                }
            }
            OpCode::Ping => {
                core.conns[slot].queue_ws(OpCode::Pong, frame.payload)?;
                core.mark_dirty(slot);
            }
            OpCode::Pong => core.conns[slot].ping.on_reply(),
            OpCode::Close => return Err(CloseReason::PeerClosed),
        }
        buf.consume(len);
        if core.conns[slot].closing.is_some() {
            break;
        }
    }
    Ok(())
}

/// Hands one message to its registration's handler, then sends whatever the handler queued.
#[inline]
fn dispatch<A: 'static>(app: &mut A, core: &mut Core<A>, slot: usize, data: &[u8], binary: bool, recv: RecvTime) {
    let (gi, epoch) = {
        let c = &core.conns[slot];
        (c.group as usize, c.epoch)
    };
    let is_reply = core.groups[gi].ping.as_ref().and_then(|p| p.expect.as_deref()).is_some_and(|e| e == data);
    if is_reply {
        core.conns[slot].ping.on_reply();
        return;
    }
    let Some(mut handler) = core.handlers[gi].take() else { return };
    let msg = Msg { data, conn: ConnId { slot: slot as u16, epoch }, handle: Handle { group: gi as u32 }, recv_ts: recv.us, recv_ticks: recv.ticks, binary };
    handler(app, &mut Cx::new(core), msg);
    if !core.groups[gi].removed {
        core.handlers[gi] = Some(handler);
    }
    core.flush_dirty();
}

fn fire_open<A: 'static>(app: &mut A, core: &mut Core<A>, slot: usize) {
    let id = core.conns[slot].id(slot);
    if let Some(mut h) = core.on_open.take() {
        h(app, &mut Cx::new(core), id);
        core.on_open.get_or_insert(h);
    }
    core.flush_dirty();
}

#[cfg(test)]
mod tests {
    use super::*;

    // Needs a kernel with io_uring, so this first runs on the host.
    #[test]
    fn external_dns_hands_out_its_port_once() {
        let mut with_port = Io::builder().wait(WaitMode::Block).external_dns().build(()).unwrap();
        assert!(matches!(with_port.core.resolver, Some(Resolver::External { .. })));
        assert!(with_port.take_dns_port().is_some());
        assert!(with_port.take_dns_port().is_none());

        let mut without = Io::builder().wait(WaitMode::Block).build(()).unwrap();
        assert!(without.core.resolver.is_none(), "the helper thread still starts lazily");
        assert!(without.take_dns_port().is_none());
    }
}
