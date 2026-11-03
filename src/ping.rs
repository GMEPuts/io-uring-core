use std::time::{Duration, Instant};

const DEFAULT_EVERY: Duration = Duration::from_secs(20);
const MAX_REPLY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub(crate) enum PingKind {
    Frame(Box<[u8]>),
    Text(Box<[u8]>),
    Binary(Box<[u8]>),
}

/// Client pings for one connection.
///
/// ```ignore
/// Ping::frame().every(secs(20))                       // WebSocket ping frame
/// Ping::text("ping").every(secs(25)).expect("pong")   // text ping, reply swallowed
/// Ping::text(r#"{"op":"ping"}"#).every(secs(20))      // JSON ping
/// ```
///
/// - The first ping goes out one interval after the connection opens, never immediately.
/// - A ping is skipped when something else was sent within the interval; `.always()` turns
///   that off.
/// - With `.expect(reply)` (and always for `Ping::frame()`, whose reply is the protocol pong),
///   a missing reply closes the connection with `PingTimeout` and it reconnects.
#[derive(Debug, Clone)]
pub struct Ping {
    pub(crate) kind: PingKind,
    pub(crate) every: Duration,
    pub(crate) expect: Option<Box<[u8]>>,
    pub(crate) when_idle: bool,
    pub(crate) reply_timeout: Option<Duration>,
}

impl Ping {
    fn new(kind: PingKind) -> Self {
        Ping { kind, every: DEFAULT_EVERY, expect: None, when_idle: true, reply_timeout: None }
    }

    /// A WebSocket ping frame (empty payload). The server's pong is expected.
    pub fn frame() -> Self {
        Self::new(PingKind::Frame(Box::new([])))
    }

    /// A text message, e.g. `"ping"` or a JSON ping.
    pub fn text(msg: impl AsRef<str>) -> Self {
        Self::new(PingKind::Text(msg.as_ref().as_bytes().into()))
    }

    /// A binary message.
    pub fn binary(msg: impl AsRef<[u8]>) -> Self {
        Self::new(PingKind::Binary(msg.as_ref().into()))
    }

    /// Interval between pings (default 20 s).
    pub fn every(mut self, every: Duration) -> Self {
        assert!(!every.is_zero(), "ping interval must be non-zero");
        self.every = every;
        self
    }

    /// The server's exact reply. Matching messages are swallowed (the handler never sees them),
    /// and a missing reply closes the connection.
    pub fn expect(mut self, reply: impl AsRef<[u8]>) -> Self {
        self.expect = Some(reply.as_ref().into());
        self
    }

    /// Ping on schedule even when other messages were sent recently.
    pub fn always(mut self) -> Self {
        self.when_idle = false;
        self
    }

    /// How long to wait for the expected reply (default: the interval, at most 10 s).
    pub fn reply_timeout(mut self, timeout: Duration) -> Self {
        self.reply_timeout = Some(timeout);
        self
    }

    fn expects_reply(&self) -> bool {
        self.expect.is_some() || matches!(self.kind, PingKind::Frame(_))
    }

    fn timeout(&self) -> Duration {
        self.reply_timeout.unwrap_or(self.every.min(MAX_REPLY_TIMEOUT))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PingAction {
    Wait,
    Send,
    TimedOut,
}

/// Per-connection ping schedule. Pure: the caller supplies the clock.
#[derive(Debug, Default)]
pub(crate) struct PingState {
    next_at: Option<Instant>,
    reply_deadline: Option<Instant>,
}

impl PingState {
    pub(crate) fn on_open(&mut self, p: &Ping, now: Instant) {
        self.next_at = Some(now + p.every);
        self.reply_deadline = None;
    }

    pub(crate) fn reset(&mut self) {
        *self = PingState::default();
    }

    pub(crate) fn on_reply(&mut self) {
        self.reply_deadline = None;
    }

    pub(crate) fn next_wake(&self) -> Option<Instant> {
        match (self.next_at, self.reply_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// What to do at `now`. `last_tx` is when anything was last sent on the connection.
    pub(crate) fn poll(&mut self, p: &Ping, now: Instant, last_tx: Instant) -> PingAction {
        if self.reply_deadline.is_some_and(|d| now >= d) {
            return PingAction::TimedOut;
        }
        let Some(next) = self.next_at else { return PingAction::Wait };
        if now < next {
            return PingAction::Wait;
        }
        if p.when_idle && last_tx + p.every > now {
            self.next_at = Some(last_tx + p.every);
            return PingAction::Wait;
        }
        // Drift-free: keep the original cadence, but never burst to catch up after a stall.
        let mut following = next + p.every;
        if following <= now {
            following = now + p.every;
        }
        self.next_at = Some(following);
        if p.expects_reply() && self.reply_deadline.is_none() {
            self.reply_deadline = Some(now + p.timeout());
        }
        PingAction::Send
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn first_ping_after_one_interval_then_on_cadence() {
        let t0 = Instant::now();
        let p = Ping::text("ping").every(s(20));
        let mut st = PingState::default();
        st.on_open(&p, t0);
        assert_eq!(st.next_wake(), Some(t0 + s(20)));
        assert_eq!(st.poll(&p, t0 + s(19), t0), PingAction::Wait);
        assert_eq!(st.poll(&p, t0 + s(20), t0), PingAction::Send);
        assert_eq!(st.next_wake(), Some(t0 + s(40)));
        // Sent a bit late: the cadence doesn't drift.
        assert_eq!(st.poll(&p, t0 + s(41), t0 + s(20)), PingAction::Send);
        assert_eq!(st.next_wake(), Some(t0 + s(60)));
    }

    #[test]
    fn skips_missed_beats_instead_of_bursting() {
        let t0 = Instant::now();
        let p = Ping::text("ping").every(s(10));
        let mut st = PingState::default();
        st.on_open(&p, t0);
        assert_eq!(st.poll(&p, t0 + s(95), t0), PingAction::Send);
        assert_eq!(st.next_wake(), Some(t0 + s(105)));
        assert_eq!(st.poll(&p, t0 + s(96), t0 + s(95)), PingAction::Wait);
    }

    #[test]
    fn idle_only_defers_after_recent_traffic() {
        let t0 = Instant::now();
        let p = Ping::text("ping").every(s(20));
        let mut st = PingState::default();
        st.on_open(&p, t0);
        // Something was sent at t0+15: the ping moves to t0+35.
        assert_eq!(st.poll(&p, t0 + s(20), t0 + s(15)), PingAction::Wait);
        assert_eq!(st.next_wake(), Some(t0 + s(35)));
        assert_eq!(st.poll(&p, t0 + s(35), t0 + s(15)), PingAction::Send);

        let p = Ping::text("ping").every(s(20)).always();
        let mut st = PingState::default();
        st.on_open(&p, t0);
        assert_eq!(st.poll(&p, t0 + s(20), t0 + s(19)), PingAction::Send);
    }

    #[test]
    fn expected_reply_times_out_unless_received() {
        let t0 = Instant::now();
        let p = Ping::text("ping").every(s(20)).expect("pong");
        let mut st = PingState::default();
        st.on_open(&p, t0);
        assert_eq!(st.poll(&p, t0 + s(20), t0), PingAction::Send);
        assert_eq!(st.next_wake(), Some(t0 + s(30))); // reply deadline: min(every, 10 s)
        assert_eq!(st.poll(&p, t0 + s(30), t0 + s(20)), PingAction::TimedOut);

        let mut st = PingState::default();
        st.on_open(&p, t0);
        assert_eq!(st.poll(&p, t0 + s(20), t0), PingAction::Send);
        st.on_reply();
        assert_eq!(st.poll(&p, t0 + s(30), t0 + s(20)), PingAction::Wait);
    }

    #[test]
    fn frame_pings_expect_pongs_text_pings_do_not() {
        assert!(Ping::frame().expects_reply());
        assert!(!Ping::text("ping").expects_reply());
        assert_eq!(Ping::frame().every(s(5)).timeout(), s(5));
        assert_eq!(Ping::frame().every(s(60)).timeout(), s(10));
        assert_eq!(Ping::frame().reply_timeout(s(3)).timeout(), s(3));
    }
}
