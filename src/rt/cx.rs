use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::error::{CloseReason, SendError};
use crate::types::{ConnId, Handle, Target};
use crate::ws::OpCode;

use super::builder::WsBuilder;
use super::conn::State;
use super::engine::Core;

/// What handlers use to act on the IO: send, register or drop connections, set timers.
///
/// Sends made inside a handler go out as soon as that handler returns: one `send()` per
/// connection touched, straight to the socket.
pub struct Cx<'a, A> {
    pub(crate) core: &'a mut Core<A>,
}

/// A snapshot of one connection.
#[derive(Debug, Clone, Copy)]
pub struct ConnInfo {
    /// Its registration.
    pub handle: Handle,
    /// Open (upgrade done) right now.
    pub open: bool,
    /// When the current session opened.
    pub open_since: Option<Instant>,
    /// The address it is connected (or connecting) to.
    pub peer: Option<SocketAddr>,
    /// Unix µs of the last data received.
    pub last_rx_us: u64,
    /// Consecutive failed attempts (reset when a session opens).
    pub failures: u32,
}

impl<'a, A: 'static> Cx<'a, A> {
    pub(crate) fn new(core: &'a mut Core<A>) -> Self {
        Cx { core }
    }

    /// Sends a text message to a connection, or to any open connection of a registration.
    #[inline]
    pub fn send_text(&mut self, to: impl Into<Target>, text: impl AsRef<[u8]>) -> Result<ConnId, SendError> {
        self.core.send_ws(to.into(), OpCode::Text, text.as_ref())
    }

    /// Sends a binary message.
    #[inline]
    pub fn send_binary(&mut self, to: impl Into<Target>, data: impl AsRef<[u8]>) -> Result<ConnId, SendError> {
        self.core.send_ws(to.into(), OpCode::Binary, data.as_ref())
    }

    /// Drops the current session and reconnects right away, to a different IP when the host
    /// has several (e.g. to replace a connection that keeps losing the race).
    pub fn reconnect(&mut self, conn: ConnId) {
        if let Some(s) = self.core.live_slot(conn) {
            self.core.request_close(s, CloseReason::Reconnect);
        }
    }

    /// Closes every connection of a registration for good.
    pub fn remove(&mut self, handle: Handle) {
        self.core.remove_group(handle.group as usize);
    }

    /// Registers a WebSocket endpoint at runtime. DNS is resolved on a helper thread; the
    /// connections start once it returns.
    pub fn ws(&mut self, url: &str) -> WsBuilder<'_, A> {
        WsBuilder::new(self.core, url, false)
    }

    /// Runs `f` once after `delay`.
    pub fn after(&mut self, delay: Duration, f: impl FnOnce(&mut A, &mut Cx<'_, A>) + 'static) {
        self.core.add_after(delay, Box::new(f));
    }

    /// The loop's cached clock (updated every pass).
    #[inline]
    pub fn now(&self) -> Instant {
        self.core.now
    }

    /// Whether the connection is open right now.
    #[inline]
    pub fn is_open(&self, conn: ConnId) -> bool {
        self.core.live_slot(conn).is_some_and(|s| self.core.conns[s].is_open())
    }

    /// A snapshot of one connection, or `None` if the id is no longer valid.
    pub fn info(&self, conn: ConnId) -> Option<ConnInfo> {
        let c = &self.core.conns[self.core.live_slot(conn)?];
        Some(ConnInfo {
            handle: Handle { group: c.group },
            open: c.is_open(),
            open_since: match c.state {
                State::Open { since } => Some(since),
                _ => None,
            },
            peer: c.peer,
            last_rx_us: c.last_rx_us,
            failures: c.failures,
        })
    }

    /// The connections of a registration.
    pub fn conns(&self, handle: Handle) -> impl Iterator<Item = ConnId> + '_ {
        let core: &Core<A> = self.core;
        let slots: &[u16] = core.groups.get(handle.group as usize).map_or(&[], |g| &g.conns);
        slots.iter().map(move |&s| core.conns[s as usize].id(s as usize))
    }
}
