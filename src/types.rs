use std::fmt;

/// One connection. Stays the same across reconnects; becomes invalid after `cx.remove`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId {
    pub(crate) slot: u16,
    pub(crate) epoch: u32,
}

impl ConnId {
    /// Small dense index (< max connections), handy for per-connection arrays in the app.
    pub fn index(self) -> usize {
        self.slot as usize
    }
}

impl fmt::Display for ConnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "conn{}", self.slot)
    }
}

/// One registration (what `.open()` returns): one or more identical connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Handle {
    pub(crate) group: u32,
}

impl Handle {
    /// Dense index in registration order.
    pub fn index(self) -> usize {
        self.group as usize
    }
}

/// Where a send goes: a specific connection, or any open connection of a registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// This connection.
    Conn(ConnId),
    /// Any open connection of this registration (round robin).
    Any(Handle),
}

impl From<ConnId> for Target {
    fn from(c: ConnId) -> Self {
        Target::Conn(c)
    }
}

impl From<Handle> for Target {
    fn from(h: Handle) -> Self {
        Target::Any(h)
    }
}

/// A complete WebSocket message, borrowed from the connection's receive buffer.
#[derive(Debug, Clone, Copy)]
pub struct Msg<'a> {
    /// Payload (fragments already joined).
    pub data: &'a [u8],
    /// Connection it arrived on.
    pub conn: ConnId,
    /// Registration it belongs to.
    pub handle: Handle,
    /// Unix µs when the receive completion was handled.
    pub recv_ts: u64,
    /// The tick counter at the same moment, from [`crate::clock::now_ticks`].
    pub recv_ticks: u64,
    /// Binary frame (otherwise text).
    pub binary: bool,
}

impl<'a> Msg<'a> {
    /// The payload as UTF-8, for text messages.
    pub fn text(&self) -> Option<&'a str> {
        if self.binary {
            None
        } else {
            std::str::from_utf8(self.data).ok()
        }
    }
}
