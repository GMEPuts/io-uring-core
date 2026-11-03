use std::time::Duration;

use crate::error::Error;
use crate::ping::Ping;
use crate::types::{Handle, Msg};
use crate::url::{Url, UrlError};

use super::cx::Cx;
use super::engine::{Core, MsgHandler, WsSpec};

const DEFAULT_STALE: Duration = Duration::from_secs(30);

/// Registers a WebSocket endpoint. Everything is optional except the URL; `.open()` starts it.
///
/// ```ignore
/// io.ws("wss://fstream.binance.com/ws")
///     .subscribe(SUB)
///     .ping(Ping::frame().every(secs(20)))
///     .on_message(|app, cx, msg| app.on_data(msg))
///     .open()?;
/// ```
pub struct WsBuilder<'a, A> {
    core: &'a mut Core<A>,
    sync_resolve: bool,
    url: Result<Url, UrlError>,
    headers: Vec<(String, String)>,
    subscribe: Vec<Box<[u8]>>,
    ping: Option<Ping>,
    stale_after: Option<Duration>,
    max_age: Option<Duration>,
    connections: usize,
    handler: Option<MsgHandler<A>>,
}

impl<'a, A: 'static> WsBuilder<'a, A> {
    pub(crate) fn new(core: &'a mut Core<A>, url: &str, sync_resolve: bool) -> Self {
        WsBuilder {
            core,
            sync_resolve,
            url: Url::parse(url),
            headers: Vec::new(),
            subscribe: Vec::new(),
            ping: None,
            stale_after: Some(DEFAULT_STALE),
            max_age: None,
            connections: 1,
            handler: None,
        }
    }

    /// A text message sent after every successful (re)connect. Call again for more.
    pub fn subscribe(mut self, msg: impl AsRef<[u8]>) -> Self {
        self.subscribe.push(msg.as_ref().into());
        self
    }

    /// Client pings. Without one, no pings are sent; server pings are always answered.
    pub fn ping(mut self, ping: Ping) -> Self {
        self.ping = Some(ping);
        self
    }

    /// Reconnect when nothing is received for this long (default 30 s).
    pub fn stale_after(mut self, d: Duration) -> Self {
        self.stale_after = Some(d);
        self
    }

    /// Never reconnect for silence (for streams that can legitimately go quiet).
    pub fn never_stale(mut self) -> Self {
        self.stale_after = None;
        self
    }

    /// An extra upgrade header (e.g. `User-Agent`, auth). Replaces the default User-Agent.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Opens `n` identical connections sharing the handler, spread over the resolved IPs
    /// (default 1).
    pub fn connections(mut self, n: usize) -> Self {
        self.connections = n.max(1);
        self
    }

    /// Rotates each connection after this long (staggered across the registration's
    /// connections). E.g. Binance drops connections at 24 h.
    pub fn max_age(mut self, d: Duration) -> Self {
        self.max_age = Some(d);
        self
    }

    /// Called with every complete message. Capture per-endpoint details (venue, asset type)
    /// in the closure.
    pub fn on_message(mut self, f: impl FnMut(&mut A, &mut Cx<'_, A>, Msg<'_>) + 'static) -> Self {
        self.handler = Some(Box::new(f));
        self
    }

    /// Registers and starts connecting. Returns the registration's handle (send to it with
    /// `cx.send_text(handle, ..)`).
    pub fn open(self) -> Result<Handle, Error> {
        let spec = WsSpec {
            url: self.url?,
            headers: self.headers,
            subscribe: self.subscribe,
            ping: self.ping,
            stale_after: self.stale_after,
            max_age: self.max_age,
            connections: self.connections,
        };
        let handler = self.handler.unwrap_or_else(|| Box::new(|_, _, _| {}));
        self.core.register_ws(spec, handler, self.sync_resolve)
    }
}
