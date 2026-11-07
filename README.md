# io_uring_core

A connection-oriented WebSocket client runtime on io_uring, for low-latency trading.

You register connections: a URL, a ping if the venue needs one, and a message handler. The IO owns everything else: DNS, TCP connect, TLS, the WebSocket upgrade, subscriptions, pings, stale detection, reconnects with backoff and IP failover, and all the io_uring plumbing.

```rust
use io_uring_core::{secs, Io, Ping};

let mut io = Io::new(App::new())?;

io.ws("wss://fstream.binance.com/ws")
    .subscribe(r#"{"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":1}"#)
    .on_message(|app, _cx, msg| app.on_binance(AssetType::Perp, msg))
    .open()?;

io.ws("wss://ws.okx.com:8443/ws/v5/public")
    .subscribe(OKX_SUB)
    .ping(Ping::text("ping").every(secs(25)).expect("pong"))
    .on_message(|app, _cx, msg| app.on_okx(msg))
    .open()?;

io.run()
```

`App` is your own state, of any type. Handlers get `&mut App`, a `Cx` for acting on the IO, and the message. `msg.data` is a `&[u8]` borrowed from the receive buffer, so it is never copied. Anything a connection needs to know, such as its venue or asset type, is captured in its handler closure.

- Linux only. Requires kernel 6.1+, or 6.9+ for `WaitMode::Napi`.
- The protocol code (URL parsing, WebSocket framing, ping scheduling, timers) builds and tests on any OS.

## What the IO does for you

| Concern | Behaviour |
|---|---|
| DNS | Resolved by `.open()`. When connections keep failing, it is re-resolved on a helper thread, never on the IO thread. `IoBuilder::external_dns` hands those lookups to a thread you own instead, through a `DnsPort`, so the crate starts no thread. |
| Connect, TLS, upgrade | Run in order, with timeouts of 5 s, 5 s and 10 s. |
| `.subscribe(msg)` | Sent after every successful connect and reconnect. |
| Server pings | WebSocket ping frames are answered automatically. |
| Dead connection | If nothing arrives for 30 s, the connection reconnects. Change this with `.stale_after(d)`, or turn it off with `.never_stale()`. |
| Reconnect | Exponential backoff with jitter (0.5 s → 30 s). The next attempt goes to another resolved IP. |
| Errors | A bad frame or a TLS error closes that one connection, which then reconnects. The loop never stops. |
| Fragmented messages | Joined before your handler sees them. |

## Per-connection settings

All of these are optional and chained on `io.ws(url)`:

| Call | Use |
|---|---|
| `.subscribe(msg)` | Message sent after each connect. Call it repeatedly to send several. |
| `.ping(Ping::…)` | Client pings (see the next section). |
| `.stale_after(d)` / `.never_stale()` | How long a silent connection lives before reconnecting. |
| `.header(k, v)` | Extra upgrade header. Setting `User-Agent` replaces the default browser-like one. |
| `.connections(n)` | Opens `n` identical connections, spread over the resolved IPs. They share one handler. |
| `.max_age(d)` | Rotates connections periodically. Connections are staggered 10 minutes apart, across all registrations, so they never rotate together. Binance drops connections at 24 h. |
| `.on_message(f)` | The handler. |

## Pings

```rust
.ping(Ping::frame().every(secs(20)))                        // WebSocket ping frame; the pong is expected
.ping(Ping::text("ping").every(secs(25)).expect("pong"))    // OKX: "pong" replies are swallowed
.ping(Ping::text(r#"{"op":"ping"}"#).every(secs(20)))       // Bybit
.ping(Ping::text(r#"{"method":"ping"}"#).every(secs(50)))   // Hyperliquid
```

- **First ping:** sent one interval after the connection opens, never immediately. Bybit breaks if you ping before the subscription ack.
- **Skipped when busy:** a ping is skipped if anything else was sent within the interval. Use `.always()` to ping on schedule regardless.
- **Expected replies:** with `.expect(reply)`, or with `Ping::frame()`, a missing reply closes the connection with `PingTimeout`. The default reply timeout is the ping interval, at most 10 s. Change it with `.reply_timeout(d)`.
- **Pings that change each time**, such as FIX heartbeats or HTX's `{"pong":ts}` echo, are sent from your handler or an `io.every` timer, using `cx.send_text`.

## App-wide events and timers

```rust
io.on_open(|app, cx, conn| { /* dynamic subscriptions, logins, "connected" status */ });
io.on_close(|app, cx, conn, reason| { /* "disconnected" status; it reconnects by itself */ });
io.on_connect_failed(|app, cx, conn, reason| { /* logging */ });
io.every(secs(10), |app, cx| app.print_stats());
cx.after(millis(500), |app, cx| { /* one-shot, from any handler */ });
```

`conn` is a `ConnId`. It stays the same across reconnects, and `conn.index()` is a small dense number you can use to index per-connection arrays.

## Sending

```rust
cx.send_text(conn, &json)?;      // a specific connection
cx.send_text(handle, &json)?;    // any open connection of a registration (the handle .open() returned)
cx.send_binary(conn, &bytes)?;
cx.reconnect(conn);              // drop the session, reconnect now (to another IP)
cx.remove(handle);               // close a registration for good
cx.ws(url)…open()?;              // register a new connection at runtime (DNS off the IO thread)
cx.flush();                      // send what's queued now, to time the send()
```

Sends made inside a handler go out as soon as that handler returns. Each connection touched costs one non-blocking `send()`, written straight to the socket. If the socket buffer is full, the rest is handed to io_uring. Two sends are never in flight on one socket at once.

## Running IO on its own thread

```rust
let (mut orders, io_thread) = io_uring_core::spawn(Some(2), move || {     // pinned to CPU 2
    let mut io = Io::builder().wait(WaitMode::Napi).build(App::new(events_tx))?;
    io.ws(MD_URL).subscribe(SUB).on_message(|app, _, m| app.on_md(m)).open()?;
    let orders = io.channel(1024, |app, cx, o: Order| app.send_order(cx, o));
    Ok((io, orders))
})?;

orders.send(order)?;   // from the strategy thread; handled on the IO thread's next loop pass
```

- `io.channel(capacity, handler)` returns a `Sender`. Each channel has one producer, so create one per producing thread.
- Every channel is checked on every loop pass.
- In `Napi` and `Block` modes, `send` wakes a sleeping loop with one `write(eventfd)`. In `Spin` mode it never makes a syscall.
- Results going back to the strategy (market data, order acks) use your own queue, pushed from your handlers.

## Runtime settings

```rust
Io::builder()
    .wait(WaitMode::Napi)          // Spin (default) | Napi | Block
    .max_connections(256)          // default 128
    .rx_buffer(256 * 1024)         // per connection; also the largest message (default 128 KiB)
    .recv_buffers(512, 16 * 1024)  // shared kernel receive buffers (default 256 × 16 KiB)
    .tls_config(my_config)         // default: webpki roots + the `ring` provider
    .build(app)?
```

| Wait mode | Use |
|---|---|
| `Spin` | Busy-polls the completion queue from user space. Needs a dedicated core. |
| `Napi` | Waits inside io_uring with NAPI busy polling of the NIC. This gave the best p99 on BinanceRelay. Needs a dedicated core and kernel 6.9+. |
| `Block` | Sleeps until a completion or a timer deadline is due. For development boxes. |

`io_uring_core::sys` has `pin_to_cpu` and `lock_memory` for the thread/core setup.

## Cargo

```toml
[dependencies]
io_uring_core = { path = "../io_uring_core" }   # or a git tag

[profile.release]                               # set these in YOUR crate: Cargo ignores dependency profiles
lto = "fat"
codegen-units = 1
panic = "abort"
```

Crypto provider features:
- `ring` is the default.
- `aws-lc-rs` is the alternative.
- With `default-features = false`, no provider is compiled in, and you pass your own config with `IoBuilder::tls_config`.

## How it works

- **Single thread, one ring.** The ring uses `SINGLE_ISSUER`, plus `DEFER_TASKRUN` in napi/block modes. Every connection shares one provided-buffer ring for multishot recv.
- **Receive path.** Each recv completion is decrypted one chunk at a time into a fixed per-connection buffer. After each chunk, frames are parsed as borrowed slices and passed to the handler, so the first message is handled before the rest of the batch is decrypted. Nothing is allocated per message.
- **Send path.** A direct `send()` is used when no io_uring send is in flight. Otherwise the remaining bytes go to io_uring in a buffer the kernel owns until the send completes.
- **Stale completions.** Each completion carries a per-connection generation, bumped on every close. Completions from an earlier socket only release the buffer they hold.
- **Timers.** Pings, timeouts, reconnects, `every` and `after` share one heap. In napi/block modes the loop sleeps exactly until the next deadline, with no fixed tick.
- **Deferred closes.** A close requested inside a handler is applied after that handler returns, so a handler never sees its connection change underneath it.

## Status

- **v1 scope:** WebSocket client, TLS, pings, channels, timers, and runtime registration.
- **Next:** HTTP/1.1 keep-alive pools for REST orders, then raw TCP (FIX) and UDP. These follow the same register-a-connection shape.
- **Not supported:** compression (permessage-deflate). Compressed frames are rejected with a clear close reason.

## Development

```bash
cargo test --no-default-features          # protocol tests; any OS
cargo check --target x86_64-unknown-linux-gnu --no-default-features --all-targets   # type-check the runtime from Windows
cargo run --example market_data           # Linux
```
