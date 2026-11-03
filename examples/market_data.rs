//! Two exchange feeds, app-wide open/close logging, and a stats timer.
//!
//! cargo run --release --example market_data            (Linux)
//! IO_WAIT=block cargo run --example market_data        (laptop-friendly)

#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    use io_uring_core::{secs, Io, Msg, Ping, WaitMode};

    #[derive(Default)]
    struct App {
        msgs: [u64; 2],
    }

    impl App {
        fn on_message(&mut self, venue: usize, msg: Msg) {
            self.msgs[venue] += 1;
            if self.msgs[venue] == 1 {
                println!("[{venue}] first message: {}", msg.text().unwrap_or("<binary>"));
            }
        }
    }

    let wait: WaitMode = std::env::var("IO_WAIT").ok().and_then(|v| v.parse().ok()).unwrap_or(WaitMode::Block);
    let mut io = Io::builder().wait(wait).build(App::default())?;

    io.ws("wss://fstream.binance.com/ws")
        .subscribe(r#"{"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":1}"#)
        .connections(2)
        .on_message(|app, _cx, msg| app.on_message(0, msg))
        .open()?;

    io.ws("wss://ws.okx.com:8443/ws/v5/public")
        .subscribe(r#"{"op":"subscribe","args":[{"channel":"bbo-tbt","instId":"BTC-USDT"}]}"#)
        .ping(Ping::text("ping").every(secs(20)).expect("pong"))
        .on_message(|app, _cx, msg| app.on_message(1, msg))
        .open()?;

    io.on_open(|_, cx, conn| println!("{conn} open: {:?}", cx.info(conn).and_then(|i| i.peer)));
    io.on_close(|_, _, conn, why| println!("{conn} closed: {why}"));
    io.on_connect_failed(|_, _, conn, why| println!("{conn} connect failed: {why}"));
    io.every(secs(5), |app, _| println!("msgs: binance={} okx={}", app.msgs[0], app.msgs[1]));

    io.run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("io_uring_core's runtime is Linux-only");
}
