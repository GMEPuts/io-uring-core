use std::io;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use super::io::Io;

/// Runs an `Io` on its own thread, optionally pinned to `cpu`.
///
/// `setup` runs on the new thread: build the `Io` there (io_uring ties the ring to its
/// creating thread), register connections, create channels, and return whatever the caller
/// needs (typically the channel `Sender`s). `spawn` returns once setup is done, with that value
/// and the thread's handle.
///
/// ```ignore
/// let (orders, io_thread) = io_uring_core::spawn(Some(2), move || {
///     let mut io = Io::builder().wait(WaitMode::Napi).build(App::new(events_tx))?;
///     io.ws(MARKET_DATA_URL).subscribe(SUB).on_message(|app, _, m| app.on_md(m)).open()?;
///     let orders = io.channel(1024, |app, cx, o: Order| app.send_order(cx, o));
///     Ok((io, orders))
/// })?;
/// ```
pub fn spawn<A, R, F>(cpu: Option<usize>, setup: F) -> io::Result<(R, JoinHandle<io::Result<()>>)>
where
    A: 'static,
    R: Send + 'static,
    F: FnOnce() -> io::Result<(Io<A>, R)> + Send + 'static,
{
    let (tx, rx) = mpsc::sync_channel::<io::Result<R>>(1);
    let handle = thread::Builder::new().name("io".into()).spawn(move || -> io::Result<()> {
        if let Some(cpu) = cpu {
            if let Err(e) = crate::sys::pin_to_cpu(cpu) {
                let _ = tx.send(Err(e));
                return Ok(());
            }
        }
        let (mut io, out) = match setup() {
            Ok(v) => v,
            Err(e) => {
                let _ = tx.send(Err(e));
                return Ok(());
            }
        };
        let _ = tx.send(Ok(out));
        io.run()
    })?;
    match rx.recv() {
        Ok(Ok(out)) => Ok((out, handle)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::other("io thread exited during setup")),
    }
}
