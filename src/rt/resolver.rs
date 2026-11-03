use std::io;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use super::channel::Waker;
use super::socket;

struct Request {
    group: u32,
    host: String,
    port: u16,
}

pub(crate) struct Resolved {
    pub group: u32,
    pub result: io::Result<Vec<SocketAddr>>,
}

/// DNS off the IO thread: a blocking getaddrinfo can take milliseconds to seconds, which would
/// stall every feed if it ran in the loop.
pub(crate) struct Resolver {
    tx: mpsc::Sender<Request>,
    rx: rtrb::Consumer<Resolved>,
}

impl Resolver {
    pub(crate) fn spawn(waker: Arc<Waker>) -> io::Result<Self> {
        let (tx, requests) = mpsc::channel::<Request>();
        let (mut out, rx) = rtrb::RingBuffer::new(256);
        // Spawned from the (possibly pinned) IO thread, so move off its core first.
        let exclude = crate::sys::current_affinity();
        thread::Builder::new().name("io-resolve".into()).spawn(move || {
            let cpus = crate::sys::housekeeping_cpus(&exclude);
            if !cpus.is_empty() {
                let _ = crate::sys::set_affinity(&cpus);
            }
            for req in requests {
                let mut item = Resolved { group: req.group, result: socket::resolve(&req.host, req.port) };
                loop {
                    match out.push(item) {
                        Ok(()) => break,
                        Err(rtrb::PushError::Full(back)) => {
                            item = back;
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
                waker.wake();
            }
        })?;
        Ok(Resolver { tx, rx })
    }

    pub(crate) fn request(&self, group: u32, host: &str, port: u16) {
        let _ = self.tx.send(Request { group, host: host.to_string(), port });
    }

    #[inline]
    pub(crate) fn pop(&mut self) -> Option<Resolved> {
        self.rx.pop().ok()
    }

    #[inline]
    pub(crate) fn has_results(&self) -> bool {
        !self.rx.is_empty()
    }
}
