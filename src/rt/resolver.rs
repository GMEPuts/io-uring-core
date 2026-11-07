use std::io;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use super::channel::Waker;
use super::socket;

const QUEUE_CAPACITY: usize = 256;

/// A lookup the IO thread wants done.
#[derive(Debug)]
pub struct DnsRequest {
    /// Hand this back unchanged in the answer.
    pub group: u32,
    pub host: String,
    pub port: u16,
}

/// The result of one [`DnsRequest`].
#[derive(Debug)]
pub struct DnsAnswer {
    pub group: u32,
    pub addresses: io::Result<Vec<SocketAddr>>,
}

/// The other end of `IoBuilder::external_dns`: whichever thread owns it does the lookups, so the crate
/// starts no thread of its own.
pub struct DnsPort {
    requests: rtrb::Consumer<DnsRequest>,
    answers: rtrb::Producer<DnsAnswer>,
    waker: Arc<Waker>,
}

impl DnsPort {
    /// The next lookup to do, if any. Never blocks.
    pub fn next_request(&mut self) -> Option<DnsRequest> {
        self.requests.pop().ok()
    }

    /// Hands back a result and wakes the IO thread. If the queue is full, the answer comes back to
    /// try again.
    pub fn answer(&mut self, answer: DnsAnswer) -> Result<(), DnsAnswer> {
        match self.answers.push(answer) {
            Ok(()) => {
                self.waker.wake();
                Ok(())
            }
            Err(rtrb::PushError::Full(back)) => Err(back),
        }
    }
}

/// DNS off the IO thread: a blocking getaddrinfo can take milliseconds to seconds, which would
/// stall every feed if it ran in the loop.
pub(crate) enum Resolver {
    /// The crate's own helper thread.
    Thread { tx: mpsc::Sender<DnsRequest>, rx: rtrb::Consumer<DnsAnswer> },
    /// A thread the program owns, through a `DnsPort`.
    External { requests: rtrb::Producer<DnsRequest>, rx: rtrb::Consumer<DnsAnswer> },
}

impl Resolver {
    pub(crate) fn spawn(waker: Arc<Waker>) -> io::Result<Self> {
        let (tx, requests) = mpsc::channel::<DnsRequest>();
        let (mut out, rx) = rtrb::RingBuffer::new(QUEUE_CAPACITY);
        // Spawned from the (possibly pinned) IO thread, so move off its core first.
        let exclude = crate::sys::current_affinity();
        thread::Builder::new().name("io-resolve".into()).spawn(move || {
            let cpus = crate::sys::housekeeping_cpus(&exclude);
            if !cpus.is_empty() {
                let _ = crate::sys::set_affinity(&cpus);
            }
            for req in requests {
                let mut item = DnsAnswer { group: req.group, addresses: socket::resolve(&req.host, req.port) };
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
        Ok(Resolver::Thread { tx, rx })
    }

    pub(crate) fn external(waker: Arc<Waker>) -> (Self, DnsPort) {
        let (requests_in, requests_out) = rtrb::RingBuffer::new(QUEUE_CAPACITY);
        let (answers_in, answers_out) = rtrb::RingBuffer::new(QUEUE_CAPACITY);
        let resolver = Resolver::External { requests: requests_in, rx: answers_out };
        (resolver, DnsPort { requests: requests_out, answers: answers_in, waker })
    }

    /// False if the request can't be queued, so the caller can retry it later.
    pub(crate) fn request(&mut self, group: u32, host: &str, port: u16) -> bool {
        let request = DnsRequest { group, host: host.to_string(), port };
        match self {
            Resolver::Thread { tx, .. } => tx.send(request).is_ok(),
            Resolver::External { requests, .. } => requests.push(request).is_ok(),
        }
    }

    #[inline]
    pub(crate) fn pop(&mut self) -> Option<DnsAnswer> {
        match self {
            Resolver::Thread { rx, .. } | Resolver::External { rx, .. } => rx.pop().ok(),
        }
    }

    #[inline]
    pub(crate) fn has_results(&self) -> bool {
        match self {
            Resolver::Thread { rx, .. } | Resolver::External { rx, .. } => !rx.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn external() -> (Resolver, DnsPort) {
        Resolver::external(Arc::new(Waker::new(-1)))
    }

    #[test]
    fn a_request_reaches_the_port_and_its_answer_comes_back() {
        let (mut resolver, mut port) = external();
        assert!(port.next_request().is_none());
        assert!(!resolver.has_results());

        assert!(resolver.request(7, "example.com", 443));
        let request = port.next_request().unwrap();
        assert_eq!((request.group, request.host.as_str(), request.port), (7, "example.com", 443));
        assert!(port.next_request().is_none());

        let address: SocketAddr = "1.2.3.4:443".parse().unwrap();
        port.answer(DnsAnswer { group: 7, addresses: Ok(vec![address]) }).unwrap();
        assert!(resolver.has_results());
        let answer = resolver.pop().unwrap();
        assert_eq!((answer.group, answer.addresses.unwrap()), (7, vec![address]));
        assert!(!resolver.has_results());
        assert!(resolver.pop().is_none());
    }

    #[test]
    fn a_full_request_queue_says_so_and_recovers() {
        let (mut resolver, mut port) = external();
        for group in 0..QUEUE_CAPACITY as u32 {
            assert!(resolver.request(group, "h", 1));
        }
        assert!(!resolver.request(999, "h", 1));
        assert_eq!(port.next_request().unwrap().group, 0);
        assert!(resolver.request(1_000, "h", 1));
    }

    #[test]
    fn a_full_answer_queue_hands_the_answer_back() {
        let (mut resolver, mut port) = external();
        for group in 0..QUEUE_CAPACITY as u32 {
            port.answer(DnsAnswer { group, addresses: Ok(Vec::new()) }).unwrap();
        }
        let back = port.answer(DnsAnswer { group: 999, addresses: Ok(Vec::new()) }).unwrap_err();
        assert_eq!(back.group, 999);
        assert_eq!(resolver.pop().unwrap().group, 0);
        assert!(port.answer(back).is_ok());
    }

    #[test]
    fn the_port_can_move_to_another_thread() {
        fn is_send<T: Send>() {}
        is_send::<DnsPort>();
    }
}
