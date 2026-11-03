use std::fmt;
use std::os::fd::RawFd;
use std::sync::atomic::{fence, AtomicBool, Ordering};
use std::sync::Arc;

use super::cx::Cx;
use super::engine::Core;
use super::io::settle;

/// Wakes the IO thread when it is asleep in the kernel (napi/block wait modes). In spin mode
/// `sleeping` is never set, so waking costs one fence and a relaxed load: no syscall.
pub(crate) struct Waker {
    pub(crate) sleeping: AtomicBool,
    efd: RawFd,
}

impl Waker {
    pub(crate) fn new(efd: RawFd) -> Self {
        Waker { sleeping: AtomicBool::new(false), efd }
    }

    pub(crate) fn efd(&self) -> RawFd {
        self.efd
    }

    /// Call after publishing work the IO thread should see.
    #[inline]
    pub(crate) fn wake(&self) {
        // Pairs with the fence in Io::wait: either the IO thread sees our item before it sleeps,
        // or we see `sleeping` and write the eventfd.
        fence(Ordering::SeqCst);
        if self.efd >= 0 && self.sleeping.load(Ordering::Relaxed) && self.sleeping.swap(false, Ordering::AcqRel) {
            let one: u64 = 1;
            unsafe { libc::write(self.efd, &one as *const u64 as *const libc::c_void, 8) };
        }
    }
}

impl Drop for Waker {
    fn drop(&mut self) {
        if self.efd >= 0 {
            unsafe { libc::close(self.efd) };
        }
    }
}

/// Sends values from another thread into the IO thread's channel handler.
///
/// One producer per channel: create a separate channel for each producing thread.
pub struct Sender<T> {
    tx: rtrb::Producer<T>,
    waker: Arc<Waker>,
}

/// The channel was full; the value is handed back.
pub struct Full<T>(pub T);

impl<T> fmt::Debug for Full<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Full(..)")
    }
}

impl<T> fmt::Display for Full<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("io channel full")
    }
}

impl<T> std::error::Error for Full<T> {}

impl<T> Sender<T> {
    /// Queues `value`; the IO thread handles it on its next loop pass.
    #[inline]
    pub fn send(&mut self, value: T) -> Result<(), Full<T>> {
        match self.tx.push(value) {
            Ok(()) => {
                self.waker.wake();
                Ok(())
            }
            Err(rtrb::PushError::Full(v)) => Err(Full(v)),
        }
    }

    /// Free space in the channel.
    pub fn free(&self) -> usize {
        self.tx.slots()
    }
}

pub(crate) trait Inbox<A> {
    fn drain(&mut self, app: &mut A, core: &mut Core<A>);
    fn has_items(&self) -> bool;
}

struct Channel<T, F> {
    rx: rtrb::Consumer<T>,
    handler: F,
}

impl<A: 'static, T, F> Inbox<A> for Channel<T, F>
where
    F: FnMut(&mut A, &mut Cx<'_, A>, T),
{
    #[inline]
    fn drain(&mut self, app: &mut A, core: &mut Core<A>) {
        while let Ok(v) = self.rx.pop() {
            (self.handler)(app, &mut Cx::new(core), v);
            settle(app, core);
        }
    }

    #[inline]
    fn has_items(&self) -> bool {
        !self.rx.is_empty()
    }
}

pub(crate) fn channel<A: 'static, T: Send + 'static>(
    capacity: usize,
    waker: Arc<Waker>,
    handler: impl FnMut(&mut A, &mut Cx<'_, A>, T) + 'static,
) -> (Sender<T>, Box<dyn Inbox<A>>) {
    let (tx, rx) = rtrb::RingBuffer::new(capacity);
    (Sender { tx, waker }, Box::new(Channel { rx, handler }))
}
