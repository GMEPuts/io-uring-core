use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::io;
use std::slice;
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::types::BufRingEntry;
use io_uring::Submitter;

/// Provided-buffer ring for multishot recv: the kernel picks a free buffer per completion and
/// reports its id in the CQE flags. Every buffer handed out must be `recycle`d, including ones
/// from stale completions, or the ring drains and recv fails with ENOBUFS.
pub(crate) struct BufRing {
    entries: *mut BufRingEntry,
    entries_layout: Layout,
    bufs: *mut u8,
    bufs_layout: Layout,
    buf_len: usize,
    mask: u16,
    tail: u16,
    bgid: u16,
}

impl BufRing {
    pub(crate) fn new(submitter: &Submitter<'_>, count: u16, buf_len: usize, bgid: u16) -> io::Result<Self> {
        assert!(count.is_power_of_two(), "buffer count must be a power of two");
        let entries_layout =
            Layout::array::<BufRingEntry>(count as usize).and_then(|l| l.align_to(4096)).expect("buf ring layout");
        let bufs_layout = Layout::from_size_align(count as usize * buf_len, 64).expect("buffer layout");

        let entries = unsafe { alloc_zeroed(entries_layout) } as *mut BufRingEntry;
        if entries.is_null() {
            handle_alloc_error(entries_layout);
        }
        let bufs = unsafe { alloc_zeroed(bufs_layout) };
        if bufs.is_null() {
            handle_alloc_error(bufs_layout);
        }

        let mut ring = Self { entries, entries_layout, bufs, bufs_layout, buf_len, mask: count - 1, tail: 0, bgid };
        unsafe { submitter.register_buf_ring_with_flags(entries as u64, count, bgid, 0)? };
        for bid in 0..count {
            ring.push(bid);
        }
        ring.publish();
        Ok(ring)
    }

    #[inline]
    pub(crate) fn bgid(&self) -> u16 {
        self.bgid
    }

    #[inline]
    pub(crate) fn get(&self, bid: u16, len: usize) -> &[u8] {
        debug_assert!(len <= self.buf_len);
        unsafe { slice::from_raw_parts(self.bufs.add(bid as usize * self.buf_len), len) }
    }

    #[inline]
    pub(crate) fn recycle(&mut self, bid: u16) {
        self.push(bid);
        self.publish();
    }

    #[inline]
    fn push(&mut self, bid: u16) {
        let entry = unsafe { &mut *self.entries.add((self.tail & self.mask) as usize) };
        entry.set_addr(unsafe { self.bufs.add(bid as usize * self.buf_len) } as u64);
        entry.set_len(self.buf_len as u32);
        entry.set_bid(bid);
        self.tail = self.tail.wrapping_add(1);
    }

    #[inline]
    fn publish(&self) {
        unsafe {
            let tail = BufRingEntry::tail(self.entries) as *const AtomicU16;
            (*tail).store(self.tail, Ordering::Release);
        }
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.entries as *mut u8, self.entries_layout);
            dealloc(self.bufs, self.bufs_layout);
        }
    }
}
