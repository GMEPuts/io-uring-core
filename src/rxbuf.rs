/// Fixed-capacity linear byte buffer: plaintext is appended at `tail`, frames are consumed from
/// `head`. Consuming only moves `head`; bytes are moved down only when the buffer is full.
#[derive(Default)]
pub(crate) struct RxBuf {
    buf: Box<[u8]>,
    head: usize,
    tail: usize,
}

impl RxBuf {
    pub(crate) fn new(capacity: usize) -> Self {
        Self { buf: vec![0; capacity].into_boxed_slice(), head: 0, tail: 0 }
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        &self.buf[self.head..self.tail]
    }

    #[inline]
    pub(crate) fn spare_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.tail..]
    }

    #[inline]
    pub(crate) fn commit(&mut self, n: usize) {
        debug_assert!(self.tail + n <= self.buf.len());
        self.tail += n;
    }

    #[inline]
    pub(crate) fn consume(&mut self, n: usize) {
        debug_assert!(self.head + n <= self.tail);
        self.head += n;
        if self.head == self.tail {
            self.head = 0;
            self.tail = 0;
        }
    }

    pub(crate) fn compact(&mut self) {
        if self.head > 0 {
            self.buf.copy_within(self.head..self.tail, 0);
            self.tail -= self.head;
            self.head = 0;
        }
    }

    /// Makes room at the end if possible; returns false when the buffer is full of unconsumed data.
    #[inline]
    pub(crate) fn ensure_spare(&mut self) -> bool {
        if self.tail == self.buf.len() {
            self.compact();
        }
        self.tail < self.buf.len()
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.head = 0;
        self.tail = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::RxBuf;

    #[test]
    fn append_consume_compact() {
        let mut b = RxBuf::new(8);
        b.spare_mut()[..6].copy_from_slice(b"abcdef");
        b.commit(6);
        b.consume(4);
        assert_eq!(b.data(), b"ef");
        b.spare_mut()[..2].copy_from_slice(b"gh");
        b.commit(2);
        assert!(b.ensure_spare());
        assert_eq!(b.data(), b"efgh");
        assert_eq!(b.spare_mut().len(), 4);
        b.consume(4);
        assert_eq!(b.spare_mut().len(), 8);
    }

    #[test]
    fn full_buffer_reports_no_spare() {
        let mut b = RxBuf::new(4);
        b.spare_mut().copy_from_slice(b"abcd");
        b.commit(4);
        assert!(!b.ensure_spare());
    }
}
