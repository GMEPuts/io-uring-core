use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimerKey {
    /// A connection's next wake-up. Stale when `seq` no longer matches the connection's.
    Conn { slot: u16, seq: u32 },
    Every(u32),
    After(u32),
}

struct Entry {
    at: Instant,
    seq: u64,
    key: TimerKey,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

/// Min-heap of deadlines. Cancellation is lazy: owners check a sequence number when an entry fires.
#[derive(Default)]
pub(crate) struct Timers {
    heap: BinaryHeap<Reverse<Entry>>,
    seq: u64,
}

impl Timers {
    pub(crate) fn push(&mut self, at: Instant, key: TimerKey) {
        self.seq += 1;
        self.heap.push(Reverse(Entry { at, seq: self.seq, key }));
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.heap.peek().map(|e| e.0.at)
    }

    pub(crate) fn pop_due(&mut self, now: Instant) -> Option<TimerKey> {
        if self.heap.peek()?.0.at <= now {
            self.heap.pop().map(|e| e.0.key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn pops_in_deadline_then_insertion_order() {
        let t0 = Instant::now();
        let ms = Duration::from_millis;
        let mut t = Timers::default();
        t.push(t0 + ms(30), TimerKey::Every(3));
        t.push(t0 + ms(10), TimerKey::Every(1));
        t.push(t0 + ms(10), TimerKey::Every(2));
        t.push(t0 + ms(20), TimerKey::After(0));
        assert_eq!(t.next_deadline(), Some(t0 + ms(10)));
        assert_eq!(t.pop_due(t0), None);
        assert_eq!(t.pop_due(t0 + ms(25)), Some(TimerKey::Every(1)));
        assert_eq!(t.pop_due(t0 + ms(25)), Some(TimerKey::Every(2)));
        assert_eq!(t.pop_due(t0 + ms(25)), Some(TimerKey::After(0)));
        assert_eq!(t.pop_due(t0 + ms(25)), None);
        assert_eq!(t.pop_due(t0 + ms(30)), Some(TimerKey::Every(3)));
        assert_eq!(t.next_deadline(), None);
    }
}
