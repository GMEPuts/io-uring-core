//! Clock helpers: the wall clock, and a cheap tick counter for measuring short spans.

#[cfg(not(target_arch = "aarch64"))]
use std::sync::OnceLock;
#[cfg(not(target_arch = "aarch64"))]
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

/// Microseconds since the Unix epoch.
#[inline]
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// A counter for measuring short spans, with no system call. On aarch64 it's `cntvct_el0`, one count
/// shared by all cores. Elsewhere it's nanoseconds since the first call, since the runtime only runs in
/// production on aarch64 and other machines can't give comparable numbers anyway.
#[cfg(target_arch = "aarch64")]
#[inline]
pub fn now_ticks() -> u64 {
    let value: u64;
    // SAFETY: Linux lets user code read this counter, and both instructions only read state.
    // Not marked `nomem`, so the compiler can't move memory accesses across the `isb`.
    unsafe {
        std::arch::asm!("isb", "mrs {value}, cntvct_el0", value = out(reg) value, options(nostack, preserves_flags));
    }
    value
}

/// A counter for measuring short spans. See the aarch64 version.
#[cfg(not(target_arch = "aarch64"))]
#[inline]
pub fn now_ticks() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_never_go_backwards_and_they_advance() {
        let first = now_ticks();
        let mut last = first;
        for _ in 0..10_000 {
            let next = now_ticks();
            assert!(next >= last);
            last = next;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(now_ticks() > first);
    }
}
