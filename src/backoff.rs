use std::time::Duration;

const BASE: Duration = Duration::from_millis(500);
const CAP: Duration = Duration::from_secs(30);

/// Exponential reconnect delay (0.5 s doubling, capped at 30 s) plus `jitter`, so a pool of
/// connections that failed together doesn't retry in lockstep.
pub(crate) fn backoff(failures: u32, jitter: Duration) -> Duration {
    (BASE * (1u32 << failures.min(6))).min(CAP) + jitter
}

pub(crate) fn jitter() -> Duration {
    Duration::from_millis(fastrand::u64(0..1000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_then_caps() {
        assert_eq!(backoff(0, Duration::ZERO), Duration::from_millis(500));
        assert_eq!(backoff(1, Duration::ZERO), Duration::from_secs(1));
        assert_eq!(backoff(5, Duration::ZERO), Duration::from_secs(16));
        assert_eq!(backoff(6, Duration::ZERO), CAP);
        assert_eq!(backoff(1000, Duration::ZERO), CAP);
        assert_eq!(backoff(0, Duration::from_millis(7)), Duration::from_millis(507));
    }
}
