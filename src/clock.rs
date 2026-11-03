//! Wall-clock helpers.

use std::time::{SystemTime, UNIX_EPOCH};

/// Microseconds since the Unix epoch.
#[inline]
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}
