//! Thread placement and memory locking for latency-critical event loops (Linux).

use std::io;
use std::mem::{size_of, zeroed};

/// Pins the calling thread to `cpu`. Returns whether that CPU is in `isolcpus`: a pinned but
/// non-isolated core still shares time with the scheduler's other work.
pub fn pin_to_cpu(cpu: usize) -> io::Result<bool> {
    set_affinity(&[cpu]).map_err(|e| io::Error::new(e.kind(), format!("pin to cpu {cpu}: {e}")))?;
    let isolated = std::fs::read_to_string("/sys/devices/system/cpu/isolated").unwrap_or_default();
    Ok(cpu_list(isolated.trim()).contains(&cpu))
}

/// Sets the calling thread's CPU affinity.
pub fn set_affinity(cpus: &[usize]) -> io::Result<()> {
    unsafe {
        let mut set: libc::cpu_set_t = zeroed();
        for &cpu in cpus {
            libc::CPU_SET(cpu, &mut set);
        }
        if libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Online CPUs that are not in `isolcpus` and not in `exclude`: where background helper threads
/// belong, so they never land on (or inherit) an event loop's isolated core.
pub fn housekeeping_cpus(exclude: &[usize]) -> Vec<usize> {
    let read = |p: &str| std::fs::read_to_string(p).unwrap_or_default();
    let online = cpu_list(read("/sys/devices/system/cpu/online").trim());
    let isolated = cpu_list(read("/sys/devices/system/cpu/isolated").trim());
    online.into_iter().filter(|c| !isolated.contains(c) && !exclude.contains(c)).collect()
}

/// The calling thread's current affinity.
pub fn current_affinity() -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = zeroed();
        if libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..libc::CPU_SETSIZE as usize).filter(|&c| libc::CPU_ISSET(c, &set)).collect()
    }
}

/// Locks current and future pages so the hot path never takes a page fault. Only when
/// RLIMIT_MEMLOCK is unlimited: with MCL_FUTURE under a finite limit, a later allocation past
/// the limit would fail. Returns whether memory was locked.
pub fn lock_memory() -> io::Result<bool> {
    let mut lim: libc::rlimit = unsafe { zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut lim) } != 0 || lim.rlim_cur != libc::RLIM_INFINITY {
        return Ok(false);
    }
    if unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(true)
}

/// Parses kernel CPU lists like `0,2-4`.
pub(crate) fn cpu_list(list: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in list.split(',').filter(|s| !s.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) {
                    out.extend(a..=b);
                }
            }
            None => {
                if let Ok(c) = part.parse() {
                    out.push(c);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::cpu_list;

    #[test]
    fn parses_cpu_lists() {
        assert_eq!(cpu_list("1"), vec![1]);
        assert_eq!(cpu_list("0,2-4"), vec![0, 2, 3, 4]);
        assert!(cpu_list("").is_empty());
    }
}
