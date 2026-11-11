//! Limit how hard the remote copier competes with the container.
//!
//! The copier thread calls [`engage`] after it has spawned the filesystem
//! check, so only that thread is affected. [`consume`] is thread-local and
//! stays unlimited on the FUSE threads. Those threads also call
//! [`crate::sparse::copy_range`] when they snapshot a large file into the
//! local queue, and that copy must not wait on the remote cap.

use std::cell::RefCell;
use std::io::{self, ErrorKind, Write};
use std::thread;
use std::time::{Duration, Instant};

/// Remote write cap used when `remote-rate` is omitted. 32 MiB/s.
pub const DEFAULT_RATE: u64 = 32 * 1024 * 1024;

/// Largest single paced write. A bigger buffer is split so one sleep stays
/// proportional to this many bytes.
const CHUNK: usize = 1024 * 1024;

const IOPRIO_WHO_PROCESS: libc::c_int = 1;
const IOPRIO_CLASS_IDLE: libc::c_int = 3;
const IOPRIO_CLASS_SHIFT: libc::c_int = 13;

thread_local! {
    static LIMIT: RefCell<Pacer> = RefCell::new(Pacer::unlimited());
}

struct Pacer {
    /// Bytes per second. Zero means do not wait.
    bps: u64,
    /// Bytes that may be written before the next wait. Never more than one
    /// second of credit, so an idle stretch cannot turn into a large burst.
    budget: u64,
    updated: Instant,
}

impl Pacer {
    fn unlimited() -> Self {
        Self::new(0)
    }

    fn new(bps: u64) -> Self {
        Self {
            bps,
            budget: bps,
            updated: Instant::now(),
        }
    }

    fn consume(&mut self, n: u64) {
        if self.bps == 0 || n == 0 {
            return;
        }
        self.refill();
        if n <= self.budget {
            self.budget -= n;
            return;
        }
        let deficit = n - self.budget;
        self.budget = 0;
        let wait_ns = u128::from(deficit).saturating_mul(1_000_000_000) / u128::from(self.bps);
        let wait_ns = u64::try_from(wait_ns).unwrap_or(u64::MAX);
        if wait_ns > 0 {
            thread::sleep(Duration::from_nanos(wait_ns));
        }
        // The wait paid for `deficit`. Do not turn that wait back into credit.
        self.updated = Instant::now();
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.updated);
        self.updated = now;
        let add = elapsed.as_nanos().saturating_mul(u128::from(self.bps)) / 1_000_000_000;
        let add = u64::try_from(add).unwrap_or(u64::MAX);
        self.budget = self.budget.saturating_add(add).min(self.bps);
    }
}

/// Account `n` bytes written to the remote by this thread.
pub fn consume(n: u64) {
    LIMIT.with(|limit| limit.borrow_mut().consume(n));
}

/// Write `data` at `offset`, at most 1 MiB at a time, and account each piece.
pub fn write_at(file: &std::fs::File, mut offset: u64, mut data: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let end = data.len().min(CHUNK);
        let n = file.write_at(&data[..end], offset)?;
        if n == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "remote write made no progress",
            ));
        }
        consume(n as u64);
        offset += n as u64;
        data = &data[n..];
    }
    Ok(())
}

/// Install the rate on this thread, drop it to nice 19 and idle I/O, and
/// log what the scheduler accepted.
pub fn engage(rate: u64) {
    LIMIT.with(|limit| *limit.borrow_mut() = Pacer::new(rate));
    // Unit tests copy through this thread onto a local directory. Idle I/O
    // there waits behind the machine's other disk traffic. The release
    // copier is the one that must yield.
    #[cfg(not(test))]
    let priority = deprioritize();
    #[cfg(test)]
    let priority = "nice=test io=test".to_string();
    eprintln!(
        "mirrorfs: remote copier {priority} rate={}",
        format_rate(rate)
    );
    let _ = std::io::stderr().flush();
}

/// `remote-rate` text. A bare number is bytes per second. `K`, `M`, and `G`
/// are binary (1024). `0` removes the cap. `32M`, `32MB`, and `32MiB` match.
pub fn parse_rate(text: &str) -> io::Result<u64> {
    let text = text.trim();
    if text.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "remote-rate is empty",
        ));
    }
    let lower = text.to_ascii_lowercase();
    let (number, mult) = strip_unit(&lower)?;
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("remote-rate {text} is not a number"),
        ));
    }
    let number: u64 = number.parse().map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("remote-rate {text} is not a number"),
        )
    })?;
    number.checked_mul(mult).ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("remote-rate {text} is too large"),
        )
    })
}

pub fn format_rate(bps: u64) -> String {
    if bps == 0 {
        return "unlimited".to_string();
    }
    if bps % (1024 * 1024) == 0 {
        return format!("{}MiB/s", bps / (1024 * 1024));
    }
    if bps % 1024 == 0 {
        return format!("{}KiB/s", bps / 1024);
    }
    format!("{bps}B/s")
}

fn strip_unit(lower: &str) -> io::Result<(&str, u64)> {
    const UNITS: &[(&str, u64)] = &[
        ("mib", 1024 * 1024),
        ("mb", 1024 * 1024),
        ("m", 1024 * 1024),
        ("kib", 1024),
        ("kb", 1024),
        ("k", 1024),
        ("gib", 1024 * 1024 * 1024),
        ("gb", 1024 * 1024 * 1024),
        ("g", 1024 * 1024 * 1024),
    ];
    for (suffix, mult) in UNITS {
        if let Some(number) = lower.strip_suffix(suffix) {
            return Ok((number, *mult));
        }
    }
    Ok((lower, 1))
}

fn deprioritize() -> String {
    let nice = match set_nice(19) {
        Ok(value) => format!("nice={value}"),
        Err(err) => format!("nice failed: {err}"),
    };
    let io = match set_idle_io() {
        Ok(()) => "io=idle".to_string(),
        Err(err) => format!("io=idle failed: {err}"),
    };
    format!("{nice} {io}")
}

fn set_nice(inc: i32) -> io::Result<i32> {
    unsafe {
        *libc::__errno_location() = 0;
        let rc = libc::nice(inc);
        if rc == -1 && *libc::__errno_location() != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(rc)
    }
}

fn set_idle_io() -> io::Result<()> {
    let class = IOPRIO_CLASS_IDLE << IOPRIO_CLASS_SHIFT;
    let rc = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, class) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn rate_text_uses_binary_units_and_zero_is_unlimited() {
        assert_eq!(parse_rate("0").unwrap(), 0);
        assert_eq!(parse_rate("32M").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_rate("32MB").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_rate("32MiB").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_rate("8k").unwrap(), 8 * 1024);
        assert_eq!(parse_rate("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_rate("4096").unwrap(), 4096);
        assert!(parse_rate("fast").is_err());
        assert!(parse_rate("").is_err());
        assert_eq!(format_rate(0), "unlimited");
        assert_eq!(format_rate(DEFAULT_RATE), "32MiB/s");
    }

    #[test]
    fn unlimited_consume_does_not_wait() {
        let mut pacer = Pacer::unlimited();
        let started = Instant::now();
        pacer.consume(64 * 1024 * 1024);
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn writes_past_the_one_second_budget_wait() {
        let mut pacer = Pacer::new(1_000_000);
        let started = Instant::now();
        // The first 1 MB is the starting credit. The rest has to wait.
        pacer.consume(1_300_000);
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(200),
            "waited only {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(900),
            "waited too long {elapsed:?}"
        );
    }

    #[test]
    fn deprioritize_affects_only_the_calling_thread() {
        let parent = read_nice();
        let child = thread::spawn(|| {
            let before = read_nice();
            let described = deprioritize();
            (before, read_nice(), read_io_class(), described)
        })
        .join()
        .unwrap();
        assert_eq!(read_nice(), parent);
        assert_eq!(child.0, parent);
        assert_eq!(child.1, 19, "{}", child.3);
        assert_eq!(child.2, IOPRIO_CLASS_IDLE, "{}", child.3);
    }

    fn read_nice() -> i64 {
        let text = fs::read_to_string("/proc/thread-self/stat").unwrap();
        let rest = text.rsplit_once(')').unwrap().1;
        let fields: Vec<&str> = rest.split_whitespace().collect();
        // Field 19 of stat is nice. Field 3 (state) is the first field after comm.
        fields[16].parse().unwrap()
    }

    fn read_io_class() -> i32 {
        let rc = unsafe { libc::syscall(libc::SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0) };
        (rc as i32) >> IOPRIO_CLASS_SHIFT
    }
}
