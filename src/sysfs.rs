//! Virtual files under `<mount>/sys`.
//!
//! A FUSE daemon cannot publish kernel sysfs entries. These inodes exist only
//! in the mount, are not backed by the local directory, and are never queued
//! for the remote. Inode numbers sit at the top of the `u64` range so they do
//! not collide with a local `st_ino`.

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::queue::Op;

pub const DIR: u64 = u64::MAX - 16;
pub const STATS: u64 = u64::MAX - 15;
pub const QUEUE: u64 = u64::MAX - 14;
pub const ERRORS: u64 = u64::MAX - 13;
pub const NAME: &str = "sys";

const ERROR_CAP: usize = 64;

pub fn is_reserved(ino: u64) -> bool {
    matches!(ino, DIR | STATS | QUEUE | ERRORS)
}

pub fn is_file(ino: u64) -> bool {
    matches!(ino, STATS | QUEUE | ERRORS)
}

pub fn rel_of(ino: u64) -> &'static str {
    match ino {
        DIR => "sys",
        STATS => "sys/stats",
        QUEUE => "sys/queue",
        ERRORS => "sys/errors",
        _ => "sys",
    }
}

/// Child of a virtual directory. `None` means the name is not one of ours.
pub fn child(parent: u64, name: &OsStr) -> Option<u64> {
    if parent == 1 && name == OsStr::new(NAME) {
        return Some(DIR);
    }
    if parent != DIR {
        return None;
    }
    match name.to_str() {
        Some("stats") => Some(STATS),
        Some("queue") => Some(QUEUE),
        Some("errors") => Some(ERRORS),
        _ => None,
    }
}

/// Fuse and remote counters. The error list keeps the newest 64 distinct
/// messages. A repeat of the latest message only refreshes its timestamp.
pub struct Counters {
    started: Instant,
    started_at: SystemTime,
    fuse_ok: AtomicU64,
    fuse_err: AtomicU64,
    remote_ok: AtomicU64,
    remote_retries: AtomicU64,
    errors: Mutex<VecDeque<(i64, String)>>,
}

impl Counters {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            started_at: SystemTime::now(),
            fuse_ok: AtomicU64::new(0),
            fuse_err: AtomicU64::new(0),
            remote_ok: AtomicU64::new(0),
            remote_retries: AtomicU64::new(0),
            errors: Mutex::new(VecDeque::new()),
        }
    }

    pub fn started_at(&self) -> SystemTime {
        self.started_at
    }

    pub fn note_fuse(&self, ok: bool) {
        if ok {
            self.fuse_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.fuse_err.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn note_remote_ok(&self) {
        self.remote_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_remote_retry(&self, message: String) {
        self.remote_retries.fetch_add(1, Ordering::Relaxed);
        self.note_error(message);
    }

    pub fn note_error(&self, message: String) {
        let mut errors = self.errors.lock().expect("error lock");
        let now = unix_secs();
        if errors
            .back()
            .is_some_and(|(_, existing)| existing == &message)
        {
            errors.back_mut().expect("error just observed").0 = now;
            return;
        }
        errors.push_back((now, message));
        while errors.len() > ERROR_CAP {
            errors.pop_front();
        }
    }

    pub fn render_stats(&self, queue_depth: u64) -> Vec<u8> {
        format!(
            "uptime_ms {}\nfuse_ok {}\nfuse_errors {}\nremote_ok {}\nremote_retries {}\nqueue_depth {}\n",
            self.started.elapsed().as_millis(),
            self.fuse_ok.load(Ordering::Relaxed),
            self.fuse_err.load(Ordering::Relaxed),
            self.remote_ok.load(Ordering::Relaxed),
            self.remote_retries.load(Ordering::Relaxed),
            queue_depth,
        )
        .into_bytes()
    }

    pub fn render_errors(&self) -> Vec<u8> {
        let errors = self.errors.lock().expect("error lock");
        let mut out = String::new();
        for (secs, message) in errors.iter() {
            out.push_str(&format!("{secs} {message}\n"));
        }
        out.into_bytes()
    }
}

pub fn render_queue(records: &[(u64, Op)]) -> Vec<u8> {
    let mut out = String::new();
    for (seq, op) in records {
        out.push_str(&format!("{seq} {}\n", op.describe()));
    }
    out.into_bytes()
}

fn unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Op;
    use std::path::PathBuf;

    #[test]
    fn stats_lists_every_counter() {
        let counters = Counters::new();
        counters.note_fuse(true);
        counters.note_fuse(false);
        counters.note_remote_ok();
        let text = String::from_utf8(counters.render_stats(3)).unwrap();
        assert!(text.contains("fuse_ok 1\n"));
        assert!(text.contains("fuse_errors 1\n"));
        assert!(text.contains("remote_ok 1\n"));
        assert!(text.contains("remote_retries 0\n"));
        assert!(text.contains("queue_depth 3\n"));
        assert!(text.contains("uptime_ms "));
    }

    #[test]
    fn repeated_errors_collapse_and_the_ring_is_capped() {
        let counters = Counters::new();
        counters.note_error("same".to_string());
        counters.note_error("same".to_string());
        let text = String::from_utf8(counters.render_errors()).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains(" same\n"));
        for n in 0..ERROR_CAP + 5 {
            counters.note_error(format!("err {n}"));
        }
        let text = String::from_utf8(counters.render_errors()).unwrap();
        assert_eq!(text.lines().count(), ERROR_CAP);
        assert!(text.contains(&format!("err {}", ERROR_CAP + 4)));
        assert!(!text.contains("err 0\n") && !text.contains(" err 0\n"));
        assert!(!text.contains("same"));
    }

    #[test]
    fn queue_file_lists_pending_records() {
        let records = vec![(
            7,
            Op::Rename {
                from: PathBuf::from("a"),
                to: PathBuf::from("b"),
                flags: 0,
            },
        )];
        let text = String::from_utf8(render_queue(&records)).unwrap();
        assert_eq!(text, "7 rename a -> b flags=0\n");
        assert!(render_queue(&[]).is_empty());
    }
}
