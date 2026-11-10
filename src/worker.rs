use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::queue::{Create, ObjectKind, Op, Queue, Setattr, Snapshot, WriteData};
use crate::remote_pin::{self, RemoteWatch};
use crate::sysfs::Counters;
use crate::util;

const RETRY_MIN: Duration = Duration::from_millis(200);
const RETRY_MAX: Duration = Duration::from_secs(5);

pub fn spawn(
    queue: Arc<Queue>,
    local: PathBuf,
    remote: PathBuf,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    debug: bool,
    watch: Option<RemoteWatch>,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("mirrorfs-remote".to_string())
        .spawn(move || {
            if let Some(watch) = watch {
                run_watched(queue, local, remote, stop, counters, debug, watch);
            } else {
                run(queue, local, remote, stop, counters, debug);
            }
        })
}

struct Pin {
    // Held so the descriptor in `root` stays open for the life of the pin.
    _file: File,
    root: PathBuf,
}

struct Gate {
    pin: Mutex<Option<Pin>>,
    ready: Condvar,
}

impl Gate {
    fn new() -> Self {
        Self {
            pin: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn current(&self) -> Option<PathBuf> {
        self.pin
            .lock()
            .expect("remote pin")
            .as_ref()
            .map(|pin| pin.root.clone())
    }

    fn publish(&self, file: File) {
        let root = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
        *self.pin.lock().expect("remote pin") = Some(Pin { _file: file, root });
        self.ready.notify_all();
    }

    fn clear(&self) {
        *self.pin.lock().expect("remote pin") = None;
    }

    fn wait_for_pin(&self, stop: &AtomicBool) -> Option<PathBuf> {
        let mut guard = self.pin.lock().expect("remote pin");
        loop {
            if stop.load(Ordering::Relaxed) {
                return None;
            }
            if let Some(pin) = guard.as_ref() {
                return Some(pin.root.clone());
            }
            let (next, _) = self
                .ready
                .wait_timeout(guard, Duration::from_millis(50))
                .expect("remote pin");
            guard = next;
        }
    }
}

fn run_watched(
    queue: Arc<Queue>,
    local: PathBuf,
    remote: PathBuf,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    debug: bool,
    watch: RemoteWatch,
) {
    let gate = Arc::new(Gate::new());
    let probe_gate = Arc::clone(&gate);
    let probe_stop = Arc::clone(&stop);
    let probe_remote = remote.clone();
    let probe = thread::Builder::new()
        .name("mirrorfs-remote-check".to_string())
        .spawn(move || probe_loop(&probe_remote, watch, &probe_stop, &probe_gate))
        .expect("spawn remote check");
    let mut backoff = RETRY_MIN;
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let Some(remote_root) = gate.wait_for_pin(&stop) else {
            break;
        };
        let head = match queue.head() {
            Ok(head) => head,
            Err(err) => {
                counters.note_error(format!("queue read: {err}"));
                eprintln!("mirrorfs: queue read failed: {err}");
                wait(&stop, backoff);
                backoff = grow(backoff);
                continue;
            }
        };
        let Some((seq, op)) = head else {
            backoff = RETRY_MIN;
            wait(&stop, RETRY_MIN);
            continue;
        };
        let recovered = match apply(&local, &remote_root, &op) {
            Ok(()) => None,
            Err(err) => {
                if stop.load(Ordering::Relaxed) {
                    log_retry(&op, &err);
                    break;
                }
                if remote_lost(&err) || !mount_available(&remote_root) {
                    note_lost(&counters, &op, &err);
                    gate.clear();
                    continue;
                }
                match repair(&local, &remote_root, &op, &err) {
                    Repair::Done => Some(err),
                    Repair::Unchanged => {
                        if !mount_available(&remote_root) {
                            note_lost(&counters, &op, &err);
                            gate.clear();
                            continue;
                        }
                        log_retry(&op, &err);
                        counters.note_remote_retry(retry_message(&op, &err));
                        wait(&stop, backoff);
                        backoff = grow(backoff);
                        continue;
                    }
                    Repair::Failed(fix_err) => {
                        if remote_lost(&fix_err) || !mount_available(&remote_root) {
                            note_lost(&counters, &op, &fix_err);
                            gate.clear();
                            continue;
                        }
                        log_retry(&op, &fix_err);
                        counters.note_remote_retry(retry_message(&op, &fix_err));
                        wait(&stop, backoff);
                        backoff = grow(backoff);
                        continue;
                    }
                }
            }
        };
        if let Err(err) = queue.finish(seq, &op) {
            counters.note_error(format!(
                "queue ack {seq} op={} path={} result={err}",
                op.kind_name(),
                op.path().display()
            ));
            eprintln!("mirrorfs: queue ack {seq} failed: {err}");
            wait(&stop, backoff);
            backoff = grow(backoff);
            continue;
        }
        counters.note_remote_ok();
        if debug {
            eprintln!(
                "mirrorfs remote op={} path={} result=ok",
                op.kind_name(),
                op.path().display()
            );
            let _ = std::io::Write::flush(&mut std::io::stderr());
        }
        if let Some(err) = recovered {
            counters.note_error(format!(
                "remote op={} path={} result={err} recovered",
                op.kind_name(),
                op.path().display()
            ));
        }
        backoff = RETRY_MIN;
    }
    stop.store(true, Ordering::Relaxed);
    let _ = probe.join();
}

fn note_lost(counters: &Counters, op: &Op, err: &io::Error) {
    eprintln!(
        "mirrorfs: remote mount lost op={} path={} result={err}",
        op.kind_name(),
        op.path().display()
    );
    let _ = std::io::Write::flush(&mut std::io::stderr());
    counters.note_remote_retry(retry_message(op, err));
}

fn probe_loop(remote: &Path, watch: RemoteWatch, stop: &AtomicBool, gate: &Gate) {
    let mut reported = String::new();
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        if gate.current().is_none() {
            match remote_pin::pin_if_match(remote, &watch.fs_types) {
                Ok(Some(file)) => {
                    let found =
                        remote_pin::fstype_of(remote).unwrap_or_else(|_| "online".to_string());
                    eprintln!(
                        "mirrorfs: remote {} is {found}, draining the queue",
                        remote.display()
                    );
                    let _ = std::io::Write::flush(&mut std::io::stderr());
                    gate.publish(file);
                    reported.clear();
                }
                Ok(None) => {
                    let found =
                        remote_pin::fstype_of(remote).unwrap_or_else(|_| "unavailable".to_string());
                    report_waiting(remote, &watch, &found, &mut reported);
                }
                Err(err) => report_waiting(remote, &watch, &err.to_string(), &mut reported),
            }
        }
        wait(stop, watch.every);
    }
}

fn report_waiting(remote: &Path, watch: &RemoteWatch, found: &str, reported: &mut String) {
    let wanted = watch.fs_types.join(":");
    let message = format!(
        "{} is {found}, waiting for {wanted}, next check {}",
        remote.display(),
        remote_pin::format_interval(watch.every)
    );
    if reported == &message {
        return;
    }
    eprintln!("mirrorfs: remote {message}");
    let _ = std::io::Write::flush(&mut std::io::stderr());
    *reported = message;
}

fn remote_lost(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(
            libc::EIO
                | libc::ESTALE
                | libc::ETIMEDOUT
                | libc::ENOTCONN
                | libc::EHOSTDOWN
                | libc::EHOSTUNREACH
                | libc::ENETDOWN
                | libc::ENETUNREACH
                | libc::ENETRESET
                | libc::ECONNRESET
                | libc::ECONNREFUSED
                | libc::ENODEV
                | libc::ESHUTDOWN
                | libc::ENONET
                | libc::EPIPE
        )
    )
}

fn run(
    queue: Arc<Queue>,
    local: PathBuf,
    remote: PathBuf,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    debug: bool,
) {
    let mut backoff = RETRY_MIN;
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let head = match queue.head() {
            Ok(head) => head,
            Err(err) => {
                counters.note_error(format!("queue read: {err}"));
                eprintln!("mirrorfs: queue read failed: {err}");
                wait(&stop, backoff);
                backoff = grow(backoff);
                continue;
            }
        };
        let Some((seq, op)) = head else {
            backoff = RETRY_MIN;
            wait(&stop, RETRY_MIN);
            continue;
        };
        let recovered = match apply(&local, &remote, &op) {
            Ok(()) => None,
            Err(err) => {
                if stop.load(Ordering::Relaxed) {
                    log_retry(&op, &err);
                    break;
                }
                match repair(&local, &remote, &op, &err) {
                    Repair::Done => Some(err),
                    Repair::Unchanged => {
                        log_retry(&op, &err);
                        counters.note_remote_retry(retry_message(&op, &err));
                        wait(&stop, backoff);
                        backoff = grow(backoff);
                        continue;
                    }
                    Repair::Failed(fix_err) => {
                        log_retry(&op, &fix_err);
                        counters.note_remote_retry(retry_message(&op, &fix_err));
                        wait(&stop, backoff);
                        backoff = grow(backoff);
                        continue;
                    }
                }
            }
        };
        if let Err(err) = queue.finish(seq, &op) {
            counters.note_error(format!(
                "queue ack {seq} op={} path={} result={err}",
                op.kind_name(),
                op.path().display()
            ));
            eprintln!("mirrorfs: queue ack {seq} failed: {err}");
            wait(&stop, backoff);
            backoff = grow(backoff);
            continue;
        }
        counters.note_remote_ok();
        if debug {
            eprintln!(
                "mirrorfs remote op={} path={} result=ok",
                op.kind_name(),
                op.path().display()
            );
            let _ = std::io::Write::flush(&mut std::io::stderr());
        }
        if let Some(err) = recovered {
            counters.note_error(format!(
                "remote op={} path={} result={err} recovered",
                op.kind_name(),
                op.path().display()
            ));
        }
        backoff = RETRY_MIN;
    }
}

fn retry_message(op: &Op, err: &io::Error) -> String {
    format!(
        "remote op={} path={} result={err}",
        op.kind_name(),
        op.path().display()
    )
}

fn log_retry(op: &Op, err: &io::Error) {
    eprintln!(
        "mirrorfs remote op={} path={} result={err} retry",
        op.kind_name(),
        op.path().display()
    );
    let _ = std::io::Write::flush(&mut std::io::stderr());
}

enum Repair {
    /// The remote now matches the operation.
    Done,
    /// Mount is down, or the record itself is invalid. Retry the original error.
    Unchanged,
    /// A repair was attempted and failed. Retry this error.
    Failed(io::Error),
}

/// One repair when `apply` fails and the remote directory can still be stated.
/// A missing remote root keeps the old backoff and does not copy anything.
fn repair(local: &Path, remote: &Path, op: &Op, err: &io::Error) -> Repair {
    if err.kind() == ErrorKind::InvalidInput || !mount_available(remote) {
        return Repair::Unchanged;
    }
    eprintln!(
        "mirrorfs: remote op={} path={} result={err}; copying the local object and retrying the operation",
        op.kind_name(),
        op.path().display()
    );
    let _ = std::io::Write::flush(&mut std::io::stderr());
    match fix(local, remote, op) {
        Ok(()) => Repair::Done,
        Err(fix_err) => Repair::Failed(fix_err),
    }
}

fn mount_available(remote_root: &Path) -> bool {
    match util::lstat(remote_root) {
        Ok(st) => st.st_mode & libc::S_IFMT == libc::S_IFDIR,
        Err(_) => false,
    }
}

/// Copy the local object onto the remote path, then apply the queued mutation.
///
/// A `Copy` record is written from the bytes it already stores. Re-reading
/// the live file would let a later mutation change an earlier snapshot.
/// An exchange copies both post-exchange local names and does not swap them:
/// swapping again would undo the copy. A plain rename moves the remote
/// source when that source still exists, so a directory's children move
/// with it instead of being replaced by a copy of the destination.
fn fix(local: &Path, remote: &Path, op: &Op) -> io::Result<()> {
    match op {
        Op::Copy(snap) => {
            let path = remote_path(remote, &snap.path)?;
            assert_child(remote, &path)?;
            clear_conflict(&path, snap.kind)?;
            apply_copy(local, remote, snap)
        }
        Op::CopyBlob(op) => {
            let path = remote_path(remote, &op.path)?;
            assert_child(remote, &path)?;
            clear_conflict(&path, op.kind)?;
            apply_copy_blob(local, remote, op)
        }
        Op::CopyStored(op) => {
            let path = remote_path(remote, &op.snap.path)?;
            assert_child(remote, &path)?;
            clear_conflict(&path, op.snap.kind)?;
            apply_copy_stored(local, remote, op)
        }
        Op::WriteStored(op) => apply_write_stored(remote, op),
        Op::Rename { from, to, flags } if *flags & libc::RENAME_EXCHANGE != 0 => {
            replace_from_local(local, remote, to)?;
            replace_from_local(local, remote, from)?;
            Ok(())
        }
        Op::Rename { from, to, flags } => fix_rename(local, remote, from, to, *flags),
        Op::Link { from, to } => fix_link(local, remote, from, to),
        Op::Unlink(path) => fix_unlink(remote, path),
        Op::Rmdir(path) => fix_rmdir(remote, path),
        other => fix_object(local, remote, other),
    }
}

fn fix_object(local: &Path, remote: &Path, op: &Op) -> io::Result<()> {
    let rel = op.path();
    let _ = remote_path(remote, rel)?;
    match replace_from_local(local, remote, rel) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    match apply(local, remote, op) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound && !local_exists(local, rel)? => Ok(()),
        Err(err) => Err(err),
    }
}

fn fix_rename(local: &Path, remote: &Path, from: &Path, to: &Path, flags: u32) -> io::Result<()> {
    let src = remote_path(remote, from)?;
    if path_exists(&src)? {
        match rename_over(local, remote, from, to, flags, &src) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound && !path_exists(&src)? => {}
            Err(err) => return Err(err),
        }
    }
    if local_exists(local, to)? {
        match replace_from_local(local, remote, to) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        return apply_rename(local, remote, from, to, flags);
    }
    match apply_rename(local, remote, from, to, flags) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => {
            let dst = remote_path(remote, to)?;
            if path_exists(&dst)? {
                Err(err)
            } else {
                Ok(())
            }
        }
        Err(err) => Err(err),
    }
}

fn rename_over(
    local: &Path,
    remote: &Path,
    from: &Path,
    to: &Path,
    flags: u32,
    src: &Path,
) -> io::Result<()> {
    match apply_rename(local, remote, from, to, flags) {
        Ok(()) => Ok(()),
        Err(err) if blocks_rename(&err) => {
            let dst = remote_path(remote, to)?;
            assert_child(remote, &dst)?;
            remove_blocking(&dst, Some(src))?;
            apply_rename(local, remote, from, to, flags)
        }
        Err(err) => Err(err),
    }
}

fn blocks_rename(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::AlreadyExists
            | ErrorKind::DirectoryNotEmpty
            | ErrorKind::IsADirectory
            | ErrorKind::NotADirectory
    )
}

fn fix_link(local: &Path, remote: &Path, from: &Path, to: &Path) -> io::Result<()> {
    let src = remote_path(remote, from)?;
    let dst = remote_path(remote, to)?;
    match replace_from_local(local, remote, from) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    match apply_link(local, remote, from, to) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::AlreadyExists && dst != src => {
            assert_child(remote, &dst)?;
            remove_blocking(&dst, Some(&src))?;
            apply_link(local, remote, from, to)
        }
        Err(err) => Err(err),
    }
}

fn fix_unlink(remote: &Path, rel: &Path) -> io::Result<()> {
    let full = remote_path(remote, rel)?;
    assert_child(remote, &full)?;
    match remove_file(&full) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::IsADirectory => remove_blocking(&full, None),
        Err(err) => Err(err),
    }
}

fn fix_rmdir(remote: &Path, rel: &Path) -> io::Result<()> {
    let full = remote_path(remote, rel)?;
    assert_child(remote, &full)?;
    match remove_dir(&full) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotADirectory => remove_file(&full),
        Err(err) if err.kind() == ErrorKind::DirectoryNotEmpty => remove_blocking(&full, None),
        Err(err) => Err(err),
    }
}

fn replace_from_local(local: &Path, remote: &Path, rel: &Path) -> io::Result<()> {
    let remote_full = remote_path(remote, rel)?;
    assert_child(remote, &remote_full)?;
    let local_full = local.join(rel);
    let st = util::lstat(&local_full)?;
    if st.st_mode & libc::S_IFMT == libc::S_IFREG && st.st_size as u64 > crate::queue::INLINE_LIMIT
    {
        return match stream_local_file(local, remote, rel, &local_full) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                remove_blocking(&remote_full, None)?;
                stream_local_file(local, remote, rel, &local_full)
            }
            Err(err) => Err(err),
        };
    }
    let snap = crate::queue::snapshot(local, rel)?;
    clear_conflict(&remote_full, snap.kind)?;
    match apply_copy(local, remote, &snap) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            // The node is there but cannot be updated in place, for example
            // a mode 0444 file. Recreate it when the parent allows the unlink.
            remove_blocking(&remote_full, None)?;
            apply_copy(local, remote, &snap)
        }
        Err(err) => Err(err),
    }
}

fn stream_local_file(local: &Path, remote: &Path, rel: &Path, src: &Path) -> io::Result<()> {
    let remote_full = remote_path(remote, rel)?;
    ensure_parents(local, remote, rel)?;
    clear_conflict(&remote_full, ObjectKind::File)?;
    crate::sparse::copy_sparse(src, &remote_full)?;
    let st = util::lstat(src)?;
    let xattrs = crate::queue::list_xattrs(src)?;
    stamp_copied(
        &remote_full,
        st.st_mode & 0o7777,
        st.st_uid,
        st.st_gid,
        (st.st_atime, st.st_atime_nsec),
        (st.st_mtime, st.st_mtime_nsec),
        &xattrs,
        false,
    )?;
    restamp_parents(local, remote, rel)
}

fn clear_conflict(path: &Path, wanted: ObjectKind) -> io::Result<()> {
    let st = match util::lstat(path) {
        Ok(st) => st,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    if kind_of(st.st_mode) == wanted {
        return Ok(());
    }
    remove_blocking(path, None)
}

fn remove_blocking(path: &Path, protect: Option<&Path>) -> io::Result<()> {
    if protect.is_some_and(|keep| keep != path && keep.starts_with(path)) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "refusing to remove a directory that contains the source",
        ));
    }
    let st = match util::lstat(path) {
        Ok(st) => st,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    if kind_of(st.st_mode) == ObjectKind::Dir {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn assert_child(root: &Path, path: &Path) -> io::Result<()> {
    if path == root || path == root.join(".") {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "refusing to replace the remote root",
        ));
    }
    Ok(())
}

fn local_exists(local: &Path, rel: &Path) -> io::Result<bool> {
    let full = remote_path(local, rel)?;
    match util::lstat(&full) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn grow(current: Duration) -> Duration {
    current.saturating_mul(2).min(RETRY_MAX)
}

fn wait(stop: &AtomicBool, total: Duration) {
    let slice = Duration::from_millis(50);
    let mut left = total;
    while !left.is_zero() {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let step = left.min(slice);
        thread::sleep(step);
        left = left.saturating_sub(step);
    }
}

pub fn apply(local_root: &Path, remote_root: &Path, op: &Op) -> io::Result<()> {
    match op {
        Op::Create(op) => apply_create(local_root, remote_root, op),
        Op::Write(op) => apply_write(remote_root, op),
        Op::Setattr(op) => apply_setattr(remote_root, op),
        Op::Unlink(path) => remove_file(&remote_path(remote_root, path)?),
        Op::Rmdir(path) => remove_dir(&remote_path(remote_root, path)?),
        Op::Fsync { path, datasync } => apply_fsync(&remote_path(remote_root, path)?, *datasync),
        Op::Copy(op) => apply_copy(local_root, remote_root, op),
        Op::CopyBlob(op) => apply_copy_blob(local_root, remote_root, op),
        Op::CopyStored(op) => apply_copy_stored(local_root, remote_root, op),
        Op::WriteStored(op) => apply_write_stored(remote_root, op),
        Op::Rename { from, to, flags } => apply_rename(local_root, remote_root, from, to, *flags),
        Op::Link { from, to } => apply_link(local_root, remote_root, from, to),
        Op::Symlink {
            path,
            target,
            uid,
            gid,
        } => apply_symlink(local_root, remote_root, path, target, *uid, *gid),
        Op::Setxattr {
            path,
            name,
            value,
            flags,
        } => apply_setxattr(remote_root, path, name, value, *flags),
        Op::Removexattr { path, name } => apply_removexattr(remote_root, path, name),
        Op::Fallocate {
            path,
            offset,
            length,
            mode,
        } => apply_fallocate(remote_root, path, *offset, *length, *mode),
    }
}

fn remote_path(root: &Path, rel: &Path) -> io::Result<PathBuf> {
    if rel.is_absolute() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "queued path is absolute",
        ));
    }
    for component in rel.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            _ => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "queued path escapes the remote root",
                ))
            }
        }
    }
    Ok(root.join(rel))
}

fn apply_create(local: &Path, remote: &Path, op: &Create) -> io::Result<()> {
    ensure_parents(local, remote, &op.path)?;
    let path = remote_path(remote, &op.path)?;
    let mode = op.mode & 0o7777;
    match op.kind {
        ObjectKind::File => create_file(&path, mode)?,
        ObjectKind::Dir => create_dir(&path, mode)?,
        ObjectKind::Fifo | ObjectKind::Char | ObjectKind::Block | ObjectKind::Socket => {
            create_node(&path, op.kind, mode, op.rdev)?;
        }
        ObjectKind::Symlink => {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "symlink is not a create record",
            ))
        }
    }
    util::chmod_path(&path, mode)?;
    util::lchown_path(&path, op.uid, op.gid)?;
    // mkdir/create bumps the parent mtime after ensure_parents stamped it.
    restamp_parents(local, remote, &op.path)?;
    Ok(())
}

fn create_file(path: &Path, mode: u32) -> io::Result<()> {
    let c = util::cstring(path)?;
    let fd = unsafe {
        libc::open(
            c.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd >= 0 {
        unsafe { libc::close(fd) };
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EEXIST) {
        return expect_kind(path, ObjectKind::File);
    }
    Err(err)
}

fn create_dir(path: &Path, mode: u32) -> io::Result<()> {
    let c = util::cstring(path)?;
    let rc = unsafe { libc::mkdir(c.as_ptr(), mode) };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EEXIST) {
        return expect_kind(path, ObjectKind::Dir);
    }
    Err(err)
}

fn create_node(path: &Path, kind: ObjectKind, mode: u32, rdev: u32) -> io::Result<()> {
    let bits = match kind {
        ObjectKind::Fifo => libc::S_IFIFO,
        ObjectKind::Char => libc::S_IFCHR,
        ObjectKind::Block => libc::S_IFBLK,
        ObjectKind::Socket => libc::S_IFSOCK,
        _ => return Err(io::Error::new(ErrorKind::InvalidInput, "not a device node")),
    };
    let c = util::cstring(path)?;
    let rc = unsafe { libc::mknod(c.as_ptr(), bits | mode, rdev as libc::dev_t) };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EEXIST) {
        return expect_kind(path, kind);
    }
    Err(err)
}

fn expect_kind(path: &Path, kind: ObjectKind) -> io::Result<()> {
    let st = util::lstat(path)?;
    let actual = kind_of(st.st_mode);
    if actual == kind {
        Ok(())
    } else {
        Err(io::Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "{} exists as {:?} and the queue asked for {:?}",
                path.display(),
                actual,
                kind
            ),
        ))
    }
}

fn kind_of(mode: u32) -> ObjectKind {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => ObjectKind::Dir,
        libc::S_IFIFO => ObjectKind::Fifo,
        libc::S_IFCHR => ObjectKind::Char,
        libc::S_IFBLK => ObjectKind::Block,
        libc::S_IFSOCK => ObjectKind::Socket,
        libc::S_IFLNK => ObjectKind::Symlink,
        _ => ObjectKind::File,
    }
}

fn ensure_parents(local: &Path, remote: &Path, rel: &Path) -> io::Result<()> {
    let mut ancestors = Vec::new();
    let mut cursor = rel.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            break;
        }
        ancestors.push(parent.to_path_buf());
        cursor = parent.parent();
    }
    ancestors.reverse();
    for ancestor in ancestors {
        let remote_dir = remote_path(remote, &ancestor)?;
        if util::lstat(&remote_dir).is_ok() {
            continue;
        }
        let local_dir = local.join(&ancestor);
        let st = util::lstat(&local_dir)?;
        if st.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(io::Error::new(
                ErrorKind::NotADirectory,
                format!("{} is not a directory", local_dir.display()),
            ));
        }
        let mode = st.st_mode & 0o7777;
        create_dir(&remote_dir, mode)?;
        util::chmod_path(&remote_dir, mode)?;
        util::lchown_path(&remote_dir, st.st_uid, st.st_gid)?;
        let times = [
            timespec((st.st_atime, st.st_atime_nsec)),
            timespec((st.st_mtime, st.st_mtime_nsec)),
        ];
        util::utimens_nofollow(&remote_dir, &times)?;
    }
    Ok(())
}

fn apply_copy(local: &Path, remote: &Path, op: &Snapshot) -> io::Result<()> {
    ensure_parents(local, remote, &op.path)?;
    let path = remote_path(remote, &op.path)?;
    let mode = op.mode & 0o7777;
    match op.kind {
        ObjectKind::File => {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            file.write_all(&op.data)?;
        }
        ObjectKind::Dir => create_dir(&path, mode)?,
        ObjectKind::Symlink => write_symlink(&path, &op.symlink)?,
        ObjectKind::Fifo | ObjectKind::Char | ObjectKind::Block | ObjectKind::Socket => {
            create_node(&path, op.kind, mode, op.rdev)?;
        }
    }
    stamp_copied(
        &path,
        mode,
        op.uid,
        op.gid,
        op.atime,
        op.mtime,
        &op.xattrs,
        op.kind == ObjectKind::Symlink,
    )?;
    restamp_parents(local, remote, &op.path)?;
    Ok(())
}

fn apply_copy_blob(local: &Path, remote: &Path, op: &crate::queue::CopyBlob) -> io::Result<()> {
    ensure_parents(local, remote, &op.path)?;
    let path = remote_path(remote, &op.path)?;
    crate::sparse::copy_sparse(&op.blob, &path)?;
    stamp_copied(
        &path,
        op.mode & 0o7777,
        op.uid,
        op.gid,
        op.atime,
        op.mtime,
        &op.xattrs,
        false,
    )?;
    restamp_parents(local, remote, &op.path)
}

fn apply_copy_stored(local: &Path, remote: &Path, op: &crate::queue::CopyStored) -> io::Result<()> {
    ensure_parents(local, remote, &op.snap.path)?;
    let path = remote_path(remote, &op.snap.path)?;
    let mode = op.snap.mode & 0o7777;
    match op.snap.kind {
        ObjectKind::File => {
            let src = File::open(&op.file)?;
            let dst = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            crate::sparse::copy_range(&src, op.offset, &dst, 0, op.len)?;
            dst.sync_all()?;
        }
        ObjectKind::Dir => create_dir(&path, mode)?,
        ObjectKind::Symlink => write_symlink(&path, &op.snap.symlink)?,
        ObjectKind::Fifo | ObjectKind::Char | ObjectKind::Block | ObjectKind::Socket => {
            create_node(&path, op.snap.kind, mode, op.snap.rdev)?;
        }
    }
    stamp_copied(
        &path,
        mode,
        op.snap.uid,
        op.snap.gid,
        op.snap.atime,
        op.snap.mtime,
        &op.snap.xattrs,
        op.snap.kind == ObjectKind::Symlink,
    )?;
    restamp_parents(local, remote, &op.snap.path)
}

fn apply_write_stored(root: &Path, op: &crate::queue::WriteStored) -> io::Result<()> {
    let path = remote_path(root, &op.path)?;
    let dst = OpenOptions::new().write(true).open(&path)?;
    let src = File::open(&op.file)?;
    crate::sparse::copy_range(&src, op.data_off, &dst, op.offset, op.len)?;
    dst.sync_all()
}

fn stamp_copied(
    path: &Path,
    mode: u32,
    uid: u32,
    gid: u32,
    atime: (i64, i64),
    mtime: (i64, i64),
    xattrs: &[(Vec<u8>, Vec<u8>)],
    symlink: bool,
) -> io::Result<()> {
    if symlink {
        util::lchown_path(path, uid, gid)?;
    } else {
        util::lchown_path(path, uid, gid)?;
        util::chmod_path(path, mode)?;
    }
    let times = [timespec(atime), timespec(mtime)];
    util::utimens_nofollow(path, &times)?;
    set_xattrs(path, xattrs)
}

/// Creating the new object updates ancestor directory mtimes. Put the local
/// timestamps back on remote ancestors this process owns. Directories owned by
/// someone else are left unchanged so a permission error cannot stall the queue.
fn restamp_parents(local: &Path, remote: &Path, rel: &Path) -> io::Result<()> {
    let mut cursor = rel.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            break;
        }
        let local_dir = local.join(parent);
        let local_st = match util::lstat(&local_dir) {
            Ok(st) => st,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                cursor = parent.parent();
                continue;
            }
            Err(err) => return Err(err),
        };
        if local_st.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(io::Error::new(
                ErrorKind::NotADirectory,
                format!("{} is not a directory", local_dir.display()),
            ));
        }
        let remote_dir = remote_path(remote, parent)?;
        let remote_st = match util::lstat(&remote_dir) {
            Ok(st) => st,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                cursor = parent.parent();
                continue;
            }
            Err(err) => return Err(err),
        };
        // NFS can serve a cached directory mtime, so do not trust a comparison.
        // Stamp from the local directory whenever we own the remote one.
        if remote_st.st_uid == unsafe { libc::geteuid() } {
            let times = [
                timespec((local_st.st_atime, local_st.st_atime_nsec)),
                timespec((local_st.st_mtime, local_st.st_mtime_nsec)),
            ];
            util::utimens_nofollow(&remote_dir, &times)?;
        }
        cursor = parent.parent();
    }
    Ok(())
}

fn write_symlink(path: &Path, target: &[u8]) -> io::Result<()> {
    if let Ok(st) = util::lstat(path) {
        if kind_of(st.st_mode) == ObjectKind::Symlink {
            let current = fs::read_link(path)?;
            if current.as_os_str().as_bytes() == target {
                return Ok(());
            }
            fs::remove_file(path)?;
        } else {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!("{} exists and is not a symlink", path.display()),
            ));
        }
    }
    let path_c = util::cstring(path)?;
    let target_c = std::ffi::CString::new(target).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "symlink target contains an interior NUL",
        )
    })?;
    let rc = unsafe { libc::symlink(target_c.as_ptr(), path_c.as_ptr()) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn set_xattrs(path: &Path, xattrs: &[(Vec<u8>, Vec<u8>)]) -> io::Result<()> {
    let path_c = util::cstring(path)?;
    for (name, value) in xattrs {
        let name_c = match std::ffi::CString::new(name.as_slice()) {
            Ok(name) => name,
            Err(_) => continue,
        };
        let rc = unsafe {
            libc::lsetxattr(
                path_c.as_ptr(),
                name_c.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                0,
            )
        };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EOPNOTSUPP) {
                eprintln!(
                    "mirrorfs: xattr {} is not supported on {}, skipping",
                    name_c.to_string_lossy(),
                    path.display()
                );
                continue;
            }
            return Err(err);
        }
    }
    Ok(())
}

fn apply_rename(local: &Path, remote: &Path, from: &Path, to: &Path, flags: u32) -> io::Result<()> {
    ensure_parents(local, remote, to)?;
    if flags & libc::RENAME_EXCHANGE != 0 {
        ensure_parents(local, remote, from)?;
    }
    let src = remote_path(remote, from)?;
    let dst = remote_path(remote, to)?;
    let src_c = util::cstring(&src)?;
    let dst_c = util::cstring(&dst)?;
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            src_c.as_ptr(),
            libc::AT_FDCWD,
            dst_c.as_ptr(),
            flags,
        )
    };
    if rc == 0 {
        return restamp_rename(local, remote, from, to, flags);
    }
    let err = io::Error::last_os_error();
    // NFS rejects RENAME_EXCHANGE with EINVAL. Swap through a temporary name
    // instead of leaving the record at the head of the queue forever.
    if flags & libc::RENAME_EXCHANGE != 0 && is_exchange_unsupported(&err) {
        eprintln!(
            "mirrorfs: remote rename exchange is not supported, swapping {} and {}",
            src.display(),
            dst.display()
        );
        exchange_fallback(&src, &dst)?;
        return restamp_rename(local, remote, from, to, flags);
    }
    // A crash after a successful plain rename retries the record. The source
    // is already gone and the destination is already there. Exchange is not
    // idempotent: replaying it swaps the two names again.
    if flags & libc::RENAME_EXCHANGE == 0
        && err.kind() == ErrorKind::NotFound
        && util::lstat(&src).is_err()
        && util::lstat(&dst).is_ok()
    {
        return Ok(());
    }
    Err(err)
}

fn restamp_rename(
    local: &Path,
    remote: &Path,
    from: &Path,
    to: &Path,
    flags: u32,
) -> io::Result<()> {
    restamp_parents(local, remote, to)?;
    if flags & libc::RENAME_EXCHANGE != 0 {
        restamp_parents(local, remote, from)?;
    }
    Ok(())
}

/// `RENAME_EXCHANGE` via three plain renames. NFS rejects the exchange flag,
/// and it also rejects `RENAME_NOREPLACE` when the destination is absent.
/// A leftover `.mirrorfs-exchange` in the destination parent means a previous
/// attempt stopped between those calls, and this finishes it.
fn exchange_fallback(src: &Path, dst: &Path) -> io::Result<()> {
    let parent = dst.parent().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "exchange destination has no parent",
        )
    })?;
    let tmp = parent.join(".mirrorfs-exchange");
    if tmp == src || tmp == dst {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "exchange path collides with the temporary name",
        ));
    }
    let src_exists = path_exists(src)?;
    let dst_exists = path_exists(dst)?;
    let tmp_exists = path_exists(&tmp)?;
    if tmp_exists && !src_exists && dst_exists {
        rename_absent(dst, src)?;
        rename_absent(&tmp, dst)?;
        return Ok(());
    }
    if tmp_exists && src_exists && !dst_exists {
        rename_absent(&tmp, dst)?;
        return Ok(());
    }
    if !tmp_exists && src_exists && dst_exists {
        rename_absent(src, &tmp)?;
        rename_absent(dst, src)?;
        rename_absent(&tmp, dst)?;
        return Ok(());
    }
    Err(io::Error::new(
        ErrorKind::NotFound,
        "exchange cannot be completed from the remote names",
    ))
}

fn path_exists(path: &Path) -> io::Result<bool> {
    match util::lstat(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn rename_absent(src: &Path, dst: &Path) -> io::Result<()> {
    if path_exists(dst)? {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            "exchange target already exists",
        ));
    }
    let src_c = util::cstring(src)?;
    let dst_c = util::cstring(dst)?;
    let rc = unsafe { libc::rename(src_c.as_ptr(), dst_c.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn is_exchange_unsupported(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::EINVAL) || is_unsupported(err)
}

fn apply_link(local: &Path, remote: &Path, from: &Path, to: &Path) -> io::Result<()> {
    ensure_parents(local, remote, to)?;
    let src = remote_path(remote, from)?;
    let dst = remote_path(remote, to)?;
    let src_c = util::cstring(&src)?;
    let dst_c = util::cstring(&dst)?;
    let rc = unsafe { libc::link(src_c.as_ptr(), dst_c.as_ptr()) };
    if rc == 0 {
        restamp_parents(local, remote, to)?;
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.kind() == ErrorKind::AlreadyExists {
        let left = util::lstat(&src)?;
        let right = util::lstat(&dst)?;
        if left.st_ino == right.st_ino && left.st_dev == right.st_dev {
            return Ok(());
        }
    }
    Err(err)
}

fn apply_symlink(
    local: &Path,
    remote: &Path,
    path: &Path,
    target: &[u8],
    uid: u32,
    gid: u32,
) -> io::Result<()> {
    ensure_parents(local, remote, path)?;
    let full = remote_path(remote, path)?;
    write_symlink(&full, target)?;
    util::lchown_path(&full, uid, gid)?;
    restamp_parents(local, remote, path)?;
    Ok(())
}

fn apply_setxattr(
    remote: &Path,
    path: &Path,
    name: &[u8],
    value: &[u8],
    flags: i32,
) -> io::Result<()> {
    let full = remote_path(remote, path)?;
    let path_c = util::cstring(&full)?;
    let name_c = xattr_name(name)?;
    let rc = unsafe {
        libc::lsetxattr(
            path_c.as_ptr(),
            name_c.as_ptr(),
            value.as_ptr() as *const libc::c_void,
            value.len(),
            flags,
        )
    };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if is_unsupported(&err) {
        log_skip_xattr(&name_c, &full);
        return Ok(());
    }
    if err.raw_os_error() == Some(libc::EEXIST) && xattr_eq(&path_c, &name_c, value)? {
        return Ok(());
    }
    Err(err)
}

fn apply_removexattr(remote: &Path, path: &Path, name: &[u8]) -> io::Result<()> {
    let full = remote_path(remote, path)?;
    let path_c = util::cstring(&full)?;
    let name_c = xattr_name(name)?;
    let rc = unsafe { libc::lremovexattr(path_c.as_ptr(), name_c.as_ptr()) };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ENODATA) || is_unsupported(&err) {
        if is_unsupported(&err) {
            log_skip_xattr(&name_c, &full);
        }
        return Ok(());
    }
    Err(err)
}

fn apply_fallocate(
    remote: &Path,
    path: &Path,
    offset: u64,
    length: u64,
    mode: i32,
) -> io::Result<()> {
    let full = remote_path(remote, path)?;
    let file = OpenOptions::new().write(true).open(&full)?;
    let rc = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            mode,
            offset as libc::off_t,
            length as libc::off_t,
        )
    };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if is_unsupported(&err) {
        eprintln!(
            "mirrorfs: fallocate is not supported on {}, skipping",
            full.display()
        );
        return Ok(());
    }
    Err(err)
}

fn xattr_name(name: &[u8]) -> io::Result<CString> {
    CString::new(name)
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "xattr name contains NUL"))
}

fn xattr_eq(path: &CString, name: &CString, value: &[u8]) -> io::Result<bool> {
    let mut size =
        unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return Ok(false);
    }
    let mut buf = vec![0u8; size as usize];
    size = unsafe {
        libc::lgetxattr(
            path.as_ptr(),
            name.as_ptr(),
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
        )
    };
    if size < 0 {
        return Ok(false);
    }
    buf.truncate(size as usize);
    Ok(buf == value)
}

fn is_unsupported(err: &io::Error) -> bool {
    // ENOTSUP and EOPNOTSUPP are the same value on Linux.
    err.raw_os_error() == Some(libc::EOPNOTSUPP)
}

fn log_skip_xattr(name: &CString, path: &Path) {
    eprintln!(
        "mirrorfs: xattr {} is not supported on {}, skipping",
        name.to_string_lossy(),
        path.display()
    );
}

fn apply_write(root: &Path, op: &WriteData) -> io::Result<()> {
    let path = remote_path(root, &op.path)?;
    let file = OpenOptions::new().write(true).open(&path)?;
    let mut offset = op.offset;
    let mut data = op.data.as_slice();
    while !data.is_empty() {
        let n = file.write_at(data, offset)?;
        if n == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "remote write made no progress",
            ));
        }
        offset += n as u64;
        data = &data[n..];
    }
    Ok(())
}

fn apply_setattr(root: &Path, op: &Setattr) -> io::Result<()> {
    let path = remote_path(root, &op.path)?;
    if let Some(size) = op.size {
        let st = util::lstat(&path)?;
        if st.st_mode & libc::S_IFMT == libc::S_IFREG {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(size)?;
        }
    }
    if let Some(mode) = op.mode {
        util::chmod_path(&path, mode & 0o7777)?;
    }
    if op.uid.is_some() || op.gid.is_some() {
        let st = util::lstat(&path)?;
        let uid = op.uid.unwrap_or(st.st_uid);
        let gid = op.gid.unwrap_or(st.st_gid);
        util::lchown_path(&path, uid, gid)?;
    }
    if op.atime.is_some() || op.mtime.is_some() {
        let times = [
            op.atime.map(timespec).unwrap_or_else(utime_omit),
            op.mtime.map(timespec).unwrap_or_else(utime_omit),
        ];
        util::utimens_nofollow(&path, &times)?;
    }
    Ok(())
}

fn timespec(stamp: (i64, i64)) -> libc::timespec {
    libc::timespec {
        tv_sec: stamp.0 as libc::time_t,
        tv_nsec: stamp.1 as libc::c_long,
    }
}

fn utime_omit() -> libc::timespec {
    libc::timespec {
        tv_sec: 0,
        tv_nsec: libc::UTIME_OMIT,
    }
}

fn apply_fsync(path: &Path, datasync: bool) -> io::Result<()> {
    let file = File::open(path)?;
    if datasync {
        file.sync_data()
    } else {
        file.sync_all()
    }
}

fn remove_file(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn remove_dir(path: &Path) -> io::Result<()> {
    match fs::remove_dir(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{Create, ObjectKind, Op, Queue, Setattr, Snapshot, WriteData};

    fn scratch(name: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "mirrorfs-worker-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let local = root.join("local");
        let remote = root.join("remote");
        fs::create_dir_all(&local).unwrap();
        fs::create_dir_all(&remote).unwrap();
        (local, remote)
    }

    fn drain(queue: &Queue, local: &Path, remote: &Path) {
        loop {
            let Some((seq, op)) = queue.head().unwrap() else {
                break;
            };
            apply(local, remote, &op).unwrap();
            queue.finish(seq, &op).unwrap();
        }
        assert!(queue.head().unwrap().is_none());
    }

    #[test]
    fn replay_matches_the_queued_end_state() {
        let (local, remote) = scratch("replay");
        let queue = Queue::open(&local).unwrap();
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let ops = [
            Op::Create(Create {
                path: PathBuf::from("qtest"),
                kind: ObjectKind::Dir,
                mode: 0o755,
                uid,
                gid,
                rdev: 0,
            }),
            Op::Create(Create {
                path: PathBuf::from("qtest/a.txt"),
                kind: ObjectKind::File,
                mode: 0o644,
                uid,
                gid,
                rdev: 0,
            }),
            Op::Write(WriteData {
                path: PathBuf::from("qtest/a.txt"),
                offset: 0,
                data: b"hello\n".to_vec(),
            }),
            Op::Write(WriteData {
                path: PathBuf::from("qtest/a.txt"),
                offset: 6,
                data: b"more\n".to_vec(),
            }),
            Op::Setattr(Setattr {
                path: PathBuf::from("qtest/a.txt"),
                mode: Some(0o600),
                uid: None,
                gid: None,
                size: Some(3),
                atime: None,
                mtime: Some((1_577_930_645, 0)),
            }),
            Op::Create(Create {
                path: PathBuf::from("qtest/sub"),
                kind: ObjectKind::Dir,
                mode: 0o755,
                uid,
                gid,
                rdev: 0,
            }),
            Op::Create(Create {
                path: PathBuf::from("qtest/sub/b.txt"),
                kind: ObjectKind::File,
                mode: 0o644,
                uid,
                gid,
                rdev: 0,
            }),
            Op::Unlink(PathBuf::from("qtest/sub/b.txt")),
            Op::Rmdir(PathBuf::from("qtest/sub")),
            Op::Create(Create {
                path: PathBuf::from("qtest/fifo"),
                kind: ObjectKind::Fifo,
                mode: 0o644,
                uid,
                gid,
                rdev: 0,
            }),
            Op::Fsync {
                path: PathBuf::from("qtest/a.txt"),
                datasync: true,
            },
        ];
        for op in &ops {
            queue.push(op).unwrap();
        }
        drain(&queue, &local, &remote);
        let body = fs::read(remote.join("qtest/a.txt")).unwrap();
        assert_eq!(body, b"hel");
        let st = util::lstat(&remote.join("qtest/a.txt")).unwrap();
        assert_eq!(st.st_mode & 0o777, 0o600);
        assert_eq!(st.st_mtime, 1_577_930_645);
        assert!(!remote.join("qtest/sub").exists());
        let fifo = util::lstat(&remote.join("qtest/fifo")).unwrap();
        assert_eq!(fifo.st_mode & libc::S_IFMT, libc::S_IFIFO);
        // Replaying a create after it has landed must not truncate the file.
        apply(&local, &remote, &ops[0]).unwrap();
        apply(&local, &remote, &ops[1]).unwrap();
        assert_eq!(fs::read(remote.join("qtest/a.txt")).unwrap(), b"hel");
        // A second unlink is success, so a crash after the remote call still drains.
        apply(&local, &remote, &Op::Unlink(PathBuf::from("qtest/missing"))).unwrap();
        apply(
            &local,
            &remote,
            &Op::Rmdir(PathBuf::from("qtest/missing-dir")),
        )
        .unwrap();
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn copied_file_keeps_parent_timestamps() {
        let (local, remote) = scratch("parents");
        fs::create_dir_all(local.join("snap/pre")).unwrap();
        fs::write(local.join("snap/pre/exist.txt"), b"original\nmore\n").unwrap();
        let old = libc::timespec {
            tv_sec: 1_557_119_289,
            tv_nsec: 0,
        };
        util::utimens_nofollow(&local.join("snap"), &[old, old]).unwrap();
        util::utimens_nofollow(&local.join("snap/pre"), &[old, old]).unwrap();
        let st = util::lstat(&local.join("snap/pre/exist.txt")).unwrap();
        apply(
            &local,
            &remote,
            &Op::Copy(Snapshot {
                path: PathBuf::from("snap/pre/exist.txt"),
                kind: ObjectKind::File,
                mode: st.st_mode & 0o7777,
                uid: st.st_uid,
                gid: st.st_gid,
                rdev: 0,
                atime: (st.st_atime, st.st_atime_nsec),
                mtime: (st.st_mtime, st.st_mtime_nsec),
                symlink: Vec::new(),
                xattrs: Vec::new(),
                data: b"original\nmore\n".to_vec(),
            }),
        )
        .unwrap();
        for rel in ["snap", "snap/pre"] {
            let remote_st = util::lstat(&remote.join(rel)).unwrap();
            assert_eq!(remote_st.st_mtime, 1_557_119_289, "{rel}");
            assert_eq!(remote_st.st_mtime_nsec, 0, "{rel}");
        }
        assert_eq!(
            fs::read(remote.join("snap/pre/exist.txt")).unwrap(),
            b"original\nmore\n"
        );
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn create_restamps_an_existing_owned_parent() {
        let (local, remote) = scratch("restamp-create");
        fs::create_dir_all(local.join("snap")).unwrap();
        fs::create_dir_all(remote.join("snap")).unwrap();
        let local_time = libc::timespec {
            tv_sec: 1_557_119_289,
            tv_nsec: 0,
        };
        let remote_time = libc::timespec {
            tv_sec: 1_000,
            tv_nsec: 0,
        };
        util::utimens_nofollow(&local.join("snap"), &[local_time, local_time]).unwrap();
        util::utimens_nofollow(&remote.join("snap"), &[remote_time, remote_time]).unwrap();
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        apply(
            &local,
            &remote,
            &Op::Create(Create {
                path: PathBuf::from("snap/a.txt"),
                kind: ObjectKind::File,
                mode: 0o644,
                uid,
                gid,
                rdev: 0,
            }),
        )
        .unwrap();
        let remote_st = util::lstat(&remote.join("snap")).unwrap();
        assert_eq!(remote_st.st_mtime, 1_557_119_289);
        assert_eq!(remote_st.st_mtime_nsec, 0);
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn rename_link_symlink_xattr_and_fallocate() {
        let (local, remote) = scratch("meta");
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let create = |path: &str, kind: ObjectKind, mode: u32| Create {
            path: PathBuf::from(path),
            kind,
            mode,
            uid,
            gid,
            rdev: 0,
        };
        apply(
            &local,
            &remote,
            &Op::Create(create("dir", ObjectKind::Dir, 0o755)),
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Create(create("dir/a.txt", ObjectKind::File, 0o644)),
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Write(WriteData {
                path: PathBuf::from("dir/a.txt"),
                offset: 0,
                data: b"hello".to_vec(),
            }),
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Rename {
                from: PathBuf::from("dir"),
                to: PathBuf::from("other"),
                flags: 0,
            },
        )
        .unwrap();
        assert_eq!(fs::read(remote.join("other/a.txt")).unwrap(), b"hello");
        assert!(!remote.join("dir").exists());
        apply(
            &local,
            &remote,
            &Op::Rename {
                from: PathBuf::from("dir"),
                to: PathBuf::from("other"),
                flags: 0,
            },
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Symlink {
                path: PathBuf::from("other/link"),
                target: b"a.txt".to_vec(),
                uid,
                gid,
            },
        )
        .unwrap();
        assert_eq!(
            fs::read_link(remote.join("other/link"))
                .unwrap()
                .as_os_str(),
            "a.txt"
        );
        apply(
            &local,
            &remote,
            &Op::Link {
                from: PathBuf::from("other/a.txt"),
                to: PathBuf::from("other/hard"),
            },
        )
        .unwrap();
        let file = util::lstat(&remote.join("other/a.txt")).unwrap();
        let hard = util::lstat(&remote.join("other/hard")).unwrap();
        assert_eq!(file.st_ino, hard.st_ino);
        apply(
            &local,
            &remote,
            &Op::Setxattr {
                path: PathBuf::from("other/a.txt"),
                name: b"user.mirrorfs".to_vec(),
                value: b"hi".to_vec(),
                flags: 0,
            },
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Setxattr {
                path: PathBuf::from("other/a.txt"),
                name: b"user.mirrorfs".to_vec(),
                value: b"hi".to_vec(),
                flags: libc::XATTR_CREATE,
            },
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Removexattr {
                path: PathBuf::from("other/a.txt"),
                name: b"user.mirrorfs".to_vec(),
            },
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Removexattr {
                path: PathBuf::from("other/a.txt"),
                name: b"user.mirrorfs".to_vec(),
            },
        )
        .unwrap();
        apply(
            &local,
            &remote,
            &Op::Fallocate {
                path: PathBuf::from("other/a.txt"),
                offset: 0,
                length: 4096,
                mode: 0,
            },
        )
        .unwrap();
        assert!(fs::metadata(remote.join("other/a.txt")).unwrap().len() >= 4096);
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn exchange_fallback_swaps_and_resumes() {
        let (local, remote) = scratch("xchg");
        fs::write(remote.join("a"), b"A").unwrap();
        fs::write(remote.join("b"), b"B").unwrap();
        exchange_fallback(&remote.join("a"), &remote.join("b")).unwrap();
        assert_eq!(fs::read(remote.join("a")).unwrap(), b"B");
        assert_eq!(fs::read(remote.join("b")).unwrap(), b"A");
        assert!(!remote.join(".mirrorfs-exchange").exists());

        fs::rename(remote.join("a"), remote.join(".mirrorfs-exchange")).unwrap();
        exchange_fallback(&remote.join("a"), &remote.join("b")).unwrap();
        assert_eq!(fs::read(remote.join("a")).unwrap(), b"A");
        assert_eq!(fs::read(remote.join("b")).unwrap(), b"B");
        assert!(!remote.join(".mirrorfs-exchange").exists());
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn missing_file_stays_an_error() {
        let (_local, remote) = scratch("missing");
        let err = apply(
            &_local,
            &remote,
            &Op::Write(WriteData {
                path: PathBuf::from("nope"),
                offset: 0,
                data: b"x".to_vec(),
            }),
        )
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
        let _ = fs::remove_dir_all(remote.parent().unwrap());
    }

    #[test]
    fn rejects_a_path_that_escapes_the_root() {
        let (_local, remote) = scratch("escape");
        let err = apply(&_local, &remote, &Op::Unlink(PathBuf::from("../outside"))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        let _ = fs::remove_dir_all(remote.parent().unwrap());
    }

    fn write_op(path: &str, offset: u64, data: &[u8]) -> Op {
        Op::Write(WriteData {
            path: PathBuf::from(path),
            offset,
            data: data.to_vec(),
        })
    }

    #[test]
    fn fix_restores_a_deleted_remote_file_then_applies_the_write() {
        let (local, remote) = scratch("fix-write");
        fs::write(local.join("a.txt"), b"hello\nmore\n").unwrap();
        let op = write_op("a.txt", 6, b"more\n");
        let err = apply(&local, &remote, &op).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        fix(&local, &remote, &op).unwrap();
        assert_eq!(fs::read(remote.join("a.txt")).unwrap(), b"hello\nmore\n");
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_replaces_a_remote_directory_that_blocks_a_file() {
        let (local, remote) = scratch("fix-dir");
        fs::write(local.join("a.txt"), b"hello").unwrap();
        fs::create_dir_all(remote.join("a.txt/nested")).unwrap();
        fs::write(remote.join("a.txt/nested/x"), b"remote-only").unwrap();
        let op = write_op("a.txt", 0, b"hello");
        fix(&local, &remote, &op).unwrap();
        assert_eq!(fs::read(remote.join("a.txt")).unwrap(), b"hello");
        assert!(!remote.join("a.txt/nested").exists());
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_replaces_a_mode_0444_remote_file() {
        let (local, remote) = scratch("fix-mode");
        fs::write(local.join("a.txt"), b"new").unwrap();
        fs::write(remote.join("a.txt"), b"old").unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(remote.join("a.txt")).unwrap().permissions();
        perms.set_mode(0o444);
        fs::set_permissions(remote.join("a.txt"), perms).unwrap();
        fix(&local, &remote, &write_op("a.txt", 0, b"new")).unwrap();
        assert_eq!(fs::read(remote.join("a.txt")).unwrap(), b"new");
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_publishes_a_renamed_file_when_the_remote_source_is_gone() {
        let (local, remote) = scratch("fix-rename-gone");
        fs::write(local.join("new.txt"), b"copied").unwrap();
        let op = Op::Rename {
            from: PathBuf::from("old.txt"),
            to: PathBuf::from("new.txt"),
            flags: 0,
        };
        fix(&local, &remote, &op).unwrap();
        assert_eq!(fs::read(remote.join("new.txt")).unwrap(), b"copied");
        assert!(!remote.join("old.txt").exists());
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_renames_when_the_remote_source_still_exists() {
        let (local, remote) = scratch("fix-rename-src");
        fs::create_dir_all(local.join("new")).unwrap();
        fs::create_dir_all(remote.join("old")).unwrap();
        fs::write(remote.join("old/child"), b"moved").unwrap();
        fs::write(remote.join("new"), b"blocker").unwrap();
        let op = Op::Rename {
            from: PathBuf::from("old"),
            to: PathBuf::from("new"),
            flags: 0,
        };
        fix(&local, &remote, &op).unwrap();
        assert_eq!(fs::read(remote.join("new/child")).unwrap(), b"moved");
        assert!(!remote.join("old").exists());
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_exchange_copies_both_sides_without_swapping() {
        let (local, remote) = scratch("fix-xchg");
        fs::write(local.join("a"), b"A2").unwrap();
        fs::write(local.join("b"), b"B2").unwrap();
        fs::write(remote.join("a"), b"A1").unwrap();
        fs::write(remote.join("b"), b"B1").unwrap();
        let op = Op::Rename {
            from: PathBuf::from("a"),
            to: PathBuf::from("b"),
            flags: libc::RENAME_EXCHANGE as u32,
        };
        fix(&local, &remote, &op).unwrap();
        assert_eq!(fs::read(remote.join("a")).unwrap(), b"A2");
        assert_eq!(fs::read(remote.join("b")).unwrap(), b"B2");
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_replays_a_stored_copy_without_reading_the_live_file() {
        let (local, remote) = scratch("fix-copy");
        fs::write(local.join("a.txt"), b"live").unwrap();
        let st = util::lstat(&local.join("a.txt")).unwrap();
        let op = Op::Copy(Snapshot {
            path: PathBuf::from("a.txt"),
            kind: ObjectKind::File,
            mode: st.st_mode & 0o7777,
            uid: st.st_uid,
            gid: st.st_gid,
            rdev: 0,
            atime: (st.st_atime, st.st_atime_nsec),
            mtime: (st.st_mtime, st.st_mtime_nsec),
            symlink: Vec::new(),
            xattrs: Vec::new(),
            data: b"stored".to_vec(),
        });
        fix(&local, &remote, &op).unwrap();
        assert_eq!(fs::read(remote.join("a.txt")).unwrap(), b"stored");
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_links_after_copying_the_source() {
        let (local, remote) = scratch("fix-link");
        fs::write(local.join("a.txt"), b"same").unwrap();
        fs::write(remote.join("b.txt"), b"other").unwrap();
        let op = Op::Link {
            from: PathBuf::from("a.txt"),
            to: PathBuf::from("b.txt"),
        };
        fix(&local, &remote, &op).unwrap();
        let left = util::lstat(&remote.join("a.txt")).unwrap();
        let right = util::lstat(&remote.join("b.txt")).unwrap();
        assert_eq!(left.st_ino, right.st_ino);
        assert_eq!(fs::read(remote.join("b.txt")).unwrap(), b"same");
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_unlink_removes_a_remote_directory() {
        let (local, remote) = scratch("fix-unlink");
        fs::create_dir_all(remote.join("gone/nested")).unwrap();
        fs::write(remote.join("gone/nested/x"), b"x").unwrap();
        fix(&local, &remote, &Op::Unlink(PathBuf::from("gone"))).unwrap();
        assert!(!remote.join("gone").exists());
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_symlink_replaces_a_remote_file() {
        let (local, remote) = scratch("fix-symlink");
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        std::os::unix::fs::symlink("target-name", local.join("link")).unwrap();
        fs::write(remote.join("link"), b"not a link").unwrap();
        fix(
            &local,
            &remote,
            &Op::Symlink {
                path: PathBuf::from("link"),
                target: b"target-name".to_vec(),
                uid,
                gid,
            },
        )
        .unwrap();
        assert_eq!(
            fs::read_link(remote.join("link")).unwrap().as_os_str(),
            "target-name"
        );
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn mount_available_requires_the_remote_root() {
        let (local, remote) = scratch("fix-mount");
        assert!(mount_available(&remote));
        assert!(!mount_available(&remote.join("missing")));
        fs::write(remote.join("file"), b"x").unwrap();
        assert!(!mount_available(&remote.join("file")));
        fs::write(local.join("a.txt"), b"hello").unwrap();
        let op = write_op("a.txt", 0, b"hello");
        let gone = remote.join("missing-root");
        let err = apply(&local, &gone, &op).unwrap_err();
        assert!(matches!(
            repair(&local, &gone, &op, &err),
            Repair::Unchanged
        ));
        assert!(!gone.join("a.txt").exists());
        let bad = Op::Unlink(PathBuf::from("../outside"));
        let err = apply(&local, &remote, &bad).unwrap_err();
        assert!(matches!(
            repair(&local, &remote, &bad, &err),
            Repair::Unchanged
        ));
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn fix_permission_error_stays_an_error() {
        let (local, remote) = scratch("fix-perm");
        fs::write(local.join("a.txt"), b"hello").unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&remote).unwrap().permissions();
        perms.set_mode(0o555);
        fs::set_permissions(&remote, perms).unwrap();
        let err = fix(&local, &remote, &write_op("a.txt", 0, b"hello")).unwrap_err();
        let mut perms = fs::metadata(&remote).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&remote, perms).unwrap();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(!remote.join("a.txt").exists());
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    fn file_op() -> Op {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        Op::Create(Create {
            path: PathBuf::from("hello.txt"),
            kind: ObjectKind::File,
            mode: 0o644,
            uid,
            gid,
            rdev: 0,
        })
    }

    struct Running {
        stop: Arc<AtomicBool>,
        handle: Option<JoinHandle<()>>,
        root: PathBuf,
    }

    impl Drop for Running {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn start_watched(
        name: &str,
        fs_type: &str,
        every: Duration,
    ) -> (Running, PathBuf, PathBuf, Arc<Queue>) {
        let (local_root, remote_root) = scratch(name);
        let scratch_root = local_root.parent().unwrap().to_path_buf();
        let local = local_root.join("nested");
        let remote = remote_root.join("nested").join("mirror");
        fs::create_dir_all(&local).unwrap();
        fs::create_dir_all(&remote).unwrap();
        let queue = Arc::new(Queue::open(&local).unwrap());
        queue.push(&file_op()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn(
            Arc::clone(&queue),
            local.clone(),
            remote.clone(),
            Arc::clone(&stop),
            Arc::new(crate::sysfs::Counters::new()),
            false,
            Some(RemoteWatch {
                fs_types: vec![fs_type.to_string()],
                every,
            }),
        )
        .unwrap();
        (
            Running {
                stop,
                handle: Some(handle),
                root: scratch_root,
            },
            local,
            remote,
            queue,
        )
    }

    #[test]
    fn watched_subdirectory_drains_when_the_filesystem_matches() {
        let sample = std::env::temp_dir();
        let fstype = remote_pin::fstype_of(&sample).unwrap();
        let (running, _local, remote, _queue) =
            start_watched("watch-match", &fstype, Duration::from_secs(300));
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline && !remote.join("hello.txt").exists() {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            remote.join("hello.txt").is_file(),
            "file was not created under the nested remote directory"
        );
        drop(running);
    }

    #[test]
    fn watched_subdirectory_stays_queued_for_a_different_filesystem() {
        let (running, _local, remote, queue) =
            start_watched("watch-miss", "not-a-filesystem", Duration::from_millis(200));
        thread::sleep(Duration::from_millis(500));
        assert!(!remote.join("hello.txt").exists());
        assert!(queue.head().unwrap().is_some());
        drop(running);
    }
}
