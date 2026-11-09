use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::queue::{Create, ObjectKind, Op, Queue, Setattr, WriteData};
use crate::util;

const RETRY_MIN: Duration = Duration::from_millis(200);
const RETRY_MAX: Duration = Duration::from_secs(5);

pub fn spawn(
    queue: Arc<Queue>,
    remote: PathBuf,
    stop: Arc<AtomicBool>,
    debug: bool,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("mirrorfs-remote".to_string())
        .spawn(move || run(queue, remote, stop, debug))
}

fn run(queue: Arc<Queue>, remote: PathBuf, stop: Arc<AtomicBool>, debug: bool) {
    let mut backoff = RETRY_MIN;
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let head = match queue.head() {
            Ok(head) => head,
            Err(err) => {
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
        match apply(&remote, &op) {
            Ok(()) => {
                if let Err(err) = queue.ack(seq) {
                    eprintln!("mirrorfs: queue ack {seq} failed: {err}");
                    wait(&stop, backoff);
                    backoff = grow(backoff);
                    continue;
                }
                if debug {
                    eprintln!(
                        "mirrorfs remote op={} path={} result=ok",
                        op.kind_name(),
                        op.path().display()
                    );
                    let _ = std::io::Write::flush(&mut std::io::stderr());
                }
                backoff = RETRY_MIN;
            }
            Err(err) => {
                eprintln!(
                    "mirrorfs remote op={} path={} result={err} retry",
                    op.kind_name(),
                    op.path().display()
                );
                let _ = std::io::Write::flush(&mut std::io::stderr());
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                wait(&stop, backoff);
                backoff = grow(backoff);
            }
        }
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

pub fn apply(remote_root: &Path, op: &Op) -> io::Result<()> {
    match op {
        Op::Create(op) => apply_create(remote_root, op),
        Op::Write(op) => apply_write(remote_root, op),
        Op::Setattr(op) => apply_setattr(remote_root, op),
        Op::Unlink(path) => remove_file(&remote_path(remote_root, path)?),
        Op::Rmdir(path) => remove_dir(&remote_path(remote_root, path)?),
        Op::Fsync { path, datasync } => apply_fsync(&remote_path(remote_root, path)?, *datasync),
        Op::Rename { .. }
        | Op::Link { .. }
        | Op::Symlink { .. }
        | Op::Setxattr { .. }
        | Op::Removexattr { .. }
        | Op::Fallocate { .. }
        | Op::Copy(_) => Err(io::Error::new(
            ErrorKind::Unsupported,
            format!("queued {} is not applied yet", op.kind_name()),
        )),
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

fn apply_create(root: &Path, op: &Create) -> io::Result<()> {
    let path = remote_path(root, &op.path)?;
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
    use crate::queue::{Create, ObjectKind, Op, Queue, Setattr, WriteData};

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

    fn drain(queue: &Queue, remote: &Path) {
        loop {
            let Some((seq, op)) = queue.head().unwrap() else {
                break;
            };
            apply(remote, &op).unwrap();
            queue.ack(seq).unwrap();
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
        drain(&queue, &remote);
        let body = fs::read(remote.join("qtest/a.txt")).unwrap();
        assert_eq!(body, b"hel");
        let st = util::lstat(&remote.join("qtest/a.txt")).unwrap();
        assert_eq!(st.st_mode & 0o777, 0o600);
        assert_eq!(st.st_mtime, 1_577_930_645);
        assert!(!remote.join("qtest/sub").exists());
        let fifo = util::lstat(&remote.join("qtest/fifo")).unwrap();
        assert_eq!(fifo.st_mode & libc::S_IFMT, libc::S_IFIFO);
        // Replaying a create after it has landed must not truncate the file.
        apply(&remote, &ops[0]).unwrap();
        apply(&remote, &ops[1]).unwrap();
        assert_eq!(fs::read(remote.join("qtest/a.txt")).unwrap(), b"hel");
        // A second unlink is success, so a crash after the remote call still drains.
        apply(&remote, &Op::Unlink(PathBuf::from("qtest/missing"))).unwrap();
        apply(&remote, &Op::Rmdir(PathBuf::from("qtest/missing-dir"))).unwrap();
        let _ = fs::remove_dir_all(local.parent().unwrap());
    }

    #[test]
    fn missing_file_stays_an_error() {
        let (_local, remote) = scratch("missing");
        let err = apply(
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
        let err = apply(&remote, &Op::Unlink(PathBuf::from("../outside"))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        let _ = fs::remove_dir_all(remote.parent().unwrap());
    }
}
