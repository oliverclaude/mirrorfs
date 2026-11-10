use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::queue::{Create, ObjectKind, Op, Queue, Setattr, Snapshot, WriteData};
use crate::util;

const RETRY_MIN: Duration = Duration::from_millis(200);
const RETRY_MAX: Duration = Duration::from_secs(5);

pub fn spawn(
    queue: Arc<Queue>,
    local: PathBuf,
    remote: PathBuf,
    stop: Arc<AtomicBool>,
    debug: bool,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("mirrorfs-remote".to_string())
        .spawn(move || run(queue, local, remote, stop, debug))
}

fn run(queue: Arc<Queue>, local: PathBuf, remote: PathBuf, stop: Arc<AtomicBool>, debug: bool) {
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
        match apply(&local, &remote, &op) {
            Ok(()) => {
                if let Err(err) = queue.finish(seq, &op) {
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

pub fn apply(local_root: &Path, remote_root: &Path, op: &Op) -> io::Result<()> {
    match op {
        Op::Create(op) => apply_create(local_root, remote_root, op),
        Op::Write(op) => apply_write(remote_root, op),
        Op::Setattr(op) => apply_setattr(remote_root, op),
        Op::Unlink(path) => remove_file(&remote_path(remote_root, path)?),
        Op::Rmdir(path) => remove_dir(&remote_path(remote_root, path)?),
        Op::Fsync { path, datasync } => apply_fsync(&remote_path(remote_root, path)?, *datasync),
        Op::Copy(op) => apply_copy(local_root, remote_root, op),
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
    if op.kind != ObjectKind::Symlink {
        util::lchown_path(&path, op.uid, op.gid)?;
        util::chmod_path(&path, mode)?;
    } else {
        util::lchown_path(&path, op.uid, op.gid)?;
    }
    let times = [timespec(op.atime), timespec(op.mtime)];
    util::utimens_nofollow(&path, &times)?;
    set_xattrs(&path, &op.xattrs)?;
    restamp_parents(local, remote, &op.path)?;
    Ok(())
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
}
