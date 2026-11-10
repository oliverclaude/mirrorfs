use std::ffi::{CString, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{FileAttr, FileType, INodeNo};

/// Name of the on-disk queue. Hidden from lookup and readdir.
pub const QUEUE_DIR_NAME: &str = ".mirrorfs-queue";

pub const TTL: Duration = Duration::from_secs(1);

pub fn is_queue_name(name: &OsStr) -> bool {
    name == OsStr::new(QUEUE_DIR_NAME)
}

pub fn local_path(root: &Path, rel: &Path) -> PathBuf {
    if rel.as_os_str().is_empty() {
        root.to_path_buf()
    } else {
        root.join(rel)
    }
}

/// Path after `from` is renamed to `to`. A name inside `from` keeps its suffix.
pub fn relocated_path(path: &Path, from: &Path, to: &Path) -> Option<PathBuf> {
    if from.as_os_str().is_empty() || path.as_os_str().is_empty() {
        return None;
    }
    if path == from {
        return Some(to.to_path_buf());
    }
    let rest = path.strip_prefix(from).ok()?;
    if rest.as_os_str().is_empty() {
        return Some(to.to_path_buf());
    }
    Some(to.join(rest))
}

pub fn lstat(path: &Path) -> io::Result<libc::stat> {
    let c = cstring(path)?;
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::lstat(c.as_ptr(), &mut st) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(st)
    }
}

pub fn cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains an interior NUL: {}", path.display()),
        )
    })
}

pub fn file_kind(mode: u32) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn system_time(sec: i64, nsec: i64) -> SystemTime {
    let nsec = nsec.clamp(0, 999_999_999) as u32;
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nsec)
    } else {
        UNIX_EPOCH - Duration::new(sec.unsigned_abs(), 0)
    }
}

pub fn attr_from_stat(fuse_ino: u64, st: &libc::stat) -> FileAttr {
    FileAttr {
        ino: INodeNo(fuse_ino),
        size: st.st_size as u64,
        blocks: st.st_blocks as u64,
        atime: system_time(st.st_atime, st.st_atime_nsec),
        mtime: system_time(st.st_mtime, st.st_mtime_nsec),
        ctime: system_time(st.st_ctime, st.st_ctime_nsec),
        crtime: UNIX_EPOCH,
        kind: file_kind(st.st_mode),
        perm: (st.st_mode & 0o7777) as u16,
        nlink: st.st_nlink as u32,
        uid: st.st_uid,
        gid: st.st_gid,
        rdev: st.st_rdev as u32,
        blksize: st.st_blksize as u32,
        flags: 0,
    }
}

pub fn chmod_path(path: &Path, mode: u32) -> io::Result<()> {
    let c = cstring(path)?;
    let rc = unsafe { libc::chmod(c.as_ptr(), mode) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn lchown_path(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
    let c = cstring(path)?;
    let rc = unsafe { libc::lchown(c.as_ptr(), uid, gid) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn fchmod_fd(fd: i32, mode: u32) -> io::Result<()> {
    let rc = unsafe { libc::fchmod(fd, mode) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn fchown_fd(fd: i32, uid: u32, gid: u32) -> io::Result<()> {
    let rc = unsafe { libc::fchown(fd, uid, gid) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn utimens_nofollow(path: &Path, times: &[libc::timespec; 2]) -> io::Result<()> {
    let c = cstring(path)?;
    let rc = unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            c.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn futimens_fd(fd: i32, times: &[libc::timespec; 2]) -> io::Result<()> {
    let rc = unsafe { libc::futimens(fd, times.as_ptr()) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn trace(debug: bool, op: &str, path: &Path, result: &str, queued: bool) {
    if debug {
        eprintln!(
            "mirrorfs op={op} path={} result={result} queued={}",
            path.display(),
            if queued { "yes" } else { "no" }
        );
        let _ = std::io::Write::flush(&mut std::io::stderr());
    }
}
