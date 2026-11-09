use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use fuser::{
    BsdFileFlags, Errno, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    LockOwner, OpenAccMode, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request, TimeOrNow,
};

use crate::inode::InodeTable;
use crate::queue::{Create, ObjectKind, Op, Queue, Setattr, WriteData};
use crate::util::{self, TTL};

pub struct MirrorFs {
    local: PathBuf,
    #[allow(dead_code)]
    remote: PathBuf,
    debug: bool,
    inodes: Mutex<InodeTable>,
    files: Mutex<HashMap<u64, OpenFile>>,
    dirs: Mutex<HashMap<u64, OpenDir>>,
    next_fh: AtomicU64,
    queue: Queue,
    stripes: Vec<Mutex<()>>,
}

struct OpenFile {
    file: Arc<File>,
    rel: PathBuf,
}

struct OpenDir {
    rel: PathBuf,
    entries: Vec<DirEntry>,
}

impl MirrorFs {
    pub fn new(local: PathBuf, remote: PathBuf, debug: bool) -> io::Result<Self> {
        let st = util::lstat(&local)?;
        let queue = Queue::open(&local)?;
        let mut stripes = Vec::with_capacity(64);
        for _ in 0..64 {
            stripes.push(Mutex::new(()));
        }
        Ok(Self {
            local,
            remote,
            debug,
            inodes: Mutex::new(InodeTable::new(st.st_ino)),
            files: Mutex::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            queue,
            stripes,
        })
    }

    fn stripe<'a>(&'a self, rel: &Path) -> MutexGuard<'a, ()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        rel.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.stripes.len();
        self.stripes[index].lock().expect("stripe lock")
    }

    /// Intern `rel`, unless the cached `st_ino` now names a different object.
    fn remember(&self, rel: PathBuf, parent: u64, local_ino: u64, bump: bool) -> (u64, u64) {
        let mut table = self.inodes.lock().expect("inode lock");
        if let Some(existing) = table.fuse_for_local(local_ino) {
            let paths = table.paths_for(existing);
            let live = paths.iter().find(|path| {
                util::lstat(&util::local_path(&self.local, path))
                    .map(|st| st.st_ino == local_ino)
                    .unwrap_or(false)
            });
            match live {
                Some(path) => {
                    let canon_live = table.rel(existing).is_some_and(|canon| {
                        util::lstat(&util::local_path(&self.local, &canon))
                            .map(|st| st.st_ino == local_ino)
                            .unwrap_or(false)
                    });
                    if !canon_live {
                        table.retarget(existing, path.clone());
                    }
                }
                None => table.detach(existing),
            }
        }
        let fuse_ino = table.intern(rel, parent, local_ino, bump);
        (fuse_ino, table.generation(fuse_ino))
    }

    fn enqueue(&self, op: &Op) -> Result<(), Errno> {
        self.queue.push(op).map(|_| ()).map_err(|err| {
            eprintln!(
                "mirrorfs: queue append failed for {}: {err}",
                op.path().display()
            );
            Errno::from(err)
        })
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    fn trace(&self, op: &str, path: &Path, result: &str) {
        self.log(op, path, result, false);
    }

    fn log(&self, op: &str, path: &Path, result: &str, queued: bool) {
        util::trace(self.debug, op, path, result, queued);
    }

    fn rel_of(&self, ino: u64) -> Result<PathBuf, Errno> {
        self.inodes
            .lock()
            .expect("inode lock")
            .rel(ino)
            .ok_or(Errno::ENOENT)
    }

    fn child_rel(&self, parent: u64, name: &OsStr) -> Result<PathBuf, Errno> {
        if name.is_empty() || name.as_bytes().contains(&0) {
            return Err(Errno::EINVAL);
        }
        if util::is_queue_name(name) {
            return Err(Errno::ENOENT);
        }
        let parent_rel = self.rel_of(parent)?;
        Ok(parent_rel.join(name))
    }

    fn reply_entry(
        &self,
        op: &str,
        rel: &std::path::Path,
        fuse_ino: u64,
        generation: u64,
        reply: ReplyEntry,
    ) {
        match util::lstat(&util::local_path(&self.local, rel)) {
            Ok(st) => {
                let attr = util::attr_from_stat(fuse_ino, &st);
                self.trace(op, rel, "ok");
                reply.entry(&TTL, &attr, Generation(generation));
            }
            Err(err) => {
                let errno = Errno::from(err);
                self.trace(op, rel, &errno_label(errno));
                reply.error(errno);
            }
        }
    }
}

impl Filesystem for MirrorFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = parent.0;
        if name == "." {
            let generation = self.inodes.lock().expect("inode lock").generation(parent);
            self.inodes.lock().expect("inode lock").bump(parent);
            let rel = match self.rel_of(parent) {
                Ok(rel) => rel,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            self.reply_entry("lookup", &rel, parent, generation, reply);
            return;
        }
        if name == ".." {
            let (parent_ino, generation) = {
                let mut inodes = self.inodes.lock().expect("inode lock");
                let parent_ino = inodes.parent(parent).unwrap_or(1);
                inodes.bump(parent_ino);
                (parent_ino, inodes.generation(parent_ino))
            };
            let rel = match self.rel_of(parent_ino) {
                Ok(rel) => rel,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            self.reply_entry("lookup", &rel, parent_ino, generation, reply);
            return;
        }

        let rel = match self.child_rel(parent, name) {
            Ok(rel) => rel,
            Err(errno) => {
                let shown = PathBuf::from(name);
                self.trace("lookup", &shown, &errno_label(errno));
                reply.error(errno);
                return;
            }
        };
        let st = match util::lstat(&util::local_path(&self.local, &rel)) {
            Ok(st) => st,
            Err(err) => {
                let errno = Errno::from(err);
                self.trace("lookup", &rel, &errno_label(errno));
                reply.error(errno);
                return;
            }
        };
        let (fuse_ino, generation) = self.remember(rel.clone(), parent, st.st_ino, true);
        self.reply_entry("lookup", &rel, fuse_ino, generation, reply);
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        self.inodes
            .lock()
            .expect("inode lock")
            .forget(ino.0, nlookup);
    }

    fn getattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: Option<fuser::FileHandle>,
        reply: ReplyAttr,
    ) {
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match util::lstat(&util::local_path(&self.local, &rel)) {
            Ok(st) => {
                let attr = util::attr_from_stat(ino.0, &st);
                self.trace("getattr", &rel, "ok");
                reply.attr(&TTL, &attr);
            }
            Err(err) => {
                let errno = Errno::from(err);
                self.trace("getattr", &rel, &errno_label(errno));
                reply.error(errno);
            }
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let full = util::local_path(&self.local, &rel);
        match std::fs::read_link(&full) {
            Ok(target) => {
                self.trace("readlink", &rel, "ok");
                reply.data(target.as_os_str().as_bytes());
            }
            Err(err) => {
                let errno = Errno::from(err);
                self.trace("readlink", &rel, &errno_label(errno));
                reply.error(errno);
            }
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self.open_local(&rel, flags) {
            Ok(file) => {
                let fh = self.alloc_fh();
                self.files.lock().expect("file lock").insert(
                    fh,
                    OpenFile {
                        file,
                        rel: rel.clone(),
                    },
                );
                self.trace("open", &rel, "ok");
                reply.opened(FileHandle(fh), FopenFlags::empty());
            }
            Err(errno) => {
                self.trace("open", &rel, &errno_label(errno));
                reply.error(errno);
            }
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let open = {
            let files = self.files.lock().expect("file lock");
            match files.get(&fh.0) {
                Some(open) => (Arc::clone(&open.file), open.rel.clone()),
                None => {
                    reply.error(Errno::EBADF);
                    return;
                }
            }
        };
        let mut buf = vec![0u8; size as usize];
        match open.0.read_at(&mut buf, offset) {
            Ok(n) => {
                self.trace("read", &open.1, "ok");
                reply.data(&buf[..n]);
            }
            Err(err) => {
                let errno = Errno::from(err);
                self.trace("read", &open.1, &errno_label(errno));
                reply.error(errno);
            }
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let rel = self
            .files
            .lock()
            .expect("file lock")
            .get(&fh.0)
            .map(|open| open.rel.clone());
        if let Some(rel) = rel {
            self.trace("flush", &rel, "ok");
        }
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if let Some(open) = self.files.lock().expect("file lock").remove(&fh.0) {
            self.trace("release", &open.rel, "ok");
        }
        reply.ok();
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self.read_dir_entries(ino.0, &rel) {
            Ok(entries) => {
                let fh = self.alloc_fh();
                self.dirs.lock().expect("dir lock").insert(
                    fh,
                    OpenDir {
                        rel: rel.clone(),
                        entries,
                    },
                );
                self.trace("opendir", &rel, "ok");
                reply.opened(FileHandle(fh), FopenFlags::empty());
            }
            Err(errno) => {
                self.trace("opendir", &rel, &errno_label(errno));
                reply.error(errno);
            }
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let snapshot = {
            let dirs = self.dirs.lock().expect("dir lock");
            dirs.get(&fh.0)
                .map(|dir| (dir.rel.clone(), dir.entries.clone()))
        };
        let Some((rel, entries)) = snapshot else {
            // A zero handle means the kernel did not use our opendir result.
            let rel = match self.rel_of(ino.0) {
                Ok(rel) => rel,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            match self.read_dir_entries(ino.0, &rel) {
                Ok(entries) => {
                    self.trace("readdir", &rel, "ok");
                    fill_dir(&entries, offset, reply);
                }
                Err(errno) => {
                    self.trace("readdir", &rel, &errno_label(errno));
                    reply.error(errno);
                }
            }
            return;
        };
        self.trace("readdir", &rel, "ok");
        fill_dir(&entries, offset, reply);
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        if let Some(dir) = self.dirs.lock().expect("dir lock").remove(&fh.0) {
            self.trace("releasedir", &dir.rel, "ok");
        }
        reply.ok();
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let rel = match self.child_rel(parent.0, name) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let _guard = self.stripe(&rel);
        let perm = (mode & 0o7777) & !umask;
        let kind_bits = mode & libc::S_IFMT;
        let full = util::local_path(&self.local, &rel);
        let result = (|| {
            let c = util::cstring(&full).map_err(Errno::from)?;
            let rc = unsafe { libc::mknod(c.as_ptr(), kind_bits | perm, rdev as libc::dev_t) };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            util::chmod_path(&full, perm).map_err(Errno::from)?;
            if let Err(err) = util::lchown_path(&full, req.uid(), req.gid()) {
                if err.raw_os_error() != Some(libc::EPERM) {
                    return Err(Errno::from(err));
                }
            }
            self.enqueue(&Op::Create(Create {
                path: rel.clone(),
                kind: object_kind(kind_bits),
                mode: perm,
                uid: req.uid(),
                gid: req.gid(),
                rdev,
            }))
        })();
        match result {
            Ok(()) => self.reply_new_entry("mknod", parent.0, &rel, reply),
            Err(errno) => {
                self.log("mknod", &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let rel = match self.child_rel(parent.0, name) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let _guard = self.stripe(&rel);
        let perm = (mode & 0o7777) & !umask;
        let full = util::local_path(&self.local, &rel);
        let result = (|| {
            let c = util::cstring(&full).map_err(Errno::from)?;
            let rc = unsafe { libc::mkdir(c.as_ptr(), perm) };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            if let Err(err) = util::chmod_path(&full, perm) {
                return Err(Errno::from(err));
            }
            if let Err(err) = util::lchown_path(&full, req.uid(), req.gid()) {
                if err.raw_os_error() != Some(libc::EPERM) {
                    return Err(Errno::from(err));
                }
            }
            self.enqueue(&Op::Create(Create {
                path: rel.clone(),
                kind: ObjectKind::Dir,
                mode: perm,
                uid: req.uid(),
                gid: req.gid(),
                rdev: 0,
            }))
        })();
        match result {
            Ok(()) => self.reply_new_entry("mkdir", parent.0, &rel, reply),
            Err(errno) => {
                self.log("mkdir", &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.remove_name("unlink", parent.0, name, false, reply);
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.remove_name("rmdir", parent.0, name, true, reply);
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        let open = {
            let files = self.files.lock().expect("file lock");
            match files.get(&fh.0) {
                Some(open) => (Arc::clone(&open.file), open.rel.clone()),
                None => {
                    reply.error(Errno::EBADF);
                    return;
                }
            }
        };
        let _guard = self.stripe(&open.1);
        match open.0.write_at(data, offset) {
            Ok(n) => {
                let op = Op::Write(WriteData {
                    path: open.1.clone(),
                    offset,
                    data: data[..n].to_vec(),
                });
                if let Err(errno) = self.enqueue(&op) {
                    self.log("write", &open.1, &errno_label(errno), false);
                    reply.error(errno);
                    return;
                }
                self.log("write", &open.1, "ok", true);
                reply.written(n as u32);
            }
            Err(err) => {
                let errno = Errno::from(err);
                self.log("write", &open.1, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        // File-map lock before the stripe. `write` takes the same locks in
        // that order; reversing them here deadlocks.
        let file = fh.and_then(|handle| {
            self.files
                .lock()
                .expect("file lock")
                .get(&handle.0)
                .map(|open| Arc::clone(&open.file))
        });
        let _guard = self.stripe(&rel);
        let full = util::local_path(&self.local, &rel);
        let result = (|| {
            if let Some(size) = size {
                if let Some(file) = &file {
                    file.set_len(size).map_err(Errno::from)?;
                } else {
                    let f = File::options()
                        .write(true)
                        .open(&full)
                        .map_err(Errno::from)?;
                    f.set_len(size).map_err(Errno::from)?;
                }
            }
            if let Some(mode) = mode {
                let perm = mode & 0o7777;
                if let Some(file) = &file {
                    util::fchmod_fd(file.as_raw_fd(), perm).map_err(Errno::from)?;
                } else {
                    util::chmod_path(&full, perm).map_err(Errno::from)?;
                }
            }
            if uid.is_some() || gid.is_some() {
                let st = util::lstat(&full).map_err(Errno::from)?;
                let new_uid = uid.unwrap_or(st.st_uid);
                let new_gid = gid.unwrap_or(st.st_gid);
                let chown_result = if let Some(file) = &file {
                    util::fchown_fd(file.as_raw_fd(), new_uid, new_gid)
                } else {
                    util::lchown_path(&full, new_uid, new_gid)
                };
                if let Err(err) = chown_result {
                    if err.raw_os_error() != Some(libc::EPERM) {
                        return Err(Errno::from(err));
                    }
                }
            }
            if atime.is_some() || mtime.is_some() {
                let times = [
                    atime.map(timespec_of).unwrap_or_else(utime_omit),
                    mtime.map(timespec_of).unwrap_or_else(utime_omit),
                ];
                let stamped = if let Some(file) = &file {
                    util::futimens_fd(file.as_raw_fd(), &times)
                } else {
                    util::utimens_nofollow(&full, &times)
                };
                stamped.map_err(Errno::from)?;
            }
            let st = util::lstat(&full).map_err(Errno::from)?;
            self.enqueue(&Op::Setattr(Setattr {
                path: rel.clone(),
                mode: mode.map(|mode| mode & 0o7777),
                uid,
                gid,
                size,
                atime: atime.map(|_| (st.st_atime, st.st_atime_nsec)),
                mtime: mtime.map(|_| (st.st_mtime, st.st_mtime_nsec)),
            }))
        })();
        match result {
            Ok(()) => match util::lstat(&full) {
                Ok(st) => {
                    self.log("setattr", &rel, "ok", true);
                    reply.attr(&TTL, &util::attr_from_stat(ino.0, &st));
                }
                Err(err) => {
                    let errno = Errno::from(err);
                    self.log("setattr", &rel, &errno_label(errno), false);
                    reply.error(errno);
                }
            },
            Err(errno) => {
                self.log("setattr", &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let rel = match self.child_rel(parent.0, name) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let perm = (mode & 0o7777) & !umask;
        let full = util::local_path(&self.local, &rel);
        // Drop the stripe before the file-map lock. `write` and `fsync` take
        // the file map first, then the stripe.
        let file = {
            let _guard = self.stripe(&rel);
            let created = (|| {
                let c = util::cstring(&full).map_err(Errno::from)?;
                let mut oflags = flags & libc::O_ACCMODE;
                oflags |= libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW;
                if flags & libc::O_APPEND != 0 {
                    oflags |= libc::O_APPEND;
                }
                if flags & libc::O_TRUNC != 0 {
                    oflags |= libc::O_TRUNC;
                }
                let fd = unsafe { libc::open(c.as_ptr(), oflags, perm) };
                if fd < 0 {
                    return Err(Errno::from(io::Error::last_os_error()));
                }
                let file = unsafe { File::from_raw_fd(fd) };
                if let Err(err) = util::fchmod_fd(file.as_raw_fd(), perm) {
                    return Err(Errno::from(err));
                }
                if let Err(err) = util::fchown_fd(file.as_raw_fd(), req.uid(), req.gid()) {
                    if err.raw_os_error() != Some(libc::EPERM) {
                        return Err(Errno::from(err));
                    }
                }
                self.enqueue(&Op::Create(Create {
                    path: rel.clone(),
                    kind: ObjectKind::File,
                    mode: perm,
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                }))?;
                Ok(Arc::new(file))
            })();
            match created {
                Ok(file) => file,
                Err(errno) => {
                    self.log("create", &rel, &errno_label(errno), false);
                    reply.error(errno);
                    return;
                }
            }
        };
        let st = match util::lstat(&full) {
            Ok(st) => st,
            Err(err) => {
                let errno = Errno::from(err);
                self.log("create", &rel, &errno_label(errno), false);
                reply.error(errno);
                return;
            }
        };
        let (fuse_ino, generation) = self.remember(rel.clone(), parent.0, st.st_ino, true);
        let fh = self.alloc_fh();
        self.files.lock().expect("file lock").insert(
            fh,
            OpenFile {
                file,
                rel: rel.clone(),
            },
        );
        self.log("create", &rel, "ok", true);
        reply.created(
            &TTL,
            &util::attr_from_stat(fuse_ino, &st),
            Generation(generation),
            FileHandle(fh),
            FopenFlags::empty(),
        );
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let open = {
            let files = self.files.lock().expect("file lock");
            match files.get(&fh.0) {
                Some(open) => (Arc::clone(&open.file), open.rel.clone()),
                None => {
                    reply.error(Errno::EBADF);
                    return;
                }
            }
        };
        let _guard = self.stripe(&open.1);
        let synced = if datasync {
            open.0.sync_data()
        } else {
            open.0.sync_all()
        };
        if let Err(err) = synced {
            let errno = Errno::from(err);
            self.log("fsync", &open.1, &errno_label(errno), false);
            reply.error(errno);
            return;
        }
        if let Err(errno) = self.enqueue(&Op::Fsync {
            path: open.1.clone(),
            datasync,
        }) {
            self.log("fsync", &open.1, &errno_label(errno), false);
            reply.error(errno);
            return;
        }
        self.log("fsync", &open.1, "ok", true);
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let c = match util::cstring(&self.local) {
            Ok(c) => c,
            Err(err) => {
                reply.error(Errno::from(err));
                return;
            }
        };
        let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(c.as_ptr(), &mut vfs) };
        if rc < 0 {
            reply.error(Errno::from(io::Error::last_os_error()));
            return;
        }
        self.trace("statfs", std::path::Path::new("/"), "ok");
        reply.statfs(
            vfs.f_blocks,
            vfs.f_bfree,
            vfs.f_bavail,
            vfs.f_files,
            vfs.f_ffree,
            vfs.f_bsize as u32,
            vfs.f_namemax as u32,
            vfs.f_frsize as u32,
        );
    }
}

#[derive(Clone)]
struct DirEntry {
    ino: u64,
    kind: FileType,
    name: OsString,
}

impl MirrorFs {
    fn remove_name(&self, op: &str, parent: u64, name: &OsStr, is_dir: bool, reply: ReplyEmpty) {
        let rel = match self.child_rel(parent, name) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let _guard = self.stripe(&rel);
        let full = util::local_path(&self.local, &rel);
        let removed = (|| {
            let c = util::cstring(&full).map_err(Errno::from)?;
            let rc = if is_dir {
                unsafe { libc::rmdir(c.as_ptr()) }
            } else {
                unsafe { libc::unlink(c.as_ptr()) }
            };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            let queued = if is_dir {
                Op::Rmdir(rel.clone())
            } else {
                Op::Unlink(rel.clone())
            };
            self.enqueue(&queued)?;
            self.inodes.lock().expect("inode lock").unbind(&rel);
            Ok(())
        })();
        match removed {
            Ok(()) => {
                self.log(op, &rel, "ok", true);
                reply.ok();
            }
            Err(errno) => {
                self.log(op, &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn reply_new_entry(&self, op: &str, parent: u64, rel: &Path, reply: ReplyEntry) {
        let st = match util::lstat(&util::local_path(&self.local, rel)) {
            Ok(st) => st,
            Err(err) => {
                let errno = Errno::from(err);
                self.log(op, rel, &errno_label(errno), false);
                reply.error(errno);
                return;
            }
        };
        let (fuse_ino, generation) = self.remember(rel.to_path_buf(), parent, st.st_ino, true);
        self.log(op, rel, "ok", true);
        reply.entry(
            &TTL,
            &util::attr_from_stat(fuse_ino, &st),
            Generation(generation),
        );
    }

    fn open_local(&self, rel: &std::path::Path, flags: OpenFlags) -> Result<Arc<File>, Errno> {
        let full = util::local_path(&self.local, rel);
        let c = util::cstring(&full).map_err(Errno::from)?;
        let mut oflags = match flags.acc_mode() {
            OpenAccMode::O_RDONLY => libc::O_RDONLY,
            OpenAccMode::O_WRONLY => libc::O_WRONLY,
            OpenAccMode::O_RDWR => libc::O_RDWR,
        };
        oflags |= libc::O_CLOEXEC | libc::O_NOFOLLOW;
        if flags.0 & libc::O_APPEND != 0 {
            oflags |= libc::O_APPEND;
        }
        let fd = unsafe { libc::open(c.as_ptr(), oflags) };
        if fd < 0 {
            return Err(Errno::from(io::Error::last_os_error()));
        }
        Ok(Arc::new(unsafe { File::from_raw_fd(fd) }))
    }

    fn read_dir_entries(
        &self,
        dir_ino: u64,
        rel: &std::path::Path,
    ) -> Result<Vec<DirEntry>, Errno> {
        let parent_ino = self
            .inodes
            .lock()
            .expect("inode lock")
            .parent(dir_ino)
            .unwrap_or(1);
        let mut entries = vec![
            DirEntry {
                ino: dir_ino,
                kind: FileType::Directory,
                name: OsString::from("."),
            },
            DirEntry {
                ino: parent_ino,
                kind: FileType::Directory,
                name: OsString::from(".."),
            },
        ];
        let full = util::local_path(&self.local, rel);
        let dir = std::fs::read_dir(&full).map_err(Errno::from)?;
        for item in dir {
            let item = item.map_err(Errno::from)?;
            let name = item.file_name();
            if util::is_queue_name(&name) {
                continue;
            }
            let child_rel = rel.join(&name);
            let st = match util::lstat(&util::local_path(&self.local, &child_rel)) {
                Ok(st) => st,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(Errno::from(err)),
            };
            let fuse_ino = self.remember(child_rel, dir_ino, st.st_ino, false).0;
            entries.push(DirEntry {
                ino: fuse_ino,
                kind: util::file_kind(st.st_mode),
                name,
            });
        }
        Ok(entries)
    }
}

fn fill_dir(entries: &[DirEntry], offset: u64, mut reply: ReplyDirectory) {
    for (index, entry) in entries.iter().enumerate().skip(offset as usize) {
        let next = (index as u64) + 1;
        if reply.add(INodeNo(entry.ino), next, entry.kind, &entry.name) {
            break;
        }
    }
    reply.ok();
}

fn errno_label(errno: Errno) -> String {
    format!("err:{}", errno.code())
}

fn object_kind(mode: u32) -> ObjectKind {
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

fn timespec_of(t: TimeOrNow) -> libc::timespec {
    match t {
        TimeOrNow::Now => libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_NOW,
        },
        TimeOrNow::SpecificTime(time) => {
            let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
            libc::timespec {
                tv_sec: duration.as_secs() as libc::time_t,
                tv_nsec: duration.subsec_nanos() as libc::c_long,
            }
        }
    }
}

fn utime_omit() -> libc::timespec {
    libc::timespec {
        tv_sec: 0,
        tv_nsec: libc::UTIME_OMIT,
    }
}
