use std::collections::HashMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use fuser::{
    BsdFileFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, LockOwner, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyXattr, Request, TimeOrNow,
};

use crate::inode::InodeTable;
use crate::queue::{Create, ObjectKind, Op, Queue, Setattr, WriteData};
use crate::sysfs::{self, Counters};
use crate::util::{self, TTL};

pub struct MirrorFs {
    local: PathBuf,
    debug: bool,
    inodes: Mutex<InodeTable>,
    files: Mutex<HashMap<u64, OpenFile>>,
    dirs: Mutex<HashMap<u64, OpenDir>>,
    next_fh: AtomicU64,
    queue: Arc<Queue>,
    stripes: Vec<Mutex<()>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    uid: u32,
    gid: u32,
    counters: Arc<Counters>,
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
        let queue = Arc::new(Queue::open(&local)?);
        let mut stripes = Vec::with_capacity(64);
        for _ in 0..64 {
            stripes.push(Mutex::new(()));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::new());
        let worker = crate::worker::spawn(
            Arc::clone(&queue),
            local.clone(),
            remote,
            Arc::clone(&stop),
            Arc::clone(&counters),
            debug,
        )?;
        Ok(Self {
            local,
            debug,
            inodes: Mutex::new(InodeTable::new(st.st_ino)),
            files: Mutex::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            queue,
            stripes,
            stop,
            worker: Some(worker),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            counters,
        })
    }

    fn stripe<'a>(&'a self, rel: &Path) -> MutexGuard<'a, ()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        rel.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.stripes.len();
        self.stripes[index].lock().expect("stripe lock")
    }

    /// Every mutation stripe. A directory rename covers child paths that hash
    /// to other stripes.
    fn lock_all(&self) -> Vec<MutexGuard<'_, ()>> {
        self.stripes
            .iter()
            .map(|stripe| stripe.lock().expect("stripe lock"))
            .collect()
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
        self.queue
            .push_for_remote(&self.local, op)
            .map(|_| ())
            .map_err(|err| {
                self.counters.note_error(format!(
                    "queue op={} path={} result={err}",
                    op.kind_name(),
                    op.path().display()
                ));
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
        self.counters.note_fuse(result == "ok");
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
        // `<mount>/sys` is the virtual status directory, not a local name.
        if parent == sysfs::DIR || (parent == 1 && name == OsStr::new(sysfs::NAME)) {
            return Err(Errno::EROFS);
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
    fn destroy(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                eprintln!("mirrorfs: remote worker panicked");
            }
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = parent.0;
        if name == "." {
            if sysfs::is_reserved(parent) {
                self.reply_sys_entry("lookup", parent, reply);
                return;
            }
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
            if parent == sysfs::DIR {
                self.reply_sys_entry("lookup", 1, reply);
                return;
            }
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
        if sysfs::is_file(parent) {
            self.trace(
                "lookup",
                Path::new(sysfs::rel_of(parent)),
                &errno_label(Errno::ENOTDIR),
            );
            reply.error(Errno::ENOTDIR);
            return;
        }
        if parent == sysfs::DIR || (parent == 1 && name == OsStr::new(sysfs::NAME)) {
            match sysfs::child(parent, name) {
                Some(ino) => self.reply_sys_entry("lookup", ino, reply),
                None => {
                    let shown = Path::new(sysfs::NAME).join(name);
                    self.trace("lookup", &shown, &errno_label(Errno::ENOENT));
                    reply.error(Errno::ENOENT);
                }
            }
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
        if sysfs::is_reserved(ino.0) {
            return;
        }
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
        if sysfs::is_reserved(ino.0) {
            self.reply_sys_attr("getattr", ino.0, reply);
            return;
        }
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
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "readlink",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::EINVAL),
            );
            reply.error(Errno::EINVAL);
            return;
        }
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
        if sysfs::is_reserved(ino.0) {
            let path = Path::new(sysfs::rel_of(ino.0));
            if !sysfs::is_file(ino.0) || flags.acc_mode() != OpenAccMode::O_RDONLY {
                let errno = if sysfs::is_file(ino.0) {
                    Errno::EROFS
                } else {
                    Errno::EISDIR
                };
                self.trace("open", path, &errno_label(errno));
                reply.error(errno);
                return;
            }
            self.trace("open", path, "ok");
            reply.opened(FileHandle(0), FopenFlags::empty());
            return;
        }
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
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        if sysfs::is_file(ino.0) {
            let bytes = self.sys_bytes(ino.0);
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let end = offset
                .saturating_add(u64::from(size))
                .try_into()
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            self.trace("read", Path::new(sysfs::rel_of(ino.0)), "ok");
            reply.data(&bytes[start..end]);
            return;
        }
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
        if ino.0 == sysfs::DIR {
            let fh = self.alloc_fh();
            let rel = PathBuf::from(sysfs::NAME);
            self.dirs.lock().expect("dir lock").insert(
                fh,
                OpenDir {
                    rel: rel.clone(),
                    entries: self.sys_dir_entries(),
                },
            );
            self.trace("opendir", &rel, "ok");
            reply.opened(FileHandle(fh), FopenFlags::empty());
            return;
        }
        if sysfs::is_file(ino.0) {
            self.trace(
                "opendir",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::ENOTDIR),
            );
            reply.error(Errno::ENOTDIR);
            return;
        }
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
            if ino.0 == sysfs::DIR {
                self.trace("readdir", Path::new(sysfs::NAME), "ok");
                fill_dir(&self.sys_dir_entries(), offset, reply);
                return;
            }
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
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "write",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::EROFS),
            );
            reply.error(Errno::EROFS);
            return;
        }
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
        let rel = self
            .files
            .lock()
            .expect("file lock")
            .get(&fh.0)
            .map(|open| open.rel.clone())
            .unwrap_or(open.1);
        match open.0.write_at(data, offset) {
            Ok(n) => {
                let op = Op::Write(WriteData {
                    path: rel.clone(),
                    offset,
                    data: data[..n].to_vec(),
                });
                if let Err(errno) = self.enqueue(&op) {
                    self.log("write", &rel, &errno_label(errno), false);
                    reply.error(errno);
                    return;
                }
                self.log("write", &rel, "ok", true);
                reply.written(n as u32);
            }
            Err(err) => {
                let errno = Errno::from(err);
                self.log("write", &rel, &errno_label(errno), false);
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
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "setattr",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::EROFS),
            );
            reply.error(Errno::EROFS);
            return;
        }
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
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
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
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        if sysfs::is_file(ino.0) {
            self.trace("fsync", Path::new(sysfs::rel_of(ino.0)), "ok");
            reply.ok();
            return;
        }
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
        let rel = self
            .files
            .lock()
            .expect("file lock")
            .get(&fh.0)
            .map(|open| open.rel.clone())
            .unwrap_or_else(|| open.1.clone());
        let synced = if datasync {
            open.0.sync_data()
        } else {
            open.0.sync_all()
        };
        if let Err(err) = synced {
            let errno = Errno::from(err);
            self.log("fsync", &rel, &errno_label(errno), false);
            reply.error(errno);
            return;
        }
        if let Err(errno) = self.enqueue(&Op::Fsync {
            path: rel.clone(),
            datasync,
        }) {
            self.log("fsync", &rel, &errno_label(errno), false);
            reply.error(errno);
            return;
        }
        self.log("fsync", &rel, "ok", true);
        reply.ok();
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &Path,
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
        let rel = match self.child_rel(parent.0, name) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let full = util::local_path(&self.local, &rel);
        let result = (|| {
            let path_c = util::cstring(&full).map_err(Errno::from)?;
            let target_c =
                CString::new(target.as_os_str().as_bytes()).map_err(|_| Errno::EINVAL)?;
            let rc = unsafe { libc::symlink(target_c.as_ptr(), path_c.as_ptr()) };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            if let Err(err) = util::lchown_path(&full, req.uid(), req.gid()) {
                if err.raw_os_error() != Some(libc::EPERM) {
                    return Err(Errno::from(err));
                }
            }
            self.enqueue(&Op::Symlink {
                path: rel.clone(),
                target: target.as_os_str().as_bytes().to_vec(),
                uid: req.uid(),
                gid: req.gid(),
            })
        })();
        match result {
            Ok(()) => self.reply_new_entry("symlink", parent.0, &rel, reply),
            Err(errno) => {
                self.log("symlink", &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if flags.contains(RenameFlags::RENAME_WHITEOUT) {
            reply.error(Errno::from(io::Error::from_raw_os_error(libc::EOPNOTSUPP)));
            return;
        }
        let from = match self.child_rel(parent.0, name) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let to = match self.child_rel(newparent.0, newname) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        if from == to {
            if flags.contains(RenameFlags::RENAME_EXCHANGE) {
                self.log("rename", &from, &errno_label(Errno::EINVAL), false);
                reply.error(Errno::EINVAL);
            } else {
                self.log("rename", &from, "ok", false);
                reply.ok();
            }
            return;
        }
        let _stripes = self.lock_all();
        let from = match self.child_rel(parent.0, name) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let to = match self.child_rel(newparent.0, newname) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        if from == to {
            if flags.contains(RenameFlags::RENAME_EXCHANGE) {
                self.log("rename", &from, &errno_label(Errno::EINVAL), false);
                reply.error(Errno::EINVAL);
            } else {
                self.log("rename", &from, "ok", false);
                reply.ok();
            }
            return;
        }
        let renamed = (|| {
            let src = util::local_path(&self.local, &from);
            let dst = util::local_path(&self.local, &to);
            let src_c = util::cstring(&src).map_err(Errno::from)?;
            let dst_c = util::cstring(&dst).map_err(Errno::from)?;
            let rc = unsafe {
                libc::renameat2(
                    libc::AT_FDCWD,
                    src_c.as_ptr(),
                    libc::AT_FDCWD,
                    dst_c.as_ptr(),
                    flags.bits(),
                )
            };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            let exchange = flags.contains(RenameFlags::RENAME_EXCHANGE);
            self.inodes.lock().expect("inode lock").rename(
                &from,
                &to,
                newparent.0,
                exchange,
                parent.0,
            );
            self.retarget_handles(&from, &to, exchange);
            self.enqueue(&Op::Rename {
                from: from.clone(),
                to: to.clone(),
                flags: flags.bits(),
            })
        })();
        match renamed {
            Ok(()) => {
                self.log("rename", &from, "ok", true);
                reply.ok();
            }
            Err(errno) => {
                self.log("rename", &from, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "link",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::EPERM),
            );
            reply.error(Errno::EPERM);
            return;
        }
        let _stripes = self.lock_all();
        let from = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let to = match self.child_rel(newparent.0, newname) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let linked = (|| {
            let src = util::local_path(&self.local, &from);
            let dst = util::local_path(&self.local, &to);
            let src_c = util::cstring(&src).map_err(Errno::from)?;
            let dst_c = util::cstring(&dst).map_err(Errno::from)?;
            let rc = unsafe { libc::link(src_c.as_ptr(), dst_c.as_ptr()) };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            self.enqueue(&Op::Link {
                from: from.clone(),
                to: to.clone(),
            })
        })();
        match linked {
            Ok(()) => self.reply_new_entry("link", newparent.0, &to, reply),
            Err(errno) => {
                self.log("link", &to, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "setxattr",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::EROFS),
            );
            reply.error(Errno::EROFS);
            return;
        }
        if position != 0 {
            reply.error(Errno::EINVAL);
            return;
        }
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let _guard = self.stripe(&rel);
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let changed = (|| {
            let full = util::local_path(&self.local, &rel);
            set_local_xattr(&full, name, value, flags)?;
            self.enqueue(&Op::Setxattr {
                path: rel.clone(),
                name: name.as_bytes().to_vec(),
                value: value.to_vec(),
                flags,
            })
        })();
        match changed {
            Ok(()) => {
                self.log("setxattr", &rel, "ok", true);
                reply.ok();
            }
            Err(errno) => {
                self.log("setxattr", &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "getxattr",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::ENODATA),
            );
            reply.error(Errno::ENODATA);
            return;
        }
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let full = util::local_path(&self.local, &rel);
        match read_local_xattr(&full, name) {
            Ok(value) => {
                self.trace("getxattr", &rel, "ok");
                reply_xattr(reply, size, &value);
            }
            Err(errno) => {
                self.trace("getxattr", &rel, &errno_label(errno));
                reply.error(errno);
            }
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        if sysfs::is_reserved(ino.0) {
            self.trace("listxattr", Path::new(sysfs::rel_of(ino.0)), "ok");
            reply_xattr(reply, size, &[]);
            return;
        }
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let full = util::local_path(&self.local, &rel);
        match list_local_xattr(&full) {
            Ok(names) => {
                self.trace("listxattr", &rel, "ok");
                reply_xattr(reply, size, &names);
            }
            Err(errno) => {
                self.trace("listxattr", &rel, &errno_label(errno));
                reply.error(errno);
            }
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "removexattr",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::EROFS),
            );
            reply.error(Errno::EROFS);
            return;
        }
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let _guard = self.stripe(&rel);
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let removed = (|| {
            let full = util::local_path(&self.local, &rel);
            let path_c = util::cstring(&full).map_err(Errno::from)?;
            let name_c = CString::new(name.as_bytes()).map_err(|_| Errno::EINVAL)?;
            let rc = unsafe { libc::lremovexattr(path_c.as_ptr(), name_c.as_ptr()) };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            self.enqueue(&Op::Removexattr {
                path: rel.clone(),
                name: name.as_bytes().to_vec(),
            })
        })();
        match removed {
            Ok(()) => {
                self.log("removexattr", &rel, "ok", true);
                reply.ok();
            }
            Err(errno) => {
                self.log("removexattr", &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        if sysfs::is_reserved(ino.0) {
            self.trace(
                "fallocate",
                Path::new(sysfs::rel_of(ino.0)),
                &errno_label(Errno::EROFS),
            );
            reply.error(Errno::EROFS);
            return;
        }
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let file = {
            let files = self.files.lock().expect("file lock");
            files.get(&fh.0).map(|open| Arc::clone(&open.file))
        };
        let _guard = self.stripe(&rel);
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let allocated = (|| {
            if offset > i64::MAX as u64 || length > i64::MAX as u64 {
                return Err(Errno::EINVAL);
            }
            let owned;
            let fd = if let Some(open) = &file {
                open.as_raw_fd()
            } else {
                owned = self.open_local(&rel, OpenFlags(libc::O_WRONLY))?;
                owned.as_raw_fd()
            };
            let rc =
                unsafe { libc::fallocate(fd, mode, offset as libc::off_t, length as libc::off_t) };
            if rc < 0 {
                return Err(Errno::from(io::Error::last_os_error()));
            }
            self.enqueue(&Op::Fallocate {
                path: rel.clone(),
                offset,
                length,
                mode,
            })
        })();
        match allocated {
            Ok(()) => {
                self.log("fallocate", &rel, "ok", true);
                reply.ok();
            }
            Err(errno) => {
                self.log("fallocate", &rel, &errno_label(errno), false);
                reply.error(errno);
            }
        }
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
            if util::is_queue_name(&name) || (dir_ino == 1 && name == OsStr::new(sysfs::NAME)) {
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
        if dir_ino == 1 {
            entries.push(DirEntry {
                ino: sysfs::DIR,
                kind: FileType::Directory,
                name: OsString::from(sysfs::NAME),
            });
        }
        Ok(entries)
    }

    /// Open file and directory paths move with a rename. Called while every
    /// stripe is held, then the file map and the directory map.
    fn retarget_handles(&self, from: &Path, to: &Path, exchange: bool) {
        let retarget = |rel: &mut PathBuf| {
            if let Some(moved) = util::relocated_path(rel, from, to) {
                *rel = moved;
                return;
            }
            if exchange {
                if let Some(moved) = util::relocated_path(rel, to, from) {
                    *rel = moved;
                }
            }
        };
        for open in self.files.lock().expect("file lock").values_mut() {
            retarget(&mut open.rel);
        }
        for open in self.dirs.lock().expect("dir lock").values_mut() {
            retarget(&mut open.rel);
        }
    }

    fn sys_dir_entries(&self) -> Vec<DirEntry> {
        vec![
            DirEntry {
                ino: sysfs::DIR,
                kind: FileType::Directory,
                name: OsString::from("."),
            },
            DirEntry {
                ino: 1,
                kind: FileType::Directory,
                name: OsString::from(".."),
            },
            DirEntry {
                ino: sysfs::STATS,
                kind: FileType::RegularFile,
                name: OsString::from("stats"),
            },
            DirEntry {
                ino: sysfs::QUEUE,
                kind: FileType::RegularFile,
                name: OsString::from("queue"),
            },
            DirEntry {
                ino: sysfs::ERRORS,
                kind: FileType::RegularFile,
                name: OsString::from("errors"),
            },
        ]
    }

    fn reply_sys_entry(&self, op: &str, ino: u64, reply: ReplyEntry) {
        let attr = self.sys_attr(ino);
        self.trace(op, Path::new(sysfs::rel_of(ino)), "ok");
        reply.entry(&TTL, &attr, Generation(0));
    }

    fn reply_sys_attr(&self, op: &str, ino: u64, reply: ReplyAttr) {
        let attr = self.sys_attr(ino);
        self.trace(op, Path::new(sysfs::rel_of(ino)), "ok");
        reply.attr(&TTL, &attr);
    }

    fn sys_attr(&self, ino: u64) -> FileAttr {
        let size = self.sys_bytes(ino).len() as u64;
        let now = SystemTime::now();
        FileAttr {
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(512),
            atime: now,
            mtime: now,
            ctime: self.counters.started_at(),
            crtime: self.counters.started_at(),
            kind: if ino == sysfs::DIR {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: if ino == sysfs::DIR { 0o555 } else { 0o444 },
            nlink: if ino == sysfs::DIR { 2 } else { 1 },
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn sys_bytes(&self, ino: u64) -> Vec<u8> {
        match ino {
            sysfs::STATS => {
                let depth = self
                    .queue
                    .pending()
                    .map(|records| records.len())
                    .unwrap_or(0);
                self.counters.render_stats(depth as u64)
            }
            sysfs::QUEUE => match self.queue.pending() {
                Ok(records) => sysfs::render_queue(&records),
                Err(err) => {
                    self.counters.note_error(format!("queue list: {err}"));
                    format!("queue list failed: {err}\n").into_bytes()
                }
            },
            sysfs::ERRORS => self.counters.render_errors(),
            _ => Vec::new(),
        }
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

fn set_local_xattr(path: &Path, name: &OsStr, value: &[u8], flags: i32) -> Result<(), Errno> {
    let path_c = util::cstring(path).map_err(Errno::from)?;
    let name_c = CString::new(name.as_bytes()).map_err(|_| Errno::EINVAL)?;
    let rc = unsafe {
        libc::lsetxattr(
            path_c.as_ptr(),
            name_c.as_ptr(),
            value.as_ptr().cast::<libc::c_void>(),
            value.len(),
            flags,
        )
    };
    if rc < 0 {
        return Err(Errno::from(io::Error::last_os_error()));
    }
    Ok(())
}

fn read_local_xattr(path: &Path, name: &OsStr) -> Result<Vec<u8>, Errno> {
    let path_c = util::cstring(path).map_err(Errno::from)?;
    let name_c = CString::new(name.as_bytes()).map_err(|_| Errno::EINVAL)?;
    let mut size =
        unsafe { libc::lgetxattr(path_c.as_ptr(), name_c.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return Err(Errno::from(io::Error::last_os_error()));
    }
    let mut buf = vec![0u8; size as usize];
    if size == 0 {
        return Ok(buf);
    }
    size = unsafe {
        libc::lgetxattr(
            path_c.as_ptr(),
            name_c.as_ptr(),
            buf.as_mut_ptr().cast::<libc::c_void>(),
            buf.len(),
        )
    };
    if size < 0 {
        return Err(Errno::from(io::Error::last_os_error()));
    }
    buf.truncate(size as usize);
    Ok(buf)
}

fn list_local_xattr(path: &Path) -> Result<Vec<u8>, Errno> {
    let path_c = util::cstring(path).map_err(Errno::from)?;
    let mut buf = Vec::new();
    for _ in 0..4 {
        let listed = unsafe { libc::llistxattr(path_c.as_ptr(), std::ptr::null_mut(), 0) };
        if listed < 0 {
            return Err(Errno::from(io::Error::last_os_error()));
        }
        buf.resize(listed as usize, 0);
        if listed == 0 {
            return Ok(buf);
        }
        let listed = unsafe {
            libc::llistxattr(
                path_c.as_ptr(),
                buf.as_mut_ptr().cast::<libc::c_char>(),
                buf.len(),
            )
        };
        if listed < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ERANGE) {
                continue;
            }
            return Err(Errno::from(err));
        }
        buf.truncate(listed as usize);
        return Ok(buf);
    }
    Err(Errno::ERANGE)
}

fn reply_xattr(reply: ReplyXattr, size: u32, value: &[u8]) {
    if size == 0 {
        reply.size(value.len() as u32);
        return;
    }
    if value.len() > size as usize {
        reply.error(Errno::ERANGE);
        return;
    }
    reply.data(value);
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
