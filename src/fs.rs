use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::os::unix::io::FromRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fuser::{
    Errno, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, LockOwner,
    OpenAccMode, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, Request,
};

use crate::inode::InodeTable;
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
        Ok(Self {
            local,
            remote,
            debug,
            inodes: Mutex::new(InodeTable::new(st.st_ino)),
            files: Mutex::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        })
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    fn trace(&self, op: &str, path: &std::path::Path, result: &str) {
        util::trace(self.debug, op, path, result, false);
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
        let (fuse_ino, generation) = {
            let mut inodes = self.inodes.lock().expect("inode lock");
            let fuse_ino = inodes.intern(rel.clone(), parent, st.st_ino, true);
            (fuse_ino, inodes.generation(fuse_ino))
        };
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
            let fuse_ino = self
                .inodes
                .lock()
                .expect("inode lock")
                .intern(child_rel, dir_ino, st.st_ino, false);
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
