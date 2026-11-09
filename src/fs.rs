use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::Mutex;

use fuser::{
    Errno, FileType, Filesystem, FopenFlags, Generation, INodeNo, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};

use crate::inode::InodeTable;
use crate::util::{self, TTL};

pub struct MirrorFs {
    local: PathBuf,
    #[allow(dead_code)]
    remote: PathBuf,
    debug: bool,
    inodes: Mutex<InodeTable>,
}

impl MirrorFs {
    pub fn new(local: PathBuf, remote: PathBuf, debug: bool) -> io::Result<Self> {
        let st = util::lstat(&local)?;
        Ok(Self {
            local,
            remote,
            debug,
            inodes: Mutex::new(InodeTable::new(st.st_ino)),
        })
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

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: fuser::OpenFlags, reply: ReplyOpen) {
        reply.opened(fuser::FileHandle(0), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let rel = match self.rel_of(ino.0) {
            Ok(rel) => rel,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let entries = match self.read_dir_entries(ino.0, &rel) {
            Ok(entries) => entries,
            Err(errno) => {
                self.trace("readdir", &rel, &errno_label(errno));
                reply.error(errno);
                return;
            }
        };
        self.trace("readdir", &rel, "ok");
        for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            let next = (index as u64) + 1;
            if reply.add(INodeNo(entry.ino), next, entry.kind, entry.name) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: fuser::FileHandle,
        _flags: fuser::OpenFlags,
        reply: fuser::ReplyEmpty,
    ) {
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

struct DirEntry {
    ino: u64,
    kind: FileType,
    name: OsString,
}

impl MirrorFs {
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

fn errno_label(errno: Errno) -> String {
    format!("err:{}", errno.code())
}
