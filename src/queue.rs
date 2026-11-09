// The worker in a later revision drains `pending` and decodes records.
// Until that call exists, a binary build does not reach the decode path.
#![cfg_attr(not(test), allow(dead_code))]

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::util::QUEUE_DIR_NAME;

const MAGIC: &[u8; 4] = b"MFQ1";
const VERSION: u32 = 1;

const KIND_CREATE: u32 = 1;
const KIND_WRITE: u32 = 2;
const KIND_SETATTR: u32 = 3;
const KIND_UNLINK: u32 = 4;
const KIND_RMDIR: u32 = 5;
const KIND_RENAME: u32 = 6;
const KIND_LINK: u32 = 7;
const KIND_SYMLINK: u32 = 8;
const KIND_SETXATTR: u32 = 9;
const KIND_REMOVEXATTR: u32 = 10;
const KIND_FSYNC: u32 = 11;
const KIND_FALLOCATE: u32 = 12;
const KIND_COPY: u32 = 13;

/// One durable mutation. Records are applied in ascending sequence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Create(Create),
    Write(WriteData),
    Setattr(Setattr),
    Unlink(PathBuf),
    Rmdir(PathBuf),
    #[allow(dead_code)]
    Rename {
        from: PathBuf,
        to: PathBuf,
        flags: u32,
    },
    #[allow(dead_code)]
    Link {
        from: PathBuf,
        to: PathBuf,
    },
    #[allow(dead_code)]
    Symlink {
        path: PathBuf,
        target: Vec<u8>,
        uid: u32,
        gid: u32,
    },
    #[allow(dead_code)]
    Setxattr {
        path: PathBuf,
        name: Vec<u8>,
        value: Vec<u8>,
        flags: i32,
    },
    #[allow(dead_code)]
    Removexattr {
        path: PathBuf,
        name: Vec<u8>,
    },
    Fsync {
        path: PathBuf,
        datasync: bool,
    },
    #[allow(dead_code)]
    Fallocate {
        path: PathBuf,
        offset: u64,
        length: u64,
        mode: i32,
    },
    /// Full local object, captured at enqueue time. Applied before a mutation
    /// when the remote does not yet have this path.
    #[allow(dead_code)]
    Copy(Snapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    pub path: PathBuf,
    pub kind: ObjectKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectKind {
    File = 1,
    Dir = 2,
    Fifo = 3,
    Char = 4,
    Block = 5,
    Socket = 6,
    Symlink = 7,
}

impl ObjectKind {
    fn from_u8(v: u8) -> io::Result<Self> {
        Ok(match v {
            1 => Self::File,
            2 => Self::Dir,
            3 => Self::Fifo,
            4 => Self::Char,
            5 => Self::Block,
            6 => Self::Socket,
            7 => Self::Symlink,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown object kind {v}"),
                ))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteData {
    pub path: PathBuf,
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Setattr {
    pub path: PathBuf,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<(i64, i64)>,
    pub mtime: Option<(i64, i64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub path: PathBuf,
    pub kind: ObjectKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub atime: (i64, i64),
    pub mtime: (i64, i64),
    pub symlink: Vec<u8>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    pub data: Vec<u8>,
}

/// Persistent FIFO stored in `<local>/.mirrorfs-queue/`.
pub struct Queue {
    dir: PathBuf,
    inner: Mutex<u64>,
}

impl Queue {
    pub fn open(local_root: &Path) -> io::Result<Self> {
        let dir = local_root.join(QUEUE_DIR_NAME);
        fs::create_dir_all(&dir)?;
        let mut perms = fs::metadata(&dir)?.permissions();
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o700);
        fs::set_permissions(&dir, perms)?;
        // A tmp file is not a committed record. Drop leftovers from a crash
        // that happened before the rename.
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().ends_with(".op.tmp") {
                let _ = fs::remove_file(entry.path());
            }
        }
        let next = pending_seqs(&dir)?.into_iter().max().unwrap_or(0) + 1;
        Ok(Self {
            dir,
            inner: Mutex::new(next),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Append one record. The directory fsync returns only after the record
    /// will survive a crash.
    pub fn push(&self, op: &Op) -> io::Result<u64> {
        let mut next = self.inner.lock().expect("queue lock");
        let seq = *next;
        let tmp = self.dir.join(format!("{seq:020}.op.tmp"));
        let final_path = self.dir.join(format!("{seq:020}.op"));
        {
            let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            file.write_all(&op.encode())?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &final_path)?;
        fsync_dir(&self.dir)?;
        *next = seq + 1;
        Ok(seq)
    }

    pub fn pending(&self) -> io::Result<Vec<(u64, Op)>> {
        let mut seqs = pending_seqs(&self.dir)?;
        seqs.sort_unstable();
        let mut out = Vec::with_capacity(seqs.len());
        for seq in seqs {
            let bytes = fs::read(self.dir.join(format!("{seq:020}.op")))?;
            out.push((seq, Op::decode(&bytes)?));
        }
        Ok(out)
    }
}

fn pending_seqs(dir: &Path) -> io::Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(seq) = name.strip_suffix(".op") else {
            continue;
        };
        if seq.len() == 20 && seq.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(n) = seq.parse::<u64>() {
                seqs.push(n);
            }
        }
    }
    Ok(seqs)
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    let file = File::open(dir)?;
    file.sync_all()
}

impl Op {
    pub fn path(&self) -> &Path {
        match self {
            Op::Create(op) => &op.path,
            Op::Write(op) => &op.path,
            Op::Setattr(op) => &op.path,
            Op::Unlink(path) | Op::Rmdir(path) => path,
            Op::Rename { from, .. } => from,
            Op::Link { to, .. } => to,
            Op::Symlink { path, .. }
            | Op::Setxattr { path, .. }
            | Op::Removexattr { path, .. }
            | Op::Fsync { path, .. }
            | Op::Fallocate { path, .. } => path,
            Op::Copy(op) => &op.path,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        put_u32(&mut out, VERSION);
        match self {
            Op::Create(op) => {
                put_u32(&mut out, KIND_CREATE);
                put_path(&mut out, &op.path);
                out.push(op.kind as u8);
                put_u32(&mut out, op.mode);
                put_u32(&mut out, op.uid);
                put_u32(&mut out, op.gid);
                put_u32(&mut out, op.rdev);
            }
            Op::Write(op) => {
                put_u32(&mut out, KIND_WRITE);
                put_path(&mut out, &op.path);
                put_u64(&mut out, op.offset);
                put_bytes(&mut out, &op.data);
            }
            Op::Setattr(op) => {
                put_u32(&mut out, KIND_SETATTR);
                put_path(&mut out, &op.path);
                let mut flags = 0u8;
                if op.mode.is_some() {
                    flags |= 1;
                }
                if op.uid.is_some() {
                    flags |= 2;
                }
                if op.gid.is_some() {
                    flags |= 4;
                }
                if op.size.is_some() {
                    flags |= 8;
                }
                if op.atime.is_some() {
                    flags |= 16;
                }
                if op.mtime.is_some() {
                    flags |= 32;
                }
                out.push(flags);
                if let Some(mode) = op.mode {
                    put_u32(&mut out, mode);
                }
                if let Some(uid) = op.uid {
                    put_u32(&mut out, uid);
                }
                if let Some(gid) = op.gid {
                    put_u32(&mut out, gid);
                }
                if let Some(size) = op.size {
                    put_u64(&mut out, size);
                }
                if let Some((sec, nsec)) = op.atime {
                    put_i64(&mut out, sec);
                    put_i64(&mut out, nsec);
                }
                if let Some((sec, nsec)) = op.mtime {
                    put_i64(&mut out, sec);
                    put_i64(&mut out, nsec);
                }
            }
            Op::Unlink(path) => {
                put_u32(&mut out, KIND_UNLINK);
                put_path(&mut out, path);
            }
            Op::Rmdir(path) => {
                put_u32(&mut out, KIND_RMDIR);
                put_path(&mut out, path);
            }
            Op::Rename { from, to, flags } => {
                put_u32(&mut out, KIND_RENAME);
                put_path(&mut out, from);
                put_path(&mut out, to);
                put_u32(&mut out, *flags);
            }
            Op::Link { from, to } => {
                put_u32(&mut out, KIND_LINK);
                put_path(&mut out, from);
                put_path(&mut out, to);
            }
            Op::Symlink {
                path,
                target,
                uid,
                gid,
            } => {
                put_u32(&mut out, KIND_SYMLINK);
                put_path(&mut out, path);
                put_bytes(&mut out, target);
                put_u32(&mut out, *uid);
                put_u32(&mut out, *gid);
            }
            Op::Setxattr {
                path,
                name,
                value,
                flags,
            } => {
                put_u32(&mut out, KIND_SETXATTR);
                put_path(&mut out, path);
                put_bytes(&mut out, name);
                put_bytes(&mut out, value);
                put_i32(&mut out, *flags);
            }
            Op::Removexattr { path, name } => {
                put_u32(&mut out, KIND_REMOVEXATTR);
                put_path(&mut out, path);
                put_bytes(&mut out, name);
            }
            Op::Fsync { path, datasync } => {
                put_u32(&mut out, KIND_FSYNC);
                put_path(&mut out, path);
                out.push(u8::from(*datasync));
            }
            Op::Fallocate {
                path,
                offset,
                length,
                mode,
            } => {
                put_u32(&mut out, KIND_FALLOCATE);
                put_path(&mut out, path);
                put_u64(&mut out, *offset);
                put_u64(&mut out, *length);
                put_i32(&mut out, *mode);
            }
            Op::Copy(op) => {
                put_u32(&mut out, KIND_COPY);
                put_path(&mut out, &op.path);
                out.push(op.kind as u8);
                put_u32(&mut out, op.mode);
                put_u32(&mut out, op.uid);
                put_u32(&mut out, op.gid);
                put_u32(&mut out, op.rdev);
                put_i64(&mut out, op.atime.0);
                put_i64(&mut out, op.atime.1);
                put_i64(&mut out, op.mtime.0);
                put_i64(&mut out, op.mtime.1);
                put_bytes(&mut out, &op.symlink);
                put_u32(&mut out, op.xattrs.len() as u32);
                for (name, value) in &op.xattrs {
                    put_bytes(&mut out, name);
                    put_bytes(&mut out, value);
                }
                put_bytes(&mut out, &op.data);
            }
        }
        out
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut cur = Cursor::new(bytes);
        let magic = cur.take(4)?;
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad queue magic",
            ));
        }
        let version = cur.u32()?;
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported queue version {version}"),
            ));
        }
        let kind = cur.u32()?;
        let op = match kind {
            KIND_CREATE => Op::Create(Create {
                path: cur.path()?,
                kind: ObjectKind::from_u8(cur.u8()?)?,
                mode: cur.u32()?,
                uid: cur.u32()?,
                gid: cur.u32()?,
                rdev: cur.u32()?,
            }),
            KIND_WRITE => Op::Write(WriteData {
                path: cur.path()?,
                offset: cur.u64()?,
                data: cur.bytes()?,
            }),
            KIND_SETATTR => {
                let path = cur.path()?;
                let flags = cur.u8()?;
                Op::Setattr(Setattr {
                    path,
                    mode: if flags & 1 != 0 {
                        Some(cur.u32()?)
                    } else {
                        None
                    },
                    uid: if flags & 2 != 0 {
                        Some(cur.u32()?)
                    } else {
                        None
                    },
                    gid: if flags & 4 != 0 {
                        Some(cur.u32()?)
                    } else {
                        None
                    },
                    size: if flags & 8 != 0 {
                        Some(cur.u64()?)
                    } else {
                        None
                    },
                    atime: if flags & 16 != 0 {
                        Some((cur.i64()?, cur.i64()?))
                    } else {
                        None
                    },
                    mtime: if flags & 32 != 0 {
                        Some((cur.i64()?, cur.i64()?))
                    } else {
                        None
                    },
                })
            }
            KIND_UNLINK => Op::Unlink(cur.path()?),
            KIND_RMDIR => Op::Rmdir(cur.path()?),
            KIND_RENAME => Op::Rename {
                from: cur.path()?,
                to: cur.path()?,
                flags: cur.u32()?,
            },
            KIND_LINK => Op::Link {
                from: cur.path()?,
                to: cur.path()?,
            },
            KIND_SYMLINK => Op::Symlink {
                path: cur.path()?,
                target: cur.bytes()?,
                uid: cur.u32()?,
                gid: cur.u32()?,
            },
            KIND_SETXATTR => Op::Setxattr {
                path: cur.path()?,
                name: cur.bytes()?,
                value: cur.bytes()?,
                flags: cur.i32()?,
            },
            KIND_REMOVEXATTR => Op::Removexattr {
                path: cur.path()?,
                name: cur.bytes()?,
            },
            KIND_FSYNC => Op::Fsync {
                path: cur.path()?,
                datasync: cur.u8()? != 0,
            },
            KIND_FALLOCATE => Op::Fallocate {
                path: cur.path()?,
                offset: cur.u64()?,
                length: cur.u64()?,
                mode: cur.i32()?,
            },
            KIND_COPY => {
                let path = cur.path()?;
                let kind = ObjectKind::from_u8(cur.u8()?)?;
                let mode = cur.u32()?;
                let uid = cur.u32()?;
                let gid = cur.u32()?;
                let rdev = cur.u32()?;
                let atime = (cur.i64()?, cur.i64()?);
                let mtime = (cur.i64()?, cur.i64()?);
                let symlink = cur.bytes()?;
                let n = cur.u32()? as usize;
                let mut xattrs = Vec::with_capacity(n);
                for _ in 0..n {
                    xattrs.push((cur.bytes()?, cur.bytes()?));
                }
                let data = cur.bytes()?;
                Op::Copy(Snapshot {
                    path,
                    kind,
                    mode,
                    uid,
                    gid,
                    rdev,
                    atime,
                    mtime,
                    symlink,
                    xattrs,
                    data,
                })
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown op kind {other}"),
                ))
            }
        };
        if !cur.rest().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing bytes in queue record",
            ));
        }
        Ok(op)
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.at..]
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.at + n > self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated queue record",
            ));
        }
        let slice = &self.bytes[self.at..self.at + n];
        self.at += n;
        Ok(slice)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }

    fn u64(&mut self) -> io::Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    fn i32(&mut self) -> io::Result<i32> {
        Ok(self.u32()? as i32)
    }

    fn i64(&mut self) -> io::Result<i64> {
        Ok(self.u64()? as i64)
    }

    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.u64()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn path(&mut self) -> io::Result<PathBuf> {
        use std::os::unix::ffi::OsStrExt;
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(&self.bytes()?)))
    }
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_i32(out: &mut Vec<u8>, v: i32) {
    put_u32(out, v as u32);
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    put_u64(out, v as u64);
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_path(out: &mut Vec<u8>, path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    put_bytes(out, path.as_os_str().as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ops() -> Vec<Op> {
        vec![
            Op::Create(Create {
                path: PathBuf::from("dir/file"),
                kind: ObjectKind::File,
                mode: 0o640,
                uid: 10,
                gid: 20,
                rdev: 0,
            }),
            Op::Write(WriteData {
                path: PathBuf::from("dir/file"),
                offset: 3,
                data: b"abc\0def".to_vec(),
            }),
            Op::Setattr(Setattr {
                path: PathBuf::from("dir/file"),
                mode: Some(0o600),
                uid: None,
                gid: Some(7),
                size: Some(4),
                atime: None,
                mtime: Some((12, 34)),
            }),
            Op::Unlink(PathBuf::from("dir/file")),
            Op::Rmdir(PathBuf::from("dir")),
            Op::Rename {
                from: PathBuf::from("a"),
                to: PathBuf::from("b"),
                flags: 1,
            },
            Op::Link {
                from: PathBuf::from("a"),
                to: PathBuf::from("c"),
            },
            Op::Symlink {
                path: PathBuf::from("link"),
                target: b"target".to_vec(),
                uid: 1,
                gid: 2,
            },
            Op::Setxattr {
                path: PathBuf::from("a"),
                name: b"user.note".to_vec(),
                value: b"v".to_vec(),
                flags: 1,
            },
            Op::Removexattr {
                path: PathBuf::from("a"),
                name: b"user.note".to_vec(),
            },
            Op::Fsync {
                path: PathBuf::from("a"),
                datasync: true,
            },
            Op::Fallocate {
                path: PathBuf::from("a"),
                offset: 1,
                length: 2,
                mode: 0,
            },
            Op::Copy(Snapshot {
                path: PathBuf::from("pre"),
                kind: ObjectKind::File,
                mode: 0o644,
                uid: 1,
                gid: 2,
                rdev: 0,
                atime: (5, 6),
                mtime: (7, 8),
                symlink: Vec::new(),
                xattrs: vec![(b"user.a".to_vec(), b"b".to_vec())],
                data: b"full".to_vec(),
            }),
        ]
    }

    #[test]
    fn round_trip_every_op() {
        for op in sample_ops() {
            let decoded = Op::decode(&op.encode()).unwrap();
            assert_eq!(decoded, op);
        }
    }

    #[test]
    fn fifo_survives_reopen() {
        let root = std::env::temp_dir().join(format!(
            "mirrorfs-queue-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let queue = Queue::open(&root).unwrap();
        assert!(queue.dir().ends_with(QUEUE_DIR_NAME));
        let ops = sample_ops();
        for op in &ops {
            queue.push(op).unwrap();
        }
        drop(queue);
        let queue = Queue::open(&root).unwrap();
        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), ops.len());
        for (index, (seq, op)) in pending.iter().enumerate() {
            assert_eq!(*seq, (index as u64) + 1);
            assert_eq!(op, &ops[index]);
        }
        let extra = Op::Unlink(PathBuf::from("later"));
        let seq = queue.push(&extra).unwrap();
        assert_eq!(seq, ops.len() as u64 + 1);
        let _ = fs::remove_dir_all(&root);
    }
}
