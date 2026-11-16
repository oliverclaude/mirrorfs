use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::dirty::Dirty;

use crate::util::{self, QUEUE_DIR_NAME};

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
const KIND_COPY_BLOB: u32 = 14;
const KIND_COPY_LIVE: u32 = 15;

/// File contents at or below this size stay inside the queue record.
/// Anything larger is streamed into a sparse blob outside the queue lock.
pub(crate) const INLINE_LIMIT: u64 = 8 * 1024 * 1024;

/// A record this size or smaller is safe to read into memory whole.
/// The extra mebibyte covers the header and xattrs around an 8 MiB payload.
const SLURP_LIMIT: u64 = INLINE_LIMIT + 1024 * 1024;

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
    /// when the remote does not yet have this path. Only used when the file
    /// is at most [`INLINE_LIMIT`] bytes.
    #[allow(dead_code)]
    Copy(Snapshot),
    /// Sparse copy of a file larger than [`INLINE_LIMIT`]. The record stores
    /// attributes only. Bytes live in `blob`.
    #[allow(dead_code)]
    CopyBlob(CopyBlob),
    /// Legacy inline copy whose payload exceeds [`INLINE_LIMIT`]. The bytes
    /// stay in the queue file and are replayed in chunks.
    #[allow(dead_code)]
    CopyStored(CopyStored),
    /// Legacy write whose payload exceeds [`INLINE_LIMIT`].
    #[allow(dead_code)]
    WriteStored(WriteStored),
    /// Copy the live local object when the copier reaches this record.
    /// Regular files are not snapshotted into the queue: the bytes stay in
    /// the local file, including writes that arrived after this record.
    CopyLive {
        path: PathBuf,
    },
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

/// Attributes of a large file plus the blob that holds its allocated extents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyBlob {
    pub path: PathBuf,
    pub kind: ObjectKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub atime: (i64, i64),
    pub mtime: (i64, i64),
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    pub logical_size: u64,
    /// Blob file name while encoded. [`load_op`] rewrites it to an absolute path.
    pub blob: PathBuf,
}

/// A copy record already on disk whose body is too large to decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyStored {
    pub snap: Snapshot,
    pub file: PathBuf,
    pub offset: u64,
    pub len: u64,
}

/// A write record already on disk whose body is too large to decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteStored {
    pub path: PathBuf,
    pub offset: u64,
    pub file: PathBuf,
    pub data_off: u64,
    pub len: u64,
}

const BASE_MAGIC: &[u8; 4] = b"MFQB";

struct State {
    next: u64,
    /// Paths the remote already has, not counting records still in the queue.
    base: HashSet<PathBuf>,
    /// `base` plus the effect of every record still queued. Updated when a
    /// record is appended, so a later mutation does not read record bodies.
    effective: HashSet<PathBuf>,
    /// Journal paths that must be applied before a dirty bitmap of that path.
    /// A live copy does not block: later writes fold into the bitmap.
    blocked: HashMap<PathBuf, u32>,
    dirty: HashMap<PathBuf, Dirty>,
}

/// Persistent FIFO stored in `<local>/.mirrorfs-queue/`.
pub struct Queue {
    dir: PathBuf,
    inner: Mutex<State>,
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
            let text = name.to_string_lossy();
            if text.ends_with(".op.tmp")
                || text.ends_with(".blob.tmp")
                || text.ends_with(".dirty.tmp")
            {
                let _ = fs::remove_file(entry.path());
            }
        }
        let mut seqs = pending_seqs(&dir)?;
        seqs.sort_unstable();
        let base = load_base(&dir.join("base.log"))?;
        let mut effective = base.clone();
        let mut blobs = HashSet::new();
        let mut blocked = HashMap::new();
        for seq in &seqs {
            let op = load_op(&dir, *seq)?;
            if let Op::CopyBlob(op) = &op {
                if let Some(name) = op.blob.file_name() {
                    blobs.insert(PathBuf::from(name));
                }
            }
            note_block(&mut blocked, &op, 1);
            note_effect(&mut effective, &op);
        }
        reap_unreferenced_blobs(&dir, &blobs)?;
        let mut dirty = HashMap::new();
        let mut dirty_seqs = crate::dirty::list_seqs(&dir)?;
        dirty_seqs.sort_unstable();
        for seq in &dirty_seqs {
            let loaded = Dirty::load(&dir.join(format!("{seq:020}.dirty")))?;
            dirty.insert(loaded.path.clone(), loaded);
        }
        let next = seqs
            .last()
            .copied()
            .unwrap_or(0)
            .max(dirty_seqs.last().copied().unwrap_or(0))
            + 1;
        Ok(Self {
            dir,
            inner: Mutex::new(State {
                next,
                base,
                effective,
                blocked,
                dirty,
            }),
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Append one record. The directory fsync returns only after the record
    /// will survive a crash.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn push(&self, op: &Op) -> io::Result<u64> {
        let dir = self.dir.clone();
        let mut state = self.inner.lock().expect("queue lock");
        write_op(&dir, &mut state, op)
    }

    /// Queue `op`. A mutation of a regular file the remote already has is
    /// folded into that file's dirty bitmap. A regular file the remote has
    /// never had is one live copy of the local file, not a second stored
    /// copy of its bytes. Directories, symlinks, and devices stay inline.
    /// A rename whose source tree is already on the remote is replayed as a
    /// rename. Otherwise the post-rename object is copied and the rename
    /// itself is not queued, because the remote has nothing to move.
    pub fn push_for_remote(&self, local_root: &Path, op: &Op) -> io::Result<u64> {
        if let Some(seq) = self.fold_dirty(local_root, op)? {
            return Ok(seq);
        }
        loop {
            let plan = {
                let state = self.inner.lock().expect("queue lock");
                plan_copies(&state.effective, op)
            };
            let prepared = prepare_copies(local_root, &self.dir, &plan)?;
            let dir = self.dir.clone();
            let mut state = self.inner.lock().expect("queue lock");
            if plan_copies(&state.effective, op) != plan {
                drop(state);
                drop(prepared);
                continue;
            }
            return commit_plan(&dir, &mut state, op, &plan, prepared);
        }
    }

    /// Record that `op` reached the remote, then drop its queue file.
    ///
    /// The in-memory path set was updated when the record was appended.
    /// Finishing only catches the durable base up and deletes a blob.
    pub fn finish(&self, seq: u64, op: &Op) -> io::Result<()> {
        let dir = self.dir.clone();
        let mut state = self.inner.lock().expect("queue lock");
        let mut base = state.base.clone();
        if note_effect(&mut base, op) {
            write_base_file(&dir, &base)?;
            state.base = base;
        }
        relocate_dirty(&dir, &mut state, op)?;
        note_block(&mut state.blocked, op, -1);
        remove_record(&dir, seq)?;
        if let Op::CopyBlob(op) = op {
            match fs::remove_file(&op.blob) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Remove one journal record or one dirty file.
    ///
    /// `seq` is the number at the start of a `sys/queue` line. The record is
    /// not applied and is not written into `base.log`. `Ok(false)` means that
    /// number is not in the queue.
    pub fn drop_seq(&self, seq: u64) -> io::Result<bool> {
        let dir = self.dir.clone();
        let mut state = self.inner.lock().expect("queue lock");
        if let Some(path) = state
            .dirty
            .iter()
            .find(|(_, dirty)| dirty.seq == seq)
            .map(|(path, _)| path.clone())
        {
            state.dirty.remove(&path);
            crate::dirty::remove(&dir, seq)?;
            return Ok(true);
        }
        let record = dir.join(format!("{seq:020}.op"));
        if !record.exists() {
            return Ok(false);
        }
        let op = load_op(&dir, seq)?;
        if let Op::CopyBlob(op) = &op {
            match fs::remove_file(&op.blob) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        note_block(&mut state.blocked, &op, -1);
        remove_record(&dir, seq)?;
        rebuild_effective(&dir, &mut state)?;
        Ok(true)
    }

    /// Lines for `sys/queue`. Record bodies are not loaded: a write payload
    /// is counted and skipped.
    pub fn listing(&self) -> io::Result<Vec<u8>> {
        let mut lines: Vec<(u64, String)> = Vec::new();
        for seq in pending_seqs(&self.dir)? {
            lines.push((seq, summarize_op(&self.dir, seq)?));
        }
        let state = self.inner.lock().expect("queue lock");
        for dirty in state.dirty.values() {
            lines.push((dirty.seq, dirty.describe()));
        }
        drop(state);
        lines.sort_by_key(|(seq, _)| *seq);
        let mut out = String::new();
        for (seq, text) in lines {
            out.push_str(&format!("{seq} {text}\n"));
        }
        Ok(out.into_bytes())
    }

    /// The oldest dirty file whose path is not waiting on a journal record.
    pub fn next_dirty(&self) -> Option<crate::dirty::Job> {
        let state = self.inner.lock().expect("queue lock");
        state
            .dirty
            .values()
            .filter(|dirty| !path_blocked(&state.blocked, &dirty.path))
            .min_by_key(|dirty| dirty.seq)
            .map(Dirty::job)
    }

    /// Forget a dirty file when the copier finished the generation it read.
    /// A write that arrived during the copy bumped the generation, so those
    /// blocks stay dirty. Returns whether the file was removed.
    pub fn ack_dirty(&self, seq: u64, generation: u64) -> io::Result<bool> {
        let dir = self.dir.clone();
        let mut state = self.inner.lock().expect("queue lock");
        let Some(path) = state
            .dirty
            .iter()
            .find(|(_, dirty)| dirty.seq == seq)
            .map(|(path, _)| path.clone())
        else {
            return Ok(false);
        };
        let generation_now = state.dirty.get(&path).map(|dirty| dirty.generation);
        if generation_now != Some(generation) {
            return Ok(false);
        }
        state.dirty.remove(&path);
        crate::dirty::remove(&dir, seq)?;
        Ok(true)
    }

    fn fold_dirty(&self, local_root: &Path, op: &Op) -> io::Result<Option<u64>> {
        if !folds_into_dirty(op) {
            return Ok(None);
        }
        let path = op.path();
        let full = local_root.join(path);
        let st = match util::lstat(&full) {
            Ok(st) => st,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        if st.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Ok(None);
        }
        let dir = self.dir.clone();
        let mut state = self.inner.lock().expect("queue lock");
        if path_blocked(&state.blocked, path) {
            return Ok(None);
        }
        if state.effective.contains(path) {
            return Ok(Some(merge_dirty(&dir, &mut state, op)?));
        }
        if !wants_snapshot(op) {
            return Ok(None);
        }
        // The remote has never had this file. One live copy reads the local
        // file when the copier runs, so the queue does not store another copy
        // of a disk image. A later write folds into the bitmap.
        let live = Op::CopyLive {
            path: path.to_path_buf(),
        };
        let seq = write_op(&dir, &mut state, &live)?;
        if matches!(op, Op::Fallocate { .. }) {
            merge_dirty(&dir, &mut state, op)?;
        }
        Ok(Some(seq))
    }

    /// Number of committed records. Does not read them.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn depth(&self) -> io::Result<usize> {
        Ok(pending_seqs(&self.dir)?.len() + crate::dirty::list_seqs(&self.dir)?.len())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn pending(&self) -> io::Result<Vec<(u64, Op)>> {
        let mut seqs = pending_seqs(&self.dir)?;
        seqs.sort_unstable();
        let mut out = Vec::with_capacity(seqs.len());
        for seq in seqs {
            out.push((seq, load_op(&self.dir, seq)?));
        }
        Ok(out)
    }

    /// The oldest committed record, if the queue is not empty.
    ///
    /// A record larger than [`SLURP_LIMIT`] is not read into one buffer.
    pub fn head(&self) -> io::Result<Option<(u64, Op)>> {
        let Some(seq) = pending_seqs(&self.dir)?.into_iter().min() else {
            return Ok(None);
        };
        Ok(Some((seq, load_op(&self.dir, seq)?)))
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

fn write_op(dir: &Path, state: &mut State, op: &Op) -> io::Result<u64> {
    let seq = state.next;
    let tmp = dir.join(format!("{seq:020}.op.tmp"));
    let final_path = dir.join(format!("{seq:020}.op"));
    {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        file.write_all(&op.encode())?;
        file.sync_all()?;
    }
    fs::rename(&tmp, &final_path)?;
    fsync_dir(dir)?;
    state.next = seq + 1;
    note_block(&mut state.blocked, op, 1);
    note_effect(&mut state.effective, op);
    Ok(seq)
}

fn folds_into_dirty(op: &Op) -> bool {
    matches!(
        op,
        Op::Write(_)
            | Op::Setattr(_)
            | Op::Fsync { .. }
            | Op::Setxattr { .. }
            | Op::Removexattr { .. }
            | Op::Fallocate { .. }
    )
}

fn note_block(blocked: &mut HashMap<PathBuf, u32>, op: &Op, delta: i32) {
    for path in blocking_paths(op) {
        let entry = blocked.entry(path).or_insert(0);
        if delta < 0 {
            *entry = entry.saturating_sub(delta.unsigned_abs());
        } else {
            *entry = entry.saturating_add(delta as u32);
        }
    }
    blocked.retain(|_, count| *count > 0);
}

fn blocking_paths(op: &Op) -> Vec<PathBuf> {
    match op {
        Op::Copy(_) | Op::CopyBlob(_) | Op::CopyStored(_) | Op::CopyLive { .. } => Vec::new(),
        Op::Rename { from, to, .. } => vec![from.clone(), to.clone()],
        Op::Link { from, to } => vec![from.clone(), to.clone()],
        other => vec![other.path().to_path_buf()],
    }
}

fn path_blocked(blocked: &HashMap<PathBuf, u32>, path: &Path) -> bool {
    blocked
        .iter()
        .any(|(have, count)| *count > 0 && (path == have || path.starts_with(have)))
}

fn remove_record(dir: &Path, seq: u64) -> io::Result<()> {
    let path = dir.join(format!("{seq:020}.op"));
    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    fsync_dir(dir)
}

fn merge_dirty(dir: &Path, state: &mut State, op: &Op) -> io::Result<u64> {
    let path = op.path().to_path_buf();
    let mut dirty = match state.dirty.remove(&path) {
        Some(dirty) => dirty,
        None => {
            let seq = state.next;
            state.next = state.next.saturating_add(1);
            Dirty::new(seq, path.clone())
        }
    };
    dirty.mark_op(op);
    crate::dirty::store(dir, &dirty)?;
    let seq = dirty.seq;
    state.dirty.insert(path, dirty);
    Ok(seq)
}

fn relocate_dirty(dir: &Path, state: &mut State, op: &Op) -> io::Result<()> {
    let Op::Rename { from, to, flags } = op else {
        return Ok(());
    };
    let exchange = flags & libc::RENAME_EXCHANGE != 0;
    let paths: Vec<PathBuf> = state.dirty.keys().cloned().collect();
    for path in paths {
        let moved = util::relocated_path(&path, from, to).or_else(|| {
            if exchange {
                util::relocated_path(&path, to, from)
            } else {
                None
            }
        });
        let Some(moved) = moved else {
            continue;
        };
        if moved == path {
            continue;
        }
        let Some(mut dirty) = state.dirty.remove(&path) else {
            continue;
        };
        dirty.path = moved.clone();
        crate::dirty::store(dir, &dirty)?;
        state.dirty.insert(moved, dirty);
    }
    Ok(())
}

fn rebuild_effective(dir: &Path, state: &mut State) -> io::Result<()> {
    let mut seqs = pending_seqs(dir)?;
    seqs.sort_unstable();
    let mut effective = state.base.clone();
    let mut blocked = HashMap::new();
    for seq in seqs {
        let op = load_op(dir, seq)?;
        note_block(&mut blocked, &op, 1);
        note_effect(&mut effective, &op);
    }
    state.effective = effective;
    state.blocked = blocked;
    Ok(())
}

fn summarize_op(dir: &Path, seq: u64) -> io::Result<String> {
    let path = dir.join(format!("{seq:020}.op"));
    let mut file = File::open(&path)?;
    let len = file.metadata()?.len();
    if len <= 64 * 1024 {
        let op = load_op(dir, seq)?;
        return Ok(op.describe());
    }
    summarize_large(&mut file)
}

fn summarize_large(file: &mut File) -> io::Result<String> {
    let magic = read_exact(file, 4)?;
    if magic.as_slice() != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad queue magic",
        ));
    }
    let _version = read_u32(file)?;
    let kind = read_u32(file)?;
    let text = match kind {
        KIND_WRITE => {
            let path = read_path(file)?;
            let offset = read_u64(file)?;
            let len = read_u64(file)?;
            format!("write {} offset={offset} bytes={len}", path.display())
        }
        KIND_COPY => {
            let path = read_path(file)?;
            format!("copy {} stored", path.display())
        }
        KIND_COPY_BLOB => {
            let path = read_path(file)?;
            format!("copy {} blob", path.display())
        }
        _ => {
            file.seek(SeekFrom::Start(0))?;
            // The record is large but not a write or copy payload. Read the
            // header only up to the slurp limit's caller; describe what we can.
            format!("op kind={kind}")
        }
    };
    Ok(text)
}

fn read_exact(file: &mut File, n: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_u32(file: &mut File) -> io::Result<u32> {
    let buf = read_exact(file, 4)?;
    Ok(u32::from_le_bytes(buf.try_into().unwrap()))
}

fn read_u64(file: &mut File) -> io::Result<u64> {
    let buf = read_exact(file, 8)?;
    Ok(u64::from_le_bytes(buf.try_into().unwrap()))
}

fn read_path(file: &mut File) -> io::Result<PathBuf> {
    let len = read_u64(file)? as usize;
    let buf = read_exact(file, len)?;
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(&buf)))
}

fn wants_snapshot(op: &Op) -> bool {
    matches!(
        op,
        Op::Write(_)
            | Op::Setattr(_)
            | Op::Fsync { .. }
            | Op::Setxattr { .. }
            | Op::Removexattr { .. }
            | Op::Fallocate { .. }
    )
}

fn note_effect(base: &mut HashSet<PathBuf>, op: &Op) -> bool {
    match op {
        Op::Create(op) => base.insert(op.path.clone()),
        Op::Copy(op) => base.insert(op.path.clone()),
        Op::CopyBlob(op) => base.insert(op.path.clone()),
        Op::CopyStored(op) => base.insert(op.snap.path.clone()),
        Op::CopyLive { path } => base.insert(path.clone()),
        Op::Unlink(path) | Op::Rmdir(path) => base.remove(path),
        Op::Link { to, .. } => base.insert(to.clone()),
        Op::Symlink { path, .. } => base.insert(path.clone()),
        Op::Rename { from, to, flags } => note_rename(base, from, to, *flags),
        _ => false,
    }
}

fn note_rename(base: &mut HashSet<PathBuf>, from: &Path, to: &Path, flags: u32) -> bool {
    let exchange = flags & libc::RENAME_EXCHANGE != 0;
    let before = base.clone();
    if !exchange {
        base.retain(|path| path != to && !path.starts_with(to));
    }
    let paths: Vec<PathBuf> = base.iter().cloned().collect();
    let mut moves = Vec::new();
    for path in paths {
        if let Some(neu) = util::relocated_path(&path, from, to) {
            moves.push((path, neu));
        } else if exchange {
            if let Some(neu) = util::relocated_path(&path, to, from) {
                moves.push((path, neu));
            }
        }
    }
    for (old, _) in &moves {
        base.remove(old);
    }
    for (_, neu) in moves {
        base.insert(neu);
    }
    base != &before
}

fn covers(set: &HashSet<PathBuf>, path: &Path) -> bool {
    set.contains(path) || set.iter().any(|have| have.starts_with(path))
}

#[derive(Clone, PartialEq, Eq)]
struct Plan {
    copies: Vec<PathBuf>,
    queue_op: bool,
}

fn plan_copies(effective: &HashSet<PathBuf>, op: &Op) -> Plan {
    match op {
        Op::Rename { from, to, flags } => {
            let has_from = covers(effective, from);
            let has_to = covers(effective, to);
            let exchange = *flags & libc::RENAME_EXCHANGE != 0;
            if exchange && has_from && has_to {
                Plan {
                    copies: Vec::new(),
                    queue_op: true,
                }
            } else if exchange || !has_from {
                // The remote is missing a side, so replaying the rename cannot
                // land on the post-rename tree. Copy the local end state.
                let mut copies = Vec::new();
                if exchange {
                    copies.push(from.clone());
                }
                copies.push(to.clone());
                Plan {
                    copies,
                    queue_op: false,
                }
            } else {
                Plan {
                    copies: Vec::new(),
                    queue_op: true,
                }
            }
        }
        Op::Link { from, .. } => {
            if effective.contains(from) {
                Plan {
                    copies: Vec::new(),
                    queue_op: true,
                }
            } else {
                Plan {
                    copies: vec![from.clone()],
                    queue_op: true,
                }
            }
        }
        _ => {
            if wants_snapshot(op) && !effective.contains(op.path()) {
                Plan {
                    copies: vec![op.path().to_path_buf()],
                    queue_op: true,
                }
            } else {
                Plan {
                    copies: Vec::new(),
                    queue_op: true,
                }
            }
        }
    }
}

struct Prepared {
    captured: Captured,
}

enum Captured {
    Inline(Snapshot),
    Live(PathBuf),
}

fn prepare_copies(local_root: &Path, queue_dir: &Path, plan: &Plan) -> io::Result<Vec<Prepared>> {
    let mut out = Vec::new();
    for path in &plan.copies {
        match capture(local_root, queue_dir, path) {
            Ok(captured) => out.push(Prepared { captured }),
            Err(err) => {
                drop(out);
                return Err(err);
            }
        }
    }
    Ok(out)
}

fn capture(local_root: &Path, _queue_dir: &Path, rel: &Path) -> io::Result<Captured> {
    let full = local_root.join(rel);
    let st = util::lstat(&full)?;
    let kind = kind_of_mode(st.st_mode);
    // A regular file is copied from the live local file when the record
    // runs. Storing an 8 GiB snapshot here would fill the queue disk while
    // the remote is down, and the next write would do it again.
    if kind == ObjectKind::File {
        return Ok(Captured::Live(rel.to_path_buf()));
    }
    Ok(Captured::Inline(snapshot(local_root, rel)?))
}

fn commit_plan(
    dir: &Path,
    state: &mut State,
    op: &Op,
    plan: &Plan,
    prepared: Vec<Prepared>,
) -> io::Result<u64> {
    let mut seq = state.next.saturating_sub(1);
    for item in &prepared {
        seq = commit_one(dir, state, item)?;
    }
    if plan.queue_op {
        seq = write_op(dir, state, op)?;
    }
    Ok(seq)
}

fn commit_one(dir: &Path, state: &mut State, item: &Prepared) -> io::Result<u64> {
    match &item.captured {
        Captured::Inline(snap) => write_op(dir, state, &Op::Copy(snap.clone())),
        Captured::Live(path) => write_op(dir, state, &Op::CopyLive { path: path.clone() }),
    }
}

fn reap_unreferenced_blobs(dir: &Path, referenced: &HashSet<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let text = name.to_string_lossy();
        if text.ends_with(".blob") && !referenced.contains(Path::new(name.as_os_str())) {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}

fn load_op(dir: &Path, seq: u64) -> io::Result<Op> {
    let path = dir.join(format!("{seq:020}.op"));
    let file = File::open(&path)?;
    let len = file.metadata()?.len();
    let mut op = if len <= SLURP_LIMIT {
        Op::decode(&fs::read(&path)?)?
    } else {
        decode_large(&path, file)?
    };
    if let Op::CopyBlob(op) = &mut op {
        if op.blob.is_relative() {
            op.blob = dir.join(&op.blob);
        }
    }
    Ok(op)
}

fn load_base(path: &Path) -> io::Result<HashSet<PathBuf>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(err) => return Err(err),
    };
    if bytes.is_empty() {
        return Ok(HashSet::new());
    }
    if bytes.len() < 16 || &bytes[..4] != BASE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "base.log has a bad header",
        ));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("base.log version {version}"),
        ));
    }
    let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let mut at = 16;
    let mut set = HashSet::with_capacity(count);
    for _ in 0..count {
        if at + 8 > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated base.log",
            ));
        }
        let len = u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()) as usize;
        at += 8;
        if at + len > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated base.log path",
            ));
        }
        set.insert(PathBuf::from(std::ffi::OsStr::from_bytes(
            &bytes[at..at + len],
        )));
        at += len;
    }
    Ok(set)
}

fn write_base_file(dir: &Path, base: &HashSet<PathBuf>) -> io::Result<()> {
    let tmp = dir.join("base.log.tmp");
    let final_path = dir.join("base.log");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BASE_MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&(base.len() as u64).to_le_bytes());
        for path in base {
            let raw = path.as_os_str().as_bytes();
            bytes.extend_from_slice(&(raw.len() as u64).to_le_bytes());
            bytes.extend_from_slice(raw);
        }
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, &final_path)?;
    fsync_dir(dir)
}

pub(crate) fn snapshot(local_root: &Path, rel: &Path) -> io::Result<Snapshot> {
    let full = local_root.join(rel);
    let mut snap = snapshot_meta(&full, rel)?;
    if snap.kind == ObjectKind::File {
        let len = util::lstat(&full)?.st_size as u64;
        if len > INLINE_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file exceeds the inline snapshot limit",
            ));
        }
        snap.data = fs::read(&full)?;
    }
    Ok(snap)
}

fn snapshot_meta(full: &Path, rel: &Path) -> io::Result<Snapshot> {
    let st = util::lstat(full)?;
    let kind = kind_of_mode(st.st_mode);
    let symlink = if kind == ObjectKind::Symlink {
        fs::read_link(full)?.as_os_str().as_bytes().to_vec()
    } else {
        Vec::new()
    };
    Ok(Snapshot {
        path: rel.to_path_buf(),
        kind,
        mode: st.st_mode & 0o7777,
        uid: st.st_uid,
        gid: st.st_gid,
        rdev: st.st_rdev as u32,
        atime: (st.st_atime, st.st_atime_nsec),
        mtime: (st.st_mtime, st.st_mtime_nsec),
        symlink,
        xattrs: list_xattrs(full)?,
        data: Vec::new(),
    })
}

fn kind_of_mode(mode: u32) -> ObjectKind {
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

pub(crate) fn list_xattrs(path: &Path) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let c = util::cstring(path)?;
    let mut size = unsafe { libc::llistxattr(c.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return empty_if_unsupported();
    }
    let mut names = vec![0u8; size as usize];
    size = unsafe {
        libc::llistxattr(
            c.as_ptr(),
            names.as_mut_ptr() as *mut libc::c_char,
            names.len(),
        )
    };
    if size < 0 {
        return empty_if_unsupported();
    }
    names.truncate(size as usize);
    let mut out = Vec::new();
    for name in names.split(|byte| *byte == 0) {
        if name.is_empty() {
            continue;
        }
        out.push((name.to_vec(), get_xattr(&c, name)?));
    }
    Ok(out)
}

fn get_xattr(path: &std::ffi::CString, name: &[u8]) -> io::Result<Vec<u8>> {
    let name = std::ffi::CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "xattr name contains NUL"))?;
    let mut size =
        unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut value = vec![0u8; size as usize];
    size = unsafe {
        libc::lgetxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_mut_ptr() as *mut libc::c_void,
            value.len(),
        )
    };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    value.truncate(size as usize);
    Ok(value)
}

fn empty_if_unsupported<T>() -> io::Result<Vec<T>> {
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EOPNOTSUPP) {
        Ok(Vec::new())
    } else {
        Err(err)
    }
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
            Op::CopyBlob(op) => &op.path,
            Op::CopyStored(op) => &op.snap.path,
            Op::WriteStored(op) => &op.path,
            Op::CopyLive { path } => path,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Op::Rename { from, to, flags } => {
                format!(
                    "rename {} -> {} flags={flags}",
                    from.display(),
                    to.display()
                )
            }
            Op::Link { from, to } => format!("link {} -> {}", from.display(), to.display()),
            Op::Symlink { path, target, .. } => format!(
                "symlink {} -> {}",
                path.display(),
                String::from_utf8_lossy(target)
            ),
            Op::Setxattr { path, name, .. } => format!(
                "setxattr {} {}",
                path.display(),
                String::from_utf8_lossy(name)
            ),
            Op::Removexattr { path, name } => format!(
                "removexattr {} {}",
                path.display(),
                String::from_utf8_lossy(name)
            ),
            Op::Fallocate {
                path,
                offset,
                length,
                mode,
            } => format!(
                "fallocate {} offset={offset} length={length} mode={mode}",
                path.display()
            ),
            Op::CopyBlob(op) => {
                format!("copy {} bytes={}", op.path.display(), op.logical_size)
            }
            Op::CopyLive { path } => format!("copy {} live", path.display()),
            Op::CopyStored(op) => format!("copy {} bytes={}", op.snap.path.display(), op.len),
            Op::WriteStored(op) => format!(
                "write {} offset={} bytes={}",
                op.path.display(),
                op.offset,
                op.len
            ),
            other => format!("{} {}", other.kind_name(), other.path().display()),
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Op::Create(_) => "create",
            Op::Write(_) => "write",
            Op::Setattr(_) => "setattr",
            Op::Unlink(_) => "unlink",
            Op::Rmdir(_) => "rmdir",
            Op::Rename { .. } => "rename",
            Op::Link { .. } => "link",
            Op::Symlink { .. } => "symlink",
            Op::Setxattr { .. } => "setxattr",
            Op::Removexattr { .. } => "removexattr",
            Op::Fsync { .. } => "fsync",
            Op::Fallocate { .. } => "fallocate",
            Op::Copy(_) | Op::CopyBlob(_) | Op::CopyStored(_) | Op::CopyLive { .. } => "copy",
            Op::WriteStored(_) => "write",
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
            Op::CopyBlob(op) => {
                put_u32(&mut out, KIND_COPY_BLOB);
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
                put_u32(&mut out, op.xattrs.len() as u32);
                for (name, value) in &op.xattrs {
                    put_bytes(&mut out, name);
                    put_bytes(&mut out, value);
                }
                put_u64(&mut out, op.logical_size);
                put_path(&mut out, &op.blob);
            }
            Op::CopyStored(_) | Op::WriteStored(_) => {
                // These exist only as a view of a record already on disk.
            }
            Op::CopyLive { path } => {
                put_u32(&mut out, KIND_COPY_LIVE);
                put_path(&mut out, path);
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
            KIND_COPY_BLOB => {
                let path = cur.path()?;
                let kind = ObjectKind::from_u8(cur.u8()?)?;
                let mode = cur.u32()?;
                let uid = cur.u32()?;
                let gid = cur.u32()?;
                let rdev = cur.u32()?;
                let atime = (cur.i64()?, cur.i64()?);
                let mtime = (cur.i64()?, cur.i64()?);
                let n = cur.u32()? as usize;
                let mut xattrs = Vec::with_capacity(n);
                for _ in 0..n {
                    xattrs.push((cur.bytes()?, cur.bytes()?));
                }
                Op::CopyBlob(CopyBlob {
                    path,
                    kind,
                    mode,
                    uid,
                    gid,
                    rdev,
                    atime,
                    mtime,
                    xattrs,
                    logical_size: cur.u64()?,
                    blob: cur.path()?,
                })
            }
            KIND_COPY_LIVE => Op::CopyLive { path: cur.path()? },
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

/// Parse a record that is too large to read whole. Copy and write payloads
/// above [`INLINE_LIMIT`] stay in the file.
fn decode_large(path: &Path, file: File) -> io::Result<Op> {
    let mut cur = Reader::new(file)?;
    let magic = cur.take(4)?;
    if magic.as_slice() != MAGIC {
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
        KIND_COPY => {
            let rel = cur.path()?;
            let obj_kind = ObjectKind::from_u8(cur.u8()?)?;
            let mode = cur.u32()?;
            let uid = cur.u32()?;
            let gid = cur.u32()?;
            let rdev = cur.u32()?;
            let atime = (cur.i64()?, cur.i64()?);
            let mtime = (cur.i64()?, cur.i64()?);
            let symlink = cur.bytes_capped()?;
            let n = cur.u32()? as usize;
            let mut xattrs = Vec::with_capacity(n);
            for _ in 0..n {
                xattrs.push((cur.bytes_capped()?, cur.bytes_capped()?));
            }
            let (offset, len) = cur.payload()?;
            if len <= INLINE_LIMIT {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "oversized record has a small copy payload",
                ));
            }
            Op::CopyStored(CopyStored {
                snap: Snapshot {
                    path: rel,
                    kind: obj_kind,
                    mode,
                    uid,
                    gid,
                    rdev,
                    atime,
                    mtime,
                    symlink,
                    xattrs,
                    data: Vec::new(),
                },
                file: path.to_path_buf(),
                offset,
                len,
            })
        }
        KIND_WRITE => {
            let rel = cur.path()?;
            let offset = cur.u64()?;
            let (data_off, len) = cur.payload()?;
            if len <= INLINE_LIMIT {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "oversized record has a small write payload",
                ));
            }
            Op::WriteStored(WriteStored {
                path: rel,
                offset,
                file: path.to_path_buf(),
                data_off,
                len,
            })
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "queue record is too large to load",
            ))
        }
    };
    if cur.at != cur.end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes in queue record",
        ));
    }
    Ok(op)
}

struct Reader {
    file: File,
    at: u64,
    end: u64,
}

impl Reader {
    fn new(file: File) -> io::Result<Self> {
        let end = file.metadata()?.len();
        Ok(Self { file, at: 0, end })
    }

    fn take(&mut self, n: usize) -> io::Result<Vec<u8>> {
        if self.at.saturating_add(n as u64) > self.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated queue record",
            ));
        }
        let mut buf = vec![0u8; n];
        self.file.read_exact_at(&mut buf, self.at)?;
        self.at += n as u64;
        Ok(buf)
    }

    fn u32(&mut self) -> io::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.as_slice().try_into().unwrap()))
    }

    fn u64(&mut self) -> io::Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.as_slice().try_into().unwrap()))
    }

    fn i64(&mut self) -> io::Result<i64> {
        Ok(self.u64()? as i64)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn bytes_capped(&mut self) -> io::Result<Vec<u8>> {
        let len = self.u64()?;
        if len > INLINE_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "queue field is too large",
            ));
        }
        self.take(len as usize)
    }

    fn path(&mut self) -> io::Result<PathBuf> {
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(
            &self.bytes_capped()?,
        )))
    }

    /// Length-prefixed payload. The bytes are not read.
    fn payload(&mut self) -> io::Result<(u64, u64)> {
        let len = self.u64()?;
        let offset = self.at;
        let end = self.at.saturating_add(len);
        if end > self.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated queue record",
            ));
        }
        self.at = end;
        Ok((offset, len))
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
    use std::fs::{File, OpenOptions};
    use std::os::unix::fs::FileExt;

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

    #[test]
    fn unknown_file_is_snapshotted_once() {
        let root = std::env::temp_dir().join(format!(
            "mirrorfs-snap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("pre")).unwrap();
        let file = root.join("pre/exist.txt");
        fs::write(&file, b"original\n").unwrap();
        fs::write(root.join("pre/skip.txt"), b"nope").unwrap();
        let queue = Queue::open(&root).unwrap();
        let write = Op::Write(WriteData {
            path: PathBuf::from("pre/exist.txt"),
            offset: 9,
            data: b"more\n".to_vec(),
        });
        // The local file already contains the post-mutation bytes.
        fs::write(&file, b"original\nmore\n").unwrap();
        queue.push_for_remote(&root, &write).unwrap();
        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), 1);
        match &pending[0].1 {
            Op::CopyLive { path } => assert_eq!(path, &PathBuf::from("pre/exist.txt")),
            other => panic!("expected live copy, got {other:?}"),
        }
        let seq = queue
            .push_for_remote(
                &root,
                &Op::Write(WriteData {
                    path: PathBuf::from("pre/exist.txt"),
                    offset: 0,
                    data: b"Z".to_vec(),
                }),
            )
            .unwrap();
        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), 1, "later writes fold into the dirty bitmap");
        let listed = String::from_utf8(queue.listing().unwrap()).unwrap();
        assert!(listed.contains(&format!("{seq} dirty pre/exist.txt")));
        assert!(queue.drop_seq(seq).unwrap());
        let listed = String::from_utf8(queue.listing().unwrap()).unwrap();
        assert!(!listed.contains("dirty pre/exist.txt"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn known_rename_moves_descendant_paths() {
        let root = std::env::temp_dir().join(format!(
            "mirrorfs-ren-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("other")).unwrap();
        fs::write(root.join("other/a"), b"child").unwrap();
        let queue = Queue::open(&root).unwrap();
        let dir = Op::Create(Create {
            path: PathBuf::from("dir"),
            kind: ObjectKind::Dir,
            mode: 0o755,
            uid: 1,
            gid: 1,
            rdev: 0,
        });
        let child = Op::Create(Create {
            path: PathBuf::from("dir/a"),
            kind: ObjectKind::File,
            mode: 0o644,
            uid: 1,
            gid: 1,
            rdev: 0,
        });
        let dir_seq = queue.push(&dir).unwrap();
        let child_seq = queue.push(&child).unwrap();
        queue.finish(dir_seq, &dir).unwrap();
        queue.finish(child_seq, &child).unwrap();
        let rename = Op::Rename {
            from: PathBuf::from("dir"),
            to: PathBuf::from("other"),
            flags: 0,
        };
        queue.push_for_remote(&root, &rename).unwrap();
        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1, rename);
        queue.finish(pending[0].0, &rename).unwrap();
        queue
            .push_for_remote(
                &root,
                &Op::Write(WriteData {
                    path: PathBuf::from("other/a"),
                    offset: 0,
                    data: b"Z".to_vec(),
                }),
            )
            .unwrap();
        let pending = queue.pending().unwrap();
        assert!(pending.is_empty(), "{pending:?}");
        let listed = String::from_utf8(queue.listing().unwrap()).unwrap();
        assert!(listed.contains("dirty other/a"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn unknown_rename_copies_the_new_path() {
        let root = std::env::temp_dir().join(format!(
            "mirrorfs-ren2-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("new.txt"), b"moved").unwrap();
        let queue = Queue::open(&root).unwrap();
        queue
            .push_for_remote(
                &root,
                &Op::Rename {
                    from: PathBuf::from("old.txt"),
                    to: PathBuf::from("new.txt"),
                    flags: 0,
                },
            )
            .unwrap();
        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), 1);
        match &pending[0].1 {
            Op::CopyLive { path } => assert_eq!(path, &PathBuf::from("new.txt")),
            other => panic!("expected live copy, got {other:?}"),
        }
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_large_file_is_a_live_copy_and_not_a_blob() {
        let root = scratch("bound");
        let big = root.join("big");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&big)
            .unwrap();
        file.write_at(b"HEAD", 0).unwrap();
        let big_len = INLINE_LIMIT + 1;
        file.set_len(big_len).unwrap();
        file.write_at(b"TAIL", big_len - 4).unwrap();
        drop(file);

        let queue = Queue::open(&root).unwrap();
        queue
            .push_for_remote(
                &root,
                &Op::Fsync {
                    path: PathBuf::from("big"),
                    datasync: false,
                },
            )
            .unwrap();
        let pending = queue.pending().unwrap();
        assert!(matches!(pending[0].1, Op::CopyLive { .. }));
        let op_len = fs::metadata(queue.dir().join(format!("{:020}.op", pending[0].0)))
            .unwrap()
            .len();
        assert!(op_len < 4096, "live copy record is {op_len} bytes");
        let blobs: Vec<_> = fs::read_dir(queue.dir())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".blob"))
            .collect();
        assert!(blobs.is_empty());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn streamed_copy_survives_reopen_and_replays_sparsely() {
        let root = scratch("replay");
        let remote = scratch("replay-remote");
        let big = root.join("disk.raw");
        let len = INLINE_LIMIT + 4096;
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&big)
            .unwrap();
        file.write_at(b"HEAD", 0).unwrap();
        file.set_len(len).unwrap();
        file.write_at(b"TAIL", len - 4).unwrap();
        drop(file);
        let live = File::options().write(true).open(&big).unwrap();
        live.write_at(b"Z", 1).unwrap();
        drop(live);
        let queue = Queue::open(&root).unwrap();
        queue
            .push_for_remote(
                &root,
                &Op::Write(WriteData {
                    path: PathBuf::from("disk.raw"),
                    offset: 0,
                    data: b"HEAD".to_vec(),
                }),
            )
            .unwrap();
        drop(queue);
        let queue = Queue::open(&root).unwrap();
        queue
            .push_for_remote(
                &root,
                &Op::Write(WriteData {
                    path: PathBuf::from("disk.raw"),
                    offset: 1,
                    data: b"Z".to_vec(),
                }),
            )
            .unwrap();
        let pending = queue.pending().unwrap();
        assert_eq!(pending.len(), 1, "{pending:?}");
        assert!(matches!(pending[0].1, Op::CopyLive { .. }));
        let job = queue.next_dirty().expect("second write is dirty");
        fs::create_dir_all(&remote).unwrap();
        for (seq, op) in &pending {
            crate::worker::apply(&root, &remote, op).unwrap();
            queue.finish(*seq, op).unwrap();
        }
        crate::worker::apply_dirty(&root, &remote, &job).unwrap();
        assert!(queue.ack_dirty(job.seq, job.generation).unwrap());
        let copied = remote.join("disk.raw");
        let out = File::open(&copied).unwrap();
        assert_eq!(out.metadata().unwrap().len(), len);
        let mut head = [0u8; 4];
        let mut tail = [0u8; 4];
        let mut hole = [0u8; 4];
        out.read_exact_at(&mut head, 0).unwrap();
        out.read_exact_at(&mut tail, len - 4).unwrap();
        out.read_exact_at(&mut hole, 1024 * 1024).unwrap();
        assert_eq!(&head, b"HZAD");
        assert_eq!(&tail, b"TAIL");
        assert_eq!(hole, [0, 0, 0, 0]);
        let blobs: Vec<_> = fs::read_dir(queue.dir())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".blob"))
            .collect();
        assert!(blobs.is_empty(), "blob left behind: {blobs:?}");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&remote);
    }

    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "mirrorfs-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }
}
