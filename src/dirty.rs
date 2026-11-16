//! Per-file dirty ranges for a regular file the remote already has.
//!
//! A guest write used to append one queue record. Many writes to one disk
//! image then sat in front of every later operation. The live local file is
//! the source of truth, so those writes collapse into 1 MiB blocks here.
//! The copier reads the live file for those blocks instead of replaying each
//! write. A file the remote has never had is still copied whole.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::queue::Op;

/// Bytes covered by one dirty bit. Matches the copier's chunk size.
pub const BLOCK: u64 = 1024 * 1024;

const MAGIC: &[u8; 4] = b"MFQD";
const VERSION: u32 = 1;
const FLAG_META: u8 = 1;
const FLAG_SYNC: u8 = 2;
const FLAG_WHOLE: u8 = 4;

/// Above this many distinct blocks the file is copied whole on the next pass.
const BLOCK_CAP: usize = 16 * 1024;

/// A durable dirty set. `seq` is the number `sys/queue` prints and `sys/drop` accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dirty {
    pub seq: u64,
    pub path: PathBuf,
    pub generation: u64,
    pub meta: bool,
    pub sync: bool,
    /// Copy the live file instead of individual blocks. Set when the block
    /// set would be larger than the file it describes.
    pub whole: bool,
    pub blocks: BTreeSet<u64>,
    pub fallocs: Vec<Falloc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Falloc {
    pub offset: u64,
    pub length: u64,
    pub mode: i32,
}

/// What the copier needs. Detached from the queue lock.
#[derive(Debug, Clone)]
pub struct Job {
    pub seq: u64,
    pub path: PathBuf,
    pub generation: u64,
    pub meta: bool,
    pub sync: bool,
    pub whole: bool,
    pub blocks: Vec<u64>,
    pub fallocs: Vec<Falloc>,
}

impl Dirty {
    pub fn new(seq: u64, path: PathBuf) -> Self {
        Self {
            seq,
            path,
            generation: 0,
            meta: false,
            sync: false,
            whole: false,
            blocks: BTreeSet::new(),
            fallocs: Vec::new(),
        }
    }

    /// Fold one queued mutation into this set and bump the generation.
    pub fn mark_op(&mut self, op: &Op) {
        match op {
            Op::Write(op) => self.mark_range(op.offset, op.data.len() as u64),
            Op::Fallocate {
                offset,
                length,
                mode,
                ..
            } => {
                self.fallocs.push(Falloc {
                    offset: *offset,
                    length: *length,
                    mode: *mode,
                });
                if self.fallocs.len() > 64 {
                    self.whole = true;
                    self.fallocs.clear();
                }
                self.mark_range(*offset, *length);
            }
            Op::Fsync { .. } => self.sync = true,
            Op::Setattr(_) | Op::Setxattr { .. } | Op::Removexattr { .. } => self.meta = true,
            _ => {}
        }
        self.generation = self.generation.saturating_add(1);
    }

    fn mark_range(&mut self, offset: u64, len: u64) {
        if self.whole || len == 0 {
            return;
        }
        let last = offset.saturating_add(len.saturating_sub(1));
        let mut block = offset / BLOCK;
        let end = last / BLOCK;
        loop {
            self.blocks.insert(block);
            if self.blocks.len() > BLOCK_CAP {
                self.blocks.clear();
                self.whole = true;
                return;
            }
            if block == end {
                break;
            }
            block += 1;
        }
    }

    pub fn job(&self) -> Job {
        Job {
            seq: self.seq,
            path: self.path.clone(),
            generation: self.generation,
            meta: self.meta,
            sync: self.sync,
            whole: self.whole,
            blocks: self.blocks.iter().copied().collect(),
            fallocs: self.fallocs.clone(),
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "dirty {} blocks={} meta={} sync={} whole={}",
            self.path.display(),
            self.blocks.len(),
            u8::from(self.meta),
            u8::from(self.sync),
            u8::from(self.whole),
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        put_path(&mut out, &self.path);
        out.extend_from_slice(&self.generation.to_le_bytes());
        let mut flags = 0u8;
        if self.meta {
            flags |= FLAG_META;
        }
        if self.sync {
            flags |= FLAG_SYNC;
        }
        if self.whole {
            flags |= FLAG_WHOLE;
        }
        out.push(flags);
        out.extend_from_slice(&(self.blocks.len() as u64).to_le_bytes());
        for block in &self.blocks {
            out.extend_from_slice(&block.to_le_bytes());
        }
        out.extend_from_slice(&(self.fallocs.len() as u64).to_le_bytes());
        for falloc in &self.fallocs {
            out.extend_from_slice(&falloc.offset.to_le_bytes());
            out.extend_from_slice(&falloc.length.to_le_bytes());
            out.extend_from_slice(&falloc.mode.to_le_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut cur = Cursor { bytes, at: 0 };
        let magic = cur.take(4)?;
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad dirty magic",
            ));
        }
        let version = cur.u32()?;
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported dirty version {version}"),
            ));
        }
        let seq = cur.u64()?;
        let path = cur.path()?;
        let generation = cur.u64()?;
        let flags = cur.u8()?;
        let nblocks = cur.u64()? as usize;
        let mut blocks = BTreeSet::new();
        for _ in 0..nblocks {
            blocks.insert(cur.u64()?);
        }
        let nfall = cur.u64()? as usize;
        let mut fallocs = Vec::with_capacity(nfall);
        for _ in 0..nfall {
            fallocs.push(Falloc {
                offset: cur.u64()?,
                length: cur.u64()?,
                mode: cur.u32()? as i32,
            });
        }
        if cur.at != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing bytes in dirty file",
            ));
        }
        Ok(Self {
            seq,
            path,
            generation,
            meta: flags & FLAG_META != 0,
            sync: flags & FLAG_SYNC != 0,
            whole: flags & FLAG_WHOLE != 0,
            blocks,
            fallocs,
        })
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Self::decode(&bytes)
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.at + n > self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated dirty file",
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

    fn path(&mut self) -> io::Result<PathBuf> {
        use std::os::unix::ffi::OsStrExt;
        let len = self.u64()? as usize;
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(self.take(len)?)))
    }
}

fn put_path(out: &mut Vec<u8>, path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let raw = path.as_os_str().as_bytes();
    out.extend_from_slice(&(raw.len() as u64).to_le_bytes());
    out.extend_from_slice(raw);
}

/// Write `dirty` durably. The rename replaces any previous file for this seq.
pub fn store(dir: &Path, dirty: &Dirty) -> io::Result<()> {
    let tmp = dir.join(format!("{:020}.dirty.tmp", dirty.seq));
    let final_path = dir.join(format!("{:020}.dirty", dirty.seq));
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&dirty.encode())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)?;
    let dir_file = File::open(dir)?;
    dir_file.sync_all()?;
    Ok(())
}

pub fn remove(dir: &Path, seq: u64) -> io::Result<()> {
    let path = dir.join(format!("{seq:020}.dirty"));
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let dir_file = File::open(dir)?;
    dir_file.sync_all()?;
    Ok(())
}

/// Seq numbers of `*.dirty` files. Ignores `*.dirty.tmp`.
pub fn list_seqs(dir: &Path) -> io::Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(seq) = name.strip_suffix(".dirty") else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::WriteData;

    #[test]
    fn writes_to_one_block_stay_one_range() {
        let mut dirty = Dirty::new(4, PathBuf::from("disk.raw"));
        dirty.mark_op(&Op::Write(WriteData {
            path: PathBuf::from("disk.raw"),
            offset: 10,
            data: vec![1, 2, 3],
        }));
        dirty.mark_op(&Op::Write(WriteData {
            path: PathBuf::from("disk.raw"),
            offset: 100,
            data: vec![4],
        }));
        assert_eq!(dirty.blocks.len(), 1);
        assert_eq!(dirty.generation, 2);
        let back = Dirty::decode(&dirty.encode()).unwrap();
        assert_eq!(back, dirty);
    }

    #[test]
    fn a_write_past_one_mebibyte_sets_two_blocks() {
        let mut dirty = Dirty::new(1, PathBuf::from("disk.raw"));
        dirty.mark_op(&Op::Write(WriteData {
            path: PathBuf::from("disk.raw"),
            offset: BLOCK - 1,
            data: vec![1, 2],
        }));
        assert_eq!(dirty.blocks.len(), 2);
    }
}
