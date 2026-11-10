//! Chunked copy that keeps holes as holes.
//!
//! `SEEK_DATA` / `SEEK_HOLE` skip unallocated extents, so a sparse disk image
//! is not read into memory as zeroes. Filesystems that reject those seeks are
//! copied densely, still one chunk at a time.

use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::Path;

const CHUNK: usize = 1024 * 1024;

/// Copy `src` onto `dst`, including the logical size. Holes stay holes when
/// the kernel reports them. `dst` is created or replaced.
pub fn copy_sparse(src: &Path, dst: &Path) -> io::Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(src)?;
    let len = input.metadata()?.len();
    let output = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)?;
    if !copy_extents(&input, &output, len)? {
        copy_range(&input, 0, &output, 0, len)?;
    }
    output.set_len(len)?;
    output.sync_all()?;
    Ok(())
}

/// Copy `len` bytes from `src` at `src_off` to `dst` at `dst_off`.
pub fn copy_range(
    src: &File,
    mut src_off: u64,
    dst: &File,
    mut dst_off: u64,
    mut len: u64,
) -> io::Result<()> {
    let mut buf = vec![0u8; CHUNK];
    while len > 0 {
        let want = CHUNK.min(len as usize);
        let got = src.read_at(&mut buf[..want], src_off)?;
        if got == 0 {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "copy ended before the requested length",
            ));
        }
        let mut wrote = 0;
        while wrote < got {
            let n = dst.write_at(&buf[wrote..got], dst_off)?;
            if n == 0 {
                return Err(io::Error::new(
                    ErrorKind::WriteZero,
                    "copy made no progress",
                ));
            }
            wrote += n;
            dst_off += n as u64;
        }
        src_off += got as u64;
        len -= got as u64;
    }
    Ok(())
}

/// Returns `Ok(false)` when the filesystem does not support data/hole seeks,
/// so the caller can fall back to a dense copy.
fn copy_extents(src: &File, dst: &File, len: u64) -> io::Result<bool> {
    if len == 0 {
        return Ok(true);
    }
    let mut offset = 0u64;
    loop {
        let data = match lseek(src, offset, libc::SEEK_DATA) {
            Ok(data) => data,
            Err(err) if unsupported(&err) && offset == 0 => return Ok(false),
            Err(err) if err.raw_os_error() == Some(libc::ENXIO) => break,
            Err(err) => return Err(err),
        };
        if data >= len {
            break;
        }
        let hole = match lseek(src, data, libc::SEEK_HOLE) {
            Ok(hole) => hole.min(len),
            Err(err) if err.raw_os_error() == Some(libc::ENXIO) => len,
            Err(err) if unsupported(&err) => len,
            Err(err) => return Err(err),
        };
        let end = hole.max(data);
        if end > data {
            copy_range(src, data, dst, data, end - data)?;
        }
        if end >= len {
            break;
        }
        // An empty extent would otherwise seek the same offset forever.
        offset = if end == data {
            data.saturating_add(1)
        } else {
            end
        };
    }
    Ok(true)
}

fn lseek(file: &File, offset: u64, whence: i32) -> io::Result<u64> {
    let rc = unsafe { libc::lseek(file.as_raw_fd(), offset as libc::off_t, whence) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc as u64)
    }
}

fn unsupported(err: &io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn sparse_copy_keeps_holes_and_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "mirrorfs-sparse-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("src");
        let dst = dir.join("dst");
        let len = 9 * 1024 * 1024;
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&src)
            .unwrap();
        file.write_at(b"HEAD", 0).unwrap();
        file.set_len(len).unwrap();
        file.write_at(b"TAIL", len - 4).unwrap();
        file.sync_all().unwrap();
        drop(file);

        copy_sparse(&src, &dst).unwrap();

        let out = File::open(&dst).unwrap();
        assert_eq!(out.metadata().unwrap().len(), len);
        let mut head = [0u8; 4];
        let mut tail = [0u8; 4];
        let mut hole = [0u8; 4];
        out.read_exact_at(&mut head, 0).unwrap();
        out.read_exact_at(&mut tail, len - 4).unwrap();
        out.read_exact_at(&mut hole, 1024 * 1024).unwrap();
        assert_eq!(&head, b"HEAD");
        assert_eq!(&tail, b"TAIL");
        assert_eq!(&hole, &[0, 0, 0, 0]);

        let src_blocks = std::fs::metadata(&src).unwrap().blocks();
        let dst_blocks = std::fs::metadata(&dst).unwrap().blocks();
        if src_blocks * 512 * 2 < len {
            assert!(
                dst_blocks * 512 * 2 < len,
                "dest allocated {} blocks for a {} byte sparse file",
                dst_blocks,
                len
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
