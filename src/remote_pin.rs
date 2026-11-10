//! Decide when a remote directory is the filesystem the operator asked for,
//! and pin a directory fd to that mount.
//!
//! Copies go through `/proc/self/fd/N/...` while the fd is held. After the
//! remote filesystem is unmounted, those operations fail on the old mount
//! instead of writing into the local directory that the path then names.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How often the check thread looks for `remote-fs` when the caller does not
/// set `remote-check`.
pub const DEFAULT_CHECK: Duration = Duration::from_secs(5 * 60);

pub struct RemoteWatch {
    pub fs_types: Vec<String>,
    pub every: Duration,
}

pub struct MountEntry {
    pub dev: String,
    pub mountpoint: PathBuf,
    pub fstype: String,
}

/// `remote-fs` values. `:` separates several types inside one option so the
/// list survives the comma-separated fstab option string. Repeated options
/// are concatenated.
pub fn split_fs_types(values: &[String]) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for value in values {
        for part in value.split(':') {
            if part.is_empty() || part.chars().any(char::is_whitespace) {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "remote-fs type is empty",
                ));
            }
            if out
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(part))
            {
                continue;
            }
            out.push(part.to_string());
        }
    }
    Ok(out)
}

pub fn watch(types: Vec<String>, check: Option<Duration>) -> io::Result<Option<RemoteWatch>> {
    if types.is_empty() {
        if check.is_some() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "remote-check requires remote-fs",
            ));
        }
        return Ok(None);
    }
    Ok(Some(RemoteWatch {
        fs_types: types,
        every: check.unwrap_or(DEFAULT_CHECK),
    }))
}

pub fn parse_check_interval(text: &str) -> io::Result<Duration> {
    let text = text.trim();
    let (number, unit) = if let Some(rest) = text.strip_suffix("ms") {
        (rest, "ms")
    } else if let Some(rest) = text.strip_suffix('s') {
        (rest, "s")
    } else if let Some(rest) = text.strip_suffix('m') {
        (rest, "m")
    } else if let Some(rest) = text.strip_suffix('h') {
        (rest, "h")
    } else {
        (text, "s")
    };
    let number: u64 = number.parse().map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("remote-check duration {text} is not a number"),
        )
    })?;
    if number == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "remote-check duration is zero",
        ));
    }
    let duration = match unit {
        "ms" => Duration::from_millis(number),
        "s" => Duration::from_secs(number),
        "m" => Duration::from_secs(number.saturating_mul(60)),
        "h" => Duration::from_secs(number.saturating_mul(60 * 60)),
        _ => unreachable!(),
    };
    Ok(duration)
}

pub fn format_interval(duration: Duration) -> String {
    let secs = duration.as_secs();
    let millis = duration.as_millis();
    if millis % 1000 != 0 {
        return format!("{millis}ms");
    }
    if secs % 3600 == 0 {
        return format!("{}h", secs / 3600);
    }
    if secs % 60 == 0 && secs >= 60 {
        return format!("{}m", secs / 60);
    }
    format!("{secs}s")
}

/// `nfs` in fstab covers the kernel types `nfs` and `nfs4`. Every other name
/// must match the type `findmnt -n -o FSTYPE` prints.
pub fn type_matches(wanted: &str, actual: &str) -> bool {
    if wanted.eq_ignore_ascii_case(actual) {
        return true;
    }
    wanted.eq_ignore_ascii_case("nfs") && actual.eq_ignore_ascii_case("nfs4")
}

pub fn accepts(wanted: &[String], actual: &str) -> bool {
    wanted.iter().any(|name| type_matches(name, actual))
}

pub fn parse_mountinfo(text: &str) -> Vec<MountEntry> {
    let mut mounts = Vec::new();
    for line in text.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        // mountinfo: id parent major:minor root mountpoint options...
        let mut left_fields = left.split_whitespace();
        let dev = match (
            left_fields.next(),
            left_fields.next(),
            left_fields.next(),
            left_fields.next(),
        ) {
            (Some(_id), Some(_parent), Some(dev), Some(_root)) => dev.to_string(),
            _ => continue,
        };
        let Some(mountpoint) = left_fields.next() else {
            continue;
        };
        let Some(fstype) = right.split_whitespace().next() else {
            continue;
        };
        mounts.push(MountEntry {
            dev,
            mountpoint: unescape_path(mountpoint),
            fstype: unescape_text(fstype),
        });
    }
    mounts
}

pub fn covering<'a>(mounts: &'a [MountEntry], path: &Path) -> Option<&'a MountEntry> {
    mounts
        .iter()
        .filter(|entry| path == entry.mountpoint || path.starts_with(&entry.mountpoint))
        .max_by_key(|entry| entry.mountpoint.as_os_str().len())
}

pub fn fstype_of(path: &Path) -> io::Result<String> {
    let text = fs::read_to_string("/proc/self/mountinfo")?;
    let mounts = parse_mountinfo(&text);
    let canon = path.canonicalize()?;
    covering(&mounts, &canon)
        .map(|entry| entry.fstype.clone())
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("no mount covers {}", path.display()),
            )
        })
}

/// Open `path` only when the directory fd sits on one of `types`.
/// `Ok(None)` means the path is some other filesystem.
pub fn pin_if_match(path: &Path, types: &[String]) -> io::Result<Option<File>> {
    let actual = fstype_of(path)?;
    if !accepts(types, &actual) {
        return Ok(None);
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let dev = file.metadata()?.dev();
    let pinned = fstype_of_dev(dev)?;
    if !accepts(types, &pinned) {
        return Ok(None);
    }
    Ok(Some(file))
}

pub fn fstype_of_dev(dev: u64) -> io::Result<String> {
    let id = format!("{}:{}", libc::major(dev), libc::minor(dev));
    let text = fs::read_to_string("/proc/self/mountinfo")?;
    parse_mountinfo(&text)
        .into_iter()
        .find(|entry| entry.dev == id)
        .map(|entry| entry.fstype)
        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, format!("no mount has device {id}")))
}

fn unescape_path(text: &str) -> PathBuf {
    PathBuf::from(OsStr::from_bytes(&unescape_bytes(text)))
}

fn unescape_text(text: &str) -> String {
    String::from_utf8_lossy(&unescape_bytes(text)).into_owned()
}

fn unescape_bytes(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 3 < bytes.len() {
            if let Ok(value) = u8::from_str_radix(
                std::str::from_utf8(&bytes[index + 1..index + 4]).unwrap_or(""),
                8,
            ) {
                out.push(value);
                index += 4;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_use_the_documented_units() {
        assert_eq!(
            parse_check_interval("5m").unwrap(),
            Duration::from_secs(300)
        );
        assert_eq!(
            parse_check_interval("30s").unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_check_interval("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_check_interval("2h").unwrap(),
            Duration::from_secs(7200)
        );
        assert_eq!(parse_check_interval("15").unwrap(), Duration::from_secs(15));
        assert!(parse_check_interval("0s").is_err());
        assert!(parse_check_interval("5x").is_err());
        assert_eq!(format_interval(DEFAULT_CHECK), "5m");
    }

    #[test]
    fn nfs_covers_the_nfs4_kernel_type() {
        assert!(type_matches("nfs", "nfs4"));
        assert!(type_matches("NFS", "nfs"));
        assert!(type_matches("nfs4", "nfs4"));
        assert!(!type_matches("nfs4", "nfs"));
        assert!(!type_matches("cifs", "nfs4"));
    }

    #[test]
    fn mountinfo_picks_the_longest_covering_mount() {
        let text = "\
36 35 98:0 / /mnt/data rw,relatime - ext4 /dev/sda1 rw
40 36 0:45 / /mnt/data/nfs rw - nfs4 remote:/export rw
41 36 0:46 / /mnt/data/nfs\\040share rw - cifs //server/share rw
";
        let mounts = parse_mountinfo(text);
        let nfs = covering(&mounts, Path::new("/mnt/data/nfs/sub/dir")).unwrap();
        assert_eq!(nfs.mountpoint, Path::new("/mnt/data/nfs"));
        assert_eq!(nfs.fstype, "nfs4");
        assert_eq!(nfs.dev, "0:45");
        let ext4 = covering(&mounts, Path::new("/mnt/data/other")).unwrap();
        assert_eq!(ext4.fstype, "ext4");
        let cifs = covering(&mounts, Path::new("/mnt/data/nfs share/folder")).unwrap();
        assert_eq!(cifs.fstype, "cifs");
    }

    #[test]
    fn a_subdirectory_pins_only_when_its_filesystem_matches() {
        let root = std::env::temp_dir().join(format!(
            "mirrorfs-pin-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let dir = root.join("nested").join("mirror");
        fs::create_dir_all(&dir).unwrap();
        let actual = fstype_of(&dir).unwrap();
        assert!(pin_if_match(&dir, &["not-a-filesystem".to_string()])
            .unwrap()
            .is_none());
        let file = pin_if_match(&dir, &[actual]).unwrap().expect("pin");
        let pinned = PathBuf::from(format!(
            "/proc/self/fd/{}",
            std::os::unix::io::AsRawFd::as_raw_fd(&file)
        ));
        fs::write(pinned.join("hello.txt"), b"via-fd").unwrap();
        assert_eq!(fs::read(dir.join("hello.txt")).unwrap(), b"via-fd");
        drop(file);
        let _ = fs::remove_dir_all(&root);
    }
}
