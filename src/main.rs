mod fs;
mod inode;
mod queue;
mod util;

use std::io::{self, ErrorKind};
use std::mem;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::thread;

use clap::Parser;
use fuser::{Config, MountOption, Session, SessionACL};

#[derive(Parser, Debug)]
#[command(
    name = "mirrorfs",
    version,
    about = "Mirror local filesystem mutations asynchronously onto a remote directory"
)]
struct Args {
    /// Local directory. Reads and the accepted state come only from here.
    #[arg(long, value_name = "PATH")]
    local: PathBuf,

    /// Remote directory that receives asynchronous copies of mutations.
    #[arg(long, value_name = "PATH")]
    remote: PathBuf,

    /// Log every operation (path, op, result, whether it was queued).
    #[arg(short = 'd', long)]
    debug: bool,

    /// Allow users other than the mounter to access the filesystem.
    ///
    /// Mounting as root does not need extra fuse.conf changes. Mounting as a
    /// non-root user also needs `user_allow_other` in /etc/fuse.conf.
    #[arg(long)]
    allow_other: bool,

    /// Directory to mount on.
    mountpoint: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let paths = match prepare(&args) {
        Ok(paths) => paths,
        Err(err) => {
            eprintln!("mirrorfs: {err}");
            return ExitCode::from(1);
        }
    };

    block_term_signals();
    let mountpoint = paths.mountpoint.clone();
    spawn_unmount_on_signal(mountpoint.clone());

    let fs = match fs::MirrorFs::new(paths.local.clone(), paths.remote.clone(), args.debug) {
        Ok(fs) => fs,
        Err(err) => {
            eprintln!("mirrorfs: local {}: {err}", paths.local.display());
            return ExitCode::from(1);
        }
    };

    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName("mirrorfs".into()),
        MountOption::Subtype("mirrorfs".into()),
        MountOption::DefaultPermissions,
    ];
    config.acl = if args.allow_other {
        SessionACL::All
    } else {
        SessionACL::Owner
    };
    config.n_threads = Some(4);
    config.clone_fd = true;

    let session = match Session::new(fs, &mountpoint, &config) {
        Ok(session) => session,
        Err(err) => {
            eprintln!("mirrorfs: mount {}: {err}", mountpoint.display());
            return ExitCode::from(1);
        }
    };
    eprintln!(
        "mirrorfs: mounted {} local={} remote={} debug={} allow_other={}",
        mountpoint.display(),
        paths.local.display(),
        paths.remote.display(),
        args.debug,
        args.allow_other
    );
    match session.run() {
        Ok(()) => {
            eprintln!("mirrorfs: unmounted {}", mountpoint.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("mirrorfs: session ended: {err}");
            ExitCode::from(1)
        }
    }
}

fn block_term_signals() {
    unsafe {
        let mut set = mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

fn spawn_unmount_on_signal(mountpoint: PathBuf) {
    thread::Builder::new()
        .name("mirrorfs-signal".into())
        .spawn(move || {
            let mut set = unsafe { mem::zeroed() };
            unsafe {
                libc::sigemptyset(&mut set);
                libc::sigaddset(&mut set, libc::SIGINT);
                libc::sigaddset(&mut set, libc::SIGTERM);
                let mut sig = 0;
                if libc::sigwait(&set, &mut sig) != 0 {
                    return;
                }
            }
            eprintln!(
                "mirrorfs: signal received, unmounting {}",
                mountpoint.display()
            );
            let status = Command::new("fusermount3")
                .arg("-u")
                .arg(&mountpoint)
                .status();
            if !status.map(|s| s.success()).unwrap_or(false) {
                let _ = Command::new("umount").arg(&mountpoint).status();
            }
        })
        .expect("spawn signal thread");
}

struct Paths {
    local: PathBuf,
    remote: PathBuf,
    mountpoint: PathBuf,
}

fn prepare(args: &Args) -> io::Result<Paths> {
    let local = require_dir("--local", &args.local)?;
    let remote = require_dir("--remote", &args.remote)?;
    let mountpoint = require_dir("mountpoint", &args.mountpoint)?;
    if same_dir(&local, &remote) || same_dir(&local, &mountpoint) || same_dir(&remote, &mountpoint)
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "local, remote, and mountpoint must be three different directories",
        ));
    }
    Ok(Paths {
        local,
        remote,
        mountpoint,
    })
}

fn require_dir(label: &str, path: &Path) -> io::Result<PathBuf> {
    let canon = path
        .canonicalize()
        .map_err(|err| io::Error::new(err.kind(), format!("{label} {}: {err}", path.display())))?;
    if !canon.is_dir() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("{label} {} is not a directory", canon.display()),
        ));
    }
    Ok(canon)
}

fn same_dir(a: &Path, b: &Path) -> bool {
    a == b
}
