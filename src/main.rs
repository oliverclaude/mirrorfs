use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;

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
    match prepare(&args) {
        Ok(paths) => {
            eprintln!(
                "mirrorfs: arguments ok (local={}, remote={}, mount={}, debug={}, allow_other={}); filesystem is not mounted yet",
                paths.local.display(),
                paths.remote.display(),
                paths.mountpoint.display(),
                args.debug,
                args.allow_other
            );
            ExitCode::from(2)
        }
        Err(err) => {
            eprintln!("mirrorfs: {err}");
            ExitCode::from(1)
        }
    }
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
    let canon = path.canonicalize().map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("{label} {}: {err}", path.display()),
        )
    })?;
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
