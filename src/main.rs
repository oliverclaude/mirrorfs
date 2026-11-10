mod fs;
mod inode;
mod queue;
mod sysfs;
mod util;
mod worker;

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

#[derive(Debug)]
struct Launch {
    local: PathBuf,
    remote: PathBuf,
    mountpoint: PathBuf,
    debug: bool,
    allow_other: bool,
    allow_root: bool,
    foreground: bool,
    /// VFS flags from `-o`, forwarded to fusermount3. `nodev` and `nosuid`
    /// must be forwarded: dropping them leaves a root mount able to honor
    /// device nodes and set-id bits.
    kernel_options: Vec<MountOption>,
}

fn main() -> ExitCode {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    // mount(8) reaches mirrorfs through mount.fuse3 as:
    // mirrorfs <local> <mountpoint> -o remote=PATH[,allow_other][,debug]
    if raw.iter().any(|arg| arg == "-o") {
        let launch = match fuse_launch(&raw) {
            Ok(launch) => launch,
            Err(err) => {
                eprintln!("mirrorfs: {err}");
                return ExitCode::from(1);
            }
        };
        let ready = if launch.foreground {
            None
        } else {
            match daemonize() {
                Ok(fd) => Some(fd),
                Err(err) => {
                    eprintln!("mirrorfs: {err}");
                    return ExitCode::from(1);
                }
            }
        };
        return serve(launch, ready);
    }

    let args = Args::parse();
    serve(
        Launch {
            local: args.local,
            remote: args.remote,
            mountpoint: args.mountpoint,
            debug: args.debug,
            allow_other: args.allow_other,
            allow_root: false,
            foreground: true,
            kernel_options: Vec::new(),
        },
        None,
    )
}

fn serve(mut launch: Launch, ready: Option<libc::c_int>) -> ExitCode {
    let paths = match prepare(&launch.local, &launch.remote, &launch.mountpoint) {
        Ok(paths) => paths,
        Err(err) => {
            eprintln!("mirrorfs: {err}");
            return ExitCode::from(1);
        }
    };

    block_term_signals();
    let mountpoint = paths.mountpoint.clone();
    spawn_unmount_on_signal(mountpoint.clone());

    let fs = match fs::MirrorFs::new(paths.local.clone(), paths.remote.clone(), launch.debug) {
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
    config.mount_options.append(&mut launch.kernel_options);
    config.acl = if launch.allow_other {
        SessionACL::All
    } else if launch.allow_root {
        SessionACL::RootAndOwner
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
    if ready.is_some() {
        redirect_daemon_stdio();
    }
    notify_ready(ready);
    eprintln!(
        "mirrorfs: mounted {} local={} remote={} debug={} allow_other={} allow_root={}",
        mountpoint.display(),
        paths.local.display(),
        paths.remote.display(),
        launch.debug,
        launch.allow_other,
        launch.allow_root
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

fn prepare(local: &Path, remote: &Path, mountpoint: &Path) -> io::Result<Paths> {
    let local = require_dir("--local", local)?;
    let remote = require_dir("--remote", remote)?;
    let mountpoint = require_dir("mountpoint", mountpoint)?;
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

/// Arguments produced by mount.fuse3: `mirrorfs <local> <mountpoint> -o <opts>`.
fn fuse_launch(args: &[String]) -> io::Result<Launch> {
    let mut positionals = Vec::new();
    let mut options = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "-o" {
            let Some(value) = args.get(index + 1) else {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "missing argument to -o",
                ));
            };
            options.extend(split_mount_options(value));
            index += 2;
            continue;
        }
        if arg == "-t" {
            index += 2;
            continue;
        }
        if matches!(arg.as_str(), "-n" | "-v" | "-s" | "-f") {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("unknown argument {arg}"),
            ));
        }
        positionals.push(arg.clone());
        index += 1;
    }
    if positionals.len() != 2 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "expected <local-directory> <mountpoint> -o remote=PATH",
        ));
    }

    let mut remote = None;
    let mut debug = false;
    let mut allow_other = false;
    let mut allow_root = false;
    let mut foreground = false;
    let mut kernel_options = Vec::new();
    for opt in options {
        if opt.is_empty() || ignored_mount_option(&opt) || opt == "default_permissions" {
            continue;
        }
        if let Some(value) = opt.strip_prefix("remote=") {
            if value.is_empty() {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "remote= path is empty",
                ));
            }
            remote = Some(PathBuf::from(value));
        } else if opt == "allow_other" {
            allow_other = true;
        } else if opt == "allow_root" {
            allow_root = true;
        } else if opt == "debug" {
            debug = true;
        } else if opt == "foreground" {
            foreground = true;
        } else if opt == "auto_unmount" {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "auto_unmount is not supported",
            ));
        } else if let Some(kernel) = kernel_mount_option(&opt) {
            kernel_options.push(kernel);
        } else {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("unknown mount option {opt}"),
            ));
        }
    }
    let Some(remote) = remote else {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "mount options must include remote=PATH",
        ));
    };
    Ok(Launch {
        local: PathBuf::from(&positionals[0]),
        remote,
        mountpoint: PathBuf::from(&positionals[1]),
        debug,
        allow_other,
        allow_root,
        foreground,
        kernel_options,
    })
}

fn kernel_mount_option(opt: &str) -> Option<MountOption> {
    match opt {
        "rw" => Some(MountOption::RW),
        "ro" => Some(MountOption::RO),
        "exec" => Some(MountOption::Exec),
        "noexec" => Some(MountOption::NoExec),
        "suid" => Some(MountOption::Suid),
        "nosuid" => Some(MountOption::NoSuid),
        "dev" => Some(MountOption::Dev),
        "nodev" => Some(MountOption::NoDev),
        "atime" => Some(MountOption::Atime),
        "noatime" => Some(MountOption::NoAtime),
        "dirsync" => Some(MountOption::DirSync),
        "sync" => Some(MountOption::Sync),
        "async" => Some(MountOption::Async),
        "relatime" | "strictatime" | "lazytime" => Some(MountOption::CUSTOM(opt.to_string())),
        _ => None,
    }
}

fn split_mount_options(value: &str) -> Vec<String> {
    value.split(',').map(str::to_string).collect()
}

fn ignored_mount_option(opt: &str) -> bool {
    matches!(
        opt,
        "defaults"
            | "auto"
            | "noauto"
            | "user"
            | "users"
            | "nouser"
            | "nofail"
            | "_netdev"
            | "comment"
    ) || opt.starts_with("x-")
        || opt.starts_with("comment=")
}

/// Fork a daemon. The parent waits until the child has mounted, then exits.
/// The child returns the write end of the ready pipe.
fn daemonize() -> io::Result<libc::c_int> {
    let mut pipes = [0; 2];
    // CLOEXEC so fusermount3 does not inherit the ready pipe and hold it open.
    if unsafe { libc::pipe2(pipes.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(pipes[0]);
            libc::close(pipes[1]);
        }
        return Err(err);
    }
    if pid > 0 {
        unsafe { libc::close(pipes[1]) };
        let code = wait_until_mounted(pid, pipes[0]);
        std::process::exit(code);
    }
    unsafe { libc::close(pipes[0]) };
    if unsafe { libc::setsid() } < 0 {
        let err = io::Error::last_os_error();
        unsafe { libc::close(pipes[1]) };
        return Err(err);
    }
    // The caller that started us (mount, a boot script) may close its end of
    // the pipe. A later log write must not die with SIGPIPE.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
    Ok(pipes[1])
}

fn wait_until_mounted(pid: libc::pid_t, fd: libc::c_int) -> i32 {
    let mut buf = [0u8; 1];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), 1) };
        if n > 0 {
            unsafe { libc::close(fd) };
            return 0;
        }
        if n == 0 {
            unsafe { libc::close(fd) };
            return reap_exit_code(pid);
        }
        if io::Error::last_os_error().kind() != ErrorKind::Interrupted {
            unsafe { libc::close(fd) };
            return 1;
        }
    }
}

fn reap_exit_code(pid: libc::pid_t) -> i32 {
    let mut status = 0;
    loop {
        let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == ErrorKind::Interrupted {
                continue;
            }
            return 1;
        }
        break;
    }
    if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        1
    }
}

fn notify_ready(fd: Option<libc::c_int>) {
    let Some(fd) = fd else {
        return;
    };
    let byte = [b'1'];
    let _ = unsafe { libc::write(fd, byte.as_ptr().cast(), 1) };
    unsafe { libc::close(fd) };
}

fn redirect_daemon_stdio() {
    let devnull = c_path("/dev/null");
    let stdin = unsafe { libc::open(devnull.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if stdin >= 0 {
        unsafe {
            libc::dup2(stdin, 0);
            libc::close(stdin);
        }
    }
    let log = c_path("/var/log/mirrorfs.log");
    let mut out = unsafe {
        libc::open(
            log.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND | libc::O_CLOEXEC,
            0o644,
        )
    };
    if out < 0 {
        out = unsafe { libc::open(devnull.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC) };
    }
    if out >= 0 {
        unsafe {
            libc::dup2(out, 1);
            libc::dup2(out, 2);
            libc::close(out);
        }
    }
}

fn c_path(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path.as_bytes()).expect("path has no interior NUL")
}

#[cfg(test)]
mod tests {
    use super::{fuse_launch, MountOption};

    fn launch(args: &[&str]) -> super::Launch {
        let owned: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        fuse_launch(&owned).unwrap()
    }

    #[test]
    fn fstab_options_select_the_remote_and_flags() {
        let got = launch(&[
            "/mnt/claude_local",
            "/mnt/claude",
            "-o",
            "remote=/mnt/claude_remote,allow_other,debug,_netdev,nodev,nosuid,x-systemd.requires-mounts-for=/mnt/claude_remote",
        ]);
        assert_eq!(got.local, std::path::PathBuf::from("/mnt/claude_local"));
        assert_eq!(got.remote, std::path::PathBuf::from("/mnt/claude_remote"));
        assert_eq!(got.mountpoint, std::path::PathBuf::from("/mnt/claude"));
        assert!(got.allow_other);
        assert!(got.debug);
        assert!(!got.foreground);
        assert!(got.kernel_options.contains(&MountOption::NoDev));
        assert!(got.kernel_options.contains(&MountOption::NoSuid));
        assert!(got
            .kernel_options
            .iter()
            .all(|opt| !matches!(opt, MountOption::CUSTOM(value) if value.starts_with("x-"))));
    }

    #[test]
    fn fstab_foreground_stays_in_the_foreground() {
        let got = launch(&[
            "/data/local",
            "/mnt/mirror",
            "-o",
            "remote=/data/remote,foreground",
        ]);
        assert!(got.foreground);
        assert!(!got.allow_other);
        assert!(!got.allow_root);
        assert!(got.kernel_options.is_empty());
    }

    #[test]
    fn fstab_forwards_read_only_and_allow_root() {
        let got = launch(&[
            "/data/local",
            "/mnt/mirror",
            "-o",
            "remote=/data/remote,ro,allow_root,noexec",
        ]);
        assert!(got.allow_root);
        assert!(!got.allow_other);
        assert_eq!(
            got.kernel_options,
            vec![MountOption::RO, MountOption::NoExec]
        );
    }

    #[test]
    fn fstab_requires_a_remote_path() {
        let owned = vec![
            "/mnt/claude_local".to_string(),
            "/mnt/claude".to_string(),
            "-o".to_string(),
            "allow_other".to_string(),
        ];
        let err = fuse_launch(&owned).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn fstab_rejects_an_unknown_option() {
        let owned = vec![
            "/mnt/claude_local".to_string(),
            "/mnt/claude".to_string(),
            "-o".to_string(),
            "remote=/mnt/claude_remote,subdir=/tmp".to_string(),
        ];
        let err = fuse_launch(&owned).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("subdir="));
    }

    #[test]
    fn fstab_rejects_auto_unmount() {
        let owned = vec![
            "/mnt/claude_local".to_string(),
            "/mnt/claude".to_string(),
            "-o".to_string(),
            "remote=/mnt/claude_remote,auto_unmount".to_string(),
        ];
        let err = fuse_launch(&owned).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("auto_unmount"));
    }
}
