# mirrorfs

`mirrorfs` is a Linux FUSE filesystem. It shows one local directory and, in the
background, replays mutations made through the mount onto a second directory
(intended to be an NFS mount).

It is not a sync tool. It does not scan either tree, and it does not reconcile
files that already differ.

## Semantics

The local directory is the only source of truth.

- Reads (`read`, `readdir`, `getattr`, `lookup`, `readlink`, `statfs`, xattr
  reads) go only to the local directory. The remote directory is never
  consulted for a read, even when the local object is missing.
- Every mutation is applied to the local directory first. The FUSE call returns
  as soon as that local call succeeds, or returns the local error if it fails.
- The same mutation is then appended to a persistent FIFO queue and applied to
  the remote directory by one background thread.
- File handles opened through the mount refer only to local files. The remote
  directory is never held open.
- Metadata changes (`chmod`, `chown`, `utimens`, `setxattr`, and the rest of
  `setattr`) use the same path: local first, then queue.
- Out-of-band changes are not detected and are not queued. A lookup still
  reads the local directory, because that directory is the source of truth, so
  a file written directly under `--local` can show up on a later lookup.
  Nothing compares the two trees, and a file that exists only on the remote is
  never served.
- If a queued mutation needs a remote object that mirrorfs has not already
  created or copied, the worker first writes a full copy of the local object
  (file bytes or symlink target, mode, uid, gid, timestamps, and xattrs) and
  only then applies the mutation. Parent directories are created as needed so
  the copy has somewhere to land. Sibling files are not copied.

The queue is a hidden directory, `.mirrorfs-queue/`, inside the local
directory. That name is not listed or looked up through the mount, and it is
not copied to the remote. Each record is a file named with a monotonic
sequence number. Records are fsynced and renamed into place, and the queue
directory itself is fsynced, so a crash or restart keeps every operation that
had been accepted. The worker deletes a record only after the remote call
succeeds. A remote outage leaves the record in place and retries it with
backoff. Order is preserved: a later record is not applied before an earlier
one.

On `SIGINT`, `SIGTERM`, or unmount, mirrorfs stops accepting FUSE calls,
tries to finish the record it is currently applying, and leaves any remaining
records on disk for the next start.

## Build

Install a stable Rust toolchain (rustup is fine; it stays in the user
account) plus FUSE 3 headers:

```sh
# Debian / Ubuntu. These packages are the system dependencies.
sudo apt-get install libfuse3-dev fuse3 pkg-config build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

On the build host used for this repo, `libfuse3-dev`, `fuse3`, and `pkg-config`
were already installed. Rust lives in `~/.cargo` via rustup for the `claude`
user. No boot-time service or kernel setting is required.

```sh
cargo build --release
```

The binary is `target/release/mirrorfs`.

## Mount

```sh
mirrorfs --local /mnt/claude_local --remote /mnt/claude_remote /mnt/claude
```

| Option | Meaning |
| --- | --- |
| `--local PATH` | Local directory. Reads and the accepted state come from here. |
| `--remote PATH` | Directory that receives the asynchronous mirror, typically NFS. |
| `--debug`, `-d` | Log every operation: path, name, result, and whether it was queued. |
| `--allow-other` | Pass FUSE `allow_other`. |
| `MOUNTPOINT` | Existing empty directory to mount on. |

The process stays in the foreground. Unmount with `fusermount3 -u MOUNTPOINT`,
or send `SIGINT` / `SIGTERM`.

`--debug` writes lines like:

```text
mirrorfs op=write path=notes.txt result=ok queued=yes
```

### Mounting as root for other users

`allow_other` lets accounts other than the mounter use the filesystem. The
kernel still enforces each file's mode, owner, and group (`default_permissions`).

As root, `/etc/fuse.conf` does not need `user_allow_other`:

```sh
sudo target/release/mirrorfs \
  --local /mnt/claude_local \
  --remote /mnt/claude_remote \
  --allow-other \
  /mnt/claude
```

As a non-root user, `fusermount3` rejects `allow_other` unless
`/etc/fuse.conf` contains a line `user_allow_other`. This repo does not change
that file. Without the flag, the mount is private to the user who started it.

## Queue directory

Location: `<local>/.mirrorfs-queue/`.

| Entry | Role |
| --- | --- |
| `NNNNNNNNNNNNNNNNNNNN.op` | One queued operation. Lowest number is applied next. |
| `*.op.tmp` | A record still being written. Ignored until it is renamed. |
| `base.log` | Paths mirrorfs has already created or fully copied on the remote, so later mutations do not snapshot the file again. |

A base copy stores the local object's content and attributes inside its `.op`
file at the moment the mutation is queued. The worker does not re-read the
live local file for that copy, so a later mutation cannot change the bytes of
an earlier snapshot.

While the worker is retrying, new FUSE calls still complete against the local
directory and append to the queue.

## Known limitations

- One way only. Remote edits are not pulled back, and pre-existing differences
  are not bulk-copied. A pre-existing local file is copied only when a
  mutation through the mount touches it (or a parent directory must be created
  to hold such a copy).
- The first mutation of a file the remote does not yet have reads the whole
  local file into the queue. That copy is buffered in memory while it is
  written to `.mirrorfs-queue/`.
- Queue order is global, not per file. A retrying record holds up later
  records until it succeeds.
- `chown` to a uid other than the mounter needs root (or `CAP_CHOWN`). If it
  fails, the failure is retried, which stalls the queue until it can succeed.
- Extended attributes that the remote filesystem rejects with `ENOTSUP` are
  logged and skipped so one unsupported xattr cannot wedge the mirror.
- POSIX byte-range locks, `ioctl`, and `copy_file_range` are not implemented.
  The kernel falls back to plain reads and writes for copies.
- Inode records for names seen during the mount are cached until `forget` or
  unmount.
- The mount does not follow paths that escape the local root. `.mirrorfs-queue`
  is hidden, not a security boundary against someone who can already write the
  local directory directly.

## Status

The checked items in `TODO.md` are what currently works. Mutations are applied
to the local directory and appended to `.mirrorfs-queue/`. Those records stay
on disk across unmount and restart. The remote directory is not updated yet;
the next piece is the background worker that drains the queue.
