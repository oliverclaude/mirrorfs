# mirrorfs implementation checklist

Each item is implemented, manually tested against `/mnt/claude_local` and
`/mnt/claude_remote`, then committed on its own.

- [x] Project skeleton, CLI, README, and this checklist
- [x] Mount, lookup, getattr, readdir, readlink, statfs, forget
- [x] Open, read, flush, release, opendir, releasedir
- [x] Local mutations (create, write, setattr, mkdir, mknod, unlink, rmdir, fsync) and a persistent FIFO under `.mirrorfs-queue/`
- [x] Background worker that applies the queue to the remote directory
- [x] Full local snapshot queued ahead of a mutation when the remote object is unknown
- [ ] xattr, rename, link, symlink, fallocate
- [x] Retry when the remote is temporarily unavailable; queue survives restart
- [ ] `--debug` logs path, operation, result, and whether it was queued
- [ ] `--allow-other` for root mounts that other users must access (flag is wired; non-root rejection tested; root confirmation deferred)
- [ ] SIGINT / SIGTERM unmounts and leaves the queue durable
- [ ] `cargo build --release` and final README pass
