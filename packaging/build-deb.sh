#!/bin/sh
# Build dist/mirrorfs_<version>_<arch>.deb from a release binary.
# Runtime dependencies come from dpkg-shlibdeps, plus fuse3 for fusermount3.
set -eu
umask 022

cd "$(dirname "$0")/.."

for tool in cargo dpkg dpkg-deb dpkg-shlibdeps install du gzip; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "mirrorfs: $tool is required to build the package" >&2
        exit 1
    fi
done

version=$(awk -F '"' '/^version = / { print $2; exit }' Cargo.toml)
if [ -z "$version" ]; then
    echo "mirrorfs: version missing from Cargo.toml" >&2
    exit 1
fi
revision=2
arch=$(dpkg --print-architecture)
pkgver="${version}-${revision}"
debname="mirrorfs_${pkgver}_${arch}.deb"

cargo_offline=
if [ "${MIRRORFS_CARGO_OFFLINE:-}" = 1 ]; then
    cargo_offline=--offline
fi
# shellcheck disable=SC2086
cargo build --release --locked $cargo_offline

stage=$(mktemp -d)
shlibdir=$(mktemp -d)
cleanup() {
    rm -rf "$stage" "$shlibdir"
}
trap cleanup EXIT
chmod 0755 "$stage"

mkdir -p "$stage/usr/bin" "$stage/usr/share/doc/mirrorfs" \
    "$stage/usr/share/man/man1" "$stage/usr/share/man/man8" "$stage/DEBIAN"
install -m 0755 target/release/mirrorfs "$stage/usr/bin/mirrorfs"
install -m 0644 README.md "$stage/usr/share/doc/mirrorfs/README.md"
gzip -n -c man/mirrorfs.1 > "$stage/usr/share/man/man1/mirrorfs.1.gz"
gzip -n -c man/fuse.mirrorfs.8 > "$stage/usr/share/man/man8/fuse.mirrorfs.8.gz"
chmod 0644 "$stage/usr/share/man/man1/mirrorfs.1.gz" \
    "$stage/usr/share/man/man8/fuse.mirrorfs.8.gz"
ln -s fuse.mirrorfs.8.gz "$stage/usr/share/man/man8/mount.fuse.mirrorfs.8.gz"
cat > "$stage/usr/share/doc/mirrorfs/copyright" << 'EOF'
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: mirrorfs
Source: https://github.com/oliverclaude/mirrorfs

Files: *
Copyright: 2026 Claude Oliver <oliver.claude@gmail.com>
License: MIT
EOF

mkdir -p "$shlibdir/debian"
cat > "$shlibdir/debian/control" << EOF
Source: mirrorfs
Maintainer: Claude Oliver <oliver.claude@gmail.com>

Package: mirrorfs
Architecture: $arch
EOF
(
    cd "$shlibdir"
    dpkg-shlibdeps -Tdebian/substvars "$stage/usr/bin/mirrorfs"
)
shlibs=$(sed -n 's/^shlibs:Depends=//p' "$shlibdir/debian/substvars")
if [ -z "$shlibs" ]; then
    echo "mirrorfs: dpkg-shlibdeps produced no library dependencies" >&2
    exit 1
fi
# fusermount3 is executed by name. It is not a shared library, so shlibdeps
# does not see it. The fuse3 package provides that program and /dev/fuse.
depends="$shlibs, fuse3"

size=$(du -sk "$stage/usr" | awk '{ print $1 }')
cat > "$stage/DEBIAN/control" << EOF
Package: mirrorfs
Version: $pkgver
Section: utils
Priority: optional
Architecture: $arch
Maintainer: Claude Oliver <oliver.claude@gmail.com>
Depends: $depends
Installed-Size: $size
Homepage: https://github.com/oliverclaude/mirrorfs
Description: mirror local filesystem mutations onto a remote directory
 mirrorfs is a FUSE filesystem. The local directory is the source of truth.
 Mutations are applied locally, then replayed asynchronously onto a remote
 directory. Reads never consult the remote.
EOF

mkdir -p dist
rm -f "dist/$debname"
dpkg-deb --root-owner-group --build "$stage" "dist/$debname"
echo "dist/$debname"
