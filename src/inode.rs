use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::util;

/// Inode cache for one mount.
///
/// Fuse inode 1 is always the local root. Other fuse inodes prefer the local
/// `st_ino` so hard links share an identity. Records live until `forget`
/// drops the lookup count, except names only observed through `readdir`
/// (lookup count 0), which stay until unmount.
#[derive(Debug)]
pub struct InodeTable {
    by_ino: HashMap<u64, Inode>,
    by_rel: HashMap<PathBuf, u64>,
    by_local: HashMap<u64, u64>,
    next_synth: u64,
    next_gen: u64,
}

#[derive(Debug, Clone)]
pub struct Inode {
    pub rel: PathBuf,
    pub parent: u64,
    pub nlookup: u64,
    pub generation: u64,
    pub local_ino: u64,
}

impl InodeTable {
    pub fn new(root_local_ino: u64) -> Self {
        let mut table = Self {
            by_ino: HashMap::new(),
            by_rel: HashMap::new(),
            by_local: HashMap::new(),
            next_synth: 1 << 32,
            next_gen: 1,
        };
        table.by_ino.insert(
            1,
            Inode {
                rel: PathBuf::new(),
                parent: 1,
                nlookup: 1,
                generation: 0,
                local_ino: root_local_ino,
            },
        );
        table.by_rel.insert(PathBuf::new(), 1);
        table.by_local.insert(root_local_ino, 1);
        table
    }

    #[cfg(test)]
    pub fn get(&self, ino: u64) -> Option<&Inode> {
        self.by_ino.get(&ino)
    }

    pub fn rel(&self, ino: u64) -> Option<PathBuf> {
        self.by_ino.get(&ino).map(|inode| inode.rel.clone())
    }

    pub fn parent(&self, ino: u64) -> Option<u64> {
        self.by_ino.get(&ino).map(|inode| inode.parent)
    }

    pub fn generation(&self, ino: u64) -> u64 {
        self.by_ino
            .get(&ino)
            .map(|inode| inode.generation)
            .unwrap_or(0)
    }

    /// Remember `rel`. When `bump` is set this counts as a fuse lookup.
    pub fn intern(&mut self, rel: PathBuf, parent: u64, local_ino: u64, bump: bool) -> u64 {
        if let Some(&existing) = self.by_local.get(&local_ino) {
            self.by_rel.insert(rel, existing);
            if bump {
                if let Some(inode) = self.by_ino.get_mut(&existing) {
                    inode.nlookup = inode.nlookup.saturating_add(1);
                }
            }
            return existing;
        }
        if let Some(&existing) = self.by_rel.get(&rel) {
            if bump {
                if let Some(inode) = self.by_ino.get_mut(&existing) {
                    inode.nlookup = inode.nlookup.saturating_add(1);
                    inode.local_ino = local_ino;
                }
            }
            self.by_local.insert(local_ino, existing);
            return existing;
        }

        let fuse_ino = self.allocate(local_ino);
        let generation = self.next_gen;
        self.next_gen = self.next_gen.saturating_add(1);
        self.by_ino.insert(
            fuse_ino,
            Inode {
                rel: rel.clone(),
                parent,
                nlookup: if bump { 1 } else { 0 },
                generation,
                local_ino,
            },
        );
        self.by_rel.insert(rel, fuse_ino);
        self.by_local.insert(local_ino, fuse_ino);
        fuse_ino
    }

    pub fn bump(&mut self, ino: u64) {
        if let Some(inode) = self.by_ino.get_mut(&ino) {
            inode.nlookup = inode.nlookup.saturating_add(1);
        }
    }

    pub fn fuse_for_local(&self, local_ino: u64) -> Option<u64> {
        self.by_local.get(&local_ino).copied()
    }

    pub fn paths_for(&self, ino: u64) -> Vec<PathBuf> {
        self.by_rel
            .iter()
            .filter_map(|(path, id)| (*id == ino).then(|| path.clone()))
            .collect()
    }

    pub fn retarget(&mut self, ino: u64, rel: PathBuf) {
        if let Some(inode) = self.by_ino.get_mut(&ino) {
            inode.rel = rel;
        }
    }

    /// Drop directory entries for `ino` but keep the record while the kernel
    /// still holds a lookup. Otherwise a recycled `st_ino` would be treated
    /// as this fuse inode.
    pub fn detach(&mut self, ino: u64) {
        if ino == 1 {
            return;
        }
        let Some(inode) = self.by_ino.get(&ino) else {
            return;
        };
        let local_ino = inode.local_ino;
        let nlookup = inode.nlookup;
        if self.by_local.get(&local_ino) == Some(&ino) {
            self.by_local.remove(&local_ino);
        }
        for path in self.paths_for(ino) {
            self.by_rel.remove(&path);
        }
        if nlookup == 0 {
            self.by_ino.remove(&ino);
        }
    }

    /// Record a successful local rename. `to_parent` is the fuse parent of the
    /// destination. On exchange, `from_parent` is the fuse parent of `from`.
    pub fn rename(
        &mut self,
        from: &Path,
        to: &Path,
        to_parent: u64,
        exchange: bool,
        from_parent: u64,
    ) {
        if from.as_os_str().is_empty() || to.as_os_str().is_empty() || from == to {
            return;
        }
        if !exchange {
            self.drop_tree(to);
        }
        let snapshot: Vec<(PathBuf, u64)> = self
            .by_rel
            .iter()
            .map(|(path, ino)| (path.clone(), *ino))
            .collect();
        let mut planned = Vec::new();
        for (path, ino) in snapshot {
            if let Some(neu) = util::relocated_path(&path, from, to) {
                planned.push((path, neu, ino, to_parent));
            } else if exchange {
                if let Some(neu) = util::relocated_path(&path, to, from) {
                    planned.push((path, neu, ino, from_parent));
                }
            }
        }
        for (old, _, _, _) in &planned {
            self.by_rel.remove(old);
        }
        for (old, new, ino, parent) in planned {
            self.by_rel.insert(new.clone(), ino);
            let Some(inode) = self.by_ino.get_mut(&ino) else {
                continue;
            };
            if inode.rel == old {
                let moved_root = old == from || (exchange && old == to);
                inode.rel = new;
                if moved_root {
                    inode.parent = parent;
                }
            }
        }
    }

    fn drop_tree(&mut self, root: &Path) {
        let paths: Vec<PathBuf> = self
            .by_rel
            .keys()
            .filter(|path| *path == root || path.starts_with(root))
            .cloned()
            .collect();
        for path in paths {
            self.unbind(&path);
        }
    }

    /// Drop one directory entry. If it was the path used for getattr, switch
    /// to another hard link that is still cached. The last name also drops
    /// the `st_ino` map so the number can be reused safely.
    pub fn unbind(&mut self, rel: &Path) {
        let Some(ino) = self.by_rel.get(rel).copied() else {
            return;
        };
        self.by_rel.remove(rel);
        let (local_ino, canonical) = {
            let Some(inode) = self.by_ino.get(&ino) else {
                return;
            };
            (inode.local_ino, inode.rel.clone())
        };
        if let Some(other) = self.paths_for(ino).into_iter().next() {
            if canonical.as_path() == rel {
                self.retarget(ino, other);
            }
            return;
        }
        if self.by_local.get(&local_ino) == Some(&ino) {
            self.by_local.remove(&local_ino);
        }
    }

    pub fn forget(&mut self, ino: u64, nlookup: u64) {
        if ino == 1 {
            return;
        }
        let Some(inode) = self.by_ino.get_mut(&ino) else {
            return;
        };
        if inode.nlookup == 0 {
            return;
        }
        inode.nlookup = inode.nlookup.saturating_sub(nlookup);
        if inode.nlookup > 0 {
            return;
        }
        let local_ino = inode.local_ino;
        for path in self.paths_for(ino) {
            self.by_rel.remove(&path);
        }
        // A reused st_ino may already belong to a newer fuse inode.
        if self.by_local.get(&local_ino) == Some(&ino) {
            self.by_local.remove(&local_ino);
        }
        self.by_ino.remove(&ino);
    }

    fn allocate(&mut self, local_ino: u64) -> u64 {
        if local_ino > 1 && !self.by_ino.contains_key(&local_ino) {
            local_ino
        } else {
            let ino = self.next_synth;
            self.next_synth = self.next_synth.saturating_add(1);
            ino
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_inode_one() {
        let table = InodeTable::new(2);
        assert_eq!(table.rel(1).unwrap().as_os_str(), "");
        assert_eq!(table.parent(1), Some(1));
    }

    #[test]
    fn hard_links_share_a_fuse_inode() {
        let mut table = InodeTable::new(2);
        let a = table.intern(PathBuf::from("a"), 1, 10, true);
        let b = table.intern(PathBuf::from("b"), 1, 10, true);
        assert_eq!(a, b);
        assert_eq!(table.get(a).unwrap().nlookup, 2);
    }

    #[test]
    fn forget_drops_the_record() {
        let mut table = InodeTable::new(2);
        let ino = table.intern(PathBuf::from("a"), 1, 10, true);
        table.forget(ino, 1);
        assert!(table.get(ino).is_none());
        assert!(table.rel(ino).is_none());
    }

    #[test]
    fn readdir_entries_are_not_removed_by_a_zero_lookup_forget() {
        let mut table = InodeTable::new(2);
        let ino = table.intern(PathBuf::from("a"), 1, 10, false);
        table.forget(ino, 1);
        assert!(table.get(ino).is_some());
    }

    #[test]
    fn unbind_retargets_the_canonical_hard_link() {
        let mut table = InodeTable::new(2);
        let ino = table.intern(PathBuf::from("a"), 1, 10, true);
        table.intern(PathBuf::from("b"), 1, 10, true);
        table.unbind(Path::new("b"));
        assert_eq!(table.rel(ino).unwrap().as_os_str(), "a");
        table.intern(PathBuf::from("b"), 1, 10, true);
        table.unbind(Path::new("a"));
        assert_eq!(table.rel(ino).unwrap().as_os_str(), "b");
        assert!(table.get(ino).is_some());
        let again = table.intern(PathBuf::from("c"), 1, 10, true);
        assert_eq!(again, ino);
    }

    #[test]
    fn unbind_last_name_does_not_alias_a_reused_inode() {
        let mut table = InodeTable::new(2);
        let first = table.intern(PathBuf::from("a"), 1, 10, true);
        table.unbind(Path::new("a"));
        let second = table.intern(PathBuf::from("b"), 1, 10, true);
        assert_ne!(first, second);
        assert_eq!(table.rel(second).unwrap().as_os_str(), "b");
        table.forget(first, 1);
        let linked = table.intern(PathBuf::from("c"), 1, 10, true);
        assert_eq!(linked, second);
        assert_eq!(table.rel(first), None);
    }

    #[test]
    fn rename_moves_the_directory_and_its_children() {
        let mut table = InodeTable::new(2);
        let dir = table.intern(PathBuf::from("dir"), 1, 10, true);
        let child = table.intern(PathBuf::from("dir/a"), dir, 11, true);
        table.rename(Path::new("dir"), Path::new("other"), 1, false, 1);
        assert_eq!(table.rel(dir).unwrap().as_os_str(), "other");
        assert_eq!(table.parent(dir), Some(1));
        assert_eq!(table.rel(child).unwrap().as_os_str(), "other/a");
        assert_eq!(table.parent(child), Some(dir));
        assert!(table
            .paths_for(dir)
            .iter()
            .all(|path| path != Path::new("dir")));
    }

    #[test]
    fn rename_replaces_the_destination_cache() {
        let mut table = InodeTable::new(2);
        let src = table.intern(PathBuf::from("a"), 1, 10, true);
        let dst = table.intern(PathBuf::from("b"), 1, 11, true);
        table.rename(Path::new("a"), Path::new("b"), 1, false, 1);
        assert_eq!(table.rel(src).unwrap().as_os_str(), "b");
        assert!(table.paths_for(dst).is_empty());
    }

    #[test]
    fn exchange_swaps_two_trees() {
        let mut table = InodeTable::new(2);
        let left = table.intern(PathBuf::from("left"), 1, 10, true);
        let right_parent = 1;
        let right = table.intern(PathBuf::from("right"), right_parent, 11, true);
        let child = table.intern(PathBuf::from("left/a"), left, 12, true);
        table.rename(Path::new("left"), Path::new("right"), 1, true, 1);
        assert_eq!(table.rel(left).unwrap().as_os_str(), "right");
        assert_eq!(table.rel(right).unwrap().as_os_str(), "left");
        assert_eq!(table.rel(child).unwrap().as_os_str(), "right/a");
        assert_eq!(table.parent(child), Some(left));
    }
}
