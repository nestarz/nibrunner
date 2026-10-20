use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{Cursor, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::{invalid, Error, MAX_ENTRIES};

type Attributes = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Clone)]
struct Node {
    header: tar::Header,
    attributes: Attributes,
    kind: Arc<Kind>,
}

enum Kind {
    Directory,
    File(Vec<u8>),
    Symlink(PathBuf),
    Hardlink(String),
}

fn directory() -> Node {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_mode(0o755);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(0);
    Node {
        header,
        attributes: Vec::new(),
        kind: Arc::new(Kind::Directory),
    }
}

pub(crate) fn path(path: &Path) -> Result<String, Error> {
    let mut names = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(name) => {
                names.push(name.to_str().ok_or_else(|| invalid("a path is not UTF-8"))?)
            }
            _ => {
                return Err(invalid(
                    "an archive path is absolute or contains a parent component",
                ))
            }
        }
    }
    Ok(names.join("/"))
}

pub(crate) struct Filesystem {
    nodes: BTreeMap<String, Node>,
}

impl Filesystem {
    pub(crate) fn new() -> Self {
        Self {
            nodes: BTreeMap::from([(String::new(), directory())]),
        }
    }

    fn resolve(&self, path: &str, follow_final: bool) -> Result<String, Error> {
        let mut pending: VecDeque<String> = path.split('/').map(str::to_owned).collect();
        let mut resolved = Vec::new();
        let mut links = 0;
        while let Some(name) = pending.pop_front() {
            match name.as_str() {
                "" | "." => continue,
                ".." => {
                    resolved.pop();
                    continue;
                }
                _ => {}
            }
            resolved.push(name);
            if let Some(node) = self.nodes.get(&resolved.join("/")) {
                match node.kind.as_ref() {
                    Kind::Symlink(target) if !pending.is_empty() || follow_final => {
                        links += 1;
                        if links > 40 {
                            return Err(invalid("an image path has too many symbolic links"));
                        }
                        resolved.pop();
                        if target.is_absolute() {
                            resolved.clear();
                        }
                        let target = target
                            .to_str()
                            .ok_or_else(|| invalid("a symbolic link is not UTF-8"))?;
                        for component in target.split('/').rev() {
                            pending.push_front(component.to_owned());
                        }
                    }
                    Kind::Directory => {}
                    _ if !pending.is_empty() => {
                        return Err(invalid("an image path traverses a non-directory"))
                    }
                    _ => {}
                }
            }
        }
        Ok(resolved.join("/"))
    }

    fn remove(&mut self, path: &str, include_self: bool) {
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        self.nodes.retain(|name, _| {
            !((include_self && name == path) || (name != path && name.starts_with(&prefix)))
        });
    }

    fn parents(&mut self, path: &str) -> Result<(), Error> {
        let mut parent = String::new();
        let components: Vec<_> = path.split('/').collect();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            if !parent.is_empty() {
                parent.push('/');
            }
            parent.push_str(component);
            match self.nodes.get(&parent) {
                Some(node) if !matches!(node.kind.as_ref(), Kind::Directory) => {
                    return Err(invalid("an entry's parent is not a directory"))
                }
                None => {
                    self.nodes.insert(parent.clone(), directory());
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn apply(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let mut entries = Vec::new();
        let mut seen = BTreeSet::new();
        for entry in tar::Archive::new(Cursor::new(bytes)).entries()? {
            if entries.len() >= MAX_ENTRIES {
                return Err(invalid("a layer has too many entries"));
            }
            let mut entry = entry?;
            let name = path(&entry.path()?)?;
            if !seen.insert(name.clone()) {
                return Err(invalid("a layer repeats an entry path"));
            }
            let mut attributes = Vec::new();
            if let Some(extensions) = entry.pax_extensions()? {
                for extension in extensions {
                    let extension = extension?;
                    let key = extension.key_bytes();
                    if key.starts_with(b"LIBARCHIVE.xattr.") {
                        return Err(invalid("LIBARCHIVE extended attributes are unsupported"));
                    }
                    if key.starts_with(b"SCHILY.xattr.") {
                        attributes.push((key.to_vec(), extension.value_bytes().to_vec()));
                    }
                }
            }
            let kind = entry.header().entry_type();
            let kind = if kind.is_dir() {
                Kind::Directory
            } else if kind.is_file() {
                let mut data = Vec::new();
                entry.read_to_end(&mut data)?;
                Kind::File(data)
            } else if kind.is_symlink() {
                Kind::Symlink(
                    entry
                        .link_name()?
                        .ok_or_else(|| invalid("a symbolic link has no target"))?
                        .into_owned(),
                )
            } else if kind.is_hard_link() {
                Kind::Hardlink(path(
                    &entry
                        .link_name()?
                        .ok_or_else(|| invalid("a hard link has no target"))?,
                )?)
            } else {
                return Err(invalid(format!(
                    "entry {name} has an unsupported device, FIFO, socket or sparse type"
                )));
            };
            entries.push((
                name,
                Node {
                    header: entry.header().clone(),
                    attributes,
                    kind: Arc::new(kind),
                },
            ));
        }
        // Whiteouts affect the preceding filesystem, irrespective of their order within this layer.
        let mut removals = Vec::new();
        for (name, node) in &entries {
            let (parent, basename) = name.rsplit_once('/').unwrap_or(("", name.as_str()));
            if let Some(deleted) = basename.strip_prefix(".wh.") {
                if !matches!(node.kind.as_ref(), Kind::File(data) if data.is_empty()) || deleted.is_empty() {
                    return Err(invalid("a whiteout must be an empty regular file with a target"));
                }
                let parent = self.resolve(parent, true)?;
                if basename == ".wh..wh..opq" {
                    removals.push((parent, false));
                } else {
                    if deleted == "." || deleted == ".." {
                        return Err(invalid("a whiteout targets a directory parent"));
                    }
                    let target = if parent.is_empty() {
                        deleted.to_owned()
                    } else {
                        format!("{parent}/{deleted}")
                    };
                    removals.push((target, true));
                }
            }
        }
        for (target, include_self) in removals {
            self.remove(&target, include_self);
        }
        for (name, mut node) in entries {
            if name
                .rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with(".wh."))
            {
                continue;
            }
            let name = self.resolve(&name, false)?;
            if name.is_empty() && !matches!(node.kind.as_ref(), Kind::Directory) {
                return Err(invalid("the filesystem root is not a directory"));
            }
            if let Kind::Hardlink(target) = node.kind.as_ref() {
                let target = self.resolve(target, false)?;
                if let Some(existing) = self.nodes.get(&target) {
                    if matches!(existing.kind.as_ref(), Kind::Directory) {
                        return Err(invalid("a hard link targets a directory"));
                    }
                    node = existing.clone();
                } else {
                    node.kind = Arc::new(Kind::Hardlink(target));
                }
            }
            let merging_directories = matches!(node.kind.as_ref(), Kind::Directory)
                && self
                    .nodes
                    .get(&name)
                    .is_some_and(|old| matches!(old.kind.as_ref(), Kind::Directory));
            if !merging_directories {
                self.remove(&name, true);
            }
            self.parents(&name)?;
            self.nodes.insert(name, node);
            if self.nodes.len() > MAX_ENTRIES {
                return Err(invalid("the filesystem has too many entries"));
            }
        }
        for _ in 0..40 {
            let pending: Vec<_> = self
                .nodes
                .iter()
                .filter_map(|(name, node)| {
                    if let Kind::Hardlink(target) = node.kind.as_ref() {
                        Some((name.clone(), target.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            if pending.is_empty() {
                return Ok(());
            }
            let mut resolved = false;
            for (name, target) in pending {
                if let Some(node) = self
                    .nodes
                    .get(&target)
                    .filter(|node| !matches!(node.kind.as_ref(), Kind::Hardlink(_)))
                {
                    if matches!(node.kind.as_ref(), Kind::Directory) {
                        return Err(invalid("a hard link targets a directory"));
                    }
                    self.nodes.insert(name, node.clone());
                    resolved = true;
                }
            }
            if !resolved {
                break;
            }
        }
        Err(invalid("a layer has unresolved or cyclic hard links"))
    }

    pub(crate) fn finish(self) -> Result<crate::Filesystem, Error> {
        let mut archive = tar::Builder::new(Vec::new());
        let mut links = BTreeMap::new();
        let mut data_bytes = 0;
        let mut ordered: Vec<_> = self.nodes.iter().collect();
        ordered.sort_by_key(|(_, node)| !matches!(node.kind.as_ref(), Kind::Directory));
        for (name, node) in ordered {
            let name = if name.is_empty() { "." } else { name.as_str() };
            let mut header = node.header.clone();
            // USTAR retains its prefix when the tar writer replaces a path shorter than 100 bytes.
            if let Some(ustar) = header.as_ustar_mut() {
                ustar.prefix.fill(0);
            }
            if !node.attributes.is_empty() {
                archive.append_pax_extensions(
                    node.attributes
                        .iter()
                        .map(|(key, value)| {
                            Ok((
                                std::str::from_utf8(key).map_err(std::io::Error::other)?,
                                value.as_slice(),
                            ))
                        })
                        .collect::<std::io::Result<Vec<_>>>()?,
                )?;
            }
            let identity = Arc::as_ptr(&node.kind) as usize;
            if let Some(target) = links
                .get(&identity)
                .filter(|_| !matches!(node.kind.as_ref(), Kind::Directory))
            {
                header.set_entry_type(tar::EntryType::Link);
                header.set_size(0);
                archive.append_link(&mut header, name, target)?;
                continue;
            }
            match node.kind.as_ref() {
                Kind::Directory => {
                    header.set_size(0);
                    archive.append_data(&mut header, name, std::io::empty())?;
                }
                Kind::File(data) => {
                    data_bytes += data.len() as u64;
                    header.set_size(data.len() as u64);
                    archive.append_data(&mut header, name, Cursor::new(data))?;
                }
                Kind::Symlink(target) => {
                    header.set_size(0);
                    archive.append_link(&mut header, name, target)?;
                }
                Kind::Hardlink(_) => return Err(invalid("a hard link was not resolved")),
            }
            links.insert(identity, name.to_owned());
        }
        Ok(crate::Filesystem {
            tar: archive.into_inner()?,
            data_bytes,
            entries: self.nodes.len() as u64,
            env: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layer(entries: &[(&str, tar::EntryType, &str)]) -> Vec<u8> {
        let mut tar = tar::Builder::new(Vec::new());
        for (name, kind, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(*kind);
            header.set_uid(42);
            header.set_gid(43);
            header.set_mode(0o751);
            header.set_mtime(123);
            if kind.is_file() {
                header.set_size(content.len() as u64);
                tar.append_data(&mut header, name, content.as_bytes()).unwrap();
            } else if kind.is_dir() {
                header.set_size(0);
                tar.append_data(&mut header, name, std::io::empty()).unwrap();
            } else {
                header.set_size(0);
                tar.append_link(&mut header, name, content).unwrap();
            }
        }
        tar.into_inner().unwrap()
    }

    fn file(filesystem: &Filesystem, name: &str) -> Vec<u8> {
        let Kind::File(bytes) = filesystem.nodes[name].kind.as_ref() else {
            panic!("expected a file at {name}");
        };
        bytes.clone()
    }

    #[test]
    fn whiteouts_remove_lower_files_and_keep_same_layer_replacements_in_either_order() {
        for reversed in [false, true] {
            let mut filesystem = Filesystem::new();
            filesystem
                .apply(&layer(&[("old", tar::EntryType::Regular, "old")]))
                .unwrap();
            let mut entries = [
                ("old", tar::EntryType::Regular, "new"),
                (".wh.old", tar::EntryType::Regular, ""),
            ];
            if reversed {
                entries.reverse();
            }
            filesystem.apply(&layer(&entries)).unwrap();
            assert_eq!(file(&filesystem, "old"), b"new");
            assert!(!filesystem.nodes.contains_key(".wh.old"));
        }
    }

    #[test]
    fn opaque_directories_discard_lower_children_and_keep_current_children() {
        let mut filesystem = Filesystem::new();
        filesystem
            .apply(&layer(&[("dir/lower", tar::EntryType::Regular, "old")]))
            .unwrap();
        filesystem
            .apply(&layer(&[
                ("dir/new", tar::EntryType::Regular, "new"),
                ("dir/.wh..wh..opq", tar::EntryType::Regular, ""),
            ]))
            .unwrap();
        assert!(!filesystem.nodes.contains_key("dir/lower"));
        assert_eq!(file(&filesystem, "dir/new"), b"new");
    }

    #[test]
    fn whiteout_targets_resolve_before_any_lower_symlink_is_removed() {
        for reversed in [false, true] {
            let mut filesystem = Filesystem::new();
            filesystem
                .apply(&layer(&[
                    ("real/file", tar::EntryType::Regular, "old"),
                    ("alias", tar::EntryType::Symlink, "/real"),
                ]))
                .unwrap();
            let mut entries = [
                (".wh.alias", tar::EntryType::Regular, ""),
                ("alias/.wh.file", tar::EntryType::Regular, ""),
            ];
            if reversed {
                entries.reverse();
            }
            filesystem.apply(&layer(&entries)).unwrap();
            assert!(!filesystem.nodes.contains_key("real/file"));
            assert!(!filesystem.nodes.contains_key("alias"));
        }
    }

    #[test]
    fn directories_merge_children_but_a_file_replacing_a_directory_removes_them() {
        let mut filesystem = Filesystem::new();
        filesystem
            .apply(&layer(&[("dir/child", tar::EntryType::Regular, "old")]))
            .unwrap();
        filesystem
            .apply(&layer(&[("dir", tar::EntryType::Directory, "")]))
            .unwrap();
        assert_eq!(file(&filesystem, "dir/child"), b"old");
        assert_eq!(filesystem.nodes["dir"].header.uid().unwrap(), 42);
        filesystem
            .apply(&layer(&[("dir", tar::EntryType::Regular, "replacement")]))
            .unwrap();
        assert!(!filesystem.nodes.contains_key("dir/child"));
        assert_eq!(file(&filesystem, "dir"), b"replacement");
    }

    #[test]
    fn absolute_symlinks_and_parent_components_stay_inside_the_virtual_root() {
        let mut filesystem = Filesystem::new();
        filesystem
            .apply(&layer(&[
                ("alias", tar::EntryType::Symlink, "/../../real"),
                ("alias/file", tar::EntryType::Regular, "safe"),
            ]))
            .unwrap();
        assert_eq!(file(&filesystem, "real/file"), b"safe");
        assert!(path(Path::new("../../outside")).is_err());
        assert!(path(Path::new("/outside")).is_err());
    }

    #[test]
    fn hardlink_aliases_keep_old_contents_when_the_original_is_replaced() {
        let mut filesystem = Filesystem::new();
        filesystem
            .apply(&layer(&[
                ("original", tar::EntryType::Regular, "old"),
                ("alias", tar::EntryType::Link, "original"),
            ]))
            .unwrap();
        filesystem
            .apply(&layer(&[("original", tar::EntryType::Regular, "new")]))
            .unwrap();
        assert_eq!(file(&filesystem, "alias"), b"old");
        assert_eq!(file(&filesystem, "original"), b"new");
    }

    #[test]
    fn forward_hardlinks_are_emitted_after_their_payload_with_shared_metadata() {
        let mut filesystem = Filesystem::new();
        filesystem
            .apply(&layer(&[
                ("alias", tar::EntryType::Link, "original"),
                ("original", tar::EntryType::Regular, "payload"),
            ]))
            .unwrap();
        let output = filesystem.finish().unwrap();
        assert_eq!(output.data_bytes, 7);
        let mut archive = tar::Archive::new(Cursor::new(output.tar));
        let entries: Vec<_> = archive.entries().unwrap().map(Result::unwrap).collect();
        assert!(entries[1].header().entry_type().is_file());
        assert!(entries[2].header().entry_type().is_hard_link());
        assert_eq!(entries[2].link_name().unwrap().unwrap(), Path::new("alias"));
        assert_eq!(entries[1].header().uid().unwrap(), 42);
        assert_eq!(entries[1].header().mode().unwrap(), 0o751);
    }

    #[test]
    fn short_ustar_directory_paths_are_not_prefixed_twice() {
        let mut input = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_ustar();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_uid(42);
        header.set_gid(43);
        header.set_mode(0o751);
        header.set_mtime(123);
        header.set_size(0);
        header.set_path("licenses").unwrap();
        let prefix = b"usr/local/lib/python3.13/site-packages/pip";
        header.as_ustar_mut().unwrap().prefix[..prefix.len()].copy_from_slice(prefix);
        header.set_cksum();
        input.append(&header, std::io::empty()).unwrap();
        let mut filesystem = Filesystem::new();
        filesystem.apply(&input.into_inner().unwrap()).unwrap();
        let output = filesystem.finish().unwrap();
        let mut archive = tar::Archive::new(Cursor::new(output.tar));
        let entries: Vec<_> = archive.entries().unwrap().map(Result::unwrap).collect();
        let expected = Path::new("usr/local/lib/python3.13/site-packages/pip/licenses");
        let directory = entries
            .iter()
            .find(|entry| entry.path().unwrap() == expected)
            .unwrap();
        assert!(directory.header().entry_type().is_dir());
        assert_eq!(directory.header().uid().unwrap(), 42);
        assert_eq!(directory.header().gid().unwrap(), 43);
        assert_eq!(directory.header().mode().unwrap(), 0o751);
        assert_eq!(directory.header().mtime().unwrap(), 123);
        assert_eq!(entries.len(), 8);
    }

    #[test]
    fn ustar_prefixes_do_not_follow_hardlinks_to_shorter_paths() {
        let original = format!("{}original", "long-directory/".repeat(8));
        let mut input = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_ustar();
        header.set_uid(42);
        header.set_gid(43);
        header.set_mode(0o751);
        header.set_mtime(123);
        header.set_size(7);
        input
            .append_data(&mut header, &original, b"payload".as_slice())
            .unwrap();
        assert_ne!(header.as_ustar().unwrap().prefix[0], 0);
        let mut filesystem = Filesystem::new();
        filesystem.apply(&input.into_inner().unwrap()).unwrap();
        filesystem
            .apply(&layer(&[("alias", tar::EntryType::Link, &original)]))
            .unwrap();
        let output = filesystem.finish().unwrap();
        let mut archive = tar::Archive::new(Cursor::new(output.tar));
        let entries: Vec<_> = archive.entries().unwrap().map(Result::unwrap).collect();
        let alias = entries
            .iter()
            .find(|entry| entry.path().unwrap() == Path::new("alias"))
            .unwrap();
        assert!(alias.header().entry_type().is_file());
        let original = entries
            .iter()
            .find(|entry| entry.path().unwrap() == Path::new(&original))
            .unwrap();
        assert!(original.header().entry_type().is_hard_link());
        assert_eq!(original.link_name().unwrap().unwrap(), Path::new("alias"));
        for entry in [alias, original] {
            assert_eq!(entry.header().uid().unwrap(), 42);
            assert_eq!(entry.header().gid().unwrap(), 43);
            assert_eq!(entry.header().mode().unwrap(), 0o751);
            assert_eq!(entry.header().mtime().unwrap(), 123);
        }
    }

    #[test]
    fn hardlinks_to_symbolic_links_preserve_the_link_inode() {
        let mut filesystem = Filesystem::new();
        filesystem
            .apply(&layer(&[
                ("symbolic", tar::EntryType::Symlink, "/missing"),
                ("alias", tar::EntryType::Link, "symbolic"),
            ]))
            .unwrap();
        assert!(Arc::ptr_eq(
            &filesystem.nodes["symbolic"].kind,
            &filesystem.nodes["alias"].kind
        ));
        assert!(matches!(
            filesystem.nodes["alias"].kind.as_ref(),
            Kind::Symlink(_)
        ));
    }

    #[test]
    fn cyclic_hardlinks_and_links_to_directories_are_rejected() {
        for entries in [
            vec![("a", tar::EntryType::Link, "b"), ("b", tar::EntryType::Link, "a")],
            vec![
                ("dir", tar::EntryType::Directory, ""),
                ("link", tar::EntryType::Link, "dir"),
            ],
        ] {
            assert!(Filesystem::new().apply(&layer(&entries)).is_err());
        }
    }

    #[test]
    fn pax_extended_attributes_survive_flattening() {
        let mut tar = tar::Builder::new(Vec::new());
        tar.append_pax_extensions([("SCHILY.xattr.user.example", b"value".as_slice())])
            .unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o755);
        header.set_size(0);
        tar.append_data(&mut header, "file", std::io::empty()).unwrap();
        let mut filesystem = Filesystem::new();
        filesystem.apply(&tar.into_inner().unwrap()).unwrap();
        let output = filesystem.finish().unwrap();
        let mut archive = tar::Archive::new(Cursor::new(output.tar));
        let mut entries = archive.entries().unwrap();
        entries.next().unwrap().unwrap();
        let mut entry = entries.next().unwrap().unwrap();
        let attribute = entry.pax_extensions().unwrap().unwrap().next().unwrap().unwrap();
        assert_eq!(attribute.key().unwrap(), "SCHILY.xattr.user.example");
        assert_eq!(attribute.value().unwrap(), "value");
    }
}
