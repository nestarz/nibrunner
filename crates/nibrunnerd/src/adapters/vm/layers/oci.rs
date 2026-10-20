use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Cursor;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use crate::json_store::make_directory;
use crate::ports::{ArtifactError, CommandRequest, CommandRunner, CommandRunnerExt};

const MINIMUM_IMAGE_BYTES: u64 = 32 * 1024 * 1024;
const BYTES_PER_ENTRY: u64 = 16 * 1024;
const SPARE_INODES: u64 = 128;
const MAXIMUM_EXT4_TIMESTAMP: u64 = 0x3_7fff_ffff;
const FORMAT_TIMEOUT: Duration = Duration::from_secs(60 * 60);
static PREPARATION: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(1)));

fn unpackable(error: impl std::fmt::Display) -> ArtifactError {
    ArtifactError::Unpackable(format!("the OCI image could not be prepared: {error}"))
}

struct StagedImage {
    directory: tempfile::TempDir,
    env: Vec<String>,
    image: PathBuf,
    root: PathBuf,
    inodes: u64,
    root_metadata: RootMetadata,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl StagedImage {
    fn unpack(
        bytes: &[u8],
        cache: &Path,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Self, ArtifactError> {
        let filesystem = oci_image::flatten(bytes).map_err(unpackable)?;
        let image_bytes = filesystem
            .data_bytes
            .checked_mul(2)
            .and_then(|bytes| {
                filesystem
                    .entries
                    .checked_mul(BYTES_PER_ENTRY)
                    .and_then(|metadata| bytes.checked_add(metadata))
            })
            .and_then(|bytes| bytes.checked_add(MINIMUM_IMAGE_BYTES))
            .ok_or_else(|| unpackable("the filesystem is too large to size its image"))?;
        let inodes = filesystem
            .entries
            .checked_add(SPARE_INODES)
            .ok_or_else(|| unpackable("the filesystem has too many entries"))?;
        make_directory(cache, super::CACHE_DIR_MODE).map_err(unpackable)?;
        let directory = tempfile::Builder::new()
            .prefix("oci-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(cache)
            .map_err(unpackable)?;
        let root = directory.path().join("rootfs");
        make_directory(&root, u32::from(super::DIRECTORY_MODE)).map_err(unpackable)?;
        unpack(&filesystem.tar, &root).map_err(unpackable)?;
        let root_metadata = RootMetadata::read(&root).map_err(unpackable)?;
        let image = directory.path().join("image.ext4");
        File::create(&image)
            .and_then(|file| file.set_len(image_bytes))
            .map_err(unpackable)?;
        Ok(Self {
            directory,
            env: filesystem.env,
            image,
            root,
            inodes,
            root_metadata,
            _permit: permit,
        })
    }

    fn request(&self) -> CommandRequest {
        let mut request = CommandRequest::new(&["mke2fs", "-q", "-t", "ext4", "-F", "-m", "0", "-N"]);
        request.command.push(self.inodes.to_string());
        request.command.extend([
            "-d".to_string(),
            self.root.display().to_string(),
            self.image.display().to_string(),
        ]);
        request.timeout = FORMAT_TIMEOUT;
        request
    }

    /// The image's `Env` first, so that an image in the cache always has its environment beside it.
    fn publish(self, destination: &Path) -> Result<Vec<String>, ArtifactError> {
        let env = self.directory.path().join("env.json");
        fs::write(&env, serde_json::to_vec(&self.env).map_err(unpackable)?)
            .and_then(|()| File::open(&env)?.sync_all())
            .map_err(unpackable)?;
        fs::rename(&env, env_path(destination)).map_err(unpackable)?;
        File::open(&self.image)
            .and_then(|file| file.sync_all())
            .map_err(unpackable)?;
        fs::rename(&self.image, destination).map_err(unpackable)?;
        File::open(
            destination
                .parent()
                .expect("the image is held inside its cache directory"),
        )
        .and_then(|file| file.sync_all())
        .map_err(unpackable)?;
        self.directory.close().map_err(unpackable)?;
        Ok(self.env)
    }
}

/// Where an OCI layer's image `Env` is kept: `<image>.env.json` beside its ext4.
pub(super) fn env_path(image: &Path) -> PathBuf {
    image.with_extension("env.json")
}

/// The `Env` kept beside a cached image; none for an image cached before it was kept, which is
/// then prepared again.
pub(super) fn cached_env(image: &Path) -> Option<Vec<String>> {
    serde_json::from_slice(&fs::read(env_path(image)).ok()?).ok()
}

struct RootMetadata {
    uid: u32,
    gid: u32,
    mode: u32,
    mtime: u64,
}

impl RootMetadata {
    fn read(root: &Path) -> std::io::Result<Self> {
        let metadata = fs::metadata(root)?;
        let mtime = u64::try_from(metadata.mtime())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        if mtime > MAXIMUM_EXT4_TIMESTAMP {
            return Err(std::io::Error::other(
                "the root timestamp cannot be represented by ext4",
            ));
        }
        Ok(Self {
            uid: metadata.uid(),
            gid: metadata.gid(),
            mode: metadata.mode() & 0o7777,
            mtime,
        })
    }

    fn request(&self, image: &Path) -> CommandRequest {
        let mut request = CommandRequest::new(&["debugfs", "-w"]);
        request.command.push(image.display().to_string());
        request.stdin = Some(format!(
            "set_inode_field / uid {}\nset_inode_field / gid {}\nset_inode_field / mode 0{:o}\nset_inode_field / atime @{}\nset_inode_field / mtime @{}\nstat /\n",
            self.uid, self.gid, self.mode | 0o40000, self.mtime, self.mtime,
        ));
        request
    }

    fn matches(&self, stat: &str) -> bool {
        let epoch = (self.mtime + (1 << 31)) >> 32;
        let time = format!("0x{:08x}:{epoch:08x}", self.mtime & 0xffff_ffff);
        stat.contains(&format!("Type: directory    Mode:  {:04o}", self.mode))
            && stat.contains(&format!("User: {:5}   Group: {:5}", self.uid, self.gid))
            && stat.contains(&format!("atime: {time}"))
            && stat.contains(&format!("mtime: {time}"))
    }
}

struct Metadata {
    path: PathBuf,
    header: tar::Header,
    attributes: Vec<(OsString, Vec<u8>)>,
}

impl Metadata {
    fn apply(self) -> std::io::Result<()> {
        let invalid = |error| std::io::Error::new(std::io::ErrorKind::InvalidData, error);
        let uid = u32::try_from(self.header.uid()?).map_err(invalid)?;
        let gid = u32::try_from(self.header.gid()?).map_err(invalid)?;
        std::os::unix::fs::lchown(&self.path, Some(uid), Some(gid))?;
        let mode = self.header.mode()?;
        if fs::symlink_metadata(&self.path)?.file_type().is_symlink() {
            if mode & 0o7777 != 0o777 {
                return Err(std::io::Error::other(
                    "a symbolic link's permissions cannot be represented by the host",
                ));
            }
        } else {
            fs::set_permissions(&self.path, fs::Permissions::from_mode(mode))?;
        }
        let mtime = self.header.mtime()?;
        if mtime > MAXIMUM_EXT4_TIMESTAMP {
            return Err(std::io::Error::other(
                "an inode timestamp cannot be represented by ext4",
            ));
        }
        let mtime = i64::try_from(mtime).map_err(invalid)?;
        let time = filetime::FileTime::from_unix_time(mtime, 0);
        filetime::set_symlink_file_times(&self.path, time, time)?;
        for (name, value) in self.attributes {
            xattr::set(&self.path, name, &value)?;
        }
        Ok(())
    }
}

fn inside(root: &Path, path: &Path) -> std::io::Result<PathBuf> {
    if path.components().any(|component| {
        matches!(
            component,
            Component::RootDir | Component::ParentDir | Component::Prefix(_)
        )
    }) {
        return Err(std::io::Error::other(
            "an assembled OCI path reaches outside its staging directory",
        ));
    }
    let path = root.join(path);
    let mut parent = path.parent();
    while let Some(directory) = parent.filter(|directory| directory.starts_with(root)) {
        if !fs::symlink_metadata(directory)?.is_dir() {
            return Err(std::io::Error::other(
                "an assembled OCI path traverses a non-directory",
            ));
        }
        parent = directory.parent();
    }
    Ok(path)
}

fn unpack(bytes: &[u8], root: &Path) -> std::io::Result<()> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let mut directories = Vec::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = inside(root, &entry.path()?)?;
        let mut attributes = Vec::new();
        if let Some(extensions) = entry.pax_extensions()? {
            for extension in extensions {
                let extension = extension?;
                if let Some(name) = extension.key_bytes().strip_prefix(b"SCHILY.xattr.") {
                    attributes.push((
                        std::ffi::OsStr::from_bytes(name).to_owned(),
                        extension.value_bytes().to_vec(),
                    ));
                }
            }
        }
        let metadata = Metadata {
            path,
            header: entry.header().clone(),
            attributes,
        };
        if metadata.header.entry_type().is_dir() {
            if !metadata.path.exists() {
                fs::create_dir(&metadata.path)?;
            }
            directories.push(metadata);
        } else {
            if metadata.header.entry_type().is_hard_link() {
                let target = entry
                    .link_name()?
                    .ok_or_else(|| std::io::Error::other("an assembled hard link has no target"))?;
                let target = inside(root, &target)?;
                if fs::symlink_metadata(&target)?.file_type().is_symlink() {
                    return Err(std::io::Error::other(
                        "mke2fs cannot preserve hard links to symbolic links",
                    ));
                }
                fs::hard_link(target, &metadata.path)?;
            } else if !entry.unpack_in(root)? {
                return Err(std::io::Error::other(
                    "an assembled OCI entry reaches outside its staging directory",
                ));
            }
            metadata.apply()?;
        }
    }
    for directory in directories.into_iter().rev() {
        directory.apply()?;
    }
    Ok(())
}

pub(super) async fn prepare(
    bytes: Vec<u8>,
    destination: PathBuf,
    commands: &Arc<dyn CommandRunner>,
) -> Result<Vec<String>, ArtifactError> {
    let cache = destination
        .parent()
        .expect("the image is held inside its cache directory")
        .to_path_buf();
    let permit = PREPARATION.clone().acquire_owned().await.map_err(unpackable)?;
    let staged = tokio::task::spawn_blocking(move || StagedImage::unpack(&bytes, &cache, permit))
        .await
        .map_err(unpackable)??;
    commands.stdout_of(staged.request()).await.map_err(unpackable)?;
    // mke2fs copies root xattrs but creates its own root owner, mode and timestamps.
    let result = commands
        .stdout_of(staged.root_metadata.request(&staged.image))
        .await
        .map_err(unpackable)?;
    if !staged.root_metadata.matches(&result) {
        return Err(unpackable(
            "debugfs did not preserve the root directory's metadata",
        ));
    }
    tokio::task::spawn_blocking(move || staged.publish(&destination))
        .await
        .map_err(unpackable)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{ArtifactStore, CommandError, CommandResult};
    use crate::test_support::{mocks, oci};
    use protocol::{DesiredLayer, ObjectKey, OciSource, Sha256Digest, StoredObject};
    use sha2::{Digest, Sha256};

    fn layer(bytes: &[u8]) -> DesiredLayer {
        DesiredLayer::Oci {
            source: OciSource::Archive(StoredObject {
                digest: Sha256Digest::parse(hex::encode(Sha256::digest(bytes))).unwrap(),
                object_key: ObjectKey::parse("image.tar").unwrap(),
            }),
        }
    }

    fn root_stat(request: &CommandRequest) -> CommandResult {
        let values: Vec<_> = request
            .stdin
            .as_ref()
            .unwrap()
            .lines()
            .take(5)
            .map(|line| line.split_whitespace().last().unwrap())
            .collect();
        let uid: u32 = values[0].parse().unwrap();
        let gid: u32 = values[1].parse().unwrap();
        let mode = u32::from_str_radix(values[2], 8).unwrap() & 0o7777;
        let mtime: u64 = values[4].trim_start_matches('@').parse().unwrap();
        let epoch = (mtime + (1 << 31)) >> 32;
        CommandResult::with_stdout(format!(
            "Type: directory    Mode:  {mode:04o}\nUser: {uid:5}   Group: {gid:5}\natime: 0x{:08x}:{epoch:08x}\nmtime: 0x{:08x}:{epoch:08x}",
            mtime & 0xffff_ffff, mtime & 0xffff_ffff,
        ))
    }

    #[tokio::test]
    async fn a_hard_link_to_a_symbolic_link_is_refused_before_formatting() {
        let cache = tempfile::tempdir().unwrap();
        let owner = fs::metadata(cache.path()).unwrap();
        let mut filesystem = tar::Builder::new(Vec::new());
        for (path, kind, target) in [
            ("symlink", tar::EntryType::Symlink, "/missing"),
            ("alias", tar::EntryType::Link, "symlink"),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o777);
            header.set_uid(u64::from(owner.uid()));
            header.set_gid(u64::from(owner.gid()));
            header.set_entry_type(kind);
            filesystem.append_link(&mut header, path, target).unwrap();
        }
        let bytes = oci::from_filesystem(&filesystem.into_inner().unwrap());
        let layer = layer(&bytes);
        let store: Arc<dyn ArtifactStore> = mocks::artifacts_holding(bytes);
        let (commands, log) = mocks::commands_succeeding();
        let commands: Arc<dyn CommandRunner> = commands;
        let error = super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("cannot preserve hard links to symbolic links"));
        assert!(log.executables().is_empty());
        let path = super::super::layer_image_path(cache.path(), &layer);
        assert!(!path.exists());
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn a_formatter_reporting_success_without_root_metadata_never_publishes_an_image() {
        let bytes = oci::archive(b"a static program");
        let layer = layer(&bytes);
        let store: Arc<dyn ArtifactStore> = mocks::artifacts_holding(bytes);
        let commands: Arc<dyn CommandRunner> = mocks::commands_formatting().0;
        let cache = tempfile::tempdir().unwrap();
        let error = super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("debugfs did not preserve"));
        let path = super::super::layer_image_path(cache.path(), &layer);
        assert!(!path.exists());
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn an_oci_layer_keeps_its_metadata_is_cached_once_and_leaves_no_staging_tree() {
        use std::os::unix::fs::PermissionsExt;
        let cache = tempfile::tempdir().unwrap();
        let bytes = oci::archive(b"a static program");
        let layer = layer(&bytes);
        let store: Arc<dyn ArtifactStore> = mocks::artifacts_holding(bytes);
        let (commands, log) = mocks::commands_answering(|request| {
            if request.executable() == "debugfs" {
                return Ok(root_stat(request));
            }
            let root_index = request
                .command
                .iter()
                .position(|argument| argument == "-d")
                .unwrap()
                + 1;
            let root = Path::new(&request.command[root_index]);
            assert_eq!(fs::read(root.join("app/tenant")).unwrap(), b"a static program");
            assert_eq!(
                fs::metadata(root.join("app/tenant"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o7777,
                0o755
            );
            assert_eq!(
                fs::metadata(root.join("app/from-image"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o7777,
                0o640
            );
            assert_eq!(
                fs::metadata(root.parent().unwrap()).unwrap().permissions().mode() & 0o777,
                0o700
            );
            mocks::lay_superblock(request.command.last().unwrap());
            Ok(CommandResult::succeeded())
        });
        let commands: Arc<dyn CommandRunner> = commands;
        let first = super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
            .await
            .unwrap();
        assert!(first.path.ends_with("oci.ext4"));
        assert!(first.fetched_bytes > 0);
        let image_env: Vec<String> = crate::test_support::oci::IMAGE_ENV.map(String::from).to_vec();
        assert_eq!(first.env.as_ref(), Some(&image_env));
        let second = super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
            .await
            .unwrap();
        assert_eq!(second.fetched_bytes, 0);
        assert_eq!(
            second.env,
            Some(image_env),
            "the image's Env is read back from beside it"
        );
        assert_eq!(log.executables(), ["mke2fs", "debugfs"]);
        assert!(first.path.with_extension("env.json").exists());
        assert_eq!(fs::read_dir(first.path.parent().unwrap()).unwrap().count(), 2);

        fs::remove_file(first.path.with_extension("env.json")).unwrap();
        let third = super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
            .await
            .unwrap();
        assert!(
            third.fetched_bytes > 0 && third.env.is_some(),
            "an image cached before its Env was kept is prepared again"
        );
    }

    #[tokio::test]
    async fn a_failed_oci_format_never_publishes_an_image_and_can_be_retried() {
        let cache = tempfile::tempdir().unwrap();
        let bytes = oci::archive(b"a static program");
        let layer = layer(&bytes);
        let store: Arc<dyn ArtifactStore> = mocks::artifacts_holding(bytes);
        let (commands, _) = mocks::commands_answering(|request| {
            mocks::lay_superblock(request.command.last().unwrap());
            Err(CommandError::TimedOut {
                executable: "mke2fs".into(),
            })
        });
        let commands: Arc<dyn CommandRunner> = commands;
        let path = super::super::layer_image_path(cache.path(), &layer);
        assert!(
            super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
                .await
                .is_err()
        );
        assert!(!path.exists());
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
        let (commands, _) = mocks::commands_answering(|request| {
            if request.executable() == "debugfs" {
                return Ok(root_stat(request));
            }
            mocks::lay_superblock(request.command.last().unwrap());
            Ok(CommandResult::succeeded())
        });
        let commands: Arc<dyn CommandRunner> = commands;
        assert!(
            super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
                .await
                .is_ok()
        );
        assert!(path.exists());
    }

    #[tokio::test]
    async fn an_invalid_oci_archive_is_refused_before_formatting_or_caching() {
        let cache = tempfile::tempdir().unwrap();
        let bytes = b"not an OCI archive".to_vec();
        let layer = layer(&bytes);
        let store: Arc<dyn ArtifactStore> = mocks::artifacts_holding(bytes);
        let (commands, log) = mocks::commands_succeeding();
        let commands: Arc<dyn CommandRunner> = commands;
        assert!(
            super::super::ensure_layer_image(&store, cache.path(), &layer, &commands)
                .await
                .is_err()
        );
        assert!(log.executables().is_empty());
        assert!(!super::super::layer_image_path(cache.path(), &layer).exists());
    }
}
