use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use backhand::compression::{CompressionOptions, Compressor, Zstd};
use backhand::{FilesystemCompressor, FilesystemWriter, NodeHeader};
use protocol::{DesiredLayer, DownloadUrl, Sha256Digest, ZipEntry};

use crate::json_store::make_directory;
use crate::ports::{ArtifactError, ArtifactStore, ArtifactStoreExt, PayloadBuilder, PreparedPayload};

pub const VERBATIM_IMAGE_FILENAME: &str = "layer.img";

pub const BINARY_MODE: u16 = 0o755;
const CONFIG_MODE: u16 = 0o600;
const DIRECTORY_MODE: u16 = 0o755;
const CACHE_DIR_MODE: u32 = 0o755;
const VM_DIR_MODE: u32 = 0o700;

/// What one program may inflate to, so that a small zip cannot fill the host's memory.
const MAX_PROGRAM_BYTES: u64 = 512 * 1024 * 1024;

const SQUASHFS_MAGIC: &[u8; 4] = b"hsqs";
const EXT4_MAGIC_OFFSET: usize = 0x438;
const EXT4_MAGIC: [u8; 2] = [0x53, 0xEF];

// zstd's own default. gzip took the host over a second for a 32 MB program, and the guest kernel
// inflates every block of it the program pages in; this packs the same bytes in a twelfth of the
// time to an image three percent larger, and the kernel reads it back in half the time.
const COMPRESSION_LEVEL: u32 = 3;

fn image_compressor() -> FilesystemCompressor {
    FilesystemCompressor::new(
        Compressor::Zstd,
        Some(CompressionOptions::Zstd(Zstd {
            compression_level: COMPRESSION_LEVEL,
        })),
    )
    .expect("zstd takes a level and nothing else")
}

const FIXED_MTIME: u32 = 0;

fn header(permissions: u16) -> NodeHeader {
    NodeHeader {
        permissions,
        uid: 0,
        gid: 0,
        mtime: FIXED_MTIME,
    }
}

/// Builds a squashfs holding exactly these files at these absolute paths, byte-identical for the
/// same input so an image is cached by what went into it.
fn pack(files: &[(&str, &[u8], u16)]) -> Result<Vec<u8>, ArtifactError> {
    let unpackable = |error: backhand::BackhandError| ArtifactError::Unpackable(error.to_string());
    let mut writer = FilesystemWriter::default();
    writer.set_compressor(image_compressor());
    writer.set_time(FIXED_MTIME);
    writer.set_root_mode(DIRECTORY_MODE);
    for (path, bytes, permissions) in files {
        if let Some(parent) = Path::new(path)
            .parent()
            .filter(|parent| *parent != Path::new("/"))
        {
            writer
                .push_dir_all(parent, header(DIRECTORY_MODE))
                .map_err(unpackable)?;
        }
        writer
            .push_file(Cursor::new(bytes.to_vec()), path, header(*permissions))
            .map_err(unpackable)?;
    }
    let mut image = Cursor::new(Vec::new());
    writer.write(&mut image).map_err(unpackable)?;
    Ok(image.into_inner())
}

pub fn is_filesystem_image(bytes: &[u8]) -> bool {
    bytes.starts_with(SQUASHFS_MAGIC)
        || bytes
            .get(EXT4_MAGIC_OFFSET..EXT4_MAGIC_OFFSET + EXT4_MAGIC.len())
            .is_some_and(|magic| magic == EXT4_MAGIC)
}

fn short_hash(text: &str) -> String {
    use sha2::Digest;
    hex::encode(&sha2::Sha256::digest(text.as_bytes())[..8])
}

/// Where a layer's image lives in the cache. The object is the same bytes whether it is attached
/// whole or packed as a program at some path, so the kind and the path are part of the name.
pub fn layer_image_path(cache_dir: &Path, layer: &DesiredLayer) -> PathBuf {
    let directory = cache_dir.join(layer.digest().as_str());
    match layer {
        DesiredLayer::Filesystem { .. } => directory.join(VERBATIM_IMAGE_FILENAME),
        DesiredLayer::Executable { destination_path, .. }
        | DesiredLayer::DownloadedExecutable { destination_path, .. } => directory.join(format!(
            "executable-{}.squashfs",
            short_hash(destination_path.as_str())
        )),
    }
}

pub struct LayerImages {
    store: Arc<dyn ArtifactStore>,
    cache_dir: PathBuf,
    access: tokio::sync::RwLock<()>,
}

impl LayerImages {
    pub fn new(store: Arc<dyn ArtifactStore>, cache_dir: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            store,
            cache_dir,
            access: tokio::sync::RwLock::new(()),
        })
    }
}

#[async_trait]
impl PayloadBuilder for LayerImages {
    async fn retain(&self, digests: &std::collections::BTreeSet<Sha256Digest>) -> Result<(), ArtifactError> {
        let _access = self.access.write().await;
        let mut entries = match tokio::fs::read_dir(&self.cache_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(ArtifactError::Transfer(error.to_string())),
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| ArtifactError::Transfer(e.to_string()))?
        {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(digest) = Sha256Digest::parse(name) else {
                continue;
            };
            if !digests.contains(&digest) {
                let kind = entry
                    .file_type()
                    .await
                    .map_err(|e| ArtifactError::Transfer(e.to_string()))?;
                let result = if kind.is_dir() {
                    tokio::fs::remove_dir_all(entry.path()).await
                } else {
                    tokio::fs::remove_file(entry.path()).await
                };
                result.map_err(|e| ArtifactError::Transfer(e.to_string()))?;
            }
        }
        Ok(())
    }
    async fn prepare(&self, layers: &[DesiredLayer]) -> Result<PreparedPayload, ArtifactError> {
        let _access = self.access.read().await;
        let mut layer_image_paths = Vec::with_capacity(layers.len());
        let mut fetched_bytes = 0;
        for layer in layers {
            let image = ensure_layer_image(&self.store, &self.cache_dir, layer).await?;
            layer_image_paths.push(image.path);
            fetched_bytes += image.fetched_bytes;
        }
        Ok(PreparedPayload {
            layer_image_paths,
            fetched_bytes,
        })
    }
}

async fn downloaded_program(
    store: &Arc<dyn ArtifactStore>,
    url: &DownloadUrl,
    digest: &Sha256Digest,
    zip_entry: Option<&ZipEntry>,
) -> Result<Vec<u8>, ArtifactError> {
    use sha2::Digest;

    let body = store.download(url).await?;
    let program = match zip_entry {
        None => body,
        Some(entry) => {
            let not_in_archive = || ArtifactError::NotInArchive {
                url: url.clone(),
                entry: entry.as_str().to_owned(),
            };
            let mut archive = zip::ZipArchive::new(Cursor::new(body))
                .map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
            let file = archive.by_name(entry.as_str()).map_err(|_| not_in_archive())?;
            let mut program = Vec::new();
            std::io::Read::take(file, MAX_PROGRAM_BYTES)
                .read_to_end(&mut program)
                .map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
            program
        }
    };
    let actual = hex::encode(sha2::Sha256::digest(&program));
    if actual != digest.as_str() {
        return Err(ArtifactError::DigestMismatch {
            expected: digest.clone(),
            actual,
        });
    }
    Ok(program)
}

/// A layer's image in the cache, and what putting it there cost: nothing for one already held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerImage {
    pub path: PathBuf,
    pub fetched_bytes: u64,
}

pub async fn ensure_layer_image(
    store: &Arc<dyn ArtifactStore>,
    cache_dir: &Path,
    layer: &DesiredLayer,
) -> Result<LayerImage, ArtifactError> {
    let image_path = layer_image_path(cache_dir, layer);
    if image_path.exists() {
        let _ = std::fs::File::open(&image_path).and_then(|file| {
            file.set_times(
                std::fs::FileTimes::new()
                    .set_accessed(std::time::SystemTime::now())
                    .set_modified(std::time::SystemTime::now()),
            )
        });
        return Ok(LayerImage {
            path: image_path,
            fetched_bytes: 0,
        });
    }

    let bytes = match layer {
        DesiredLayer::Filesystem { object } | DesiredLayer::Executable { object, .. } => {
            store.read_verified(object).await?
        }
        DesiredLayer::DownloadedExecutable {
            url,
            digest,
            zip_entry,
            ..
        } => downloaded_program(store, url, digest, zip_entry.as_ref()).await?,
    };
    let fetched_bytes = bytes.len() as u64;
    let image = match layer {
        DesiredLayer::Filesystem { .. } if is_filesystem_image(&bytes) => bytes,
        DesiredLayer::Filesystem { object } => {
            return Err(ArtifactError::NotAnImage {
                digest: object.digest.clone(),
            })
        }
        DesiredLayer::Executable { destination_path, .. }
        | DesiredLayer::DownloadedExecutable { destination_path, .. } => {
            pack(&[(destination_path.as_str(), &bytes, BINARY_MODE)])?
        }
    };

    let directory = image_path
        .parent()
        .expect("the image is one level inside the cache");
    make_directory(directory, CACHE_DIR_MODE)
        .map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
    let filename = image_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("the image has a name");
    let staged = directory.join(format!("{filename}.{}.tmp", std::process::id()));
    std::fs::write(&staged, &image).map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
    std::fs::rename(&staged, &image_path).map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
    tracing::info!(
        digest = %layer.digest(),
        size_bytes = fetched_bytes,
        image_bytes = image.len(),
        packed = matches!(layer, DesiredLayer::Executable { .. }),
        "layer image ready"
    );
    Ok(LayerImage {
        path: image_path,
        fetched_bytes,
    })
}

pub fn build_instance_config_image(working_dir: &Path, rendered: &str) -> Result<PathBuf, ArtifactError> {
    make_directory(working_dir, VM_DIR_MODE).map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
    let image = pack(&[(
        &format!("/{}", guest_contract::instance_env::INSTANCE_ENV_FILENAME),
        rendered.as_bytes(),
        CONFIG_MODE,
    )])?;
    let image_path = working_dir.join(guest_contract::instance_env::INSTANCE_CONFIG_IMAGE);
    let staged = working_dir.join(format!("config.{}.tmp", std::process::id()));
    std::fs::write(&staged, &image).map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&staged, &image_path).map_err(|error| ArtifactError::Unpackable(error.to_string()))?;
    Ok(image_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mocks;
    use crate::test_support::{base_layer, layer, ARTIFACT_BYTES, ARTIFACT_DIGEST, BASE_LAYER_BYTES};
    use protocol::ExecutablePath;
    use sha2::Digest;

    fn artifact_bytes() -> Vec<u8> {
        ARTIFACT_BYTES.to_vec()
    }

    fn as_filesystem(layer: DesiredLayer) -> DesiredLayer {
        DesiredLayer::Filesystem {
            object: layer.stored_object().unwrap().clone(),
        }
    }

    fn placed_at(layer: DesiredLayer, path: &str) -> DesiredLayer {
        DesiredLayer::Executable {
            object: layer.stored_object().unwrap().clone(),
            destination_path: ExecutablePath::parse(path).unwrap(),
        }
    }

    fn store(bytes: Vec<u8>) -> Arc<dyn ArtifactStore> {
        mocks::artifacts_holding(bytes)
    }

    fn read_back(image: &[u8], path: &str) -> Vec<u8> {
        use std::io::Read;
        let filesystem = backhand::FilesystemReader::from_reader(Cursor::new(image.to_vec())).unwrap();
        let node = filesystem
            .files()
            .find(|node| node.fullpath.to_string_lossy() == path)
            .unwrap_or_else(|| panic!("the image holds nothing at {path}"));
        let backhand::InnerNode::File(file) = &node.inner else {
            panic!("{path} is not a file");
        };
        let mut bytes = Vec::new();
        filesystem.file(file).reader().read_to_end(&mut bytes).unwrap();
        bytes
    }

    #[tokio::test]
    async fn unused_images_are_removed_and_recreated_from_the_store() {
        let directory = tempfile::tempdir().unwrap();
        let builder = LayerImages::new(store(artifact_bytes()), directory.path().to_owned());
        let layer = layer(|_| {});
        let path = builder
            .prepare(std::slice::from_ref(&layer))
            .await
            .unwrap()
            .layer_image_paths[0]
            .clone();
        let retained = [layer.digest().clone()].into();
        builder.retain(&retained).await.unwrap();
        assert!(
            path.exists(),
            "a desired or live instance protects its shared layer"
        );
        std::fs::write(directory.path().join("unrelated"), b"keep").unwrap();
        builder.retain(&Default::default()).await.unwrap();
        assert!(!path.parent().unwrap().exists());
        assert!(directory.path().join("unrelated").exists());
        let restored = builder.prepare(std::slice::from_ref(&layer)).await.unwrap();
        assert_eq!(restored.fetched_bytes, ARTIFACT_BYTES.len() as u64);
        assert!(path.exists());
        builder.retain(&Default::default()).await.unwrap();
        builder.retain(&Default::default()).await.unwrap();
    }

    #[tokio::test]
    async fn an_executable_is_packed_once_per_digest_and_holds_the_program_where_the_guest_runs_it() {
        let directory = tempfile::tempdir().unwrap();
        let store = store(artifact_bytes());
        let fetched = ensure_layer_image(&store, directory.path(), &layer(|_| {}))
            .await
            .unwrap();
        let image_path = fetched.path.clone();
        assert!(image_path.starts_with(directory.path().join(ARTIFACT_DIGEST)));
        assert_eq!(
            fetched.fetched_bytes,
            ARTIFACT_BYTES.len() as u64,
            "what was pulled is what the store held, not what the image packed it into"
        );
        let image = std::fs::read(&image_path).unwrap();
        assert_eq!(&image[..4], b"hsqs");
        assert_eq!(read_back(&image, "/app/server"), artifact_bytes());

        let before = std::fs::metadata(&image_path).unwrap().len();
        let again = ensure_layer_image(&store, directory.path(), &layer(|_| {}))
            .await
            .unwrap();
        assert_eq!(again.path, image_path);
        assert_eq!(again.fetched_bytes, 0, "a cache hit pulls nothing");
        assert_eq!(std::fs::metadata(&image_path).unwrap().len(), before);
        let siblings = std::fs::read_dir(image_path.parent().unwrap()).unwrap().count();
        assert_eq!(siblings, 1);
    }

    #[tokio::test]
    async fn the_program_sits_where_the_document_put_it_and_the_directories_above_are_made() {
        let directory = tempfile::tempdir().unwrap();
        let deep = placed_at(layer(|_| {}), "/usr/local/bin/server");
        let image_path = ensure_layer_image(&store(artifact_bytes()), directory.path(), &deep)
            .await
            .unwrap()
            .path;
        let image = std::fs::read(&image_path).unwrap();
        assert_eq!(read_back(&image, "/usr/local/bin/server"), artifact_bytes());
        assert_ne!(image_path, layer_image_path(directory.path(), &layer(|_| {})));
    }

    #[tokio::test]
    async fn a_filesystem_is_attached_as_it_was_uploaded() {
        let directory = tempfile::tempdir().unwrap();
        let image_path =
            ensure_layer_image(&store(BASE_LAYER_BYTES.to_vec()), directory.path(), &base_layer())
                .await
                .unwrap()
                .path;
        assert!(image_path.ends_with(VERBATIM_IMAGE_FILENAME));
        assert_eq!(std::fs::read(&image_path).unwrap(), BASE_LAYER_BYTES);
    }

    #[tokio::test]
    async fn a_filesystem_that_is_not_one_is_refused_by_name() {
        let directory = tempfile::tempdir().unwrap();
        let not_an_image = as_filesystem(layer(|_| {}));
        let error = ensure_layer_image(&store(artifact_bytes()), directory.path(), &not_an_image)
            .await
            .unwrap_err();
        assert!(matches!(error, ArtifactError::NotAnImage { .. }), "{error}");
        assert!(error.message().contains(ARTIFACT_DIGEST));
        assert!(!layer_image_path(directory.path(), &not_an_image).exists());
    }

    #[test]
    fn a_filesystem_is_known_by_its_magic() {
        assert!(is_filesystem_image(b"hsqs and then anything"));
        let mut ext4 = vec![0u8; 4096];
        ext4[EXT4_MAGIC_OFFSET..EXT4_MAGIC_OFFSET + 2].copy_from_slice(&EXT4_MAGIC);
        assert!(is_filesystem_image(&ext4));
        assert!(!is_filesystem_image(b"\x7fELF"));
        assert!(!is_filesystem_image(b""));
        assert!(!is_filesystem_image(&vec![0u8; 4096]));
    }

    #[tokio::test]
    async fn bytes_that_are_not_what_they_claim_never_reach_a_guest() {
        let directory = tempfile::tempdir().unwrap();
        let wrong = store(b"something else entirely\n".to_vec());
        let error = ensure_layer_image(&wrong, directory.path(), &layer(|_| {}))
            .await
            .unwrap_err();
        assert!(matches!(error, ArtifactError::DigestMismatch { .. }));
        assert!(error.message().contains("not to the"));
        assert!(!layer_image_path(directory.path(), &layer(|_| {})).exists());
    }

    #[test]
    fn the_config_image_is_rebuilt_in_place_on_every_boot() {
        let directory = tempfile::tempdir().unwrap();
        let first = build_instance_config_image(directory.path(), "NIBRUN_HTTP_PORT=3000\n").unwrap();
        assert!(first.ends_with(guest_contract::instance_env::INSTANCE_CONFIG_IMAGE));
        let image = std::fs::read(&first).unwrap();
        assert_eq!(&image[..4], b"hsqs");
        assert_eq!(read_back(&image, "/instance.env"), b"NIBRUN_HTTP_PORT=3000\n");

        let second = build_instance_config_image(directory.path(), "NIBRUN_HTTP_PORT=8080\n").unwrap();
        assert_eq!(second, first);
        let rebuilt = std::fs::read(&second).unwrap();
        assert_eq!(read_back(&rebuilt, "/instance.env"), b"NIBRUN_HTTP_PORT=8080\n");
    }

    // The guest kernel reads the image, so what it is compressed with is a contract with the kernel
    // config the guest image is built from — and one the superblock states, at the offset where
    // squashfs keeps its compressor id.
    #[test]
    fn an_image_is_compressed_with_zstd_which_the_guest_kernel_has() {
        const COMPRESSOR_ID_OFFSET: usize = 20;
        const ZSTD: u16 = 6;
        let image = pack(&[("/app/server", &artifact_bytes(), BINARY_MODE)]).unwrap();
        let id = u16::from_le_bytes([image[COMPRESSOR_ID_OFFSET], image[COMPRESSOR_ID_OFFSET + 1]]);
        assert_eq!(id, ZSTD);
    }

    #[test]
    fn packing_the_same_bytes_twice_is_byte_identical() {
        let once = pack(&[("/app/server", &artifact_bytes(), BINARY_MODE)]).unwrap();
        let twice = pack(&[("/app/server", &artifact_bytes(), BINARY_MODE)]).unwrap();
        assert_eq!(once, twice);
    }

    #[tokio::test]
    async fn a_store_that_will_not_hand_over_the_bytes_is_not_read_as_an_empty_layer() {
        let directory = tempfile::tempdir().unwrap();
        let refusing = mocks::artifacts_refusing(ArtifactError::Transfer("no such key".into()))
            as Arc<dyn ArtifactStore>;
        let error = ensure_layer_image(&refusing, directory.path(), &layer(|_| {}))
            .await
            .unwrap_err();
        assert!(matches!(error, ArtifactError::Transfer(_)), "{error}");
        assert!(!layer_image_path(directory.path(), &layer(|_| {})).exists());
    }

    #[tokio::test]
    async fn bytes_that_match_are_handed_back_whole_before_anything_is_built_from_them() {
        let store = store(artifact_bytes());
        assert_eq!(
            store
                .read_verified(layer(|_| {}).stored_object().unwrap())
                .await
                .unwrap(),
            artifact_bytes()
        );
    }

    #[tokio::test]
    async fn the_images_come_back_in_the_order_the_document_listed_the_layers() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = crate::ports::MockArtifactStore::new();
        store.expect_read().returning(|key| {
            Ok(if key == &base_layer().stored_object().unwrap().object_key {
                BASE_LAYER_BYTES.to_vec()
            } else {
                ARTIFACT_BYTES.to_vec()
            })
        });
        let images = LayerImages::new(Arc::new(store), directory.path().to_path_buf());
        let prepared = images.prepare(&[base_layer(), layer(|_| {})]).await.unwrap();
        assert_eq!(
            prepared.layer_image_paths,
            vec![
                layer_image_path(directory.path(), &base_layer()),
                layer_image_path(directory.path(), &layer(|_| {})),
            ]
        );
        assert!(prepared.layer_image_paths.iter().all(|path| path.exists()));
        assert_eq!(
            prepared.fetched_bytes,
            (BASE_LAYER_BYTES.len() + ARTIFACT_BYTES.len()) as u64,
            "what was pulled is counted as it was read, layer by layer"
        );

        let again = images.prepare(&[base_layer(), layer(|_| {})]).await.unwrap();
        assert_eq!(again.fetched_bytes, 0, "the cache held both");
    }

    #[test]
    fn every_digest_kind_and_path_gets_its_own_place_in_the_cache() {
        let cache = Path::new("/var/lib/nibrunner/artifacts");
        let packed = layer_image_path(cache, &layer(|_| {}));
        let elsewhere = layer_image_path(cache, &placed_at(layer(|_| {}), "/bin/server"));
        let whole = layer_image_path(cache, &as_filesystem(layer(|_| {})));
        let other = layer_image_path(
            cache,
            &layer(|object| object.digest = Sha256Digest::parse("0".repeat(64)).unwrap()),
        );
        for (a, b) in [(&packed, &elsewhere), (&packed, &whole), (&packed, &other)] {
            assert_ne!(a, b);
        }
        assert_eq!(packed.parent(), whole.parent(), "one digest, one directory");
        assert!(packed.starts_with(cache));
        assert!(whole.ends_with(VERBATIM_IMAGE_FILENAME));
    }

    #[test]
    fn an_image_that_has_nowhere_to_be_written_is_named_rather_than_left_half_built() {
        let directory = tempfile::tempdir().unwrap();
        let occupied = directory.path().join("vm");
        std::fs::write(&occupied, b"a file, not a directory").unwrap();
        let error = build_instance_config_image(&occupied, "NIBRUN_HTTP_PORT=3000\n").unwrap_err();
        assert!(matches!(error, ArtifactError::Unpackable(_)), "{error}");
    }

    #[test]
    fn the_binary_a_guest_runs_is_packed_executable_and_the_config_it_reads_is_not() {
        let image = pack(&[
            ("/app/server", &artifact_bytes(), BINARY_MODE),
            ("/instance.env", b"NIBRUN_HTTP_PORT=3000\n", CONFIG_MODE),
        ])
        .unwrap();
        let filesystem = backhand::FilesystemReader::from_reader(Cursor::new(image)).unwrap();
        let mode = |path: &str| {
            filesystem
                .files()
                .find(|node| node.fullpath.to_string_lossy() == path)
                .map(|node| node.header.permissions)
                .unwrap_or_else(|| panic!("the image holds nothing at {path}"))
        };
        assert_eq!(mode("/app/server"), BINARY_MODE);
        assert_eq!(mode("/instance.env"), CONFIG_MODE);
    }

    #[cfg(unix)]
    #[test]
    fn a_config_image_on_disk_is_readable_only_by_the_user_this_daemon_runs_as() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let image = build_instance_config_image(directory.path(), "NIBRUN_HTTP_PORT=3000\n").unwrap();
        assert_eq!(
            std::fs::metadata(&image).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let siblings = std::fs::read_dir(directory.path()).unwrap().count();
        assert_eq!(siblings, 1, "nothing staged is left beside the image");
    }

    #[tokio::test]
    async fn an_image_already_in_the_cache_is_touched_so_a_reap_takes_the_cold_ones_first() {
        let directory = tempfile::tempdir().unwrap();
        let store = store(artifact_bytes());
        let image_path = ensure_layer_image(&store, directory.path(), &layer(|_| {}))
            .await
            .unwrap()
            .path;
        let backdated = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        std::fs::File::options()
            .write(true)
            .open(&image_path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(backdated))
            .unwrap();

        ensure_layer_image(&store, directory.path(), &layer(|_| {}))
            .await
            .unwrap();
        let touched = std::fs::metadata(&image_path).unwrap().modified().unwrap();
        assert!(touched > backdated, "the image was not touched on a cache hit");
    }

    fn downloaded(body: &[u8], zip_entry: Option<&str>) -> (DesiredLayer, Arc<dyn ArtifactStore>) {
        let served = body.to_vec();
        let mut artifacts = crate::ports::MockArtifactStore::new();
        artifacts.expect_download().returning(move |_| Ok(served.clone()));
        let layer = DesiredLayer::DownloadedExecutable {
            url: DownloadUrl::parse("https://example.test/program").unwrap(),
            digest: Sha256Digest::parse(hex::encode(sha2::Sha256::digest(artifact_bytes()))).unwrap(),
            zip_entry: zip_entry.map(|entry| ZipEntry::parse(entry).unwrap()),
            destination_path: ExecutablePath::parse("/opt/tool/tool").unwrap(),
        };
        (layer, Arc::new(artifacts))
    }

    fn zipped(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write;
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, bytes) in entries {
            writer
                .start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[tokio::test]
    async fn a_downloaded_program_is_packed_where_the_document_put_it() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded(&artifact_bytes(), None);
        let image = ensure_layer_image(&store, directory.path(), &layer)
            .await
            .unwrap();
        assert_eq!(
            read_back(&std::fs::read(image.path).unwrap(), "/opt/tool/tool"),
            artifact_bytes()
        );
    }

    #[tokio::test]
    async fn the_program_is_taken_from_the_zip_entry_the_document_names() {
        let directory = tempfile::tempdir().unwrap();
        let archive = zipped(&[("README", b"not it"), ("tool", &artifact_bytes())]);
        let (layer, store) = downloaded(&archive, Some("tool"));
        let image = ensure_layer_image(&store, directory.path(), &layer)
            .await
            .unwrap();
        assert_eq!(
            read_back(&std::fs::read(image.path).unwrap(), "/opt/tool/tool"),
            artifact_bytes()
        );
    }

    #[tokio::test]
    async fn a_download_that_hashes_to_something_else_never_reaches_a_guest() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded(b"something else", None);
        let error = ensure_layer_image(&store, directory.path(), &layer)
            .await
            .unwrap_err();
        assert!(matches!(error, ArtifactError::DigestMismatch { .. }));
        assert!(!layer_image_path(directory.path(), &layer).exists());
    }

    #[tokio::test]
    async fn an_entry_the_archive_does_not_hold_is_named() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded(&zipped(&[("README", b"x")]), Some("tool"));
        let error = ensure_layer_image(&store, directory.path(), &layer)
            .await
            .unwrap_err();
        assert!(matches!(error, ArtifactError::NotInArchive { ref entry, .. } if entry == "tool"));
    }

    #[tokio::test]
    async fn a_downloaded_program_is_fetched_once_however_often_it_is_asked_for() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded(&artifact_bytes(), None);
        ensure_layer_image(&store, directory.path(), &layer)
            .await
            .unwrap();
        let again = ensure_layer_image(&store, directory.path(), &layer)
            .await
            .unwrap();
        assert_eq!(again.fetched_bytes, 0);
    }
}
