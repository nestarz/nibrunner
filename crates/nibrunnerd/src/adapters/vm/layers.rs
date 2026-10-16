use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use backhand::compression::{CompressionOptions, Compressor, Zstd};
use backhand::{FilesystemCompressor, FilesystemWriter, NodeHeader};
use protocol::{DesiredLayer, OciSource};

use crate::json_store::make_directory;
use crate::ports::{
    ArtifactError, ArtifactStore, ArtifactStoreExt, CommandRunner, PayloadBuilder, PreparedPayload,
};

mod oci;
mod registry;

pub const VERBATIM_IMAGE_FILENAME: &str = "layer.img";

pub const BINARY_MODE: u16 = 0o755;
const CONFIG_MODE: u16 = 0o600;
const DIRECTORY_MODE: u16 = 0o755;
const CACHE_DIR_MODE: u32 = 0o755;
const VM_DIR_MODE: u32 = 0o700;

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
    let mut image = std::io::Cursor::new(Vec::new());
    pack_into(
        files
            .iter()
            .map(|(path, bytes, permissions)| (*path, Box::new(*bytes) as Box<dyn Read>, *permissions))
            .collect(),
        &mut image,
    )?;
    Ok(image.into_inner())
}

/// [`pack`], read from and written to wherever the bytes are, so that a program the size of the
/// cap is never held in memory whole to be packed.
fn pack_into<'a>(
    files: Vec<(&str, Box<dyn Read + 'a>, u16)>,
    image: &mut (impl std::io::Write + std::io::Seek),
) -> Result<(), ArtifactError> {
    let unpackable = |error: backhand::BackhandError| ArtifactError::Unpackable(error.to_string());
    let mut writer = FilesystemWriter::default();
    writer.set_compressor(image_compressor());
    writer.set_time(FIXED_MTIME);
    writer.set_root_mode(DIRECTORY_MODE);
    for (path, reader, permissions) in files {
        if let Some(parent) = Path::new(path)
            .parent()
            .filter(|parent| *parent != Path::new("/"))
        {
            writer
                .push_dir_all(parent, header(DIRECTORY_MODE))
                .map_err(unpackable)?;
        }
        writer
            .push_file(reader, path, header(permissions))
            .map_err(unpackable)?;
    }
    writer.write(image).map_err(unpackable)?;
    Ok(())
}

fn starts_as_filesystem_image(path: &Path) -> Result<bool, ArtifactError> {
    let mut head = Vec::with_capacity(EXT4_MAGIC_OFFSET + EXT4_MAGIC.len());
    std::fs::File::open(path)
        .and_then(|file| {
            file.take((EXT4_MAGIC_OFFSET + EXT4_MAGIC.len()) as u64)
                .read_to_end(&mut head)
        })
        .map_err(unpackable)?;
    Ok(is_filesystem_image(&head))
}

fn unpackable(error: impl std::fmt::Display) -> ArtifactError {
    ArtifactError::Unpackable(error.to_string())
}

/// The files a layer passes through on its way into the cache, beside where its image goes and
/// named for this daemon, so that a rename puts the image in place whole. Whatever is left of
/// them is removed when the fetch is done, however it went.
struct Staged {
    program: PathBuf,
    image: PathBuf,
}

impl Staged {
    fn beside(image_path: &Path) -> Self {
        let named = |suffix: &str| {
            let mut name = image_path.as_os_str().to_owned();
            name.push(format!(".{}.{suffix}", std::process::id()));
            PathBuf::from(name)
        };
        Self {
            program: named("program"),
            image: named("tmp"),
        }
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        for path in [&self.program, &self.image] {
            let _ = std::fs::remove_file(path);
        }
    }
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
        DesiredLayer::Oci { .. } => directory.join("oci.ext4"),
        DesiredLayer::Filesystem { .. } => directory.join(VERBATIM_IMAGE_FILENAME),
        DesiredLayer::Executable { destination_path, .. } => directory.join(format!(
            "executable-{}.squashfs",
            short_hash(destination_path.as_str())
        )),
    }
}

pub struct LayerImages {
    store: Arc<dyn ArtifactStore>,
    cache_dir: PathBuf,
    commands: Arc<dyn CommandRunner>,
}

impl LayerImages {
    pub fn new(
        store: Arc<dyn ArtifactStore>,
        cache_dir: PathBuf,
        commands: Arc<dyn CommandRunner>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            cache_dir,
            commands,
        })
    }
}

#[async_trait]
impl PayloadBuilder for LayerImages {
    async fn prepare(&self, layers: &[DesiredLayer]) -> Result<PreparedPayload, ArtifactError> {
        let mut layer_image_paths = Vec::with_capacity(layers.len());
        let mut fetched_bytes = 0;
        for layer in layers {
            let image = ensure_layer_image(&self.store, &self.cache_dir, layer, &self.commands).await?;
            layer_image_paths.push(image.path);
            fetched_bytes += image.fetched_bytes;
        }
        Ok(PreparedPayload {
            layer_image_paths,
            fetched_bytes,
        })
    }
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
    commands: &Arc<dyn CommandRunner>,
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

    let directory = image_path
        .parent()
        .expect("the image is one level inside the cache");
    make_directory(directory, CACHE_DIR_MODE).map_err(unpackable)?;
    let staged = Staged::beside(&image_path);
    // An OCI image is flattened from the archive whole, so it is read into memory as before; a
    // filesystem or a program is written to the cache as it arrives.
    let fetched_bytes = match layer {
        DesiredLayer::Oci {
            source: OciSource::Registry { repository, digest },
        } => {
            let pulled = registry::pull(repository, digest).await?;
            oci::prepare(pulled.archive, image_path.clone(), commands).await?;
            return Ok(LayerImage {
                path: image_path,
                fetched_bytes: pulled.fetched_bytes,
            });
        }
        DesiredLayer::Oci {
            source: OciSource::Archive(object),
        } => {
            let bytes = store.read_verified(object).await?;
            let fetched_bytes = bytes.len() as u64;
            oci::prepare(bytes, image_path.clone(), commands).await?;
            return Ok(LayerImage {
                path: image_path,
                fetched_bytes,
            });
        }
        DesiredLayer::Filesystem { object } => {
            let bytes = store.land_verified(object, &staged.image).await?;
            if !starts_as_filesystem_image(&staged.image)? {
                return Err(ArtifactError::NotAnImage {
                    digest: object.digest.clone(),
                });
            }
            bytes
        }
        DesiredLayer::Executable {
            object,
            destination_path,
        } => {
            let bytes = store.land_verified(object, &staged.program).await?;
            let program = std::fs::File::open(&staged.program).map_err(unpackable)?;
            let mut image = std::fs::File::create(&staged.image).map_err(unpackable)?;
            pack_into(
                vec![(destination_path.as_str(), Box::new(program), BINARY_MODE)],
                &mut image,
            )?;
            bytes
        }
    };
    std::fs::rename(&staged.image, &image_path).map_err(unpackable)?;
    tracing::info!(
        digest = %layer.digest(),
        size_bytes = fetched_bytes,
        image_bytes = std::fs::metadata(&image_path).map(|image| image.len()).unwrap_or_default(),
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
    use protocol::{ExecutablePath, Sha256Digest};
    use std::io::Cursor;

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

    async fn prepare_layer(
        store: &Arc<dyn ArtifactStore>,
        cache_dir: &Path,
        layer: &DesiredLayer,
    ) -> Result<LayerImage, ArtifactError> {
        let commands: Arc<dyn CommandRunner> = mocks::commands_succeeding().0;
        super::ensure_layer_image(store, cache_dir, layer, &commands).await
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
    async fn an_executable_is_packed_once_per_digest_and_holds_the_program_where_the_guest_runs_it() {
        let directory = tempfile::tempdir().unwrap();
        let store = store(artifact_bytes());
        let fetched = prepare_layer(&store, directory.path(), &layer(|_| {}))
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
        let again = prepare_layer(&store, directory.path(), &layer(|_| {}))
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
        let image_path = prepare_layer(&store(artifact_bytes()), directory.path(), &deep)
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
        let image_path = prepare_layer(&store(BASE_LAYER_BYTES.to_vec()), directory.path(), &base_layer())
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
        let error = prepare_layer(&store(artifact_bytes()), directory.path(), &not_an_image)
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
        let error = prepare_layer(&wrong, directory.path(), &layer(|_| {}))
            .await
            .unwrap_err();
        assert!(matches!(error, ArtifactError::DigestMismatch { .. }));
        assert!(error.message().contains("not to the"));
        assert!(!layer_image_path(directory.path(), &layer(|_| {})).exists());
    }

    #[tokio::test]
    async fn a_layer_that_lands_wrong_leaves_nothing_beside_the_cache_and_one_that_lands_right_only_its_image(
    ) {
        let directory = tempfile::tempdir().unwrap();
        let held = |directory: &Path| -> Vec<String> {
            let digest = directory.join(layer(|_| {}).digest().as_str());
            let mut names: Vec<_> = std::fs::read_dir(digest)
                .map(|entries| {
                    entries
                        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            names.sort();
            names
        };
        let wrong = store(b"something else entirely\n".to_vec());
        prepare_layer(&wrong, directory.path(), &layer(|_| {}))
            .await
            .unwrap_err();
        assert!(held(directory.path()).is_empty(), "{:?}", held(directory.path()));

        let image = prepare_layer(&store(artifact_bytes()), directory.path(), &layer(|_| {}))
            .await
            .unwrap();
        assert_eq!(image.fetched_bytes, artifact_bytes().len() as u64);
        assert_eq!(
            held(directory.path()),
            vec![image.path.file_name().unwrap().to_string_lossy().into_owned()]
        );
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
        let error = prepare_layer(&refusing, directory.path(), &layer(|_| {}))
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
            Ok(mocks::body(
                if key == &base_layer().stored_object().unwrap().object_key {
                    BASE_LAYER_BYTES.to_vec()
                } else {
                    ARTIFACT_BYTES.to_vec()
                },
            ))
        });
        let images = LayerImages::new(
            Arc::new(store),
            directory.path().to_path_buf(),
            mocks::commands_succeeding().0,
        );
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
        let image_path = prepare_layer(&store, directory.path(), &layer(|_| {}))
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

        prepare_layer(&store, directory.path(), &layer(|_| {}))
            .await
            .unwrap();
        let touched = std::fs::metadata(&image_path).unwrap().modified().unwrap();
        assert!(touched > backdated, "the image was not touched on a cache hit");
    }
}
