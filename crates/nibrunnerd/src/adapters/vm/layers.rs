use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use backhand::compression::{CompressionOptions, Compressor, Zstd};
use backhand::{FilesystemCompressor, FilesystemWriter, NodeHeader};
use protocol::{DesiredLayer, DownloadUrl, OciSource, Sha256Digest, TarEntry, ZipEntry};

use crate::json_store::make_directory;
use crate::ports::{
    ArtifactError, ArtifactStore, ArtifactStoreExt, CommandRunner, LayerRetention, PayloadBuilder,
    PreparedPayload, RetainedLayers,
};

mod oci;
mod registry;

pub const VERBATIM_IMAGE_FILENAME: &str = "layer.img";

pub const BINARY_MODE: u16 = 0o755;
const CONFIG_MODE: u16 = 0o600;
const DIRECTORY_MODE: u16 = 0o755;
const CACHE_DIR_MODE: u32 = 0o755;
const VM_DIR_MODE: u32 = 0o700;

/// What one program may inflate to, so that a small zip cannot fill the host's memory.
const MAX_PROGRAM_BYTES: u64 = 512 * 1024 * 1024;

/// What the xz decoder may hold. `xz -9`, the most any writer asks of a reader, needs 65 MiB; a
/// stream that asks for more is refused rather than handed the host's memory.
const XZ_DECODER_LIMIT_KIB: u32 = 128 * 1024;

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
    body: PathBuf,
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
            body: named("body"),
            program: named("program"),
            image: named("tmp"),
        }
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        for path in [&self.body, &self.program, &self.image] {
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
    commands: Arc<dyn CommandRunner>,
    max_bytes: Option<u64>,
    // A prune holds it whole, a prepare shares it, so no layer is removed from under a boot.
    access: tokio::sync::RwLock<()>,
}

impl LayerImages {
    pub fn new(
        store: Arc<dyn ArtifactStore>,
        cache_dir: PathBuf,
        commands: Arc<dyn CommandRunner>,
    ) -> Arc<Self> {
        Self::bounded(store, cache_dir, commands, None)
    }

    pub fn bounded(
        store: Arc<dyn ArtifactStore>,
        cache_dir: PathBuf,
        commands: Arc<dyn CommandRunner>,
        max_bytes: Option<u64>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            cache_dir,
            commands,
            max_bytes,
            access: tokio::sync::RwLock::new(()),
        })
    }
}

// A start prepares its layers and only then hands them to Firecracker, outside the lock a prune
// takes, and a cold boot from a wake does so before its record says it is starting. A layer
// touched this recently is one a boot may be about to open, so no cap evicts it.
const RECENTLY_PREPARED: std::time::Duration = std::time::Duration::from_secs(60);

/// One digest's directory in the cache: what it holds, and when a prepare last touched it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedLayer {
    digest: Sha256Digest,
    bytes: u64,
    last_used: std::time::SystemTime,
}

/// The layers a cap takes, least recently used first, until what is left fits under it. A pinned
/// layer, or one a boot may be opening, is never taken, even when the cache stays over the cap.
fn evictions<'a>(
    held: &'a [CachedLayer],
    pinned: &std::collections::BTreeSet<Sha256Digest>,
    max_bytes: u64,
    now: std::time::SystemTime,
) -> Vec<&'a CachedLayer> {
    let mut total: u64 = held.iter().map(|layer| layer.bytes).sum();
    let mut coldest_first: Vec<&CachedLayer> = held
        .iter()
        .filter(|layer| !pinned.contains(&layer.digest))
        .filter(|layer| now.duration_since(layer.last_used).unwrap_or_default() >= RECENTLY_PREPARED)
        .collect();
    coldest_first.sort_by_key(|layer| layer.last_used);
    coldest_first
        .into_iter()
        .take_while(|layer| {
            let over = total > max_bytes;
            total = total.saturating_sub(layer.bytes);
            over
        })
        .collect()
}

fn measure(directory: &Path, digest: Sha256Digest) -> std::io::Result<CachedLayer> {
    let mut layer = CachedLayer {
        digest,
        bytes: 0,
        last_used: std::time::SystemTime::UNIX_EPOCH,
    };
    for entry in std::fs::read_dir(directory)? {
        let metadata = entry?.metadata()?;
        layer.bytes += metadata.len();
        layer.last_used = layer.last_used.max(metadata.modified()?);
    }
    Ok(layer)
}

fn remove(path: &Path) -> std::io::Result<()> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}

fn prune(
    cache_dir: &Path,
    retention: &LayerRetention,
    max_bytes: Option<u64>,
) -> std::io::Result<RetainedLayers> {
    let entries = match std::fs::read_dir(cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(RetainedLayers::default()),
        Err(error) => return Err(error),
    };
    let mut held = Vec::new();
    for entry in entries {
        let entry = entry?;
        let Some(digest) = entry
            .file_name()
            .to_str()
            .and_then(|name| Sha256Digest::parse(name).ok())
        else {
            continue;
        };
        if !retention.named.contains(&digest) {
            remove(&entry.path())?;
        } else if entry.file_type()?.is_dir() {
            held.push(measure(&entry.path(), digest)?);
        }
    }
    let mut retained = RetainedLayers {
        held_bytes: held.iter().map(|layer| layer.bytes).sum(),
        evicted: 0,
    };
    let Some(max_bytes) = max_bytes else {
        return Ok(retained);
    };
    for layer in evictions(&held, &retention.pinned, max_bytes, std::time::SystemTime::now()) {
        std::fs::remove_dir_all(cache_dir.join(layer.digest.as_str()))?;
        retained.held_bytes -= layer.bytes;
        retained.evicted += 1;
        tracing::info!(digest = %layer.digest, bytes = layer.bytes, "layer evicted from the cache to keep it under cache_max_gib");
    }
    Ok(retained)
}

#[async_trait]
impl PayloadBuilder for LayerImages {
    async fn retain(&self, retention: &LayerRetention) -> Result<RetainedLayers, ArtifactError> {
        let _access = self.access.write().await;
        let (cache_dir, retention, max_bytes) = (self.cache_dir.clone(), retention.clone(), self.max_bytes);
        tokio::task::spawn_blocking(move || prune(&cache_dir, &retention, max_bytes))
            .await
            .map_err(|error| ArtifactError::Transfer(error.to_string()))?
            .map_err(|error| ArtifactError::Transfer(error.to_string()))
    }

    async fn prepare(&self, layers: &[DesiredLayer]) -> Result<PreparedPayload, ArtifactError> {
        let _access = self.access.read().await;
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

/// Where in what was downloaded the program is.
#[derive(Clone, Copy)]
enum Within<'a> {
    Body,
    Zip(&'a ZipEntry),
    TarXz(&'a TarEntry),
}

impl<'a> Within<'a> {
    fn of(zip_entry: Option<&'a ZipEntry>, tar_entry: Option<&'a TarEntry>) -> Self {
        match (zip_entry, tar_entry) {
            (Some(entry), _) => Within::Zip(entry),
            (None, Some(entry)) => Within::TarXz(entry),
            (None, None) => Within::Body,
        }
    }
}

/// Copies at most a program's worth of `from` into `program`, hashing it on the way.
fn copy_program(from: impl Read, program: &Path) -> Result<String, ArtifactError> {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    let mut file = std::fs::File::create(program).map_err(unpackable)?;
    let mut limited = from.take(MAX_PROGRAM_BYTES);
    let mut buffer = vec![0u8; 1 << 16];
    loop {
        let read = limited.read(&mut buffer).map_err(unpackable)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        std::io::Write::write_all(&mut file, &buffer[..read]).map_err(unpackable)?;
    }
    Ok(hex::encode(hasher.finalize()))
}

fn from_zip(
    body: &Path,
    entry: &ZipEntry,
    url: &DownloadUrl,
    program: &Path,
) -> Result<String, ArtifactError> {
    let mut archive =
        zip::ZipArchive::new(std::fs::File::open(body).map_err(unpackable)?).map_err(unpackable)?;
    let file = archive
        .by_name(entry.as_str())
        .map_err(|_| ArtifactError::NotInArchive {
            url: url.clone(),
            entry: entry.as_str().to_owned(),
        })?;
    copy_program(file, program)
}

// GNU tar run as `tar -C dir -cJf x.tar.xz .` names every entry from `./`, so the same archive is
// read the same whether or not its writer put that in front.
fn names_entry(path: &[u8], entry: &TarEntry) -> bool {
    path.strip_prefix(b"./").unwrap_or(path) == entry.as_str().as_bytes()
}

fn from_tar_xz(
    body: &Path,
    entry: &TarEntry,
    url: &DownloadUrl,
    program: &Path,
) -> Result<String, ArtifactError> {
    let compressed = std::io::BufReader::new(std::fs::File::open(body).map_err(unpackable)?);
    let decoder = lzma_rust2::XzReader::new_mem_limit(compressed, true, XZ_DECODER_LIMIT_KIB);
    let mut archive = tar::Archive::new(decoder);
    for file in archive.entries().map_err(unpackable)? {
        let file = file.map_err(unpackable)?;
        if !names_entry(&file.path_bytes(), entry) {
            continue;
        }
        if !file.header().entry_type().is_file() {
            return Err(ArtifactError::NotAFile {
                url: url.clone(),
                entry: entry.as_str().to_owned(),
            });
        }
        return copy_program(file, program);
    }
    Err(ArtifactError::NotInArchive {
        url: url.clone(),
        entry: entry.as_str().to_owned(),
    })
}

/// Downloads `url` beside the cache and leaves the program in it at the staged program path,
/// checked against `digest`: the body itself, or the entry of the archive it is that the
/// document names.
async fn downloaded_program(
    store: &Arc<dyn ArtifactStore>,
    url: &DownloadUrl,
    digest: &Sha256Digest,
    within: Within<'_>,
    staged: &Staged,
) -> Result<u64, ArtifactError> {
    let landed = crate::ports::land_within(
        store.download(url).await?,
        &staged.body,
        crate::ports::MAX_DOWNLOAD_BYTES,
    )
    .await?;
    let actual = match within {
        Within::Body => {
            std::fs::rename(&staged.body, &staged.program).map_err(unpackable)?;
            landed.sha256
        }
        Within::Zip(entry) => from_zip(&staged.body, entry, url, &staged.program)?,
        Within::TarXz(entry) => from_tar_xz(&staged.body, entry, url, &staged.program)?,
    };
    if actual != digest.as_str() {
        return Err(ArtifactError::DigestMismatch {
            expected: digest.clone(),
            actual,
        });
    }
    Ok(landed.bytes)
}

/// Packs the staged program into the staged image, at the path the guest runs it from.
fn pack_program(staged: &Staged, destination_path: &protocol::ExecutablePath) -> Result<(), ArtifactError> {
    let program = std::fs::File::open(&staged.program).map_err(unpackable)?;
    let mut image = std::fs::File::create(&staged.image).map_err(unpackable)?;
    pack_into(
        vec![(destination_path.as_str(), Box::new(program), BINARY_MODE)],
        &mut image,
    )
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
        DesiredLayer::DownloadedExecutable {
            url,
            digest,
            zip_entry,
            tar_entry,
            destination_path,
        } => {
            let bytes = downloaded_program(
                store,
                url,
                digest,
                Within::of(zip_entry.as_ref(), tar_entry.as_ref()),
                &staged,
            )
            .await?;
            pack_program(&staged, destination_path)?;
            bytes
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
            pack_program(&staged, destination_path)?;
            bytes
        }
    };
    std::fs::rename(&staged.image, &image_path).map_err(unpackable)?;
    tracing::info!(
        digest = %layer.digest(),
        size_bytes = fetched_bytes,
        image_bytes = std::fs::metadata(&image_path).map(|image| image.len()).unwrap_or_default(),
        packed = matches!(
            layer,
            DesiredLayer::Executable { .. } | DesiredLayer::DownloadedExecutable { .. }
        ),
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

    fn downloaded(body: &[u8], zip_entry: Option<&str>) -> (DesiredLayer, Arc<dyn ArtifactStore>) {
        downloaded_from(body, zip_entry, None)
    }

    fn downloaded_from(
        body: &[u8],
        zip_entry: Option<&str>,
        tar_entry: Option<&str>,
    ) -> (DesiredLayer, Arc<dyn ArtifactStore>) {
        let served = body.to_vec();
        let mut artifacts = crate::ports::MockArtifactStore::new();
        artifacts
            .expect_download()
            .returning(move |_| Ok(mocks::body(served.clone())));
        let layer = DesiredLayer::DownloadedExecutable {
            url: DownloadUrl::parse("https://example.test/program").unwrap(),
            digest: Sha256Digest::parse(hex::encode(sha2::Sha256::digest(artifact_bytes()))).unwrap(),
            zip_entry: zip_entry.map(|entry| ZipEntry::parse(entry).unwrap()),
            tar_entry: tar_entry.map(|entry| TarEntry::parse(entry).unwrap()),
            destination_path: ExecutablePath::parse("/opt/tool/tool").unwrap(),
        };
        (layer, Arc::new(artifacts))
    }

    enum Member<'a> {
        File(&'a str, &'a [u8]),
        Directory(&'a str),
        Symlink(&'a str, &'a str),
    }

    fn tarred_xz(members: &[Member<'_>]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for member in members {
            let mut header = tar::Header::new_gnu();
            match member {
                Member::File(path, bytes) => {
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_mode(0o755);
                    header.set_size(bytes.len() as u64);
                    builder.append_data(&mut header, path, *bytes).unwrap();
                }
                Member::Directory(path) => {
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_mode(0o755);
                    header.set_size(0);
                    builder.append_data(&mut header, path, std::io::empty()).unwrap();
                }
                Member::Symlink(path, target) => {
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_size(0);
                    builder.append_link(&mut header, path, target).unwrap();
                }
            }
        }
        let tar = builder.into_inner().unwrap();
        let mut writer =
            lzma_rust2::XzWriter::new(Vec::new(), lzma_rust2::XzOptions::with_preset(6)).unwrap();
        std::io::Write::write_all(&mut writer, &tar).unwrap();
        writer.finish().unwrap()
    }

    #[tokio::test]
    async fn the_program_is_taken_from_the_tar_entry_the_document_names_and_its_digest_is_the_programs() {
        for written_as in ["zig-linux/zig", "./zig-linux/zig"] {
            let directory = tempfile::tempdir().unwrap();
            let archive = tarred_xz(&[
                Member::Directory("zig-linux/"),
                Member::File("zig-linux/README", b"not it"),
                Member::File(written_as, &artifact_bytes()),
            ]);
            let (layer, store) = downloaded_from(&archive, None, Some("zig-linux/zig"));
            let image = prepare_layer(&store, directory.path(), &layer).await.unwrap();
            assert_eq!(
                read_back(&std::fs::read(image.path).unwrap(), "/opt/tool/tool"),
                artifact_bytes(),
                "{written_as}"
            );
        }
    }

    #[tokio::test]
    async fn a_tar_entry_the_archive_does_not_hold_is_named() {
        let directory = tempfile::tempdir().unwrap();
        let archive = tarred_xz(&[Member::File("README", b"x")]);
        let (layer, store) = downloaded_from(&archive, None, Some("zig"));
        let error = prepare_layer(&store, directory.path(), &layer).await.unwrap_err();
        assert!(matches!(error, ArtifactError::NotInArchive { ref entry, .. } if entry == "zig"));
    }

    #[tokio::test]
    async fn a_tar_entry_that_is_a_symlink_or_a_directory_is_no_program_to_run() {
        for member in [Member::Symlink("zig", "/bin/sh"), Member::Directory("zig")] {
            let directory = tempfile::tempdir().unwrap();
            let (layer, store) = downloaded_from(&tarred_xz(&[member]), None, Some("zig"));
            let error = prepare_layer(&store, directory.path(), &layer).await.unwrap_err();
            assert!(
                matches!(error, ArtifactError::NotAFile { ref entry, .. } if entry == "zig"),
                "{error}"
            );
            assert!(!layer_image_path(directory.path(), &layer).exists());
        }
    }

    #[tokio::test]
    async fn a_tar_entry_named_in_something_that_is_not_a_tar_xz_is_refused_as_unpackable() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded_from(&zipped(&[("zig", &artifact_bytes())]), None, Some("zig"));
        let error = prepare_layer(&store, directory.path(), &layer).await.unwrap_err();
        assert!(matches!(error, ArtifactError::Unpackable(_)), "{error}");
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
        let image = prepare_layer(&store, directory.path(), &layer).await.unwrap();
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
        let image = prepare_layer(&store, directory.path(), &layer).await.unwrap();
        assert_eq!(
            read_back(&std::fs::read(image.path).unwrap(), "/opt/tool/tool"),
            artifact_bytes()
        );
    }

    #[tokio::test]
    async fn a_download_that_hashes_to_something_else_never_reaches_a_guest() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded(b"something else", None);
        let error = prepare_layer(&store, directory.path(), &layer).await.unwrap_err();
        assert!(matches!(error, ArtifactError::DigestMismatch { .. }));
        assert!(!layer_image_path(directory.path(), &layer).exists());
    }

    #[tokio::test]
    async fn an_entry_the_archive_does_not_hold_is_named() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded(&zipped(&[("README", b"x")]), Some("tool"));
        let error = prepare_layer(&store, directory.path(), &layer).await.unwrap_err();
        assert!(matches!(error, ArtifactError::NotInArchive { ref entry, .. } if entry == "tool"));
    }

    #[tokio::test]
    async fn a_downloaded_program_is_fetched_once_however_often_it_is_asked_for() {
        let directory = tempfile::tempdir().unwrap();
        let (layer, store) = downloaded(&artifact_bytes(), None);
        prepare_layer(&store, directory.path(), &layer).await.unwrap();
        let again = prepare_layer(&store, directory.path(), &layer).await.unwrap();
        assert_eq!(again.fetched_bytes, 0);
    }

    fn cached(digest: char, bytes: u64, idle_for_secs: u64, now: std::time::SystemTime) -> CachedLayer {
        CachedLayer {
            digest: Sha256Digest::parse(digest.to_string().repeat(64)).unwrap(),
            bytes,
            last_used: now - std::time::Duration::from_secs(idle_for_secs),
        }
    }

    #[test]
    fn a_cap_takes_the_least_recently_used_layers_until_what_is_left_fits() {
        let now = std::time::SystemTime::now();
        let held = [
            cached('a', 40, 600, now),
            cached('b', 40, 6_000, now),
            cached('c', 40, 3_000, now),
        ];
        let taken: Vec<_> = evictions(&held, &Default::default(), 60, now)
            .into_iter()
            .map(|layer| layer.digest.clone())
            .collect();
        assert_eq!(taken, vec![held[1].digest.clone(), held[2].digest.clone()]);
        assert!(
            evictions(&held, &Default::default(), 120, now).is_empty(),
            "a cache at its cap is left alone"
        );
    }

    #[test]
    fn a_layer_an_app_stands_on_or_a_boot_may_be_opening_is_kept_even_over_the_cap() {
        let now = std::time::SystemTime::now();
        let held = [
            cached('a', 40, 6_000, now),
            cached('b', 40, 5, now),
            cached('c', 40, 3_000, now),
        ];
        let pinned = [held[0].digest.clone()].into();
        let taken: Vec<_> = evictions(&held, &pinned, 0, now)
            .into_iter()
            .map(|layer| layer.digest.clone())
            .collect();
        assert_eq!(taken, vec![held[2].digest.clone()]);
    }

    fn backdate(directory: &Path, by_secs: u64) {
        let then = std::time::SystemTime::now() - std::time::Duration::from_secs(by_secs);
        for entry in std::fs::read_dir(directory).unwrap() {
            std::fs::File::options()
                .write(true)
                .open(entry.unwrap().path())
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(then))
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_capped_cache_evicts_the_coldest_named_layer_and_fetches_it_again_when_asked() {
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
        let images = LayerImages::bounded(
            Arc::new(store),
            directory.path().to_path_buf(),
            mocks::commands_succeeding().0,
            Some(1),
        );
        let both = images.prepare(&[base_layer(), layer(|_| {})]).await.unwrap();
        backdate(both.layer_image_paths[0].parent().unwrap(), 7_200);
        backdate(both.layer_image_paths[1].parent().unwrap(), 3_600);
        let retention = LayerRetention {
            named: [base_layer().digest().clone(), layer(|_| {}).digest().clone()].into(),
            pinned: [layer(|_| {}).digest().clone()].into(),
        };

        let retained = images.retain(&retention).await.unwrap();

        assert_eq!(retained.evicted, 1);
        assert!(!both.layer_image_paths[0].exists());
        assert!(
            both.layer_image_paths[1].exists(),
            "a pinned layer outlives the cap"
        );
        assert_eq!(
            retained.held_bytes,
            std::fs::metadata(&both.layer_image_paths[1]).unwrap().len()
        );
        let again = images.prepare(&[base_layer()]).await.unwrap();
        assert_eq!(again.fetched_bytes, BASE_LAYER_BYTES.len() as u64);
    }

    #[tokio::test]
    async fn a_layer_no_document_or_record_names_is_removed_and_a_named_one_kept() {
        let directory = tempfile::tempdir().unwrap();
        let images = LayerImages::new(
            store(artifact_bytes()),
            directory.path().to_path_buf(),
            mocks::commands_succeeding().0,
        );
        let path = images.prepare(&[layer(|_| {})]).await.unwrap().layer_image_paths[0].clone();
        let named = LayerRetention {
            named: [layer(|_| {}).digest().clone()].into(),
            pinned: Default::default(),
        };
        images.retain(&named).await.unwrap();
        assert!(path.exists(), "a named layer is kept");
        images.retain(&LayerRetention::default()).await.unwrap();
        assert!(!path.exists(), "an unnamed layer is gone");
        images.retain(&LayerRetention::default()).await.unwrap();
    }
}
