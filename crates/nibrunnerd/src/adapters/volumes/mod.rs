pub mod initial_contents;
pub mod local_file;
pub mod nbd;
pub mod zerofs;

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use protocol::{AppId, CheckpointId, DesiredVolume, ObjectKey, VolumeId};

use crate::adapters::volumes::initial_contents::StagedRoot;
use crate::ports::CommandRequest;

pub const SECTOR_SIZE_BYTES: u64 = 512;

pub fn align_to_sector(size_bytes: u64) -> u64 {
    size_bytes.div_ceil(SECTOR_SIZE_BYTES) * SECTOR_SIZE_BYTES
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VolumeError {
    #[error("a volume of {current} bytes cannot be resized down to {requested}")]
    ShrinkRefused { current: u64, requested: u64 },
    #[error("{device_path} did not answer a read of its superblock")]
    SuperblockUnreadable { device_path: String },
    #[error("the volume could not be made ready: {0}")]
    Unusable(String),
    #[error("the initial contents could not be laid out: {0}")]
    ContentsUnusable(String),
    #[error("this host does not serve {volume_id}")]
    NotHere { volume_id: VolumeId },
    #[error("{what} cannot be checkpointed")]
    NoCheckpoints { what: &'static str },
}

impl VolumeError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedVolume {
    pub volume_id: VolumeId,
    pub device_path: String,
    pub size_bytes: u64,
    pub storage_prefix: ObjectKey,
}

/// A volume as the host finds it. `attached` and `formatted` are two different facts: a device
/// can answer reads and still carry no filesystem, which is what a seeded format that was refused
/// leaves behind, and a guest booted onto that only dies failing to mount it. `attached` is
/// whether the device answers and proves it carries this volume, not whether the host holds one:
/// a `device_path` under a volume that is not attached is a device that stopped answering under
/// whatever holds it, or one that has yet to prove which volume it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedBacking {
    pub volume_id: VolumeId,
    pub size_bytes: u64,
    pub attached: bool,
    pub formatted: bool,
    pub device_path: Option<String>,
    pub storage_prefix: ObjectKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheReservation {
    pub disk_bytes: u64,
    pub memory_bytes: u64,
}

impl CacheReservation {
    pub fn memory_mib(self) -> u64 {
        self.memory_bytes.div_ceil(1_048_576)
    }
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait VolumeBackend: Send + Sync {
    async fn provision(&self, desired: &DesiredVolume) -> Result<AttachedVolume, VolumeError>;

    async fn attach(&self, volume_id: &VolumeId, app_id: &AppId) -> Result<AttachedVolume, VolumeError>;

    async fn detach(&self, volume_id: &VolumeId, app_id: &AppId) -> Result<(), VolumeError>;

    async fn teardown(&self, volume_id: &VolumeId, app_id: &AppId) -> Result<(), VolumeError>;

    async fn flush(&self) -> Result<(), VolumeError>;

    async fn create_checkpoint(&self, checkpoint_id: &CheckpointId) -> Result<(), VolumeError>;

    async fn delete_checkpoint(&self, checkpoint_id: &CheckpointId) -> Result<(), VolumeError>;

    async fn observe_checkpoints(&self) -> Vec<CheckpointId>;

    async fn observe(&self, owners: &BTreeMap<VolumeId, AppId>) -> Vec<ObservedBacking>;

    fn reserved_cache(&self) -> CacheReservation {
        CacheReservation::default()
    }
}

pub const SUPERBLOCK_MAGIC_OFFSET: u64 = 1080;
const EXT_MAGIC: u16 = 0xef53;

pub fn has_ext_magic(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && u16::from_le_bytes([bytes[0], bytes[1]]) == EXT_MAGIC
}

/// `s_uuid`, sixteen bytes at 0x68 into a superblock that itself starts 1024 bytes in.
const SUPERBLOCK_UUID_OFFSET: u64 = 1128;
pub const FILESYSTEM_UUID_BYTES: usize = 16;

pub type FilesystemUuid = [u8; FILESYSTEM_UUID_BYTES];

/// `s_log_block_size`, at 0x18 into the superblock: the block size is 1024 shifted by it.
const SUPERBLOCK_LOG_BLOCK_SIZE_OFFSET: u64 = 1048;

/// The block size of the ext filesystem on a device, or `None` when it holds none.
pub fn filesystem_block_bytes(path: &str) -> Option<u32> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path).ok()?;
    file.seek(SeekFrom::Start(SUPERBLOCK_MAGIC_OFFSET)).ok()?;
    let mut magic = [0u8; 2];
    file.read_exact(&mut magic).ok()?;
    if !has_ext_magic(&magic) {
        return None;
    }
    file.seek(SeekFrom::Start(SUPERBLOCK_LOG_BLOCK_SIZE_OFFSET))
        .ok()?;
    let mut log = [0u8; 4];
    file.read_exact(&mut log).ok()?;
    1024u32.checked_shl(u32::from_le_bytes(log))
}

/// The uuid mke2fs gave the filesystem, read from a device or from the image an export is served
/// from. `None` for anything that cannot name a filesystem apart from another: bytes that are not
/// an ext filesystem at all, and the all-zero uuid, which is what a filesystem made by something
/// that never set one carries. Telling volumes apart is all this is for, so unlike
/// [`has_ext_magic`] it says nothing about whether a device holds a filesystem to be mounted.
pub fn filesystem_uuid(path: &str) -> Option<FilesystemUuid> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path).ok()?;
    file.seek(SeekFrom::Start(SUPERBLOCK_MAGIC_OFFSET)).ok()?;
    let mut magic = [0u8; 2];
    file.read_exact(&mut magic).ok()?;
    if !has_ext_magic(&magic) {
        return None;
    }
    file.seek(SeekFrom::Start(SUPERBLOCK_UUID_OFFSET)).ok()?;
    let mut uuid = [0u8; FILESYSTEM_UUID_BYTES];
    file.read_exact(&mut uuid).ok()?;
    (uuid != [0u8; FILESYSTEM_UUID_BYTES]).then_some(uuid)
}

pub const FILESYSTEM_LABEL: &str = "nibrun-data";

/// A format that copies contents in is as long as the contents; one that does not is seconds.
const SEEDED_FORMAT_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// One ext4 over `device_path`, holding `contents` from the start when there are any. `-F`
/// is for a regular file, which mke2fs otherwise stops to ask about.
pub(crate) fn format_request(
    device_path: &str,
    contents: Option<&StagedRoot>,
    regular_file: bool,
) -> CommandRequest {
    // 4 KiB blocks whatever the size (mke2fs gives a small filesystem 1 KiB ones), to match the
    // drive's sectors; discard, so a fresh file starts with every block a hole.
    let mut command = vec!["mke2fs", "-q", "-t", "ext4", "-b", "4096", "-E", "discard"];
    if regular_file {
        command.push("-F");
    }
    command.extend(["-L", FILESYSTEM_LABEL]);
    let root;
    if let Some(staged) = contents {
        root = staged.path().display().to_string();
        command.extend(["-d", root.as_str()]);
    }
    command.push(device_path);
    let mut request = CommandRequest::new(&command);
    if contents.is_some() {
        request.timeout = SEEDED_FORMAT_TIMEOUT;
    }
    request
}

/// Zero the bytes an ext filesystem's magic lives in. A seeded format lays the superblock down
/// and only then copies the contents in, so a `mke2fs -d` that fails partway can leave the magic
/// behind on a filesystem that was never finished. Wiping it means the superblock short-circuit
/// reads the device as unformatted and the next pass formats it afresh, rather than taking a
/// half-made filesystem as done and booting a guest onto it.
pub(crate) fn discard_superblock(device_path: &str) {
    use std::io::{Seek, SeekFrom, Write};
    let wiped = std::fs::OpenOptions::new()
        .write(true)
        .open(device_path)
        .and_then(|mut device| {
            device.seek(SeekFrom::Start(SUPERBLOCK_MAGIC_OFFSET))?;
            device.write_all(&[0u8; 2])
        });
    if let Err(error) = wiped {
        tracing::warn!(
            device = %device_path,
            %error,
            "a failed format's superblock could not be wiped; a stale one may be read as formatted"
        );
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn a_size_is_rounded_up_to_a_sector_so_no_tail_is_invisible_to_the_guest() {
        assert_eq!(align_to_sector(0), 0);
        assert_eq!(align_to_sector(1), SECTOR_SIZE_BYTES);
        assert_eq!(align_to_sector(SECTOR_SIZE_BYTES), SECTOR_SIZE_BYTES);
        assert_eq!(align_to_sector(SECTOR_SIZE_BYTES + 1), SECTOR_SIZE_BYTES * 2);
    }

    #[test]
    fn only_the_ext_magic_counts_as_a_formatted_filesystem() {
        assert!(has_ext_magic(&0xef53u16.to_le_bytes()));
        assert!(!has_ext_magic(&0u16.to_le_bytes()));
        assert!(!has_ext_magic(&[0x53]));
    }

    pub(crate) fn image(
        directory: &std::path::Path,
        name: &str,
        formatted: bool,
        uuid: FilesystemUuid,
    ) -> String {
        let path = directory.join(name);
        let mut bytes = vec![0u8; 2048];
        if formatted {
            bytes[SUPERBLOCK_MAGIC_OFFSET as usize..SUPERBLOCK_MAGIC_OFFSET as usize + 2]
                .copy_from_slice(&0xef53u16.to_le_bytes());
        }
        bytes[SUPERBLOCK_UUID_OFFSET as usize..SUPERBLOCK_UUID_OFFSET as usize + FILESYSTEM_UUID_BYTES]
            .copy_from_slice(&uuid);
        std::fs::write(&path, &bytes).unwrap();
        path.display().to_string()
    }

    #[test]
    fn a_filesystem_is_named_by_the_uuid_in_its_superblock() {
        let directory = tempfile::tempdir().unwrap();
        let uuid = [7u8; FILESYSTEM_UUID_BYTES];
        assert_eq!(
            filesystem_uuid(&image(directory.path(), "one", true, uuid)),
            Some(uuid)
        );
        assert_eq!(
            filesystem_uuid(&image(
                directory.path(),
                "another",
                true,
                [9u8; FILESYSTEM_UUID_BYTES]
            )),
            Some([9u8; FILESYSTEM_UUID_BYTES])
        );
    }

    #[test]
    fn a_filesystem_block_size_is_read_from_its_superblock_and_nothing_else_has_one() {
        let directory = tempfile::tempdir().unwrap();
        let path = image(directory.path(), "one", true, [7u8; FILESYSTEM_UUID_BYTES]);
        for (log, bytes) in [(0u32, 1024), (2, 4096)] {
            let mut image = std::fs::read(&path).unwrap();
            image[1048..1052].copy_from_slice(&log.to_le_bytes());
            std::fs::write(&path, image).unwrap();
            assert_eq!(filesystem_block_bytes(&path), Some(bytes));
        }
        let blank = image(directory.path(), "blank", false, [7u8; FILESYSTEM_UUID_BYTES]);
        assert_eq!(filesystem_block_bytes(&blank), None);
    }

    #[test]
    fn nothing_that_cannot_tell_one_volume_from_another_is_read_as_a_uuid() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            filesystem_uuid(&image(
                directory.path(),
                "bare",
                false,
                [7u8; FILESYSTEM_UUID_BYTES]
            )),
            None,
            "bytes that are not a filesystem name no volume"
        );
        assert_eq!(
            filesystem_uuid(&image(
                directory.path(),
                "unnamed",
                true,
                [0u8; FILESYSTEM_UUID_BYTES]
            )),
            None,
            "the uuid every filesystem without one carries names no volume either"
        );
        std::fs::write(directory.path().join("short"), b"tiny").unwrap();
        assert_eq!(
            filesystem_uuid(&directory.path().join("short").display().to_string()),
            None
        );
        assert_eq!(
            filesystem_uuid(&directory.path().join("absent").display().to_string()),
            None
        );
    }

    #[test]
    fn the_magic_is_read_little_endian_so_the_bytes_the_other_way_round_are_not_it() {
        assert!(!has_ext_magic(&0xef53u16.to_be_bytes()));
        assert!(!has_ext_magic(&[]));
        assert!(has_ext_magic(&[0x53, 0xef, 0x00, 0x01]));
    }

    #[test]
    fn a_cache_is_held_back_in_whole_mebibytes_so_none_of_it_is_handed_out_twice() {
        assert_eq!(CacheReservation::default().memory_mib(), 0);
        assert_eq!(
            CacheReservation {
                memory_bytes: 1,
                disk_bytes: 0
            }
            .memory_mib(),
            1
        );
        assert_eq!(
            CacheReservation {
                memory_bytes: 2 * 1_048_576,
                disk_bytes: 0
            }
            .memory_mib(),
            2
        );
        assert_eq!(
            CacheReservation {
                memory_bytes: 2 * 1_048_576 + 1,
                disk_bytes: 0
            }
            .memory_mib(),
            3
        );
    }

    #[test]
    fn every_way_a_volume_can_refuse_reads_as_a_sentence_naming_what_went_wrong() {
        assert_eq!(
            VolumeError::ShrinkRefused {
                current: 2048,
                requested: 1024
            }
            .message(),
            "a volume of 2048 bytes cannot be resized down to 1024"
        );
        assert_eq!(
            VolumeError::SuperblockUnreadable {
                device_path: "/dev/nbd0".into()
            }
            .message(),
            "/dev/nbd0 did not answer a read of its superblock"
        );
        assert_eq!(
            VolumeError::NotHere {
                volume_id: VolumeId::parse("vol-1").unwrap()
            }
            .message(),
            "this host does not serve vol-1"
        );
        assert_eq!(
            VolumeError::NoCheckpoints { what: "a local file" }.message(),
            "a local file cannot be checkpointed"
        );
        assert_eq!(
            VolumeError::Unusable("the device would not open".into()).message(),
            "the volume could not be made ready: the device would not open"
        );
        assert_eq!(
            VolumeError::ContentsUnusable("the archive could not be read".into()).message(),
            "the initial contents could not be laid out: the archive could not be read"
        );
    }

    #[tokio::test]
    async fn a_format_names_the_label_the_device_last_and_a_root_only_when_it_has_one() {
        let plain = format_request("/dev/nbd0", None, false);
        assert_eq!(
            plain.command,
            [
                "mke2fs",
                "-q",
                "-t",
                "ext4",
                "-b",
                "4096",
                "-E",
                "discard",
                "-L",
                FILESYSTEM_LABEL,
                "/dev/nbd0"
            ]
        );
        assert_eq!(plain.timeout, CommandRequest::new(&[]).timeout);

        let directory = tempfile::tempdir().unwrap();
        let archive = initial_contents::tests::archive();
        let staged = initial_contents::tests::staging(directory.path(), archive.clone())
            .stage(
                &VolumeId::parse("vol-1").unwrap(),
                &crate::test_support::initial_contents(&archive, "/"),
            )
            .await
            .unwrap();
        let seeded = format_request("/var/lib/nibrunner/volumes/vol-1", Some(&staged), true);
        assert_eq!(
            seeded.command,
            [
                "mke2fs",
                "-q",
                "-t",
                "ext4",
                "-b",
                "4096",
                "-E",
                "discard",
                "-F",
                "-L",
                FILESYSTEM_LABEL,
                "-d",
                &staged.path().display().to_string(),
                "/var/lib/nibrunner/volumes/vol-1"
            ]
        );
        assert_eq!(seeded.timeout, SEEDED_FORMAT_TIMEOUT);
    }

    #[test]
    fn a_size_that_would_overflow_a_sector_count_is_not_rounded_into_nothing() {
        assert_eq!(align_to_sector(u64::MAX - SECTOR_SIZE_BYTES), u64::MAX - 511);
    }
}
