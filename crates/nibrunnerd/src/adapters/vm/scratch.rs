use std::num::NonZeroU32;
use std::path::Path;

use crate::ports::{CommandRunner, CommandRunnerExt};

/// A disk scratch lives in its microVM's working directory, so whatever takes that directory away
/// takes the scratch with it.
pub const SCRATCH_DISK_FILENAME: &str = "scratch.ext4";
const SCRATCH_DISK_MODE: u32 = 0o600;
const MIB: u64 = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("the scratch disk {path} could not be made: {reason}")]
pub struct ScratchDiskError {
    pub path: String,
    pub reason: String,
}

/// An empty ext4 filesystem of `mib` at `path`, sparse, so the host's disk is spent only as the
/// guest writes. Whatever was at `path` is replaced: what an earlier boot wrote is not the next
/// one's to find.
pub async fn make_disk(
    commands: &dyn CommandRunner,
    path: &Path,
    mib: NonZeroU32,
) -> Result<(), ScratchDiskError> {
    use std::os::unix::fs::OpenOptionsExt;
    let device_path = path.display().to_string();
    let failed = |reason: String| ScratchDiskError {
        path: device_path.clone(),
        reason,
    };
    remove_disk(path).map_err(|error| failed(error.to_string()))?;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(SCRATCH_DISK_MODE)
        .open(path)
        .and_then(|file| file.set_len(u64::from(mib.get()) * MIB))
        .map_err(|error| failed(error.to_string()))?;
    commands
        .stdout_of(crate::adapters::volumes::format_request(&device_path, None, true))
        .await
        .map_err(|error| failed(error.message()))?;
    Ok(())
}

pub fn remove_disk(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}
