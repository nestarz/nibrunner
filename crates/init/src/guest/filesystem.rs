use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use guest_contract::filesystem::{
    decode_request, decode_request_header, encode_compute, encode_details, encode_listing, encode_refusal,
    encode_reply, encode_usage, encode_written, EntryDetails, GuestFilesystemRequest, MeasuredBytes,
    MeasuredCompute, FRAME_HEADER_BYTES, STATUS_MALFORMED_REQUEST,
};
use guest_contract::paths;
use protocol::{FilesystemEntryKind, GuestPath};

use crate::guest::{log, transport};
use guest_contract::channels::{Channel, ChannelTransport};

const STATUS_OK: u8 = 0;
const STATUS_NOT_FOUND: u8 = 1;
const STATUS_WRONG_KIND: u8 = 2;
const STATUS_ALREADY_THERE: u8 = 3;
const STATUS_NOT_EMPTY: u8 = 4;
const STATUS_OUT_OF_VOLUME: u8 = 5;
const STATUS_FAILED: u8 = 7;

pub(crate) fn serve(channel_transport: ChannelTransport) -> ! {
    let Ok(listener) = transport::listener(channel_transport, Channel::Filesystem) else {
        log("the filesystem port could not be opened; this tenant's files cannot be browsed");
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    };
    loop {
        match transport::accept_one(&listener) {
            Ok(connection) => answer_all(connection),
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn answer_all(connection: OwnedFd) {
    let mut wire = std::fs::File::from(connection);
    loop {
        let mut header = [0u8; FRAME_HEADER_BYTES];
        if wire.read_exact(&mut header).is_err() {
            return;
        }
        let Ok(header) = decode_request_header(&header) else {
            let _ = wire.write_all(&encode_refusal(STATUS_MALFORMED_REQUEST));
            return;
        };
        let mut body = vec![0u8; header.body_length];
        if wire.read_exact(&mut body).is_err() {
            return;
        }
        let reply = match decode_request(header, &body) {
            Ok(request) => answer(&request),
            Err(error) => {
                log(&format!("{error}"));
                encode_refusal(STATUS_MALFORMED_REQUEST)
            }
        };
        if wire.write_all(&reply).is_err() {
            return;
        }
    }
}

fn resolve(path: &GuestPath) -> Option<PathBuf> {
    let root = Path::new(paths::VOLUME_UPPER_DIR);
    let relative = path.as_str().trim_start_matches('/');
    let resolved = if relative.is_empty() {
        root.to_path_buf()
    } else {
        root.join(relative)
    };
    resolved.starts_with(root).then_some(resolved)
}

fn answer(request: &GuestFilesystemRequest) -> Vec<u8> {
    match request {
        GuestFilesystemRequest::Usage => usage(),
        GuestFilesystemRequest::Compute => compute(),
        GuestFilesystemRequest::List { path } => with_path(path, list),
        GuestFilesystemRequest::Stat { path } => with_path(path, stat),
        GuestFilesystemRequest::MakeDirectory { path } => with_path(path, make_directory),
        GuestFilesystemRequest::Remove { path } => with_path(path, remove),
        GuestFilesystemRequest::Read { path, offset, length } => {
            with_path(path, |resolved| read(resolved, *offset, *length))
        }
        GuestFilesystemRequest::Write {
            path,
            offset,
            content,
            truncate,
        } => with_path(path, |resolved| write(resolved, *offset, content, *truncate)),
        GuestFilesystemRequest::Move { path, destination } => match (resolve(path), resolve(destination)) {
            (Some(from), Some(to)) => match std::fs::rename(&from, &to) {
                Ok(()) => encode_reply(STATUS_OK, &[]),
                Err(error) => encode_refusal(status_for(&error)),
            },
            _ => encode_refusal(STATUS_OUT_OF_VOLUME),
        },
    }
}

fn with_path(path: &GuestPath, answer: impl FnOnce(&Path) -> Vec<u8>) -> Vec<u8> {
    match resolve(path) {
        Some(resolved) => answer(&resolved),
        None => encode_refusal(STATUS_OUT_OF_VOLUME),
    }
}

fn status_for(error: &std::io::Error) -> u8 {
    match error.kind() {
        std::io::ErrorKind::NotFound => STATUS_NOT_FOUND,
        std::io::ErrorKind::AlreadyExists => STATUS_ALREADY_THERE,
        std::io::ErrorKind::DirectoryNotEmpty => STATUS_NOT_EMPTY,
        std::io::ErrorKind::NotADirectory | std::io::ErrorKind::IsADirectory => STATUS_WRONG_KIND,
        _ => STATUS_FAILED,
    }
}

fn details_of(metadata: &std::fs::Metadata) -> EntryDetails {
    use std::os::unix::fs::MetadataExt;
    EntryDetails {
        kind: if metadata.is_file() {
            FilesystemEntryKind::File
        } else if metadata.is_dir() {
            FilesystemEntryKind::Directory
        } else {
            FilesystemEntryKind::Other
        },
        size_bytes: metadata.len(),
        modified_seconds: metadata.mtime(),
    }
}

fn list(path: &Path) -> Vec<u8> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) => return encode_refusal(status_for(&error)),
    };
    let mut described = Vec::new();
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        described.push((
            entry.file_name().to_string_lossy().into_owned(),
            details_of(&metadata),
        ));
    }
    described.sort_by(|left, right| left.0.cmp(&right.0));
    encode_listing(&described)
}

fn stat(path: &Path) -> Vec<u8> {
    match std::fs::metadata(path) {
        Ok(metadata) => encode_details(&details_of(&metadata)),
        Err(error) => encode_refusal(status_for(&error)),
    }
}

fn make_directory(path: &Path) -> Vec<u8> {
    match std::fs::create_dir(path) {
        Ok(()) => encode_reply(STATUS_OK, &[]),
        Err(error) => encode_refusal(status_for(&error)),
    }
}

fn remove(path: &Path) -> Vec<u8> {
    let removed = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir(path),
        Ok(_) => std::fs::remove_file(path),
        Err(error) => return encode_refusal(status_for(&error)),
    };
    match removed {
        Ok(()) => encode_reply(STATUS_OK, &[]),
        Err(error) => encode_refusal(status_for(&error)),
    }
}

fn read(path: &Path, offset: u64, length: u32) -> Vec<u8> {
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) => return encode_refusal(status_for(&error)),
    };
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return encode_refusal(STATUS_FAILED);
    }
    let mut content = vec![0u8; length as usize];
    let mut filled = 0;
    while filled < content.len() {
        match file.read(&mut content[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) => return encode_refusal(status_for(&error)),
        }
    }
    content.truncate(filled);
    encode_reply(STATUS_OK, &content)
}

fn write(path: &Path, offset: u64, content: &[u8], truncate: bool) -> Vec<u8> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path);
    let mut file = match file {
        Ok(file) => file,
        Err(error) => return encode_refusal(status_for(&error)),
    };
    if truncate && file.set_len(offset).is_err() {
        return encode_refusal(STATUS_FAILED);
    }
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return encode_refusal(STATUS_FAILED);
    }
    match file.write_all(content) {
        Ok(()) => encode_written(content.len() as u32),
        Err(error) => encode_refusal(status_for(&error)),
    }
}

fn usage() -> Vec<u8> {
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let Ok(mount) = std::ffi::CString::new(paths::VOLUME_MOUNT) else {
        return encode_refusal(STATUS_FAILED);
    };
    if unsafe { libc::statvfs(mount.as_ptr(), stats.as_mut_ptr()) } < 0 {
        return encode_refusal(STATUS_FAILED);
    }
    let stats = unsafe { stats.assume_init() };
    let block = stats.f_frsize;
    let total = stats.f_blocks * block;
    let used = total - (stats.f_bfree * block);
    encode_usage(&MeasuredBytes {
        total_bytes: total,
        used_bytes: used,
    })
}

fn compute() -> Vec<u8> {
    let memory = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let total_kb = meminfo_kb(&memory, "MemTotal:");
    let available_kb = meminfo_kb(&memory, "MemAvailable:");
    let stat = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let (total_ticks, idle_ticks) = cpu_ticks(&stat);
    encode_compute(&MeasuredCompute {
        memory_total_bytes: total_kb * 1024,
        memory_used_bytes: total_kb.saturating_sub(available_kb) * 1024,
        cpu_total_ticks: total_ticks,
        cpu_busy_ticks: total_ticks.saturating_sub(idle_ticks),
    })
}

pub(crate) fn meminfo_kb(meminfo: &str, field: &str) -> u64 {
    meminfo
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn cpu_ticks(stat: &str) -> (u64, u64) {
    let Some(line) = stat.lines().find(|line| line.starts_with("cpu ")) else {
        return (0, 0);
    };
    let fields: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|value| value.parse().ok())
        .collect();
    let total: u64 = fields.iter().sum();
    let idle = fields.get(3).copied().unwrap_or_default() + fields.get(4).copied().unwrap_or_default();
    (total, idle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reading_of_memory_is_what_is_total_less_what_is_available() {
        let meminfo =
            "MemTotal:         262144 kB\nMemFree:          100000 kB\nMemAvailable:     200000 kB\n";
        assert_eq!(meminfo_kb(meminfo, "MemTotal:"), 262_144);
        assert_eq!(meminfo_kb(meminfo, "MemAvailable:"), 200_000);
        assert_eq!(meminfo_kb(meminfo, "NotThere:"), 0);
    }

    #[test]
    fn waiting_on_the_disk_is_not_time_a_tenant_spent() {
        let stat = "cpu  100 20 30 400 50 0 0 0 0 0\ncpu0 50 10 15 200 25 0 0 0 0 0\n";
        let (total, idle) = cpu_ticks(stat);
        assert_eq!(total, 600);
        assert_eq!(idle, 450, "idle and iowait together");
        assert_eq!(total - idle, 150);
    }

    #[test]
    fn a_machine_whose_counters_cannot_be_read_reports_nothing_rather_than_guessing() {
        assert_eq!(cpu_ticks(""), (0, 0));
        assert_eq!(meminfo_kb("", "MemTotal:"), 0);
    }
}
