use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const TRANSITION_TIMEOUT: Duration = Duration::from_secs(1);
const RECLAIM_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_RECLAIM_BYTES: u64 = 64 * 1_048_576;

pub(crate) fn cgroup_path(membership: &str) -> Option<PathBuf> {
    let relative = membership.lines().find_map(|line| line.strip_prefix("0::/"))?;
    let path = Path::new(relative);
    if relative.is_empty()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return None;
    }
    Some(Path::new("/sys/fs/cgroup").join(path))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemoryGroup {
    membership: String,
    device: u64,
    inode: u64,
}

impl MemoryGroup {
    pub(crate) fn capture(owner: &str, unit: &str) -> Option<Self> {
        let owner = cgroup_path(&format!("0::{owner}"))?;
        let path = owner
            .ancestors()
            .find(|path| path.file_name().is_some_and(|name| name == unit))?;
        let metadata = std::fs::metadata(path).ok()?;
        Some(Self {
            membership: format!("/{}", path.strip_prefix("/sys/fs/cgroup").ok()?.to_str()?),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub(crate) fn belongs_to(&self, owner: &str, unit: &str) -> bool {
        let Some(path) = cgroup_path(&format!("0::{}", self.membership)) else {
            return false;
        };
        path.file_name().is_some_and(|name| name == unit)
            && cgroup_path(&format!("0::{owner}")).is_some_and(|owner| owner.starts_with(path))
    }

    pub(crate) fn resident_bytes(&self, grant: u64) -> Option<u64> {
        self.resident_at(&cgroup_path(&format!("0::{}", self.membership))?, grant)
    }

    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        let path = Path::new(&self.membership);
        let other = Path::new(&other.membership);
        path.starts_with(other) || other.starts_with(path)
    }

    fn resident_at(&self, path: &Path, grant: u64) -> Option<u64> {
        let same_group = || {
            std::fs::metadata(path)
                .is_ok_and(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
        };
        if !same_group() {
            return None;
        }
        let read = |name| {
            std::fs::read_to_string(path.join(name))
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()
        };
        let limit = read("memory.max")?;
        if limit > grant {
            return None;
        }
        let stat = std::fs::read_to_string(path.join("memory.stat")).ok()?;
        let counter = |name| {
            stat.lines().find_map(|line| {
                let (key, value) = line.split_once(' ')?;
                (key == name).then(|| value.parse::<u64>().ok()).flatten()
            })
        };
        let anonymous_lru = counter("active_anon")?
            .saturating_add(counter("inactive_anon")?)
            .saturating_sub(counter("shmem")?);
        // File-LRU pages, including MADV_FREE, are already credited by MemAvailable. Swap is future growth.
        let resident = counter("anon")?
            .min(anonymous_lru)
            .min(read("memory.current")?)
            .min(limit);
        same_group().then_some(resident)
    }
}

pub(crate) struct Group {
    path: PathBuf,
}

struct UnfinishedFreeze<'a>(&'a Path, bool);

impl Drop for UnfinishedFreeze<'_> {
    fn drop(&mut self) {
        if self.1 {
            let _ = std::fs::write(self.0.join("cgroup.freeze"), "0");
        }
    }
}

impl Group {
    pub(crate) fn for_process(pid: i32) -> io::Result<Self> {
        let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
        let path = cgroup_path(&membership)
            .ok_or_else(|| io::Error::other("the process has no isolated cgroup v2 membership"))?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if !name.ends_with(".scope") && !name.ends_with(".service") {
            return Err(io::Error::other("refusing to freeze a shared slice"));
        }
        Ok(Self { path })
    }

    pub(crate) fn frozen(&self) -> io::Result<bool> {
        let events = std::fs::read_to_string(self.path.join("cgroup.events"))?;
        match events.lines().find_map(|line| line.strip_prefix("frozen ")) {
            Some("1") => Ok(true),
            Some("0") => Ok(false),
            _ => Err(io::Error::other("the cgroup has no valid frozen observation")),
        }
    }

    async fn wait_for(&self, frozen: bool, timeout: Duration) -> io::Result<()> {
        tokio::time::timeout(timeout, async {
            while self.frozen()? != frozen {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cgroup freeze transition timed out"))?
    }

    pub(crate) async fn freeze(&self) -> io::Result<()> {
        self.freeze_with_timeout(TRANSITION_TIMEOUT).await
    }

    async fn freeze_with_timeout(&self, timeout: Duration) -> io::Result<()> {
        std::fs::write(self.path.join("cgroup.freeze"), "1")?;
        let mut rollback = UnfinishedFreeze(&self.path, true);
        self.wait_for(true, timeout).await?;
        rollback.1 = false;
        Ok(())
    }

    pub(crate) async fn thaw(&self) -> io::Result<()> {
        std::fs::write(self.path.join("cgroup.freeze"), "0")?;
        self.wait_for(false, TRANSITION_TIMEOUT).await
    }

    pub(crate) async fn reclaim(&self, bytes: u64) -> io::Result<u64> {
        if bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reclaim needs a positive amount",
            ));
        }
        let current = || -> io::Result<u64> {
            std::fs::read_to_string(self.path.join("memory.current"))?
                .trim()
                .parse()
                .map_err(io::Error::other)
        };
        let before = current()?;
        // A kernel reclaim write can block; a disposable child keeps that work out of the runtime's threads.
        let output = tokio::time::timeout(
            RECLAIM_TIMEOUT,
            tokio::process::Command::new("/bin/sh")
                .args(["-c", "printf '%s' \"$2\" > \"$1/memory.reclaim\"", "reclaim"])
                .arg(&self.path)
                .arg(bytes.min(MAX_RECLAIM_BYTES).to_string())
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .status(),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cgroup reclaim timed out"))??;
        if !output.success() {
            return Err(io::Error::other(
                "the kernel could not complete the requested reclaim",
            ));
        }
        Ok(before.saturating_sub(current()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_resident_anonymous_memory_in_the_original_bounded_group_is_discounted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("build.slice");
        std::fs::create_dir(&path).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        let group = MemoryGroup {
            membership: "/build.slice".into(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        std::fs::write(path.join("memory.current"), "800").unwrap();
        std::fs::write(
            path.join("memory.stat"),
            "file 600\nanon 250\nactive_anon 180\ninactive_anon 60\nshmem 40\n",
        )
        .unwrap();
        std::fs::write(path.join("memory.swap.current"), "400").unwrap();
        for (limit, expected) in [("1024", Some(200)), ("2048", None), ("max", None)] {
            std::fs::write(path.join("memory.max"), limit).unwrap();
            assert_eq!(group.resident_at(&path, 1024), expected);
        }
        std::fs::write(path.join("memory.max"), "1024").unwrap();
        std::fs::rename(&path, directory.path().join("old.slice")).unwrap();
        std::fs::create_dir(&path).unwrap();
        for name in ["memory.current", "memory.stat", "memory.max"] {
            std::fs::copy(directory.path().join("old.slice").join(name), path.join(name)).unwrap();
        }
        assert_eq!(group.resident_at(&path, 1024), None);
    }

    #[test]
    fn a_tracked_budget_must_contain_its_owner_and_overlap_uses_path_components() {
        let group = MemoryGroup {
            membership: "/build.slice".into(),
            device: 0,
            inode: 1,
        };
        assert!(group.belongs_to("/build.slice/parent.service", "build.slice"));
        assert!(!group.belongs_to("/build.slice-other/parent.service", "build.slice"));
        assert!(!group.belongs_to("/build.slice/parent.service", "other.slice"));
        let mut other = group.clone();
        assert!(group.overlaps(&other));
        other.membership = "/build.slice/child.service".into();
        assert!(group.overlaps(&other));
        other.membership = "/build.slice-other".into();
        assert!(!group.overlaps(&other));
    }

    fn group(directory: &Path, frozen: bool) -> Group {
        std::fs::write(
            directory.join("cgroup.events"),
            if frozen { "frozen 1\n" } else { "frozen 0\n" },
        )
        .unwrap();
        std::fs::write(directory.join("cgroup.freeze"), "0").unwrap();
        std::fs::write(directory.join("memory.current"), "1024").unwrap();
        Group {
            path: directory.into(),
        }
    }

    #[test]
    fn only_named_unified_groups_are_addressable() {
        assert_eq!(
            cgroup_path("0::/system.slice/run-test.scope\n"),
            Some(PathBuf::from("/sys/fs/cgroup/system.slice/run-test.scope"))
        );
        for invalid in ["0::/", "0::/../outside", "2:memory:/legacy", "0::/./relative"] {
            assert!(cgroup_path(invalid).is_none(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn an_unconfirmed_freeze_is_undone_after_its_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let group = group(directory.path(), false);
        assert_eq!(
            group
                .freeze_with_timeout(Duration::from_millis(10))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            std::fs::read_to_string(directory.path().join("cgroup.freeze")).unwrap(),
            "0"
        );
    }

    #[tokio::test]
    async fn cancelling_an_unconfirmed_freeze_undoes_the_kernel_request() {
        let directory = tempfile::tempdir().unwrap();
        let group = group(directory.path(), false);
        let task = tokio::spawn(async move { group.freeze().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while std::fs::read_to_string(directory.path().join("cgroup.freeze")).unwrap() != "1" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            std::fs::read_to_string(directory.path().join("cgroup.freeze")).unwrap(),
            "0"
        );
    }

    #[tokio::test]
    async fn reclaim_is_bounded_and_works_without_a_host_freezer() {
        let directory = tempfile::tempdir().unwrap();
        let group = group(directory.path(), false);
        assert_eq!(
            group.reclaim(0).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(!directory.path().join("memory.reclaim").exists());
        assert_eq!(group.reclaim(u64::MAX).await.unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(directory.path().join("memory.reclaim")).unwrap(),
            MAX_RECLAIM_BYTES.to_string()
        );
    }
}
