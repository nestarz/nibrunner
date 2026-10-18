use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

pub(crate) fn read_pool(
    pool: &crate::config::WorkloadPool,
) -> io::Result<crate::domain::memory_admission::PoolMemory> {
    let path = cgroup_path(&format!("0::{}", pool.membership()))
        .ok_or_else(|| io::Error::other("invalid workload pool membership"))?;
    read_pool_at(pool, &path)
}

fn read_pool_at(
    pool: &crate::config::WorkloadPool,
    path: &Path,
) -> io::Result<crate::domain::memory_admission::PoolMemory> {
    let original = std::fs::metadata(path)?;
    let read = |name| std::fs::read_to_string(path.join(name));
    let number = |name| read(name)?.trim().parse::<u64>().map_err(io::Error::other);
    let max_bytes = number("memory.max")?;
    if max_bytes == 0 || max_bytes > u64::from(pool.memory_mib.get()) * 1_048_576 {
        return Err(io::Error::other(
            "workload pool exceeds its configured hard limit",
        ));
    }
    let high = read("memory.high")?;
    let high_bytes = if high.trim() == "max" {
        max_bytes
    } else {
        high.trim()
            .parse::<u64>()
            .map_err(io::Error::other)?
            .min(max_bytes)
    };
    let current_bytes = number("memory.current")?;
    let reclaimable_file_bytes = read("memory.stat")
        .ok()
        .and_then(|stat| reclaimable_file_bytes(&stat))
        .unwrap_or(0)
        .min(current_bytes);
    let current = std::fs::metadata(path)?;
    if current.dev() != original.dev() || current.ino() != original.ino() {
        return Err(io::Error::other("workload pool changed during observation"));
    }
    Ok(crate::domain::memory_admission::PoolMemory {
        membership: pool.membership(),
        current_bytes,
        reclaimable_file_bytes,
        high_bytes,
        max_bytes,
        all_workloads_contained: false,
    })
}

fn reclaimable_file_bytes(stat: &str) -> Option<u64> {
    let field = |name| {
        stat.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == name).then(|| fields.next()?.parse::<u64>().ok())?
        })
    };
    // These counters overlap; subtracting all exclusions before crediting cache is conservative.
    Some(
        field("inactive_file")?
            .min(field("file")?)
            .saturating_sub(field("shmem")?)
            .saturating_sub(field("file_dirty")?)
            .saturating_sub(field("file_writeback")?),
    )
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_clean_inactive_file_pages_are_reclaimable_capacity() {
        let stat = "anon 900\nfile 800\ninactive_file 600\nshmem 100\nfile_dirty 50\nfile_writeback 25\n";
        assert_eq!(reclaimable_file_bytes(stat), Some(425));
        assert_eq!(
            reclaimable_file_bytes(&stat.replace("file 800", "file 500")),
            Some(325)
        );
        assert_eq!(
            reclaimable_file_bytes(&stat.replace("shmem 100", "shmem 700")),
            Some(0)
        );
        for field in ["file", "inactive_file", "shmem", "file_dirty", "file_writeback"] {
            let missing = stat
                .lines()
                .filter(|line| !line.starts_with(&format!("{field} ")))
                .collect::<Vec<_>>()
                .join("\n");
            assert_eq!(reclaimable_file_bytes(&missing), None, "{field}");
            let malformed = stat.replace(&format!("{field} "), &format!("{field} invalid"));
            assert_eq!(reclaimable_file_bytes(&malformed), None, "{field}");
        }
    }

    #[test]
    fn workload_pool_observations_require_a_bounded_kernel_group() {
        let directory = tempfile::tempdir().unwrap();
        let pool = crate::config::WorkloadPool {
            slice: "workloads.slice".into(),
            memory_mib: 1.try_into().unwrap(),
        };
        assert!(read_pool_at(&pool, directory.path()).is_err());
        std::fs::write(directory.path().join("memory.high"), "max").unwrap();
        std::fs::write(directory.path().join("memory.current"), "512").unwrap();
        for limit in ["max", "0", "1048577", "broken"] {
            std::fs::write(directory.path().join("memory.max"), limit).unwrap();
            assert!(read_pool_at(&pool, directory.path()).is_err(), "{limit}");
        }
        std::fs::write(directory.path().join("memory.max"), "1048576").unwrap();
        let observed = read_pool_at(&pool, directory.path()).unwrap();
        assert_eq!(observed.current_bytes, 512);
        assert_eq!(observed.reclaimable_file_bytes, 0);
        assert_eq!(observed.high_bytes, observed.max_bytes);
        assert!(!observed.all_workloads_contained);
        std::fs::write(
            directory.path().join("memory.stat"),
            "file 1024\ninactive_file 1024\nshmem 0\nfile_dirty 0\nfile_writeback 0\n",
        )
        .unwrap();
        assert_eq!(
            read_pool_at(&pool, directory.path())
                .unwrap()
                .reclaimable_file_bytes,
            512
        );
        std::fs::write(directory.path().join("memory.high"), "900000").unwrap();
        assert_eq!(read_pool_at(&pool, directory.path()).unwrap().high_bytes, 900000);
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
}
