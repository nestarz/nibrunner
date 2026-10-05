use std::path::{Component, Path, PathBuf};

use protocol::ReportedMemory;

fn cgroup_path(membership: &str) -> Option<PathBuf> {
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

fn number(path: &Path, name: &str) -> Option<u64> {
    std::fs::read_to_string(path.join(name)).ok()?.trim().parse().ok()
}

fn field(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then(|| value.trim().parse().ok()).flatten()
    })
}

fn pressure_total(text: &str, kind: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        if parts.next()? != kind {
            return None;
        }
        parts.find_map(|part| part.strip_prefix("total=")?.parse().ok())
    })
}

fn read_cgroup(path: &Path) -> Option<ReportedMemory> {
    let events = std::fs::read_to_string(path.join("memory.events")).ok()?;
    let pressure = std::fs::read_to_string(path.join("memory.pressure")).ok()?;
    Some(ReportedMemory {
        measured_at: crate::clock::now_timestamp(),
        current_bytes: number(path, "memory.current")?,
        peak_bytes: number(path, "memory.peak"),
        swap_bytes: number(path, "memory.swap.current")?,
        high_events: field(&events, "high")?,
        oom_kills: field(&events, "oom_kill")?,
        pressure_some_us: pressure_total(&pressure, "some")?,
        pressure_full_us: pressure_total(&pressure, "full")?,
    })
}

pub(super) fn read_process(pid: i32) -> Option<ReportedMemory> {
    let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
    read_cgroup(&cgroup_path(&membership)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_named_unified_cgroup_can_be_measured() {
        assert_eq!(
            cgroup_path("0::/system.slice/run-test.scope\n"),
            Some(PathBuf::from("/sys/fs/cgroup/system.slice/run-test.scope"))
        );
        for invalid in ["0::/", "0::/../outside", "2:memory:/legacy", "0::/./relative"] {
            assert!(cgroup_path(invalid).is_none(), "{invalid}");
        }
    }

    #[test]
    fn host_measurements_include_swap_and_pressure_and_do_not_invent_missing_samples() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        assert!(read_cgroup(path).is_none());
        for (name, value) in [
            ("memory.current", "314572800\n"),
            ("memory.swap.current", "4096\n"),
            ("memory.events", "low 0\nhigh 7\nmax 0\noom 0\noom_kill 2\n"),
            ("memory.pressure", "some avg10=0.00 avg60=0.00 avg300=0.00 total=123\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=45\n"),
        ] {
            std::fs::write(path.join(name), value).unwrap();
        }
        let measured = read_cgroup(path).unwrap();
        assert_eq!(measured.current_bytes, 314572800);
        assert_eq!(measured.peak_bytes, None);
        assert_eq!(measured.swap_bytes, 4096);
        assert_eq!(measured.oom_kills, 2);
        assert_eq!(measured.pressure_full_us, 45);
        std::fs::write(path.join("memory.peak"), "419430400").unwrap();
        assert_eq!(read_cgroup(path).unwrap().peak_bytes, Some(419430400));
        std::fs::remove_file(path.join("memory.current")).unwrap();
        assert!(read_cgroup(path).is_none());
    }
}
