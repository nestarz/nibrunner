use std::path::{Component, Path, PathBuf};

use protocol::ReportedMemory;

use crate::config::VmBudget;

const BYTES_PER_MIB: u64 = 1_048_576;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Controls {
    pub low: u64,
    pub high: u64,
    pub max: u64,
    pub swap: u64,
    pub oom_score: &'static str,
}

impl Controls {
    pub(super) fn for_budget(budget: &VmBudget) -> Self {
        let max = u64::from(budget.memory_mib.get()) * BYTES_PER_MIB;
        let mut controls = Self {
            low: 0,
            high: max,
            max,
            swap: 0,
            oom_score: "0",
        };
        if let Some(policy) = budget.memory {
            let target = u64::from(policy.target_mib.get().min(budget.memory_mib.get())) * BYTES_PER_MIB;
            controls.swap = u64::from(policy.swap_mib) * BYTES_PER_MIB;
            match policy.priority {
                protocol::MemoryPriority::Production => {
                    controls.low = target;
                    controls.oom_score = "-500";
                }
                protocol::MemoryPriority::Standard => {}
                protocol::MemoryPriority::Preview => {
                    controls.high = target;
                    controls.oom_score = "500";
                }
                protocol::MemoryPriority::Build => {
                    controls.high = target;
                    controls.oom_score = "1000";
                }
            }
        }
        controls
    }
}

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

fn limits(path: &Path) -> Option<protocol::ReportedMemoryLimits> {
    let limit = |name: &str| {
        let text = std::fs::read_to_string(path.join(name)).ok()?;
        if text.trim() == "max" {
            Some(None)
        } else {
            text.trim().parse().ok().map(Some)
        }
    };
    Some(protocol::ReportedMemoryLimits {
        low_bytes: number(path, "memory.low")?,
        high_bytes: limit("memory.high")?,
        max_bytes: limit("memory.max")?,
        swap_max_bytes: limit("memory.swap.max")?,
    })
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
        cgroup: path
            .strip_prefix("/sys/fs/cgroup")
            .ok()
            .map(|relative| format!("/{}", relative.display())),
        proportional_set_bytes: None,
        limits: limits(path),
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
    let mut memory = read_cgroup(&cgroup_path(&membership)?)?;
    memory.proportional_set_bytes = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup"))
        .ok()
        .and_then(|text| proportional_set_bytes(&text));
    Some(memory)
}

fn proportional_set_bytes(text: &str) -> Option<u64> {
    let value = text.lines().find_map(|line| line.strip_prefix("Pss:"))?;
    let mut parts = value.split_whitespace();
    let kib = parts.next()?.parse::<u64>().ok()?;
    (parts.next()? == "kB" && parts.next().is_none())
        .then(|| kib.checked_mul(1024))
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proportional_memory_reads_only_the_total_with_known_units() {
        assert_eq!(
            proportional_set_bytes("Rss: 4096 kB\nPss: 2048 kB\nPss_Anon: 1024 kB\n"),
            Some(2 << 20)
        );
        for text in [
            "Pss_Anon: 1024 kB",
            "Pss: 2 MB",
            "Pss: 18446744073709551615 kB",
            "Pss: no kB",
        ] {
            assert_eq!(proportional_set_bytes(text), None);
        }
    }

    #[test]
    fn memory_priorities_protect_production_and_throttle_disposable_work_without_lowering_hard_limits() {
        let mut budget = VmBudget {
            cpu_percent: 100.try_into().unwrap(),
            memory_mib: 1024.try_into().unwrap(),
            memory: Some(protocol::MemoryPolicy {
                target_mib: 384.try_into().unwrap(),
                swap_mib: 512,
                priority: protocol::MemoryPriority::Production,
            }),
        };
        let production = Controls::for_budget(&budget);
        assert_eq!(production.low, 384 * BYTES_PER_MIB);
        assert_eq!(production.high, production.max);
        assert_eq!(production.max, 1024 * BYTES_PER_MIB);
        assert_eq!(production.swap, 512 * BYTES_PER_MIB);
        assert_eq!(production.oom_score, "-500");
        for (priority, score) in [
            (protocol::MemoryPriority::Preview, "500"),
            (protocol::MemoryPriority::Build, "1000"),
        ] {
            budget.memory.as_mut().unwrap().priority = priority;
            let disposable = Controls::for_budget(&budget);
            assert_eq!(disposable.low, 0);
            assert_eq!(disposable.high, 384 * BYTES_PER_MIB);
            assert_eq!(disposable.max, production.max);
            assert_eq!(disposable.oom_score, score);
        }
        budget.memory.as_mut().unwrap().target_mib = 4096.try_into().unwrap();
        assert_eq!(Controls::for_budget(&budget).high, production.max);
        budget.memory = None;
        assert_eq!(
            Controls::for_budget(&budget),
            Controls {
                low: 0,
                high: production.max,
                max: production.max,
                swap: 0,
                oom_score: "0"
            }
        );
    }

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
        assert_eq!(measured.limits, None);
        assert_eq!(measured.current_bytes, 314572800);
        assert_eq!(measured.peak_bytes, None);
        assert_eq!(measured.swap_bytes, 4096);
        assert_eq!(measured.oom_kills, 2);
        assert_eq!(measured.pressure_full_us, 45);
        std::fs::write(path.join("memory.peak"), "419430400").unwrap();
        assert_eq!(read_cgroup(path).unwrap().peak_bytes, Some(419430400));
        for (name, value) in [
            ("memory.low", "134217728"),
            ("memory.high", "max"),
            ("memory.max", "1073741824"),
            ("memory.swap.max", "536870912"),
        ] {
            std::fs::write(path.join(name), value).unwrap();
        }
        assert_eq!(
            read_cgroup(path).unwrap().limits,
            Some(protocol::ReportedMemoryLimits {
                low_bytes: 134217728,
                high_bytes: None,
                max_bytes: Some(1073741824),
                swap_max_bytes: Some(536870912)
            })
        );
        std::fs::remove_file(path.join("memory.current")).unwrap();
        assert!(read_cgroup(path).is_none());
    }
}
