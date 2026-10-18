use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nibrunnerd::adapters::vm::process::VmProcesses;
use nibrunnerd::adapters::vm::VmExit;
use nibrunnerd::test_support::machine::{MemorySnapshots, RunningHost, Tenant};
use protocol::{InstanceResources, InstanceState};

const BYTES_PER_MIB: u64 = 1024 * 1024;
const IDLE_TIMEOUT_MS: u64 = 60_000;

fn processes(host: &RunningHost) -> VmProcesses {
    VmProcesses::new(host.host.config.runtime_dir.clone())
}

fn cgroup(host: &RunningHost, app: &Tenant) -> PathBuf {
    let record = processes(host).read_record(&app.app_id).expect("the VMM record");
    let membership = std::fs::read_to_string(format!("/proc/{}/cgroup", record.pid)).unwrap();
    let relative = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap();
    assert!(relative.starts_with("/nibrunner-jailer/"), "{membership}");
    Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'))
}

fn number(path: &Path, file: &str) -> u64 {
    std::fs::read_to_string(path.join(file))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn counter(path: &Path, file: &str, name: &str) -> u64 {
    std::fs::read_to_string(path.join(file))
        .unwrap()
        .lines()
        .find_map(|line| line.split_once(' ').filter(|(key, _)| *key == name))
        .unwrap()
        .1
        .parse()
        .unwrap()
}

fn assert_limits(host: &RunningHost, app: &Tenant) -> PathBuf {
    let path = cgroup(host, app);
    let resources = app.instance.config.resources;
    assert_eq!(
        std::fs::read_to_string(path.join("cpu.max")).unwrap().trim(),
        format!("{} 100000", u64::from(resources.vcpu_count) * 100_000)
    );
    let memory = u64::from(resources.memory_mib) * BYTES_PER_MIB;
    assert_eq!(
        number(&path, "memory.max"),
        memory + 64 * BYTES_PER_MIB + memory / 8
    );
    assert_eq!(number(&path, "memory.swap.max"), 0);
    let record = processes(host).read_record(&app.app_id).unwrap();
    assert_eq!(
        std::fs::read_to_string(format!("/proc/{}/oom_score_adj", record.pid))
            .unwrap()
            .trim(),
        "0"
    );
    path
}

#[tokio::test(flavor = "multi_thread")]
async fn resource_limits_preserve_allocated_memory_across_sleep_restore_and_redeployment() {
    memory_survives_sleep_restore_and_redeployment(None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_pages_do_not_kill_a_vm_or_consume_its_restored_running_limit() {
    if !std::env::var("NIBRUNNER_INTEGRATION").is_ok_and(|value| value == "1") {
        return;
    }
    let snapshots = MemorySnapshots::new();
    memory_survives_sleep_restore_and_redeployment(Some(snapshots.path())).await;
}

async fn memory_survives_sleep_restore_and_redeployment(snapshot_directory: Option<&Path>) {
    let Some(host) = crate::host_with(|config| {
        if let Some(directory) = snapshot_directory {
            config.snapshot_dir = directory.to_owned();
        }
    })
    .await
    else {
        return;
    };
    for (cpus, memory, allocated) in [(1_u32, 256_u32, 160_u32), (2, 512, 384)] {
        let app = host
            .tenant(1)
            .on_request(IDLE_TIMEOUT_MS)
            .redeployed(&format!("{cpus}-{memory}"))
            .edited(|instance| {
                instance.config.resources = InstanceResources {
                    vcpu_count: cpus,
                    memory_mib: memory,
                }
            });
        host.deploy(std::slice::from_ref(&app)).await;
        host.until("the requested deployment to be running", |report| {
            report.instances.iter().any(|instance| {
                instance.app_id == app.app_id
                    && instance.deployment_id == app.instance.deployment_id
                    && instance.state == InstanceState::Running
            })
        })
        .await;
        let path = assert_limits(&host, &app);
        let first_pid = processes(&host).read_record(&app.app_id).unwrap().pid;
        let answer = host
            .get(&app, &format!("/allocate?mib={allocated}"))
            .await
            .unwrap();
        assert_eq!(
            answer.status,
            200,
            "{answer:?}; memory events: {}; console: {}",
            std::fs::read_to_string(path.join("memory.events")).unwrap(),
            std::fs::read_to_string(processes(&host).console_path(&app.app_id)).unwrap_or_default()
        );
        assert_eq!(
            host.get(&app, "/write?path=kept&body=limited")
                .await
                .unwrap()
                .status,
            200
        );
        assert!(number(&path, "memory.current") < number(&path, "memory.max"));
        host.let_sleep(&app).await;
        assert!(
            !path.exists(),
            "snapshot page charges must leave the stopped VMM's cgroup"
        );
        assert_eq!(
            host.get(&app, "/allocated").await.unwrap().body,
            format!("{}:1", u64::from(allocated) * BYTES_PER_MIB)
        );
        let restored = assert_limits(&host, &app);
        assert_ne!(processes(&host).read_record(&app.app_id).unwrap().pid, first_pid);
        let answer = host
            .get(&app, &format!("/allocate?mib={allocated}"))
            .await
            .unwrap();
        assert_eq!(
            answer.status,
            200,
            "{answer:?}; memory events: {}; console: {}",
            std::fs::read_to_string(path.join("memory.events")).unwrap(),
            std::fs::read_to_string(processes(&host).console_path(&app.app_id)).unwrap_or_default()
        );
        assert_eq!(
            host.get(&app, "/allocated").await.unwrap().body,
            format!("{}:2", u64::from(allocated) * BYTES_PER_MIB)
        );
        assert_eq!(host.get(&app, "/read?path=kept").await.unwrap().body, "limited");
        host.host.vms.readopt(&app.app_id).await.unwrap();
        assert_eq!(assert_limits(&host, &app), restored);
        assert_eq!(host.get(&app, "/").await.unwrap().status, 200);
    }
    let path = cgroup(&host, &host.tenant(1));
    host.deploy(&[]).await;
    host.until("the removed VM's cgroup to be collected", |_| !path.exists())
        .await;
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn instance_limits_tighten_the_cgroup_live_and_survive_a_sleep() {
    let Some(host) = crate::host().await else {
        return;
    };
    let limited = |cpu_percent: u16, priority| protocol::InstanceLimits {
        concurrent: None,
        cpu_percent: Some(cpu_percent.try_into().unwrap()),
        memory: Some(protocol::MemoryPolicy {
            target_mib: 128.try_into().unwrap(),
            swap_mib: 0,
            priority,
        }),
    };
    let app = host
        .tenant(1)
        .on_request(IDLE_TIMEOUT_MS)
        .edited(|instance| instance.limits = Some(limited(25, protocol::MemoryPriority::Build)));
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let path = cgroup(&host, &app);
    let oom_score = |host: &RunningHost| {
        let pid = processes(host).read_record(&app.app_id).unwrap().pid;
        std::fs::read_to_string(format!("/proc/{pid}/oom_score_adj")).unwrap()
    };
    assert_eq!(
        std::fs::read_to_string(path.join("cpu.max")).unwrap().trim(),
        "25000 100000"
    );
    assert_eq!(number(&path, "memory.high"), 128 * BYTES_PER_MIB);
    assert_eq!(oom_score(&host).trim(), "1000");

    let relaxed = app
        .clone()
        .edited(|instance| instance.limits = Some(limited(50, protocol::MemoryPriority::Production)));
    host.deploy(std::slice::from_ref(&relaxed)).await;
    host.until("the new limits to reach the running VM", |_| {
        std::fs::read_to_string(path.join("cpu.max")).is_ok_and(|max| max.trim() == "50000 100000")
    })
    .await;
    assert_eq!(
        std::fs::read_to_string(path.join("memory.high")).unwrap().trim(),
        "max"
    );
    assert_eq!(number(&path, "memory.low"), 128 * BYTES_PER_MIB);
    assert_eq!(oom_score(&host).trim(), "-500");

    host.let_sleep(&relaxed).await;
    assert_eq!(host.get(&relaxed, "/").await.unwrap().status, 200);
    let restored = cgroup(&host, &relaxed);
    assert_eq!(
        std::fs::read_to_string(restored.join("cpu.max")).unwrap().trim(),
        "50000 100000"
    );
    assert_eq!(oom_score(&host).trim(), "-500");
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_throttles_cpu_work_at_the_vms_resource_ceiling() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let path = assert_limits(&host, &app);
    let throttled = counter(&path, "cpu.stat", "nr_throttled");
    // A smaller test ceiling makes throttling observable even on an oversubscribed CI runner.
    std::fs::write(path.join("cpu.max"), "10000 100000").unwrap();
    assert_eq!(
        host.get(&app, "/burn?ms=5000&threads=2").await.unwrap().status,
        200
    );
    assert!(counter(&path, "cpu.stat", "nr_throttled") > throttled);
    assert!(counter(&path, "cpu.stat", "throttled_usec") > 0);
    assert_eq!(host.get(&app, "/").await.unwrap().status, 200);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_memory_limit_kill_is_reported_and_restarted_without_interrupting_another_vm() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).edited(|instance| {
        instance.config.restart_policy.initial_backoff_ms = 5000;
        instance.config.restart_policy.max_backoff_ms = 5000;
    });
    let neighbour = host.tenant(2);
    host.deploy(&[app.clone(), neighbour.clone()]).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    host.until_state(&neighbour.app_id, InstanceState::Running).await;
    assert_eq!(
        host.get(&app, "/write?path=kept&body=survived")
            .await
            .unwrap()
            .status,
        200
    );
    let path = assert_limits(&host, &app);
    let first_pid = processes(&host).read_record(&app.app_id).unwrap().pid;
    let other_pid = processes(&host).read_record(&neighbour.app_id).unwrap().pid;
    let killed = counter(&path, "memory.events", "oom_kill");
    std::fs::write(path.join("memory.max"), BYTES_PER_MIB.to_string()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let record = processes(&host).read_record(&app.app_id).unwrap();
        if record.exit() == Some(VmExit::Signal(libc::SIGKILL)) {
            break;
        }
        assert_eq!(
            record.pid, first_pid,
            "the OOM exit must be recorded before restart"
        );
        assert!(
            Instant::now() < deadline,
            "the kernel did not kill the constrained VMM"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(counter(&path, "memory.events", "oom_kill") > killed);
    assert!(processes(&host).status(&app.app_id).failed);
    assert_eq!(host.get(&neighbour, "/").await.unwrap().status, 200);
    assert_eq!(
        processes(&host).read_record(&neighbour.app_id).unwrap().pid,
        other_pid
    );
    host.until("the OOM-killed VM to restart", |report| {
        report
            .instances
            .iter()
            .any(|instance| instance.app_id == app.app_id && instance.state == InstanceState::Running)
            && processes(&host)
                .read_record(&app.app_id)
                .is_some_and(|record| record.pid != first_pid)
    })
    .await;
    assert_limits(&host, &app);
    assert_eq!(host.get(&app, "/read?path=kept").await.unwrap().body, "survived");
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_guests_memory_is_backed_by_transparent_huge_pages() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    assert_eq!(host.get(&app, "/allocate?mib=64").await.unwrap().status, 200);
    let pid = processes(&host).read_record(&app.app_id).unwrap().pid;
    let rollup = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup")).unwrap();
    let huge_kib: u64 = rollup
        .lines()
        .find_map(|line| line.strip_prefix("AnonHugePages:"))
        .and_then(|value| value.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap();
    assert!(huge_kib > 0, "the VMM holds no huge pages:\n{rollup}");
    host.stop().await;
}
