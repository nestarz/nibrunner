#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::sync::Arc;

use nibrunnerd::ports::{CommandRunner, CommandRunnerExt};
use nibrunnerd::test_support::mocks;

fn enabled() -> bool {
    std::env::var("NIBRUNNER_INTEGRATION").is_ok_and(|value| value == "1")
}

fn require_root() {
    #[cfg(unix)]
    #[allow(unsafe_code, reason = "asking who this process is has no safe spelling")]
    if unsafe { libc::geteuid() } != 0 {
        panic!("NIBRUNNER_INTEGRATION=1 was set but this is not running as root");
    }
}

fn commands() -> Arc<dyn CommandRunner> {
    Arc::new(nibrunnerd::adapters::exec::HostCommands)
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn real_build_services_hold_memory_until_their_work_has_exited() {
    if !enabled() {
        return;
    }
    require_root();
    let mut host = nibrunnerd::test_support::test_host().await;
    Arc::get_mut(&mut host.host).unwrap().commands = commands();
    std::fs::create_dir_all(&host.config.runtime_dir).unwrap();
    let service = nibrunnerd::memory_service::MemoryService::restore(host.arc().clone()).unwrap();
    let server = service.serve().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let socket = host.config.runtime_dir.join(protocol::memory::SOCKET_NAME);
    let unit = format!("nibrunner-memory-client-{id}");
    let executable = std::env::current_exe().unwrap();
    let runner = commands();
    let request = nibrunnerd::ports::CommandRequest::new(&[
        "systemd-run",
        "--wait",
        "--pipe",
        "--collect",
        "--unit",
        &unit,
        "--property=RuntimeMaxSec=60",
        "--setenv=NIBRUNNER_INTEGRATION=1",
        &format!("--setenv=NIBRUNNER_MEMORY_TEST_SOCKET={}", socket.display()),
        &format!("--setenv=NIBRUNNER_MEMORY_TEST_ID={id}"),
        executable.to_str().unwrap(),
        "--exact",
        "memory_lease_client_process",
        "--nocapture",
    ]);
    let observed = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
        while tokio::time::Instant::now() < deadline {
            if nibrunnerd::test_support::external_resident_memory(&host.state)
                .get(&id)
                .is_some_and(|bytes| *bytes >= 16 * 1_048_576)
            {
                std::fs::write(socket.with_extension("measured"), b"").unwrap();
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        false
    };
    let (result, observed) = tokio::join!(runner.run(request), observed);
    let result = result.unwrap();
    server.abort();
    assert_eq!(result.code, 0, "{}\n{}", result.stdout, result.stderr);
    assert!(
        observed,
        "a live bounded build must expose its resident anonymous memory"
    );
}

#[cfg(target_os = "linux")]
fn memory_request(socket: &str, request: protocol::memory::Request) -> protocol::memory::Reply {
    use std::io::{Read, Write};
    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let bytes = serde_json::to_vec(&request).unwrap();
    stream.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
    stream.write_all(&bytes).unwrap();
    let mut length = [0; 4];
    stream.read_exact(&mut length).unwrap();
    let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[cfg(target_os = "linux")]
#[test]
fn memory_lease_client_process() {
    use protocol::memory::{Reply, Request};
    let Ok(socket) = std::env::var("NIBRUNNER_MEMORY_TEST_SOCKET") else {
        return;
    };
    require_root();
    let id = std::env::var("NIBRUNNER_MEMORY_TEST_ID").unwrap();
    let unit = format!("nibrunner-memory-work-{id}.service");
    let request = |request| memory_request(&socket, request);
    for slice in [
        None,
        Some(format!("nibrunnerbuild-{}.slice", id.replace('-', ""))),
    ] {
        let acquire = Request::Acquire {
            minimum_mib: Some(32.try_into().unwrap()),
            id: id.clone(),
            unit: slice.clone().unwrap_or_else(|| unit.clone()),
            memory_mib: u32::MAX.try_into().unwrap(),
        };
        let granted = request(acquire.clone());
        let Reply::Granted { memory_mib } = granted else {
            panic!("{granted:?}");
        };
        assert!((32..u32::MAX).contains(&memory_mib.get()));
        assert_eq!(request(acquire), granted);
        let start = |name: &str| {
            let mut command = std::process::Command::new("systemd-run");
            if let Some(slice) = &slice {
                command.arg(format!("--slice={slice}"));
            }
            assert!(command
                .args([
                    "--unit",
                    name,
                    "--collect",
                    &format!("--property=MemoryMax={}M", memory_mib.get()),
                    "--property=RuntimeMaxSec=20",
                    "/bin/sleep",
                    "20",
                ])
                .status()
                .unwrap()
                .success());
        };
        let stop = |name: &str| {
            assert!(std::process::Command::new("systemctl")
                .args(["stop", name])
                .status()
                .unwrap()
                .success());
        };
        start(&unit);
        let sibling = format!("nibrunner-memory-sibling-{id}.service");
        if slice.is_some() {
            start(&sibling);
        }
        let busy = request(Request::Release { id: id.clone() });
        assert!(matches!(busy, Reply::Waiting { .. }), "{busy:?}");
        stop(&unit);
        if slice.is_some() {
            let busy = request(Request::Release { id: id.clone() });
            assert!(
                matches!(busy, Reply::Waiting { .. }),
                "a sibling still holds the slice: {busy:?}"
            );
            stop(&sibling);
        }
        assert_eq!(request(Request::Release { id: id.clone() }), Reply::Released);
    }
    let own_unit = format!("nibrunner-memory-client-{id}.service");
    assert!(std::process::Command::new("systemctl")
        .args(["set-property", "--runtime", &own_unit, "MemoryMax=256M"])
        .status()
        .unwrap()
        .success());
    let granted = request(Request::Acquire {
        id,
        unit: own_unit,
        memory_mib: 256.try_into().unwrap(),
        minimum_mib: None,
    });
    assert!(matches!(granted, Reply::Granted { .. }), "{granted:?}");
    let mut allocation = vec![0u8; 32 * 1_048_576];
    for page in allocation.chunks_mut(4096) {
        page[0] = 1;
    }
    std::hint::black_box(&allocation);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let measured = std::path::Path::new(&socket).with_extension("measured");
    while !measured.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the broker did not measure its build"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    std::hint::black_box(&allocation);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_real_systemd_scope_starts_with_the_requested_memory_controls() {
    if !enabled() {
        return;
    }
    require_root();
    use nibrunnerd::adapters::vm::process::VmProcesses;
    use nibrunnerd::config::{VmBudget, VmBudgets};
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let helper = directory.path().join("helper.sh");
    std::fs::write(&helper, "#!/bin/sh\nexec sleep 30\n").unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();
    for priority in [
        protocol::MemoryPriority::Production,
        protocol::MemoryPriority::Preview,
    ] {
        let processes = VmProcesses::with_budgets(
            directory.path().join("run"),
            Some(VmBudgets {
                default: VmBudget {
                    cpu_percent: 100.try_into().unwrap(),
                    memory_mib: 128.try_into().unwrap(),
                    memory: Some(protocol::MemoryPolicy {
                        target_mib: 32.try_into().unwrap(),
                        swap_mib: 64,
                        priority,
                    }),
                },
                apps: Default::default(),
            }),
        );
        let app = protocol::AppId::parse("scope-check").unwrap();
        processes
            .spawn(&app, &helper, directory.path(), None)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let observed = loop {
            let memory = processes.memory(&app);
            if memory
                .as_ref()
                .and_then(|m| m.limits.as_ref())
                .is_some_and(|l| l.max_bytes == Some(128 * 1024 * 1024))
            {
                break memory;
            }
            if tokio::time::Instant::now() >= deadline || processes.status(&app).exit.is_some() {
                break None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        let console = std::fs::read_to_string(processes.console_path(&app)).unwrap_or_default();
        if observed.is_some() {
            let record = processes.read_record(&app).unwrap();
            let mut reused = record.clone();
            reused.start_ticks = reused.start_ticks.map(|ticks| ticks + 1);
            processes.write_record(&reused).unwrap();
            assert!(
                processes.memory(&app).is_none(),
                "a reused PID must not supply another workload's memory"
            );
            processes.write_record(&record).unwrap();
            processes.freeze(&app).await.unwrap();
            assert!(processes.frozen(&app).unwrap());
            let _ = processes.reclaim_frozen(&app, 1024 * 1024).await;
            processes.thaw(&app).await.unwrap();
            assert!(!processes.frozen(&app).unwrap());
            processes.freeze(&app).await.unwrap();
        }
        let stopping = std::time::Instant::now();
        processes.stop(&app).await;
        assert!(
            stopping.elapsed() < std::time::Duration::from_secs(3),
            "stopping a frozen process must thaw before SIGTERM"
        );
        let memory = observed
            .as_ref()
            .unwrap_or_else(|| panic!("the scope did not start: {console}"));
        let cgroup = memory.cgroup.as_ref().expect("the cgroup path is reported");
        assert_eq!(
            std::path::Path::new(cgroup).parent(),
            Some(std::path::Path::new("/"))
        );
        assert!(memory.proportional_set_bytes.is_some_and(|bytes| bytes > 0));
        assert!(memory.anonymous_set_bytes.is_some_and(|bytes| bytes > 0));
        let limits = observed
            .and_then(|m| m.limits)
            .unwrap_or_else(|| panic!("the scope did not start: {console}"));
        let production = priority == protocol::MemoryPriority::Production;
        assert_eq!(limits.low_bytes, if production { 32 * 1024 * 1024 } else { 0 });
        assert_eq!(
            limits.high_bytes,
            Some(if production {
                128 * 1024 * 1024
            } else {
                32 * 1024 * 1024
            })
        );
        assert_eq!(limits.swap_max_bytes, Some(64 * 1024 * 1024));
    }
}

#[cfg(target_os = "linux")]
struct WorkloadSlice(String);

#[cfg(target_os = "linux")]
impl Drop for WorkloadSlice {
    fn drop(&mut self) {
        for operation in ["stop", "revert"] {
            let _ = std::process::Command::new("systemctl")
                .args([operation, &self.0])
                .status();
        }
    }
}

#[cfg(target_os = "linux")]
impl WorkloadSlice {
    fn new(high: &str) -> Self {
        let slice = Self(format!("nibpooltest{}.slice", uuid::Uuid::new_v4().simple()));
        let high = format!("MemoryHigh={high}");
        for args in [
            vec![
                "set-property",
                "--runtime",
                &slice.0,
                "MemoryMax=384M",
                &high,
                "MemoryLow=384M",
                "MemorySwapMax=0",
            ],
            vec!["start", &slice.0],
        ] {
            let output = std::process::Command::new("systemctl")
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        slice
    }
}

#[cfg(target_os = "linux")]
#[test]
fn cached_file_memory_client_process() {
    use std::io::Write;
    let Ok(socket) = std::env::var("NIBRUNNER_CACHE_TEST_SOCKET") else {
        return;
    };
    require_root();
    let id = std::env::var("NIBRUNNER_CACHE_TEST_ID").unwrap();
    let directory = tempfile::tempdir_in("/var/tmp").unwrap();
    let mut file = std::fs::File::create(directory.path().join("cache")).unwrap();
    let block = vec![1u8; 1_048_576];
    for _ in 0..256 {
        file.write_all(&block).unwrap();
    }
    file.sync_all().unwrap();
    let membership = std::fs::read_to_string("/proc/self/cgroup").unwrap();
    let group = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::/"))
        .unwrap();
    let group = std::path::Path::new("/sys/fs/cgroup").join(group);
    let parent = group.parent().unwrap();
    let cached = || {
        std::fs::read_to_string(parent.join("memory.stat"))
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("inactive_file "))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let cached_before = cached();
    assert!(cached_before >= 128 * 1_048_576, "{cached_before}");
    let current: u64 = std::fs::read_to_string(parent.join("memory.current"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        current > 128 * 1_048_576,
        "the cached file must prevent admission without cache credit: {current}"
    );
    let reply = memory_request(
        &socket,
        protocol::memory::Request::Acquire {
            id: id.clone(),
            unit: format!("nibrunner-cache-{id}.service"),
            memory_mib: 256.try_into().unwrap(),
            minimum_mib: None,
        },
    );
    assert!(
        matches!(reply, protocol::memory::Reply::Granted { .. }),
        "{reply:?}"
    );
    let mut allocation = vec![0u8; 200 * 1_048_576];
    for page in allocation.chunks_mut(4096) {
        page[0] = 1;
    }
    // Hierarchical memory.stat counters are batched; enforcement can reclaim before
    // the next read reports the lower inactive-file total.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while cached() >= cached_before && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        cached() < cached_before,
        "allocation must reclaim cached file pages: before {cached_before}, after {}",
        cached()
    );
    for name in ["memory.events.local", "memory.events"] {
        let events = std::fs::read_to_string(parent.join(name)).unwrap();
        assert!(events.lines().any(|line| line == "oom 0"), "{events}");
        assert!(events.lines().any(|line| line == "oom_kill 0"), "{events}");
    }
    std::hint::black_box(&allocation);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn cached_files_leave_capacity_for_a_new_allocation_without_oom() {
    if !enabled() {
        return;
    }
    require_root();
    use nibrunnerd::config::{MemoryAdmission, MemoryAdmissionMode, WorkloadPool};
    let slice = WorkloadSlice::new("384M");
    let mut host = nibrunnerd::test_support::test_host().await;
    let inner = Arc::get_mut(&mut host.host).unwrap();
    inner.commands = commands();
    inner.guest_memory_mib = 384;
    inner.config.memory_admission = Some(MemoryAdmission {
        mode: MemoryAdmissionMode::Adaptive,
        pool: Some(WorkloadPool {
            slice: slice.0.clone(),
            memory_mib: 384.try_into().unwrap(),
        }),
        freeze_after_ms: None,
        reclaim: false,
        headroom_mib: 128.try_into().unwrap(),
    });
    std::fs::create_dir_all(&host.config.runtime_dir).unwrap();
    let service = nibrunnerd::memory_service::MemoryService::restore(host.arc().clone()).unwrap();
    let server = service.serve().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let socket = host.config.runtime_dir.join(protocol::memory::SOCKET_NAME);
    let executable = std::env::current_exe().unwrap();
    let result = commands()
        .run(nibrunnerd::ports::CommandRequest::new(&[
            "systemd-run",
            "--wait",
            "--pipe",
            "--collect",
            &format!("--unit=nibrunner-cache-{id}"),
            &format!("--slice={}", slice.0),
            "--property=MemoryMax=384M",
            "--property=RuntimeMaxSec=30",
            &format!("--setenv=NIBRUNNER_CACHE_TEST_SOCKET={}", socket.display()),
            &format!("--setenv=NIBRUNNER_CACHE_TEST_ID={id}"),
            executable.to_str().unwrap(),
            "--exact",
            "cached_file_memory_client_process",
            "--nocapture",
        ]))
        .await
        .unwrap();
    server.abort();
    assert_eq!(result.code, 0, "{}\n{}", result.stdout, result.stderr);
}

#[cfg(target_os = "linux")]
#[test]
fn workload_pool_allocation_process() {
    if !std::path::Path::new("allocate.marker").exists() {
        return;
    }
    let mut allocation = vec![0u8; 200 * 1_048_576];
    for page in allocation.chunks_mut(4096) {
        page[0] = 1;
    }
    std::fs::write("ready.marker", b"").unwrap();
    std::thread::sleep(std::time::Duration::from_secs(30));
    std::hint::black_box(&allocation);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_shared_workload_pool_contains_a_spike_and_prefers_production_over_preview() {
    if !enabled() {
        return;
    }
    require_root();
    use nibrunnerd::adapters::vm::process::VmProcesses;
    use nibrunnerd::config::{VmBudget, VmBudgets, WorkloadPool};
    use std::os::unix::fs::PermissionsExt;

    let slice = WorkloadSlice::new("infinity");
    let root = tempfile::tempdir().unwrap();
    let executable = std::env::current_exe().unwrap();
    let helper = root.path().join("helper.sh");
    let quoted = executable.to_str().unwrap().replace('\'', "'\"'\"'");
    std::fs::write(
        &helper,
        format!("#!/bin/sh\nexec '{quoted}' --exact workload_pool_allocation_process --nocapture\n"),
    )
    .unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let budget = |priority| VmBudget {
        cpu_percent: 100.try_into().unwrap(),
        memory_mib: 256.try_into().unwrap(),
        memory: Some(protocol::MemoryPolicy {
            target_mib: 256.try_into().unwrap(),
            swap_mib: 0,
            priority,
        }),
    };
    let production = protocol::AppId::parse("pool-production").unwrap();
    let preview = protocol::AppId::parse("pool-preview").unwrap();
    let pool = WorkloadPool {
        slice: slice.0.clone(),
        memory_mib: 384.try_into().unwrap(),
    };
    let processes = VmProcesses::with_budgets(
        root.path().join("run"),
        Some(VmBudgets {
            default: budget(protocol::MemoryPriority::Preview),
            apps: [(production.clone(), budget(protocol::MemoryPriority::Production))].into(),
        }),
    )
    .with_pool(Some(&pool));
    for app in [&production, &preview] {
        let directory = root.path().join(app.as_str());
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("allocate.marker"), b"").unwrap();
        processes.spawn(app, &helper, &directory, None).await.unwrap();
        if app == &production {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            while !directory.join("ready.marker").exists() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "production did not allocate: {}",
                    std::fs::read_to_string(processes.console_path(app)).unwrap_or_default()
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    let parent = std::path::Path::new("/sys/fs/cgroup").join(&slice.0);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let parent_ooms = loop {
        let events = std::fs::read_to_string(parent.join("memory.events.local")).unwrap();
        let parent_ooms = events
            .lines()
            .find_map(|line| line.strip_prefix("oom "))
            .unwrap()
            .parse::<u64>()
            .unwrap();
        if parent_ooms > 0 || tokio::time::Instant::now() >= deadline {
            break parent_ooms;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    let production_active = processes.status(&production).active;
    let production_memory = processes.memory(&production);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while processes.status(&preview).active && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let preview_active = processes.status(&preview).active;
    processes.stop(&production).await;
    processes.stop(&preview).await;
    assert!(
        parent_ooms > 0,
        "the combined allocations must reach the parent limit"
    );
    assert!(
        production_active,
        "the production process must survive the preview spike"
    );
    assert!(!preview_active, "the preview must be the OOM victim");
    let memory = production_memory.unwrap();
    assert!(memory.cgroup.unwrap().starts_with(&format!("/{}/", slice.0)));
    let limits = memory.limits.unwrap();
    assert_eq!(limits.max_bytes, Some(256 * 1_048_576));
    assert_eq!(limits.low_bytes, 256 * 1_048_576);
}

/// Volumes as files under `directory`, over a store holding `archive` for whichever volume asks.
fn local_volumes(
    directory: &std::path::Path,
    commands: Arc<dyn CommandRunner>,
    archive: Vec<u8>,
) -> nibrunnerd::adapters::volumes::local_file::LocalFileVolumes {
    nibrunnerd::adapters::volumes::local_file::LocalFileVolumes::new(
        directory.join("volumes"),
        protocol::ObjectKey::parse("volumes").unwrap(),
        commands.clone(),
        nibrunnerd::adapters::volumes::initial_contents::ContentsStaging::new(
            mocks::artifacts_holding(archive),
            directory.join("initial-contents"),
        ),
    )
}

/// One `debugfs` request against the volume, answered from the filesystem without mounting it.
async fn debugfs(device: &std::path::Path, request: &str) -> String {
    commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "debugfs",
            "-R",
            request,
            &device.display().to_string(),
        ]))
        .await
        .expect("debugfs answers")
}

// The real tool copies the archive in as it formats, and what it copied is read back off the
// device the way an export reads it: by debugfs, so nothing here mounts a tenant's volume.
#[tokio::test]
async fn a_volume_is_formatted_holding_the_contents_the_document_gave_it() {
    if !enabled() {
        return;
    }
    require_root();
    let directory = tempfile::tempdir().unwrap();
    let archive = {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_uid(1000);
        header.set_gid(1000);
        header.set_size(6);
        builder
            .append_data(&mut header, "nested/hello.txt", &b"hello\n"[..])
            .unwrap();
        builder.into_inner().unwrap()
    };
    let volumes = local_volumes(directory.path(), commands(), archive.clone());
    let desired = protocol::DesiredVolume {
        volume_id: protocol::VolumeId::parse("vol-1").unwrap(),
        app_id: protocol::AppId::parse("app-1").unwrap(),
        size_bytes: 16 * 1024 * 1024,
        desired_state: protocol::DesiredPresence::Present,
        initial_contents: Some(nibrunnerd::test_support::initial_contents(&archive, "/app/data")),
    };

    use nibrunnerd::adapters::volumes::VolumeBackend;
    let attached = volumes.provision(&desired).await.expect("the volume is made");
    let device = std::path::PathBuf::from(&attached.device_path);

    let listed = debugfs(&device, "ls -l /upper/app/data/nested").await;
    assert!(listed.contains("hello.txt"), "{listed}");
    let shown = debugfs(&device, "cat /upper/app/data/nested/hello.txt").await;
    assert_eq!(shown, "hello\n");
    // debugfs prints the owner five wide: `User: 65534`, and `User:     0` for root.
    let tenant = format!("User: {:>5}", guest_contract::paths::TENANT_UID);
    let owner = debugfs(&device, "stat /upper/app/data/nested/hello.txt").await;
    assert!(owner.contains(&tenant), "the file is not the tenant's: {owner}");
    let given = debugfs(&device, "stat /upper/app/data").await;
    assert!(
        given.contains(&tenant),
        "the destination is not the tenant's: {given}"
    );
    let above = debugfs(&device, "stat /upper/app").await;
    assert!(
        above.contains("User:     0"),
        "the directory above is not root's: {above}"
    );
    assert!(
        !directory.path().join("initial-contents/vol-1").exists(),
        "the staging is gone once the format has read it"
    );

    let (recorded, log) = mocks::commands_succeeding();
    let second = local_volumes(directory.path(), recorded, archive);
    second
        .provision(&desired)
        .await
        .expect("a converged volume needs nothing");
    assert!(
        log.executables().is_empty(),
        "a formatted volume must never be seeded again"
    );
}

// A disk scratch is a file the guest formats nothing on: the real tool has to leave a filesystem
// the guest mounts, and leave it sparse, or every job would cost its whole scratch at boot.
#[tokio::test]
async fn a_disk_scratch_is_an_ext4_filesystem_that_costs_the_host_only_what_is_written_to_it() {
    if !enabled() {
        return;
    }
    require_root();
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join(nibrunnerd::adapters::vm::scratch::SCRATCH_DISK_FILENAME);
    std::fs::write(&path, b"what the last boot wrote").unwrap();

    nibrunnerd::adapters::vm::scratch::make_disk(
        commands().as_ref(),
        &path,
        std::num::NonZeroU32::new(1024).unwrap(),
    )
    .await
    .expect("the scratch is made");

    let made = std::fs::metadata(&path).unwrap();
    assert_eq!(made.len(), 1024 * 1024 * 1024);
    assert!(
        made.blocks() * 512 < made.len() / 8,
        "{} bytes allocated for a fresh scratch",
        made.blocks() * 512
    );
    let stats = debugfs(&path, "stats").await;
    assert!(stats.contains("0xEF53"), "{stats}");
    assert!(
        stats.contains("has_journal") || stats.contains("extent"),
        "{stats}"
    );

    nibrunnerd::adapters::vm::scratch::remove_disk(&path).unwrap();
    assert!(!path.exists());
    nibrunnerd::adapters::vm::scratch::remove_disk(&path).expect("a scratch already gone is no error");
}

#[tokio::test]
async fn the_isolation_ruleset_loads_into_the_kernel() {
    if !enabled() {
        return;
    }
    require_root();
    let firewall = nibrunnerd::adapters::net::firewall::HostFirewall::new(commands());
    let state = nft_render::FirewallState {
        instances: vec![nft_render::ForwardedInstance {
            app_id: protocol::AppId::parse("app-1").unwrap(),
            ports: vec![nft_render::ForwardedPort {
                host_port: protocol::HostPort::new(21_000).unwrap(),
                guest_port: protocol::GuestPort::new(3000).unwrap(),
                raw: false,
            }],
            host_ipv4: protocol::Ipv4Address::parse("10.201.0.1").unwrap(),
            guest_ipv4: protocol::Ipv4Address::parse("10.201.0.2").unwrap(),
        }],
        allowed_host_tcp_endpoints: vec![
            "203.0.113.10:443".parse().unwrap(),
            "[2001:db8::10]:443".parse().unwrap(),
        ],
        denied_egress_addresses_v4: vec!["10.43.0.0/16".into()],
        denied_egress_addresses_v6: vec!["2600:1f18:abcd::/56".into()],
    };
    firewall
        .apply(&state)
        .await
        .expect("the kernel takes the ruleset");

    let held = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "nft", "list", "table", "ip", "nibrun",
        ]))
        .await
        .expect("the kernel names the table");
    assert!(held.contains("reject comment \"instance metadata endpoint\""));
    assert!(held.contains("reject comment \"guest to guest\""));
    assert!(held.contains("reject comment \"guest to host\""));
    assert!(held.contains("ip daddr 203.0.113.10 tcp dport 443 accept"));
    assert!(held.contains("ct direction reply ct state established,related accept"));
    let activity_rules: Vec<_> = held
        .lines()
        .filter(|line| line.contains("counter name") && line.contains("work_app-1"))
        .collect();
    assert_eq!(activity_rules.len(), 2, "{held}");
    assert!(
        activity_rules
            .iter()
            .all(|line| line.contains("ct direction original")),
        "{held}"
    );
    assert!(
        held.lines()
            .any(|line| line.contains("counter name") && line.contains("rx_app-1")),
        "{held}"
    );
    assert!(held.contains("dnat to 10.201.0.2:3000"));
    assert!(held.contains("masquerade"));
    assert!(held.contains("hook output"));
    assert!(!held.contains("drop"));

    let v6 = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "nft", "list", "table", "ip6", "nibrun",
        ]))
        .await
        .expect("the kernel names the v6 table");
    assert!(v6.contains("ip6 daddr 2001:db8::10 tcp dport 443 accept"));
    assert!(v6.contains("fe80::/10"));
    assert!(v6.contains("2600:1f18:abcd::/56"));

    let traffic = firewall.traffic().await.expect("the kernel lists its counters");
    assert!(traffic.contains_key(&protocol::AppId::parse("app-1").unwrap()));

    firewall.apply(&state).await.expect("a rerun is not an error");
}

#[tokio::test]
async fn a_volume_is_formatted_by_the_real_tool_and_read_back_as_formatted() {
    if !enabled() {
        return;
    }
    require_root();
    let directory = tempfile::tempdir().unwrap();
    let volumes = local_volumes(directory.path(), commands(), Vec::new());
    let desired = protocol::DesiredVolume {
        volume_id: protocol::VolumeId::parse("vol-1").unwrap(),
        app_id: protocol::AppId::parse("app-1").unwrap(),
        size_bytes: 16 * 1024 * 1024,
        desired_state: protocol::DesiredPresence::Present,
        initial_contents: None,
    };

    use nibrunnerd::adapters::volumes::VolumeBackend;
    let attached = volumes.provision(&desired).await.expect("the volume is made");
    assert_eq!(attached.size_bytes, desired.size_bytes);

    let (recorded, log) = mocks::commands_succeeding();
    let second = local_volumes(directory.path(), recorded, Vec::new());
    second
        .provision(&desired)
        .await
        .expect("a converged volume needs nothing");
    assert!(
        log.executables().is_empty(),
        "a formatted volume must never be formatted again"
    );
}

// A device under a slot that has changed hands is told from the volume it should carry by the
// uuid the real tool writes into the filesystem, so what that tool gives two volumes has to be
// two different things.
#[tokio::test]
async fn volumes_the_real_tool_formats_carry_a_filesystem_that_names_one_apart_from_another() {
    if !enabled() {
        return;
    }
    require_root();
    let directory = tempfile::tempdir().unwrap();
    let volumes = local_volumes(directory.path(), commands(), Vec::new());
    let volume_of = |name: &str| protocol::DesiredVolume {
        volume_id: protocol::VolumeId::parse(name).unwrap(),
        app_id: protocol::AppId::parse("app-1").unwrap(),
        size_bytes: 16 * 1024 * 1024,
        desired_state: protocol::DesiredPresence::Present,
        initial_contents: None,
    };

    use nibrunnerd::adapters::volumes::VolumeBackend;
    let one = volume_of("vol-1");
    let another = volume_of("vol-2");
    volumes.provision(&one).await.expect("the volume is made");
    volumes.provision(&another).await.expect("the volume is made");

    use nibrunnerd::adapters::volumes::filesystem_uuid;
    let read = |desired: &protocol::DesiredVolume| {
        filesystem_uuid(&volumes.path_for(&desired.volume_id).display().to_string())
            .expect("the real tool names every filesystem it makes")
    };
    assert_eq!(read(&one), read(&one), "a volume names itself the same twice");
    assert_ne!(
        read(&one),
        read(&another),
        "two volumes that named themselves the same could not be told apart on a device"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_tap_is_created_addressed_and_given_the_guest_it_will_hold() {
    if !enabled() {
        return;
    }
    require_root();
    use nibrunnerd::adapters::net::attachment::{HostInterface, HostNetwork, KernelNetwork, Neighbour};

    let namespace_dir = tempfile::tempdir().unwrap();
    let network = KernelNetwork::open(namespace_dir.path().to_owned()).expect("a netlink socket");
    let slot = nft_render::describe_slot(
        nft_render::most_apps_the_ports_fit() - 1,
        protocol::AppId::parse("integration").unwrap(),
    );
    let tap = HostInterface {
        interface_name: slot.interface_name.clone(),
        host_ipv4: slot.host_ipv4.clone(),
        subnet_prefix_length: slot.subnet_prefix_length,
    };
    network.ensure_tap(&tap).await.expect("the tap is made");
    network
        .ensure_tap(&tap)
        .await
        .expect("a second pass changes nothing");
    assert!(network.attachment_names().await.contains(&slot.interface_name));

    network
        .refresh_neighbour(&Neighbour {
            guest_ipv4: slot.guest_ipv4.clone(),
            guest_mac: slot.guest_mac.clone(),
            interface_name: slot.interface_name.clone(),
        })
        .await
        .expect("the neighbour entry is written");

    let neighbours = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "ip",
            "neigh",
            "show",
            "dev",
            &slot.interface_name,
        ]))
        .await
        .unwrap_or_default();
    assert!(
        neighbours.contains(slot.guest_ipv4.as_str()) || neighbours.is_empty(),
        "the neighbour entry should name the guest this slot holds"
    );

    network
        .delete_attachment(&slot.interface_name)
        .await
        .expect("the tap is taken back");
    assert!(
        !network.attachment_names().await.contains(&slot.interface_name),
        "a tap a persistent flag kept alive is gone once the app that held it is"
    );
    network
        .delete_attachment(&slot.interface_name)
        .await
        .expect("a tap that is already gone is the state being asked for, not a failure");
}

#[cfg(target_os = "linux")]
fn namespace_interface(offset: u32) -> nibrunnerd::adapters::net::attachment::NamespaceInterface {
    use nibrunnerd::adapters::net::attachment::{HostInterface, NamespaceInterface};
    let slot = nft_render::describe_slot(
        nft_render::most_apps_the_ports_fit() - offset,
        protocol::AppId::parse("namespace-integration").unwrap(),
    );
    NamespaceInterface {
        host: HostInterface {
            interface_name: slot.interface_name,
            host_ipv4: slot.host_ipv4,
            subnet_prefix_length: slot.subnet_prefix_length,
        },
        guest_ipv4: slot.guest_ipv4,
        guest_mac: slot.guest_mac,
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_namespace_survives_reopening_isolates_ports_and_reclaims_orphaned_links() {
    if !enabled() {
        return;
    }
    require_root();
    use nibrunnerd::adapters::net::attachment::{HostNetwork, KernelNetwork};
    use std::os::unix::fs::MetadataExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let directory = tempfile::tempdir().unwrap();
    let namespace_dir = directory.path().join("network");
    let network = KernelNetwork::open(namespace_dir.clone()).unwrap();
    let interface = namespace_interface(2);
    let name = &interface.host.interface_name;
    let original_namespace = std::fs::read_link("/proc/thread-self/ns/net").unwrap();
    let namespace = network.ensure_namespace(&interface).await.unwrap();
    let inode = std::fs::metadata(&namespace).unwrap().ino();
    let host_index = std::fs::read_to_string(format!("/sys/class/net/{name}/ifindex")).unwrap();
    drop(network);
    let network = KernelNetwork::open(namespace_dir).unwrap();
    assert_eq!(network.ensure_namespace(&interface).await.unwrap(), namespace);
    assert_eq!(std::fs::metadata(&namespace).unwrap().ino(), inode);
    assert_eq!(
        std::fs::read_to_string(format!("/sys/class/net/{name}/ifindex")).unwrap(),
        host_index
    );
    assert_eq!(
        std::fs::read_link("/proc/thread-self/ns/net").unwrap(),
        original_namespace
    );
    assert!(network.attachment_names().await.contains(name));
    assert!(network.ensure_tap(&interface.host).await.is_err());

    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    let ready = directory.path().join("ready");
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "namespace_tcp_server_process", "--nocapture"])
        .env("NIBRUNNER_TEST_NAMESPACE", &namespace)
        .env(
            "NIBRUNNER_TEST_NAMESPACE_INTERFACE",
            serde_json::json!({
                "address": interface.guest_ipv4.as_str(),
                "gateway": interface.host.host_ipv4.as_str(),
                "mac": interface.guest_mac,
                "port": port,
                "ready": ready,
            })
            .to_string(),
        )
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                return Err(format!("namespace server exited before binding: {status}"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let mut stream = tokio::net::TcpStream::connect((interface.guest_ipv4.addr(), port))
            .await
            .map_err(|error| error.to_string())?;
        stream
            .write_all(b"ping")
            .await
            .map_err(|error| error.to_string())?;
        let mut reply = [0; 4];
        stream
            .read_exact(&mut reply)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(reply)
    })
    .await;
    let _ = child.kill().await;

    commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "ip", "link", "delete", name,
        ]))
        .await
        .unwrap();
    assert!(
        network.attachment_names().await.contains(name),
        "an orphan namespace must remain discoverable"
    );
    network.delete_attachment(name).await.unwrap();
    network.delete_attachment(name).await.unwrap();
    assert!(!namespace.exists());
    assert!(!network.attachment_names().await.contains(name));
    assert_eq!(response.unwrap().unwrap(), *b"pong");
}

#[cfg(target_os = "linux")]
#[test]
fn namespace_tcp_server_process() {
    use std::io::{Read, Write};
    let Ok(namespace) = std::env::var("NIBRUNNER_TEST_NAMESPACE") else {
        return;
    };
    require_root();
    let config: serde_json::Value =
        serde_json::from_str(&std::env::var("NIBRUNNER_TEST_NAMESPACE_INTERFACE").unwrap()).unwrap();
    nix::sched::setns(
        std::fs::File::open(namespace).unwrap(),
        nix::sched::CloneFlags::CLONE_NEWNET,
    )
    .unwrap();
    let ip = |args: &[&str]| {
        let output = std::process::Command::new("ip").args(args).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    };
    let addresses = ip(&["-j", "address", "show", "dev", "eth0"]);
    assert_eq!(addresses[0]["address"], config["mac"]);
    assert!(addresses[0]["addr_info"]
        .as_array()
        .unwrap()
        .iter()
        .any(|address| address["local"] == config["address"]));
    let routes = ip(&["-j", "route", "show", "default"]);
    assert_eq!(routes[0]["gateway"], config["gateway"]);
    let port = u16::try_from(config["port"].as_u64().unwrap()).unwrap();
    let _same_port = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    let listener = std::net::TcpListener::bind((config["address"].as_str().unwrap(), port)).unwrap();
    std::fs::write(config["ready"].as_str().unwrap(), b"").unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let mut request = [0; 4];
    stream.read_exact(&mut request).unwrap();
    assert_eq!(&request, b"ping");
    stream.write_all(b"pong").unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn namespace_attachments_refuse_foreign_interfaces_and_unsafe_handles() {
    if !enabled() {
        return;
    }
    require_root();
    use nibrunnerd::adapters::net::attachment::{HostNetwork, KernelNetwork};
    let directory = tempfile::tempdir().unwrap();
    let network = KernelNetwork::open(directory.path().to_owned()).unwrap();
    let interface = namespace_interface(3);
    let name = &interface.host.interface_name;
    let path = directory.path().join(name);
    commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "ip",
            "link",
            "add",
            name,
            "type",
            "veth",
            "peer",
            "name",
            "nib-test-peer",
        ]))
        .await
        .unwrap();
    let index_path = format!("/sys/class/net/{name}/ifindex");
    let index = std::fs::read_to_string(&index_path).unwrap();
    assert!(network.ensure_namespace(&interface).await.is_err());
    assert!(network.ensure_tap(&interface.host).await.is_err());
    assert!(network.delete_attachment(name).await.is_err());
    assert!(!network.attachment_names().await.contains(name));
    assert_eq!(std::fs::read_to_string(index_path).unwrap(), index);
    commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "ip", "link", "delete", name,
        ]))
        .await
        .unwrap();
    std::fs::write(&path, b"unrelated").unwrap();
    assert!(network.ensure_namespace(&interface).await.is_err());
    assert!(network.delete_attachment(name).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"unrelated");
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink("/proc/self/ns/net", &path).unwrap();
    assert!(network.ensure_namespace(&interface).await.is_err());
    assert!(network.delete_attachment(name).await.is_err());
    assert!(path.is_symlink());
    std::fs::remove_file(&path).unwrap();
    let mut invalid = interface.clone();
    invalid.guest_mac = "1:2:3:4:5:6".into();
    assert!(network.ensure_namespace(&invalid).await.is_err());
    for name in ["../outside", "nbr1234567890123456", "lo"] {
        invalid = interface.clone();
        invalid.host.interface_name = name.into();
        assert!(network.ensure_namespace(&invalid).await.is_err());
        assert!(network.delete_attachment(name).await.is_err());
    }
    assert!(!path.exists());
    assert!(!std::path::Path::new(&format!("/sys/class/net/{}", interface.host.interface_name)).exists());
}

#[tokio::test]
async fn the_embedded_hypervisor_runs_on_this_host() {
    if !enabled() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let binary =
        nibrunnerd::adapters::vm::process::extract_firecracker(directory.path()).expect("a hypervisor");
    let version = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            &binary.display().to_string(),
            "--version",
        ]))
        .await
        .expect("the hypervisor answers");
    assert!(
        version.contains(nibrunnerd::adapters::vm::process::FIRECRACKER_VERSION),
        "it should name the version this build pins, said: {version}"
    );
}

// The browse half of the guest contract has never been driven from outside its own unit tests:
// removing the control plane took away the only thing that asked, and it has been waiting for
// something to ask ever since. This is that, without putting a service back into the daemon —
// point it at a running guest's control socket and it speaks every verb the contract defines.
//
//   NIBRUNNER_GUEST_VSOCK=/var/lib/nibrunner/vm/<app>/<socket> NIBRUNNER_INTEGRATION=1 \
//     cargo test -p nibrunnerd --test integration -- --nocapture browse
#[tokio::test]
async fn every_browse_verb_answers_from_a_running_guest() {
    if !enabled() {
        return;
    }
    let Ok(socket) = std::env::var("NIBRUNNER_GUEST_VSOCK") else {
        eprintln!("NIBRUNNER_GUEST_VSOCK names no guest, so the browse verbs are not driven");
        return;
    };
    require_root();
    use nibrunnerd::domain::filesystem::client::GuestFilesystem;
    use protocol::{FilesystemEntryKind, GuestPath};

    let app_id = protocol::AppId::parse("browse").unwrap();
    let socket = std::path::PathBuf::from(socket);
    let mut guest = GuestFilesystem::dial(
        &app_id,
        &guest_contract::channels::ChannelEndpoint {
            path: socket,
            vsock_port: Some(guest_contract::vsock::GUEST_FILESYSTEM_VSOCK_PORT),
        },
    )
    .await
    .expect("a guest answers on its control socket");

    // Every path a guest is asked for is resolved inside the volume its own app owns, so the
    // root here is the tenant's data directory rather than the guest's filesystem.
    let data = GuestPath::parse("/").unwrap();
    let listing = guest.list(&data).await.expect("list");
    println!("list {}: {} entries", data.as_str(), listing.entries.len());

    let usage = guest.usage().await.expect("usage");
    let compute = guest.compute().await.expect("compute");
    println!(
        "usage {}/{} bytes, compute {}/{} bytes",
        usage.used_bytes, usage.total_bytes, compute.memory_used_bytes, compute.memory_total_bytes
    );
    assert!(
        usage.total_bytes > 0,
        "a volume with no size is not one a tenant has"
    );

    let directory = GuestPath::parse("/browse-check").unwrap();
    let file = GuestPath::parse("/browse-check/note").unwrap();
    let _ = guest.remove(&file).await;
    let _ = guest.remove(&directory).await;

    guest.make_directory(&directory).await.expect("mkdir");
    let written = guest
        .write(&file, 0, b"what the guest was handed".to_vec(), true)
        .await
        .expect("write");
    assert_eq!(written as usize, b"what the guest was handed".len());

    let read = guest.read(&file, 0, 4096).await.expect("read");
    assert_eq!(
        read, b"what the guest was handed",
        "what came back is not what went in"
    );

    let details = guest.stat(&file).await.expect("stat");
    assert_eq!(details.kind, FilesystemEntryKind::File);
    assert_eq!(details.size_bytes, read.len() as u64);

    let listed = guest.list(&directory).await.expect("list the new directory");
    assert!(
        listed.entries.iter().any(|entry| entry.name == "note"),
        "a file just written is not in its own directory"
    );

    // A directory that still holds something is refused rather than emptied.
    let refusal = guest.remove(&directory).await;
    assert!(refusal.is_err(), "a directory with a file in it was removed");
    println!("removing a full directory: {}", refusal.unwrap_err());

    let moved = GuestPath::parse("/browse-check/moved").unwrap();
    guest.move_entry(&file, &moved).await.expect("move");
    assert!(guest.stat(&file).await.is_err(), "the old name still answers");
    assert_eq!(
        guest.stat(&moved).await.expect("stat the new name").size_bytes,
        read.len() as u64
    );

    guest.remove(&moved).await.expect("remove the file");
    guest
        .remove(&directory)
        .await
        .expect("remove the empty directory");
    assert!(
        guest.stat(&directory).await.is_err(),
        "a directory that was removed still answers"
    );
    println!("every verb answered");
}
