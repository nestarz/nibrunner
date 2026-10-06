use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use protocol::AppId;
use serde::{Deserialize, Serialize};

use crate::adapters::vm::status::{VmExit, VmStatus};
use crate::json_store::{make_directory, read_json, write_json};

const FIRECRACKER: &[u8] = include_bytes!(env!("NIBRUNNER_FIRECRACKER_PATH"));
pub const FIRECRACKER_VERSION: &str = env!("NIBRUNNER_FIRECRACKER_VERSION");

const RUNTIME_DIR_MODE: u32 = 0o700;
const EXECUTABLE_MODE: u32 = 0o755;

pub fn carries_firecracker() -> bool {
    option_env!("NIBRUNNER_FIRECRACKER_EMBEDDED").is_some()
}

pub fn extract_firecracker(directory: &Path) -> std::io::Result<PathBuf> {
    let versioned = directory.join(FIRECRACKER_VERSION);
    let binary = versioned.join("firecracker");
    if !carries_firecracker() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "this build carries no hypervisor, so it can boot nothing",
        ));
    }
    if std::fs::metadata(&binary).is_ok_and(|info| info.len() == FIRECRACKER.len() as u64) {
        return Ok(binary);
    }
    make_directory(&versioned, RUNTIME_DIR_MODE)?;
    let staged = versioned.join(format!("firecracker.{}.tmp", std::process::id()));
    std::fs::write(&staged, FIRECRACKER)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(EXECUTABLE_MODE))?;
    }
    std::fs::rename(&staged, &binary)?;
    Ok(binary)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VmRecord {
    pub app_id: AppId,
    pub pid: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_ticks: Option<u64>,
    #[serde(default)]
    pub frozen: bool,
    #[serde(default)]
    pub firecracker_version: Option<String>,
    pub host_boot_id: String,
    pub started_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// The signal the process died under, when it did not exit with a code of its own. Beside
    /// `exit_code` rather than folded into it so a record an earlier daemon wrote still reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default)]
    pub stop_requested: bool,
}

impl VmRecord {
    pub fn exit(&self) -> Option<VmExit> {
        self.signal
            .map(VmExit::Signal)
            .or(self.exit_code.map(VmExit::Code))
    }

    fn ended(&mut self, exit: VmExit) {
        match exit {
            VmExit::Code(code) => self.exit_code = Some(code),
            VmExit::Signal(signal) => self.signal = Some(signal),
        }
    }
}

fn exit_of(waited: std::io::Result<std::process::ExitStatus>) -> VmExit {
    use std::os::unix::process::ExitStatusExt;
    let Ok(status) = waited else {
        return VmExit::Code(-1);
    };
    status
        .code()
        .map(VmExit::Code)
        .or_else(|| status.signal().map(VmExit::Signal))
        .unwrap_or(VmExit::Code(-1))
}

const HOST_BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

pub fn read_host_boot_id() -> std::io::Result<String> {
    std::fs::read_to_string(HOST_BOOT_ID_PATH).map(|value| value.trim().to_string())
}

pub fn host_boot_id_or_session() -> String {
    read_host_boot_id().unwrap_or_else(|_| format!("session-{}", std::process::id()))
}

pub struct VmProcesses {
    policy: Arc<crate::runtime_policy::RuntimePolicy>,
    runtime_dir: PathBuf,
    boot_id: String,
}

impl VmProcesses {
    pub fn with_budgets(runtime_dir: PathBuf, budgets: Option<crate::config::VmBudgets>) -> Self {
        Self::with_policy(
            runtime_dir,
            Arc::new(crate::runtime_policy::RuntimePolicy::new(None, budgets)),
        )
    }

    pub fn with_policy(runtime_dir: PathBuf, policy: Arc<crate::runtime_policy::RuntimePolicy>) -> Self {
        Self {
            policy,
            ..Self::new(runtime_dir)
        }
    }

    fn command(&self, app_id: &AppId, binary: &Path) -> tokio::process::Command {
        let Some(budget) = self.policy.vm_budget(app_id) else {
            return tokio::process::Command::new(binary);
        };
        let mut command = tokio::process::Command::new("systemd-run");
        let memory = super::memory::Controls::for_budget(&budget);
        // Scope mode execs the VMM in place; a fresh unit name avoids waiting for the old scope to be collected.
        command
            .args(["--scope", "--quiet", "--collect", "--slice=-.slice"])
            .arg(format!("--property=CPUQuota={}%", budget.cpu_percent))
            .arg(format!("--property=MemoryLow={}", memory.low))
            .arg(format!("--property=MemoryHigh={}", memory.high))
            .arg(format!("--property=MemoryMax={}", memory.max))
            .arg(format!("--property=MemorySwapMax={}", memory.swap))
            .arg("--")
            .arg(binary);
        command
    }

    pub fn new(runtime_dir: PathBuf) -> Self {
        Self {
            policy: Arc::default(),
            runtime_dir,
            boot_id: host_boot_id_or_session(),
        }
    }

    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub fn api_socket(&self, app_id: &AppId) -> PathBuf {
        self.runtime_dir.join(format!("vm-{app_id}.sock"))
    }

    pub fn record_path(&self, app_id: &AppId) -> PathBuf {
        self.runtime_dir.join(format!("vm-{app_id}.json"))
    }

    pub fn console_path(&self, app_id: &AppId) -> PathBuf {
        self.runtime_dir.join(format!("vm-{app_id}.console"))
    }

    pub fn read_record(&self, app_id: &AppId) -> Option<VmRecord> {
        read_json(&self.record_path(app_id)).ok().flatten()
    }

    pub fn write_record(&self, record: &VmRecord) -> std::io::Result<()> {
        make_directory(&self.runtime_dir, RUNTIME_DIR_MODE)?;
        write_json(&self.record_path(&record.app_id), record)
            .map_err(|error| std::io::Error::other(error.message()))
    }

    pub fn forget(&self, app_id: &AppId) {
        let _ = std::fs::remove_file(self.record_path(app_id));
        let _ = std::fs::remove_file(self.api_socket(app_id));
        let _ = std::fs::remove_file(self.console_path(app_id));
    }

    pub fn adopted_app_ids(&self) -> Vec<AppId> {
        let Ok(entries) = std::fs::read_dir(&self.runtime_dir) else {
            return Vec::new();
        };
        entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let stem = name.strip_prefix("vm-")?.strip_suffix(".json")?;
                AppId::parse(stem).ok()
            })
            .collect()
    }

    pub fn status(&self, app_id: &AppId) -> VmStatus {
        let Some(record) = self.read_record(app_id) else {
            return VmStatus::default();
        };
        let started_this_boot = record.host_boot_id == self.boot_id;
        let exit = record.exit();
        let active = started_this_boot && exit.is_none() && is_alive(record.pid);
        VmStatus {
            loaded: true,
            active,
            frozen: active && record.frozen,
            failed: !active && !record.stop_requested && exit.is_some_and(|exit| exit != VmExit::Code(0)),
            started_this_boot,
            exit,
        }
    }

    pub fn memory(&self, app_id: &AppId) -> Option<protocol::ReportedMemory> {
        let record = self.read_record(app_id)?;
        let started = record.start_ticks?;
        if record.host_boot_id != self.boot_id
            || record.exit().is_some()
            || process_start_ticks(record.pid) != Some(started)
        {
            return None;
        }
        let memory = super::memory::read_process(record.pid)?;
        (process_start_ticks(record.pid) == Some(started)).then_some(memory)
    }

    pub fn remember_frozen(&self, app_id: &AppId, frozen: bool) -> std::io::Result<()> {
        let mut record = self
            .read_record(app_id)
            .ok_or_else(|| std::io::Error::other("the VM has no process record"))?;
        record.frozen = frozen;
        self.write_record(&record)
    }

    fn cgroup(&self, app_id: &AppId) -> std::io::Result<crate::adapters::cgroup::Group> {
        let record = self
            .read_record(app_id)
            .filter(|record| {
                record.host_boot_id == self.boot_id
                    && record.exit().is_none()
                    && is_alive(record.pid)
                    && record
                        .start_ticks
                        .is_some_and(|ticks| process_start_ticks(record.pid) == Some(ticks))
            })
            .ok_or_else(|| std::io::Error::other("the VM process is not running"))?;
        crate::adapters::cgroup::Group::for_process(record.pid)
    }

    pub async fn freeze(&self, app_id: &AppId) -> std::io::Result<()> {
        self.cgroup(app_id)?.freeze().await
    }

    pub async fn thaw(&self, app_id: &AppId) -> std::io::Result<()> {
        self.cgroup(app_id)?.thaw().await
    }

    pub fn frozen(&self, app_id: &AppId) -> std::io::Result<bool> {
        self.cgroup(app_id)?.frozen()
    }

    pub async fn reclaim_frozen(&self, app_id: &AppId, bytes: u64) -> std::io::Result<u64> {
        if !self.frozen(app_id)? {
            return Err(std::io::Error::other("reclaim needs a frozen workload"));
        }
        self.reclaim_memory(app_id, bytes).await
    }

    pub async fn reclaim_memory(&self, app_id: &AppId, bytes: u64) -> std::io::Result<u64> {
        self.cgroup(app_id)?.reclaim(bytes).await
    }

    pub async fn spawn(
        &self,
        app_id: &AppId,
        binary: &Path,
        working_dir: &Path,
        config_file: Option<&Path>,
    ) -> std::io::Result<VmRecord> {
        make_directory(&self.runtime_dir, RUNTIME_DIR_MODE)?;
        let api_socket = self.api_socket(app_id);
        let _ = std::fs::remove_file(&api_socket);
        let _ = std::fs::remove_file(working_dir.join(guest_contract::vsock::GUEST_VSOCK_FILENAME));

        let console = std::fs::File::create(self.console_path(app_id))?;
        let mut command = self.command(app_id, binary);

        command
            .arg("--api-sock")
            .arg(&api_socket)
            .current_dir(working_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(console.try_clone()?))
            .stderr(Stdio::from(console))
            .kill_on_drop(false);
        if let Some(config_file) = config_file {
            command.arg("--config-file").arg(config_file);
        }
        #[cfg(unix)]
        {
            let oom_score = self
                .policy
                .vm_budget(app_id)
                .map(|budget| super::memory::Controls::for_budget(&budget).oom_score)
                .unwrap_or("0");
            #[allow(
                unsafe_code,
                reason = "session and OOM policy must be set between fork and exec"
            )]
            unsafe {
                let constrained = command.as_std().get_program() == "systemd-run";
                command.pre_exec(move || {
                    if libc::setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    #[cfg(target_os = "linux")]
                    if constrained {
                        let descriptor = libc::open(
                            c"/proc/self/oom_score_adj".as_ptr(),
                            libc::O_WRONLY | libc::O_CLOEXEC,
                        );
                        if descriptor < 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                        let written = libc::write(descriptor, oom_score.as_ptr().cast(), oom_score.len());
                        let error = std::io::Error::last_os_error();
                        libc::close(descriptor);
                        if written != oom_score.len() as isize {
                            return Err(error);
                        }
                    }
                    #[cfg(not(target_os = "linux"))]
                    if constrained {
                        let _ = oom_score;
                        return Err(std::io::Error::other("VM budgets require Linux and systemd"));
                    }
                    Ok(())
                });
            }
        }
        let mut child = command.spawn()?;
        let pid = child.id().map(|pid| pid as i32).unwrap_or(-1);
        let record = VmRecord {
            app_id: app_id.clone(),
            pid,
            start_ticks: process_start_ticks(pid),
            frozen: false,
            firecracker_version: Some(FIRECRACKER_VERSION.into()),
            host_boot_id: self.boot_id.clone(),
            started_at_ms: crate::clock::now_ms(),
            exit_code: None,
            signal: None,
            stop_requested: false,
        };
        self.write_record(&record)?;

        let processes = Self {
            policy: self.policy.clone(),
            runtime_dir: self.runtime_dir.clone(),
            boot_id: self.boot_id.clone(),
        };
        let app_id = app_id.clone();
        tokio::spawn(async move {
            let exit = exit_of(child.wait().await);
            if let Some(mut record) = processes.read_record(&app_id) {
                if record.pid == pid {
                    record.ended(exit);
                    let _ = processes.write_record(&record);
                }
            }
        });
        Ok(record)
    }

    pub async fn stop(&self, app_id: &AppId) {
        let Some(mut record) = self.read_record(app_id) else {
            return;
        };
        record.stop_requested = true;
        let _ = self.write_record(&record);
        if !is_alive(record.pid) {
            return;
        }
        if self.frozen(app_id).unwrap_or(false) {
            let _ = self.thaw(app_id).await;
        }
        signal(record.pid, libc::SIGTERM);
        let deadline =
            std::time::Duration::from_millis(guest_contract::control::GUEST_SHUTDOWN_GRACE_MS + 5_000);
        let started = std::time::Instant::now();
        while started.elapsed() < deadline {
            if !is_alive(record.pid) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        signal(record.pid, libc::SIGKILL);
    }
}

pub(crate) fn process_start_ticks(pid: i32) -> Option<u64> {
    start_ticks(&std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?)
}

fn start_ticks(stat: &str) -> Option<u64> {
    stat.rsplit_once(") ")?.1.split_whitespace().nth(19)?.parse().ok()
}

fn is_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    #[allow(
        unsafe_code,
        reason = "asking the kernel whether a pid exists has no safe spelling"
    )]
    unsafe {
        libc::kill(pid, 0) == 0
    }
}

fn signal(pid: i32, signal: libc::c_int) {
    if pid > 0 {
        #[allow(unsafe_code, reason = "signalling a recorded pid has no safe spelling")]
        unsafe {
            libc::kill(pid, signal);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::app_id;

    fn processes(directory: &Path) -> VmProcesses {
        VmProcesses {
            policy: Arc::default(),
            runtime_dir: directory.to_path_buf(),
            boot_id: "boot-1".into(),
        }
    }

    #[test]
    fn process_identity_uses_the_start_field_after_a_name_with_spaces_and_parentheses() {
        let fields = std::iter::repeat_n("0", 19)
            .chain(["1234", "5678"])
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(
            start_ticks(&format!("42 (a ) strange ( name) {fields}")),
            Some(1234)
        );
        assert_eq!(start_ticks("42 (incomplete) S 0"), None);
    }

    #[tokio::test]
    async fn freeze_refuses_a_missing_or_reused_process_identity() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        for ticks in [None, Some(u64::MAX)] {
            processes
                .write_record(&VmRecord {
                    app_id: app_id(),
                    pid: std::process::id() as i32,
                    start_ticks: ticks,
                    frozen: false,
                    firecracker_version: None,
                    host_boot_id: "boot-1".into(),
                    started_at_ms: 0,
                    exit_code: None,
                    signal: None,
                    stop_requested: false,
                })
                .unwrap();
            assert!(processes.freeze(&app_id()).await.is_err());
        }
    }

    #[test]
    fn vm_budgets_are_opt_in_and_scope_the_same_executable_with_explicit_limits() {
        let root = tempfile::tempdir().unwrap();
        let direct = VmProcesses::new(root.path().into());
        assert_eq!(
            direct
                .command(&app_id(), Path::new("/bin/firecracker"))
                .as_std()
                .get_program(),
            "/bin/firecracker"
        );
        let scoped = VmProcesses::with_budgets(
            root.path().into(),
            Some(crate::config::VmBudgets {
                default: crate::config::VmBudget {
                    cpu_percent: 50.try_into().unwrap(),
                    memory_mib: 1280.try_into().unwrap(),
                    memory: None,
                },
                apps: Default::default(),
            }),
        );
        let command = scoped.command(&app_id(), Path::new("/bin/firecracker"));
        let args: Vec<_> = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_str().unwrap())
            .collect();
        assert_eq!(command.as_std().get_program(), "systemd-run");
        assert!(args.contains(&"--scope"));
        assert!(args.contains(&"--slice=-.slice"));
        assert!(args.contains(&"--property=CPUQuota=50%"));
        assert!(args.contains(&"--property=MemoryMax=1342177280"));
        assert_eq!(args.last(), Some(&"/bin/firecracker"));
    }

    #[test]
    fn reloaded_budgets_are_used_by_the_next_process_without_changing_a_prepared_command() {
        let processes = VmProcesses::new(PathBuf::from("/run/nibrunner"));
        let before = processes.command(&app_id(), Path::new("/bin/firecracker"));
        processes.policy.replace(
            None,
            Some(crate::config::VmBudgets {
                default: crate::config::VmBudget {
                    cpu_percent: 25.try_into().unwrap(),
                    memory_mib: 512.try_into().unwrap(),
                    memory: None,
                },
                apps: Default::default(),
            }),
        );
        let after = processes.command(&app_id(), Path::new("/bin/firecracker"));
        assert_eq!(before.as_std().get_program(), "/bin/firecracker");
        assert_eq!(after.as_std().get_program(), "systemd-run");
        assert!(after
            .as_std()
            .get_args()
            .any(|arg| arg == "--property=CPUQuota=25%"));
        assert!(after
            .as_std()
            .get_args()
            .any(|arg| arg == "--property=MemoryMax=536870912"));
    }

    #[test]
    fn document_budget_overrides_the_next_scope_command() {
        let processes = VmProcesses::new(PathBuf::from("/run/nibrunner"));
        processes
            .policy
            .replace_instances(&[crate::test_support::desired_instance(|instance| {
                instance.limits = Some(protocol::InstanceLimits {
                    concurrent: 8.try_into().unwrap(),
                    cpu_percent: 75.try_into().unwrap(),
                    memory_mib: 640.try_into().unwrap(),
                    memory: None,
                });
            })]);
        let command = processes.command(&app_id(), Path::new("/bin/firecracker"));
        assert_eq!(command.as_std().get_program(), "systemd-run");
        assert!(command
            .as_std()
            .get_args()
            .any(|arg| arg == "--property=CPUQuota=75%"));
        assert!(command
            .as_std()
            .get_args()
            .any(|arg| arg == "--property=MemoryMax=671088640"));
    }

    #[test]
    fn a_host_with_no_record_holds_no_microvm() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        assert_eq!(processes.status(&app_id()), VmStatus::default());
        assert!(processes.adopted_app_ids().is_empty());
    }

    #[test]
    fn a_record_from_before_the_host_rebooted_has_not_run_this_boot() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        processes
            .write_record(&VmRecord {
                app_id: app_id(),
                pid: std::process::id() as i32,
                start_ticks: None,
                frozen: false,
                firecracker_version: None,
                host_boot_id: "an-earlier-boot".into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
            })
            .unwrap();
        let status = processes.status(&app_id());
        assert!(status.loaded);
        assert!(!status.active);
        assert!(!status.started_this_boot);
        assert!(!status.failed);
    }

    #[test]
    fn a_process_this_daemon_can_still_signal_is_running() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        processes
            .write_record(&VmRecord {
                app_id: app_id(),
                pid: std::process::id() as i32,
                start_ticks: None,
                frozen: false,
                firecracker_version: None,
                host_boot_id: "boot-1".into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
            })
            .unwrap();
        let status = processes.status(&app_id());
        assert!(status.active);
        assert!(status.started_this_boot);
        assert_eq!(processes.adopted_app_ids(), vec![app_id()]);
    }

    #[test]
    fn an_exit_nobody_asked_for_is_a_failure_and_one_that_was_asked_for_is_a_stop() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        let record = VmRecord {
            app_id: app_id(),
            pid: 1,
            start_ticks: None,
            frozen: false,
            firecracker_version: None,
            host_boot_id: "boot-1".into(),
            started_at_ms: 0,
            exit_code: Some(1),
            signal: None,
            stop_requested: false,
        };
        processes.write_record(&record).unwrap();
        let crashed = processes.status(&app_id());
        assert!(crashed.failed);
        assert_eq!(crashed.exit, Some(VmExit::Code(1)));

        processes
            .write_record(&VmRecord {
                stop_requested: true,
                ..record.clone()
            })
            .unwrap();
        assert!(!processes.status(&app_id()).failed);
        processes
            .write_record(&VmRecord {
                exit_code: Some(0),
                ..record.clone()
            })
            .unwrap();
        assert!(!processes.status(&app_id()).failed);

        // A process that died under a signal has no code of its own, and that is a failure too.
        processes
            .write_record(&VmRecord {
                exit_code: None,
                signal: Some(9),
                ..record
            })
            .unwrap();
        let killed = processes.status(&app_id());
        assert!(killed.failed);
        assert!(!killed.active);
        assert_eq!(killed.exit, Some(VmExit::Signal(9)));
    }

    #[test]
    fn a_record_an_earlier_daemon_wrote_still_reads_with_the_code_it_carried() {
        let written = serde_json::json!({
            "appId": "app-1",
            "pid": 1,
            "hostBootId": "boot-1",
            "startedAtMs": 0,
            "exitCode": 137
        });
        let record: VmRecord = serde_json::from_value(written).unwrap();
        assert_eq!(record.exit(), Some(VmExit::Code(137)));
        assert_eq!(record.signal, None);
    }

    #[test]
    fn how_a_process_ended_is_read_off_what_wait_said() {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            exit_of(Ok(std::process::ExitStatus::from_raw(0))),
            VmExit::Code(0)
        );
        // A status word of 9 is death by SIGKILL; 137 << 8 is an exit with code 137.
        assert_eq!(
            exit_of(Ok(std::process::ExitStatus::from_raw(9))),
            VmExit::Signal(9)
        );
        assert_eq!(
            exit_of(Ok(std::process::ExitStatus::from_raw(137 << 8))),
            VmExit::Code(137)
        );
        assert_eq!(
            exit_of(Err(std::io::Error::other("the child was never waited for"))),
            VmExit::Code(-1)
        );
    }

    #[test]
    fn forgetting_a_microvm_takes_its_record_socket_and_console_with_it() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        processes
            .write_record(&VmRecord {
                app_id: app_id(),
                pid: 1,
                start_ticks: None,
                frozen: false,
                firecracker_version: None,
                host_boot_id: "boot-1".into(),
                started_at_ms: 0,
                exit_code: Some(0),
                signal: None,
                stop_requested: true,
            })
            .unwrap();
        std::fs::write(processes.console_path(&app_id()), b"[nibrun] gone\n").unwrap();
        processes.forget(&app_id());
        assert!(processes.read_record(&app_id()).is_none());
        assert!(!processes.console_path(&app_id()).exists());
        processes.forget(&app_id());
    }

    #[test]
    fn everything_one_microvm_owns_is_named_after_it_and_shared_with_no_other() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        let neighbour = AppId::parse("app-2").unwrap();
        assert_eq!(
            processes.api_socket(&app_id()),
            directory.path().join("vm-app-1.sock")
        );
        assert_eq!(
            processes.record_path(&app_id()),
            directory.path().join("vm-app-1.json")
        );
        assert_eq!(
            processes.console_path(&app_id()),
            directory.path().join("vm-app-1.console")
        );
        assert_ne!(processes.api_socket(&app_id()), processes.api_socket(&neighbour));
        assert_eq!(processes.boot_id(), "boot-1");
    }

    #[test]
    fn a_record_that_lost_its_shape_is_read_as_no_record_rather_than_a_half_one() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        std::fs::write(processes.record_path(&app_id()), "{ not json").unwrap();
        assert!(processes.read_record(&app_id()).is_none());
        assert_eq!(processes.status(&app_id()), VmStatus::default());
    }

    #[test]
    fn a_file_that_is_not_a_record_is_not_read_as_a_microvm_this_host_adopted() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        for name in ["vm-app-1.sock", "vm-app-1.console", "notes.json", "vm-.json"] {
            std::fs::write(directory.path().join(name), b"").unwrap();
        }
        assert!(processes.adopted_app_ids().is_empty());
        assert!(VmProcesses::new(directory.path().join("nowhere"))
            .adopted_app_ids()
            .is_empty());
    }

    #[test]
    fn a_pid_no_process_could_have_is_not_alive() {
        assert!(!is_alive(0));
        assert!(!is_alive(-1));
        assert!(is_alive(std::process::id() as i32));
        signal(0, libc::SIGTERM);
        signal(-1, libc::SIGTERM);
    }

    #[test]
    fn a_host_with_no_boot_id_to_read_still_tells_one_run_of_this_daemon_from_the_next() {
        let named = host_boot_id_or_session();
        assert!(!named.is_empty());
        if cfg!(not(target_os = "linux")) {
            assert_eq!(named, format!("session-{}", std::process::id()));
            assert!(read_host_boot_id().is_err());
        }
    }

    #[tokio::test]
    async fn a_hypervisor_that_exits_has_its_code_written_back_onto_the_record_it_left() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        let working_dir = directory.path().join("vm");
        make_directory(&working_dir, 0o700).unwrap();

        let record = processes
            .spawn(&app_id(), Path::new("/bin/echo"), &working_dir, None)
            .await
            .unwrap();
        assert_eq!(record.app_id, app_id());
        assert_eq!(record.host_boot_id, "boot-1");
        assert_eq!(record.exit_code, None);
        assert!(!record.stop_requested);
        assert_eq!(processes.adopted_app_ids(), vec![app_id()]);

        for _ in 0..200 {
            if processes.read_record(&app_id()).and_then(|held| held.exit_code) == Some(0) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let settled = processes.status(&app_id());
        assert_eq!(settled.exit, Some(VmExit::Code(0)));
        assert!(settled.loaded);
        assert!(!settled.active);
        assert!(!settled.failed, "an exit of 0 is not a failure");
        assert!(std::fs::read_to_string(processes.console_path(&app_id()))
            .unwrap()
            .contains("--api-sock"));
    }

    #[tokio::test]
    async fn a_hypervisor_killed_from_outside_has_the_signal_written_back_rather_than_a_made_up_code() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        let working_dir = directory.path().join("vm");
        make_directory(&working_dir, 0o700).unwrap();
        // Something that stays up whatever it is handed on its command line.
        let lingering = directory.path().join("linger.sh");
        std::fs::write(&lingering, "#!/bin/sh\nexec sleep 30\n").unwrap();
        std::fs::set_permissions(
            &lingering,
            std::os::unix::fs::PermissionsExt::from_mode(EXECUTABLE_MODE),
        )
        .unwrap();

        let record = processes
            .spawn(&app_id(), &lingering, &working_dir, None)
            .await
            .unwrap();
        signal(record.pid, libc::SIGKILL);

        for _ in 0..200 {
            if processes
                .read_record(&app_id())
                .is_some_and(|held| held.exit().is_some())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let killed = processes.status(&app_id());
        assert_eq!(killed.exit, Some(VmExit::Signal(libc::SIGKILL)));
        assert!(killed.failed);
        assert!(!killed.active);
    }

    #[tokio::test]
    async fn a_hypervisor_that_would_not_start_is_a_failure_rather_than_a_record() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        let working_dir = directory.path().join("vm");
        make_directory(&working_dir, 0o700).unwrap();
        assert!(processes
            .spawn(
                &app_id(),
                Path::new("/nowhere/nibrunner-no-such-hypervisor"),
                &working_dir,
                None
            )
            .await
            .is_err());
        assert!(processes.read_record(&app_id()).is_none());
    }

    #[tokio::test]
    async fn stopping_a_microvm_this_host_holds_no_record_of_asks_nothing_of_the_kernel() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        processes.stop(&app_id()).await;
        assert!(processes.read_record(&app_id()).is_none());
    }

    #[tokio::test]
    async fn a_stop_is_written_down_before_the_signal_so_the_exit_is_not_read_as_a_crash() {
        let directory = tempfile::tempdir().unwrap();
        let processes = processes(directory.path());
        processes
            .write_record(&VmRecord {
                app_id: app_id(),
                pid: -1,
                start_ticks: None,
                frozen: false,
                firecracker_version: None,
                host_boot_id: "boot-1".into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
            })
            .unwrap();
        processes.stop(&app_id()).await;
        assert!(processes.read_record(&app_id()).unwrap().stop_requested);
    }
}

#[cfg(test)]
mod embedding {
    use super::*;

    #[test]
    fn the_embedded_hypervisor_is_extracted_to_a_versioned_directory() {
        let directory = tempfile::tempdir().unwrap();
        if !carries_firecracker() {
            assert!(extract_firecracker(directory.path()).is_err());
            return;
        }
        let binary = extract_firecracker(directory.path()).unwrap();
        assert!(binary.starts_with(directory.path().join(FIRECRACKER_VERSION)));
        let size = std::fs::metadata(&binary).unwrap().len();
        assert!(size > 1_000_000, "a hypervisor is more than a megabyte");
        let before = std::fs::metadata(&binary).unwrap().modified().unwrap();
        assert_eq!(extract_firecracker(directory.path()).unwrap(), binary);
        assert_eq!(std::fs::metadata(&binary).unwrap().modified().unwrap(), before);
    }
}
