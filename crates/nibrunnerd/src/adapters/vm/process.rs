use std::path::{Path, PathBuf};
use std::process::Stdio;

use protocol::AppId;
use serde::{Deserialize, Serialize};

use crate::adapters::vm::status::{VmExit, VmStatus};
use crate::json_store::{make_directory, read_json, write_json};

const JAILER: &[u8] = include_bytes!(env!("NIBRUNNER_JAILER_PATH"));
const FIRECRACKER: &[u8] = include_bytes!(env!("NIBRUNNER_FIRECRACKER_PATH"));
pub const FIRECRACKER_VERSION: &str = env!("NIBRUNNER_FIRECRACKER_VERSION");

const RUNTIME_DIR_MODE: u32 = 0o700;
const EXECUTABLE_MODE: u32 = 0o755;

pub fn carries_firecracker() -> bool {
    option_env!("NIBRUNNER_FIRECRACKER_EMBEDDED").is_some()
}

pub fn extract_firecracker(directory: &Path) -> std::io::Result<PathBuf> {
    extract_binary(directory, "firecracker", FIRECRACKER)
}

pub fn extract_jailer(directory: &Path) -> std::io::Result<PathBuf> {
    extract_binary(directory, "jailer", JAILER)
}

fn extract_binary(directory: &Path, name: &str, bytes: &[u8]) -> std::io::Result<PathBuf> {
    let versioned = directory.join(FIRECRACKER_VERSION);
    let binary = versioned.join(name);
    if !carries_firecracker() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "this build carries no hypervisor, so it can boot nothing",
        ));
    }
    if std::fs::metadata(&binary).is_ok_and(|info| info.len() == bytes.len() as u64) {
        return Ok(binary);
    }
    make_directory(&versioned, RUNTIME_DIR_MODE)?;
    let staged = versioned.join(format!("{name}.{}.tmp", std::process::id()));
    std::fs::write(&staged, bytes)?;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jail_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jail_uid: Option<u32>,
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
    runtime_dir: PathBuf,
    boot_id: String,
}

impl VmProcesses {
    pub fn new(runtime_dir: PathBuf) -> Self {
        Self {
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
        #[cfg(target_os = "linux")]
        let _ = std::fs::remove_dir(super::limits::cgroup_path(app_id));
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
            failed: !active && !record.stop_requested && exit.is_some_and(|exit| exit != VmExit::Code(0)),
            started_this_boot,
            exit,
        }
    }

    pub fn memory(&self, app_id: &AppId) -> Option<protocol::ReportedMemory> {
        let record = self.read_record(app_id)?;
        if record.host_boot_id != self.boot_id || record.exit().is_some() || !is_alive(record.pid) {
            return None;
        }
        // A pid can be reused once its VMM is gone; a jailed VMM still runs as its jail's uid.
        if record.jail_uid.is_some() && process_uid(record.pid) != record.jail_uid {
            return None;
        }
        let cgroup = record
            .jail_root
            .is_some()
            .then(|| super::limits::cgroup_path(app_id));
        super::memory::read_process(record.pid, cgroup)
    }

    pub(crate) async fn spawn(
        &self,
        app_id: &AppId,
        binary: &Path,
        jailer: &super::jailer::Jailer,
        jail: &super::jailer::Jail,
        boot: bool,
    ) -> std::io::Result<VmRecord> {
        make_directory(&self.runtime_dir, RUNTIME_DIR_MODE)?;
        let api_socket = self.api_socket(app_id);
        let _ = std::fs::remove_file(&api_socket);
        let _ = std::fs::remove_file(jail.root.join(guest_contract::vsock::GUEST_VSOCK_FILENAME));
        let base = jail
            .root
            .ancestors()
            .nth(3)
            .ok_or_else(|| std::io::Error::other("the jail has no base directory"))?;
        std::os::unix::fs::symlink(jail.root.join("api.sock"), &api_socket)?;
        let mut command = tokio::process::Command::new(&jailer.binary);
        command
            .arg("--id")
            .arg(super::jailer::jail_id(app_id))
            .arg("--exec-file")
            .arg(binary)
            .arg("--uid")
            .arg(jail.uid.to_string())
            .arg("--gid")
            .arg(jail.gid.to_string())
            .arg("--chroot-base-dir")
            .arg(base)
            .arg("--cgroup-version")
            .arg("2")
            .arg("--parent-cgroup")
            .arg(super::limits::CGROUP_PARENT);
        jail.limits.configure(&mut command);
        command.arg("--").arg("--api-sock").arg("/api.sock");
        if boot {
            command.arg("--config-file").arg("/firecracker.json");
        }
        jail.configure_command(&mut command)?;
        self.launch(app_id, command, jail).await
    }

    #[cfg(test)]
    async fn spawn_test_process(
        &self,
        app_id: &AppId,
        binary: &Path,
        working_dir: &Path,
        config_file: Option<&Path>,
    ) -> std::io::Result<VmRecord> {
        let jail = super::jailer::Jail::for_testing(working_dir.into());
        let jailer = super::jailer::Jailer::for_testing(binary.into());
        self.spawn(app_id, binary, &jailer, &jail, config_file.is_some())
            .await
    }

    async fn launch(
        &self,
        app_id: &AppId,
        mut command: tokio::process::Command,
        jail: &super::jailer::Jail,
    ) -> std::io::Result<VmRecord> {
        let working_dir = &jail.root;
        let console = std::fs::File::create(self.console_path(app_id))?;
        command
            .current_dir(working_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(console.try_clone()?))
            .stderr(Stdio::from(console))
            .kill_on_drop(false);
        #[cfg(unix)]
        {
            #[allow(
                unsafe_code,
                reason = "session and OOM policy must be set between fork and exec"
            )]
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    #[cfg(target_os = "linux")]
                    {
                        // The daemon is OOM-protected; a VMM must remain killable at its memory ceiling.
                        let descriptor = libc::open(
                            c"/proc/self/oom_score_adj".as_ptr(),
                            libc::O_WRONLY | libc::O_CLOEXEC,
                        );
                        if descriptor < 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                        let written = libc::write(descriptor, c"0".as_ptr().cast(), 1);
                        let error = std::io::Error::last_os_error();
                        libc::close(descriptor);
                        if written != 1 {
                            return Err(error);
                        }
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
            host_boot_id: self.boot_id.clone(),
            started_at_ms: crate::clock::now_ms(),
            exit_code: None,
            signal: None,
            stop_requested: false,
            jail_root: Some(jail.root.clone()),
            jail_uid: Some(jail.uid),
        };
        self.write_record(&record)?;

        let processes = Self {
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

fn process_uid(pid: i32) -> Option<u32> {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
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
            runtime_dir: directory.to_path_buf(),
            boot_id: "boot-1".into(),
        }
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
                host_boot_id: "an-earlier-boot".into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
                jail_root: None,
                jail_uid: None,
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
                host_boot_id: "boot-1".into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
                jail_root: None,
                jail_uid: None,
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
            host_boot_id: "boot-1".into(),
            started_at_ms: 0,
            exit_code: Some(1),
            signal: None,
            stop_requested: false,
            jail_root: None,
            jail_uid: None,
        };
        processes.write_record(&record).unwrap();
        let crashed = processes.status(&app_id());
        assert!(crashed.failed);
        assert_eq!(crashed.exit, Some(VmExit::Code(1)));

        processes
            .write_record(&VmRecord {
                stop_requested: true,
                jail_root: None,
                jail_uid: None,
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
                host_boot_id: "boot-1".into(),
                started_at_ms: 0,
                exit_code: Some(0),
                signal: None,
                stop_requested: true,
                jail_root: None,
                jail_uid: None,
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
            .spawn_test_process(&app_id(), Path::new("/bin/echo"), &working_dir, None)
            .await
            .unwrap();
        assert_eq!(record.app_id, app_id());
        assert_eq!(record.host_boot_id, "boot-1");
        assert_eq!(record.exit_code, None);
        assert!(!record.stop_requested);
        assert_eq!(record.jail_root, Some(working_dir.clone()));
        assert!(record.jail_uid.is_some());
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
        let console = std::fs::read_to_string(processes.console_path(&app_id())).unwrap();
        for argument in [
            "--id",
            "--exec-file",
            "--uid",
            "--gid",
            "--chroot-base-dir",
            "--cgroup cpu.max=100000 100000",
            "--cgroup memory.max=369098752",
            "--cgroup memory.swap.max=0 -- --api-sock /api.sock",
        ] {
            assert!(console.contains(argument), "{console}");
        }
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
            .spawn_test_process(&app_id(), &lingering, &working_dir, None)
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
            .spawn_test_process(
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
                host_boot_id: "boot-1".into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
                jail_root: None,
                jail_uid: None,
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
    #[test]
    fn the_jailer_is_extracted_beside_the_matching_firecracker_release() {
        let directory = tempfile::tempdir().unwrap();
        if !carries_firecracker() {
            assert!(extract_jailer(directory.path()).is_err());
            return;
        }
        let firecracker = extract_firecracker(directory.path()).unwrap();
        let jailer = extract_jailer(directory.path()).unwrap();
        assert_eq!(jailer.parent(), firecracker.parent());
        assert_eq!(jailer.file_name().unwrap(), "jailer");
        assert_eq!(std::fs::read(&jailer).unwrap(), JAILER);
        std::fs::write(&jailer, b"truncated").unwrap();
        extract_jailer(directory.path()).unwrap();
        assert_eq!(std::fs::read(&jailer).unwrap(), JAILER);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&jailer).unwrap().permissions().mode() & 0o777,
                EXECUTABLE_MODE
            );
        }
    }
}
