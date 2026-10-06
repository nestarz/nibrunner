#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use guest_contract::channels::{Channel, ChannelTransport};
use guest_contract::instance_env::{render_instance_env, InstanceEnvContent};
use guest_contract::logs::{decode_frames, GuestLogFrame};
use protocol::{GuestPath, TenantArguments, TenantEnvironment, TenantLogStream};

struct Runtime {
    unit: String,
    child: Child,
    console: PathBuf,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = Command::new("systemctl").args(["stop", &self.unit]).output();
        let _ = self.child.wait();
    }
}

fn command(program: &str, args: &[&str]) -> String {
    let output = Command::new(program).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{program}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn private_directory(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

fn start(scratch: &Path, iteration: u32) -> Runtime {
    let unit = format!(
        "nibrunner-prepared-{}-{}-{iteration}.service",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let console = scratch.join(format!("console-{iteration}"));
    let output = std::fs::File::create(&console).unwrap();
    let child = Command::new("systemd-run")
        .args([
            "--wait",
            "--pipe",
            "--collect",
            "--quiet",
            "--unit",
            &unit,
            "--property=Type=exec",
            "--property=Delegate=yes",
            "--property=MemoryMax=134217728",
            "--property=MemorySwapMax=33554432",
            "--property=TimeoutStopSec=10",
            "--property=RuntimeMaxSec=30",
            "--property=CapabilityBoundingSet=~CAP_SYS_TIME CAP_SYS_BOOT",
        ])
        .arg("/bin/sh")
        .arg(scratch.join("scope.sh"))
        .arg(scratch)
        .arg(env!("CARGO_BIN_EXE_nibrunner-init"))
        .stdin(Stdio::null())
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap();
    Runtime { unit, child, console }
}

fn await_logs(runtime: &mut Runtime, listener: &UnixListener) {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut wire = None;
    let mut remainder = Vec::new();
    let (mut stdout, mut stderr) = (String::new(), String::new());
    while Instant::now() < deadline {
        if wire.is_none() {
            if let Ok((stream, _)) = listener.accept() {
                stream.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
                wire = Some(stream);
            }
        }
        if let Some(stream) = &mut wire {
            let mut bytes = [0; 4096];
            match stream.read(&mut bytes) {
                Ok(0) => break,
                Ok(count) => {
                    let (frames, rest) = decode_frames(&remainder, &bytes[..count]).unwrap();
                    remainder = rest;
                    for frame in frames {
                        if let GuestLogFrame::Data { stream, bytes } = frame {
                            match stream {
                                TenantLogStream::Stdout => stdout.push_str(&String::from_utf8_lossy(&bytes)),
                                TenantLogStream::Stderr => stderr.push_str(&String::from_utf8_lossy(&bytes)),
                            }
                        }
                    }
                    if stdout.contains("ready") && stderr.contains("error") {
                        return;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                    ) => {}
                Err(error) => panic!("reading runtime logs: {error}"),
            }
        }
        if runtime.child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "prepared runtime did not forward both streams: {}",
        std::fs::read_to_string(&runtime.console).unwrap()
    );
}

fn stop(runtime: &mut Runtime) {
    let main = command(
        "systemctl",
        &["show", "--property=MainPID", "--value", &runtime.unit],
    );
    let main: i32 = main.trim().parse().unwrap();
    assert!(main > 1);
    let children = std::fs::read_to_string(format!("/proc/{main}/task/{main}/children")).unwrap();
    let children: Vec<i32> = children
        .split_whitespace()
        .map(|value| value.parse().unwrap())
        .collect();
    assert_eq!(children.len(), 1, "unshare must hold only the runtime's PID 1");
    let pid = children[0];
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    assert!(status
        .lines()
        .any(|line| line.starts_with("NSpid:") && line.split_whitespace().last() == Some("1")));
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), nix::sys::signal::Signal::SIGTERM).unwrap();
    let deadline = Instant::now() + Duration::from_secs(12);
    while Instant::now() < deadline {
        if let Some(status) = runtime.child.try_wait().unwrap() {
            assert!(
                status.success(),
                "{}",
                std::fs::read_to_string(&runtime.console).unwrap()
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("prepared runtime did not exit after SIGTERM");
}

#[test]
fn prepared_init_requires_pid_one_before_touching_host_state() {
    let output = Command::new(env!("CARGO_BIN_EXE_nibrunner-init"))
        .arg("--prepared-root")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must run as PID 1"));
}

#[test]
fn a_prepared_runtime_uses_host_limits_and_unix_channels_and_can_cold_start_again() {
    if !std::env::var("NIBRUNNER_INTEGRATION").is_ok_and(|value| value == "1") {
        return;
    }
    assert!(nix::unistd::geteuid().is_root());
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path();
    private_directory(&scratch.join("channels"));
    for path in ["root/app", "root/proc", "root/dev", "volume/upper"] {
        std::fs::create_dir_all(scratch.join(path)).unwrap();
    }
    std::fs::File::create(scratch.join("root/dev/null")).unwrap();
    nix::unistd::chown(
        &scratch.join("root/app"),
        Some(nix::unistd::Uid::from_raw(65534)),
        Some(nix::unistd::Gid::from_raw(65534)),
    )
    .unwrap();
    std::fs::write(
        scratch.join("scope.sh"),
        include_str!("fixtures/prepared-scope.sh"),
    )
    .unwrap();
    std::fs::write(
        scratch.join("namespace.sh"),
        include_str!("fixtures/prepared-namespace.sh"),
    )
    .unwrap();
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
    for iteration in 0..2 {
        for channel in [Channel::Logs, Channel::Control, Channel::Filesystem] {
            let _ = std::fs::remove_file(
                ChannelTransport::Unix
                    .endpoint(&scratch.join("channels"), channel)
                    .path,
            );
        }
        let listener = UnixListener::bind(
            ChannelTransport::Unix
                .endpoint(&scratch.join("channels"), Channel::Logs)
                .path,
        )
        .unwrap();
        let value = format!("pass-{iteration}");
        let environment = TenantEnvironment::try_from(std::collections::BTreeMap::from([(
            "TEST_VALUE".into(),
            protocol::TenantValue::parse(value).unwrap(),
        )]))
        .unwrap();
        let arguments = TenantArguments::try_from(vec!["-c".into(),
            "printf '%s\n' \"$TEST_VALUE\" >> /app/runs; printf 'ready\n'; printf 'error\n' >&2; while :; do /bin/sleep 1; done".into()]).unwrap();
        let config = render_instance_env(&InstanceEnvContent {
            http_port: protocol::DEFAULT_HTTP_PORT,
            layers: 0,
            hostnames: &[],
            program: &GuestPath::parse("/bin/sh").unwrap(),
            working_directory: &GuestPath::parse("/app").unwrap(),
            args: &arguments,
            environment: &environment,
            restart_policy: &protocol::DEFAULT_RESTART_POLICY,
        })
        .unwrap();
        std::fs::write(scratch.join("instance.env"), config).unwrap();
        let mut runtime = start(scratch, iteration);
        await_logs(&mut runtime, &listener);
        let scope = PathBuf::from(std::fs::read_to_string(scratch.join("cgroup")).unwrap());
        assert_eq!(
            std::fs::read_to_string(scope.join("tenant/memory.max"))
                .unwrap()
                .trim(),
            "67108864"
        );
        assert_eq!(
            std::fs::read_to_string(scope.join("tenant/memory.swap.max"))
                .unwrap()
                .trim(),
            "33554432"
        );
        let mut control = BufReader::new(
            UnixStream::connect(
                ChannelTransport::Unix
                    .endpoint(&scratch.join("channels"), Channel::Control)
                    .path,
            )
            .unwrap(),
        );
        control
            .get_mut()
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        control.get_mut().write_all(b"WAKE 0\n").unwrap();
        let mut reply = String::new();
        control.read_line(&mut reply).unwrap();
        assert_eq!(reply, "REFUSED\n");
        stop(&mut runtime);
        assert!(
            !scope.exists()
                || std::fs::read_to_string(scope.join("cgroup.events"))
                    .unwrap()
                    .contains("populated 0")
        );
        assert_eq!(
            std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap(),
            boot_id
        );
    }
    assert_eq!(
        std::fs::read_to_string(scratch.join("root/app/runs")).unwrap(),
        "pass-0\npass-1\n"
    );
}
