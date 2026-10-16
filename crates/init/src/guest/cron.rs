use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use guest_contract::cron_execution::{self, ExecutionFrame, ExecutionRequest, Rejection};
use guest_contract::instance_env::InstanceConfig;
use guest_contract::paths;
use nix::sys::signal::{kill, killpg, SigSet, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{ForkResult, Gid, Pid, Uid};

use crate::guest::{log, memory, vsock};

const MAX_WORKERS: usize = 64;
const POLL_MS: i32 = 100;
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(5);
const SPAWN_FAILURE_EXIT_CODE: i32 = 126;
static STOPPING: AtomicBool = AtomicBool::new(false);

extern "C" fn stop_requested(_: libc::c_int) {
    STOPPING.store(true, Ordering::Relaxed);
}

fn prepare_signals() -> nix::Result<()> {
    STOPPING.store(false, Ordering::Relaxed);
    let action = nix::sys::signal::SigAction::new(
        nix::sys::signal::SigHandler::Handler(stop_requested),
        nix::sys::signal::SaFlags::empty(),
        SigSet::empty(),
    );
    unsafe { nix::sys::signal::sigaction(Signal::SIGTERM, &action)? };
    let mut blocked = SigSet::empty();
    blocked.add(Signal::SIGTERM);
    blocked.thread_unblock()
}

pub(crate) fn start(config: &InstanceConfig, ceiling: &memory::Ceiling) -> Option<Pid> {
    let parent = nix::unistd::getpid();
    match unsafe { nix::unistd::fork() } {
        Ok(ForkResult::Parent { child }) => Some(child),
        Ok(ForkResult::Child) => {
            if nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0)).is_err()
                || unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) } < 0
                || nix::unistd::getppid() != parent
                || prepare_signals().is_err()
            {
                unsafe { libc::_exit(1) }
            }
            serve(config, ceiling);
            unsafe { libc::_exit(0) }
        }
        Err(error) => {
            log(&format!("the cron channel could not be started: {error}"));
            None
        }
    }
}

pub(crate) fn stop(pid: Pid) {
    let _ = kill(pid, Signal::SIGTERM);
    wait_for(pid);
}

fn wait_for(pid: Pid) {
    while matches!(waitpid(pid, None), Err(nix::errno::Errno::EINTR)) {}
}

fn serve(config: &InstanceConfig, ceiling: &memory::Ceiling) {
    let Ok(listener) = vsock::listener(guest_contract::vsock::CRON_EXECUTION_PORT) else {
        log("the cron execution port could not be opened");
        return;
    };
    let _ = nix::fcntl::fcntl(
        &listener,
        nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_NONBLOCK),
    );
    let mut workers = std::collections::BTreeSet::new();
    while !STOPPING.load(Ordering::Relaxed) {
        while let Ok(WaitStatus::Exited(pid, _) | WaitStatus::Signaled(pid, _, _)) =
            waitpid(None, Some(WaitPidFlag::WNOHANG))
        {
            workers.remove(&pid);
        }
        if !ready(listener.as_raw_fd(), libc::POLLIN, POLL_MS).unwrap_or(false) {
            continue;
        }
        let Ok(connection) = vsock::accept_from_host(&listener) else {
            continue;
        };
        let _ = nix::fcntl::fcntl(
            &connection,
            nix::fcntl::FcntlArg::F_SETFD(nix::fcntl::FdFlag::FD_CLOEXEC),
        );
        if workers.len() >= MAX_WORKERS {
            let _ = send_reply(connection.as_raw_fd(), &ExecutionFrame::Rejected(Rejection::Busy));
            continue;
        }
        let mut blocked = SigSet::empty();
        blocked.add(Signal::SIGTERM);
        let _ = blocked.thread_block();
        let parent = nix::unistd::getpid();
        match unsafe { nix::unistd::fork() } {
            Ok(ForkResult::Parent { child }) => {
                workers.insert(child);
            }
            Ok(ForkResult::Child) => {
                drop(listener);
                if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) } < 0
                    || nix::unistd::getppid() != parent
                    || prepare_signals().is_err()
                {
                    unsafe { libc::_exit(1) }
                }
                answer(connection.as_raw_fd(), config, ceiling);
                unsafe { libc::_exit(0) }
            }
            Err(_) => {
                let _ = send_reply(
                    connection.as_raw_fd(),
                    &ExecutionFrame::Rejected(Rejection::SpawnFailed),
                );
            }
        }
        let _ = blocked.thread_unblock();
    }
    for pid in &workers {
        let _ = kill(*pid, Signal::SIGTERM);
    }
    for pid in workers {
        wait_for(pid);
    }
}

fn ready(fd: RawFd, events: i16, timeout_ms: i32) -> io::Result<bool> {
    let mut watched = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    let count = unsafe { libc::poll(&mut watched, 1, timeout_ms) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(count > 0)
}

fn transfer(fd: RawFd, bytes: &mut [u8], sending: bool, deadline: Instant) -> io::Result<()> {
    let mut position = 0;
    while position < bytes.len() && !STOPPING.load(Ordering::Relaxed) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        match ready(
            fd,
            if sending { libc::POLLOUT } else { libc::POLLIN },
            POLL_MS.min(remaining.as_millis() as i32),
        ) {
            Ok(false) => continue,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
            Ok(true) => {}
        }
        let rest = &mut bytes[position..];
        let count = unsafe {
            if sending {
                libc::send(
                    fd,
                    rest.as_ptr().cast(),
                    rest.len(),
                    libc::MSG_NOSIGNAL | libc::MSG_DONTWAIT,
                )
            } else {
                libc::recv(fd, rest.as_mut_ptr().cast(), rest.len(), libc::MSG_DONTWAIT)
            }
        };
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if count < 0 {
            let error = io::Error::last_os_error();
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) {
                continue;
            }
            return Err(error);
        }
        position += count as usize;
    }
    if STOPPING.load(Ordering::Relaxed) {
        return Err(io::ErrorKind::Interrupted.into());
    }
    Ok(())
}

fn send_reply(fd: RawFd, reply: &ExecutionFrame) -> io::Result<()> {
    let deadline = Instant::now() + TRANSFER_TIMEOUT;
    let mut bytes = cron_execution::encode_reply(reply).map_err(io::Error::other)?;
    transfer(fd, &mut bytes, true, deadline)?;
    if matches!(
        reply,
        ExecutionFrame::Started | ExecutionFrame::Stdout(_) | ExecutionFrame::Stderr(_)
    ) {
        let mut acknowledgement = [0u8];
        transfer(fd, &mut acknowledgement, false, deadline)?;
        if acknowledgement[0] != cron_execution::ACK {
            return Err(io::ErrorKind::InvalidData.into());
        }
    }
    Ok(())
}

fn read_request(fd: RawFd) -> io::Result<ExecutionRequest> {
    let deadline = Instant::now() + TRANSFER_TIMEOUT;
    let mut header = [0u8; cron_execution::HEADER_BYTES];
    transfer(fd, &mut header, false, deadline)?;
    let header = cron_execution::decode_request_header(&header).map_err(io::Error::other)?;
    let mut body = vec![0u8; header.body_length];
    transfer(fd, &mut body, false, deadline)?;
    cron_execution::decode_request(header, &body).map_err(io::Error::other)
}

fn answer(fd: RawFd, config: &InstanceConfig, ceiling: &memory::Ceiling) {
    let Ok(request) = read_request(fd) else {
        let _ = send_reply(fd, &ExecutionFrame::Rejected(Rejection::Malformed));
        return;
    };
    let Ok(mut command) = spawn(&request, config, ceiling) else {
        let _ = send_reply(fd, &ExecutionFrame::Rejected(Rejection::SpawnFailed));
        return;
    };
    let completed = send_reply(fd, &ExecutionFrame::Started).is_ok() && command.forward(fd).is_ok();
    let status = command.finish();
    if completed {
        if let Ok(CommandExit { code, signal }) = status {
            let _ = send_reply(fd, &ExecutionFrame::Exit { code, signal });
        }
    }
}

struct CommandProcess {
    pid: Pid,
    stdout: Option<OwnedFd>,
    stderr: Option<OwnedFd>,
    exited: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommandExit {
    code: u32,
    signal: u32,
}

impl CommandProcess {
    fn forward(&mut self, connection: RawFd) -> io::Result<()> {
        while !STOPPING.load(Ordering::Relaxed) {
            if !self.exited {
                let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
                if unsafe {
                    libc::waitid(
                        libc::P_PID,
                        self.pid.as_raw() as u32,
                        &mut status,
                        libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }
                if unsafe { status.si_pid() } == self.pid.as_raw() {
                    // Keep the leader unreaped until its group is cancelled, so its PID cannot
                    // be reused while background descendants still own these pipes.
                    let _ = killpg(self.pid, Signal::SIGKILL);
                    self.exited = true;
                }
            }
            if self.exited && self.stdout.is_none() && self.stderr.is_none() {
                return Ok(());
            }
            let mut watched = [
                libc::pollfd {
                    fd: connection,
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.stdout.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.stderr.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            if unsafe { libc::poll(watched.as_mut_ptr(), watched.len() as libc::nfds_t, POLL_MS) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if watched[0].revents != 0 {
                return Err(io::ErrorKind::ConnectionAborted.into());
            }
            for (index, pipe) in [(1, &mut self.stdout), (2, &mut self.stderr)] {
                if watched[index].revents == 0 {
                    continue;
                }
                let Some(open) = pipe.as_ref() else {
                    continue;
                };
                let mut bytes = [0u8; cron_execution::MAX_OUTPUT_BYTES];
                match nix::unistd::read(open, &mut bytes) {
                    Ok(0) => *pipe = None,
                    Ok(count) => {
                        let reply = if index == 1 {
                            ExecutionFrame::Stdout(bytes[..count].to_vec())
                        } else {
                            ExecutionFrame::Stderr(bytes[..count].to_vec())
                        };
                        send_reply(connection, &reply)?;
                    }
                    Err(nix::errno::Errno::EINTR | nix::errno::Errno::EAGAIN) => {}
                    Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
                }
            }
        }
        Err(io::ErrorKind::Interrupted.into())
    }

    fn finish(&mut self) -> io::Result<CommandExit> {
        if !self.exited {
            // The host can disconnect before the child reaches setsid.
            let _ = kill(self.pid, Signal::SIGKILL);
            let _ = killpg(self.pid, Signal::SIGKILL);
        }
        loop {
            let mut status = 0;
            let reaped = unsafe { libc::waitpid(self.pid.as_raw(), &mut status, 0) };
            if reaped < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            return Ok(CommandExit {
                code: if libc::WIFEXITED(status) {
                    libc::WEXITSTATUS(status) as u32
                } else {
                    0
                },
                signal: if libc::WIFSIGNALED(status) {
                    libc::WTERMSIG(status) as u32
                } else {
                    0
                },
            });
        }
    }
}

fn environment(config: &InstanceConfig, request: &ExecutionRequest) -> Vec<(String, String)> {
    let mut environment = vec![
        ("HOME".to_string(), config.working_directory.clone()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
    ];
    let overrides = config
        .tenant_environment()
        .into_iter()
        .chain(request.environment.iter().filter_map(|entry| {
            entry
                .split_once('=')
                .map(|(name, value)| (name.to_string(), value.to_string()))
        }));
    for (name, value) in overrides {
        if let Some((_, previous)) = environment.iter_mut().find(|(key, _)| *key == name) {
            *previous = value;
        } else {
            environment.push((name, value));
        }
    }
    environment
}

fn output_pipe() -> nix::Result<(OwnedFd, OwnedFd)> {
    let (read, write) = nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC)?;
    nix::fcntl::fcntl(
        &read,
        nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_NONBLOCK),
    )?;
    Ok((read, write))
}

fn spawn(
    request: &ExecutionRequest,
    config: &InstanceConfig,
    ceiling: &memory::Ceiling,
) -> io::Result<CommandProcess> {
    let environment = environment(config, request);
    let shell = environment
        .iter()
        .find(|(name, _)| name == "SHELL")
        .map_or("/bin/sh", |(_, value)| value.as_str());
    let cstring = |value: &str| CString::new(value).map_err(io::Error::other);
    let shell = cstring(shell)?;
    let arguments = [shell.clone(), cstring("-c")?, cstring(&request.command)?];
    let environment = environment
        .iter()
        .map(|(name, value)| cstring(&format!("{name}={value}")))
        .collect::<Result<Vec<_>, _>>()?;
    let root = cstring(paths::ROOT_MOUNT)?;
    let cwd = cstring(&config.working_directory)?;
    let input = std::fs::File::open("/dev/null")?;
    let (stdout_read, stdout_write) = output_pipe()?;
    let (stderr_read, stderr_write) = output_pipe()?;
    let parent = nix::unistd::getpid();
    match unsafe { nix::unistd::fork() }? {
        ForkResult::Parent { child } => Ok(CommandProcess {
            pid: child,
            stdout: Some(stdout_read),
            stderr: Some(stderr_read),
            exited: false,
        }),
        ForkResult::Child => {
            let launch = || -> nix::Result<()> {
                nix::unistd::setsid()?;
                if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } < 0
                    || nix::unistd::getppid() != parent
                {
                    return Err(nix::errno::Errno::ECHILD);
                }
                SigSet::all().thread_unblock()?;
                let reset = nix::sys::signal::SigAction::new(
                    nix::sys::signal::SigHandler::SigDfl,
                    nix::sys::signal::SaFlags::empty(),
                    SigSet::empty(),
                );
                for signal in [Signal::SIGTERM, Signal::SIGPIPE] {
                    unsafe { nix::sys::signal::sigaction(signal, &reset)? };
                }
                nix::unistd::dup2_stdin(&input)?;
                nix::unistd::dup2_stdout(&stdout_write)?;
                nix::unistd::dup2_stderr(&stderr_write)?;
                memory::join(ceiling.procs_file())?;
                nix::unistd::chroot(root.as_c_str())?;
                nix::unistd::chdir(cwd.as_c_str())?;
                nix::unistd::setgid(Gid::from_raw(paths::TENANT_GID))?;
                nix::unistd::setgroups(&[])?;
                nix::unistd::setuid(Uid::from_raw(paths::TENANT_UID))?;
                nix::unistd::execve(&shell, &arguments, &environment)?;
                Ok(())
            };
            if let Err(error) = launch() {
                log(&format!("the cron command could not be started: {error}"));
            }
            unsafe { libc::_exit(SPAWN_FAILURE_EXIT_CODE) }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    use std::sync::Arc;

    use super::*;

    fn frame(connection: &mut UnixStream) -> ExecutionFrame {
        let mut header = [0; cron_execution::HEADER_BYTES];
        connection.read_exact(&mut header).unwrap();
        let header = cron_execution::decode_reply_header(&header).unwrap();
        let mut body = vec![0; header.body_length];
        connection.read_exact(&mut body).unwrap();
        cron_execution::decode_reply(header, &body).unwrap()
    }

    #[allow(
        clippy::zombie_processes,
        reason = "CommandProcess takes the PID and reaps it with waitpid"
    )]
    fn shell(command: &str) -> CommandProcess {
        let mut launch = std::process::Command::new("/bin/sh");
        launch
            .args(["-c", command])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            launch.pre_exec(|| nix::unistd::setsid().map(|_| ()).map_err(io::Error::from));
        }
        let mut child = launch.spawn().unwrap();
        let stdout = OwnedFd::from(child.stdout.take().unwrap());
        let stderr = OwnedFd::from(child.stderr.take().unwrap());
        for pipe in [&stdout, &stderr] {
            nix::fcntl::fcntl(pipe, nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_NONBLOCK)).unwrap();
        }
        CommandProcess {
            pid: Pid::from_raw(child.id() as i32),
            stdout: Some(stdout),
            stderr: Some(stderr),
            exited: false,
        }
    }

    #[test]
    fn started_and_each_output_frame_wait_for_the_hosts_acknowledgement() {
        let (mut host, guest) = UnixStream::pair().unwrap();
        host.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let started = Arc::new(AtomicBool::new(false));
        let progress = Arc::clone(&started);
        let worker = std::thread::spawn(move || {
            send_reply(guest.as_raw_fd(), &ExecutionFrame::Started).unwrap();
            progress.store(true, Ordering::SeqCst);
            send_reply(guest.as_raw_fd(), &ExecutionFrame::Stdout(b"hello".to_vec())).unwrap();
            send_reply(guest.as_raw_fd(), &ExecutionFrame::Exit { code: 0, signal: 0 }).unwrap();
        });
        assert_eq!(frame(&mut host), ExecutionFrame::Started);
        assert!(!started.load(Ordering::SeqCst));
        host.write_all(&[cron_execution::ACK]).unwrap();
        assert_eq!(frame(&mut host), ExecutionFrame::Stdout(b"hello".to_vec()));
        host.write_all(&[cron_execution::ACK]).unwrap();
        assert_eq!(frame(&mut host), ExecutionFrame::Exit { code: 0, signal: 0 });
        worker.join().unwrap();
    }

    #[test]
    fn a_finished_shell_cancels_descendants_that_would_keep_its_output_open() {
        let _one = crate::guest::supervisor::tests::one_tenant_at_a_time();
        let (mut host, guest) = UnixStream::pair().unwrap();
        let reader = std::thread::spawn(move || {
            let mut output = Vec::new();
            while let Ok(()) = host.set_read_timeout(Some(Duration::from_secs(2))) {
                let mut header = [0; cron_execution::HEADER_BYTES];
                if host.read_exact(&mut header).is_err() {
                    break;
                }
                let header = cron_execution::decode_reply_header(&header).unwrap();
                let mut body = vec![0; header.body_length];
                host.read_exact(&mut body).unwrap();
                match cron_execution::decode_reply(header, &body).unwrap() {
                    ExecutionFrame::Stdout(bytes) => output.extend(bytes),
                    other => panic!("unexpected frame: {other:?}"),
                }
                host.write_all(&[cron_execution::ACK]).unwrap();
            }
            output
        });
        let mut command = shell("sleep 30 & printf done; exit 7");
        let started = Instant::now();
        command.forward(guest.as_raw_fd()).unwrap();
        assert_eq!(command.finish().unwrap(), CommandExit { code: 7, signal: 0 });
        drop(guest);
        assert_eq!(reader.join().unwrap(), b"done");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn a_host_disconnect_cancels_the_running_command_process_group() {
        let _one = crate::guest::supervisor::tests::one_tenant_at_a_time();
        let (host, guest) = UnixStream::pair().unwrap();
        let mut command = shell("sleep 30");
        drop(host);
        assert!(command.forward(guest.as_raw_fd()).is_err());
        assert_eq!(
            command.finish().unwrap(),
            CommandExit {
                code: 0,
                signal: Signal::SIGKILL as u32
            }
        );
    }

    #[test]
    fn cancelling_one_command_leaves_an_overlapping_command_running() {
        let _one = crate::guest::supervisor::tests::one_tenant_at_a_time();
        let (first_host, first_guest) = UnixStream::pair().unwrap();
        let (second_host, second_guest) = UnixStream::pair().unwrap();
        let mut first = shell("sleep 30");
        let mut second = shell("sleep 30");
        drop(first_host);
        assert!(first.forward(first_guest.as_raw_fd()).is_err());
        first.finish().unwrap();
        assert!(kill(second.pid, None).is_ok());
        drop(second_host);
        assert!(second.forward(second_guest.as_raw_fd()).is_err());
        assert_eq!(
            second.finish().unwrap(),
            CommandExit {
                code: 0,
                signal: Signal::SIGKILL as u32
            }
        );
    }

    #[test]
    fn fragmented_transfers_share_the_callers_absolute_deadline() {
        let (mut host, guest) = UnixStream::pair().unwrap();
        host.write_all(b"a").unwrap();
        let mut bytes = [0; 2];
        let deadline = Instant::now() + Duration::from_millis(20);
        let error = transfer(guest.as_raw_fd(), &mut bytes, false, deadline).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(bytes[0], b'a');
    }

    #[test]
    fn registered_environment_overrides_app_environment_and_shell_defaults() {
        let config = InstanceConfig {
            http_port: 8080,
            layers: 0,
            writable: paths::WritableRoot::VolumeDrive,
            program: "/app/server".into(),
            working_directory: "/app".into(),
            hostname: None,
            max_restarts: 0,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            backoff_factor: 1.0,
            reset_after_ms: 0,
            nameservers: vec![],
            arguments: vec![],
            environment: vec![
                ("HOME".into(), "/app/custom".into()),
                ("TOKEN".into(), "app-token".into()),
            ],
        };
        let mut request = ExecutionRequest {
            command: "true".into(),
            environment: vec![],
        };
        let exported = environment(&config, &request);
        let value = |name: &str| exported.iter().find(|(key, _)| key == name).unwrap().1.as_str();
        assert_eq!(value("HOME"), "/app/custom");
        assert_eq!(value("TMPDIR"), "/tmp");
        assert_eq!(value("PATH"), "/usr/bin:/bin");
        assert_eq!(value("PORT"), "8080");
        request.environment = vec![
            "HOME=/app/job".into(),
            "TOKEN=$literal 'quoted' héllo".into(),
            "SHELL=/bin/bash".into(),
        ];
        let exported = environment(&config, &request);
        assert_eq!(exported.iter().filter(|(key, _)| key == "HOME").count(), 1);
        assert_eq!(
            exported.iter().find(|(key, _)| key == "HOME").unwrap().1,
            "/app/job"
        );
        assert_eq!(
            exported.iter().find(|(key, _)| key == "TOKEN").unwrap().1,
            "$literal 'quoted' héllo"
        );
        assert_eq!(
            exported.iter().find(|(key, _)| key == "SHELL").unwrap().1,
            "/bin/bash"
        );
    }

    #[test]
    fn a_realtime_signal_exit_preserves_its_linux_signal_number() {
        let _one = crate::guest::supervisor::tests::one_tenant_at_a_time();
        let (_host, guest) = UnixStream::pair().unwrap();
        let mut command = shell("kill -34 $$");
        command.forward(guest.as_raw_fd()).unwrap();
        assert_eq!(command.finish().unwrap(), CommandExit { code: 0, signal: 34 });
    }
}
