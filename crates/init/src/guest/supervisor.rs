use std::ffi::CString;
use std::os::fd::{AsFd, OwnedFd};
use std::time::{Duration, Instant};

use guest_contract::instance_env::InstanceConfig;
use guest_contract::paths;
use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
use nix::sys::signal::{SigSet, Signal};
use nix::sys::signalfd::{SfdFlags, SignalFd};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{ForkResult, Gid, Pid, Uid};
use protocol::{TenantExit, TenantRestart};

use crate::ceiling::{self, Verdict, Watch};
use crate::guest::log;
use crate::guest::logs::Forwarder;
use crate::guest::memory::{self, Ceiling};
use crate::supervise::{Budget, Outcome, SHUTDOWN_GRACE_MS};

pub(crate) use crate::supervise::Outcome as Ended;

pub(crate) fn block_signals() {
    let mut blocked = SigSet::empty();
    blocked.add(Signal::SIGINT);
    blocked.add(Signal::SIGTERM);
    blocked.add(Signal::SIGCHLD);
    let _ = blocked.thread_block();
}

fn waited_signals() -> SigSet {
    let mut set = SigSet::empty();
    set.add(Signal::SIGINT);
    set.add(Signal::SIGTERM);
    set.add(Signal::SIGCHLD);
    set
}

/// The waited signals as a descriptor, so that one poll waits on them and on the tenant's pipes
/// together. Nothing is ever read from it: `wait_for_signal` takes the signals themselves, and
/// the descriptor stops reading as ready once it has.
fn signals_as_fd() -> Option<SignalFd> {
    match SignalFd::with_flags(&waited_signals(), SfdFlags::SFD_CLOEXEC) {
        Ok(signals) => Some(signals),
        Err(error) => {
            log(&format!(
                "signals cannot be waited on beside the tenant's output ({error}); they are looked at every {}ms instead",
                POLL_INTERVAL.as_millis()
            ));
            None
        }
    }
}

pub(crate) fn supervise(config: &InstanceConfig, ceiling: &Ceiling) -> Ended {
    let mut budget = Budget::default();
    let mut forwarder = Forwarder::new();
    let signals = signals_as_fd();
    loop {
        let started = Instant::now();
        let Some(started_tenant) = spawn(config, ceiling) else {
            return Outcome::SpawnFailed;
        };
        let Tenant { pid: tenant, output } = started_tenant;
        match watch(
            tenant,
            output,
            &mut forwarder,
            signals.as_ref(),
            ceiling.limit_bytes,
        ) {
            Watched::ShutdownRequested => {
                stop(tenant);
                return Outcome::ShutdownRequested;
            }
            Watched::Exited { exit, because } => {
                let uptime_ms = started.elapsed().as_millis() as u64;
                let Some(restart) = budget.exited(config, uptime_ms, exit, &because) else {
                    return Outcome::RestartBudgetExhausted;
                };
                announce(&restart, &mut forwarder);
                if wait_for_signal(Duration::from_millis(restart.backoff_ms)) == Arrived::Shutdown {
                    return Outcome::ShutdownRequested;
                }
            }
        }
    }
}

/// The restart, on the console and to the host. After the tenant's last output, which `watch`
/// drained before handing back the exit, so the host reads it where it happened in the stream.
fn announce(restart: &TenantRestart, forwarder: &mut Forwarder) {
    log(restart.reason.as_str());
    forwarder.restarted(restart);
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Watched {
    ShutdownRequested,
    /// `because` is what this runtime knows about the exit that the status does not say: empty,
    /// or a clause naming the memory ceiling.
    Exited {
        exit: TenantExit,
        because: String,
    },
}

fn watch(
    tenant: Pid,
    mut output: TenantOutput,
    forwarder: &mut Forwarder,
    signals: Option<&SignalFd>,
    limit_bytes: u64,
) -> Watched {
    let mut memory = Watch::new(limit_bytes);
    // The kernel's OOM counter is the cgroup's for life, so what says the kernel killed this
    // tenant is the counter having moved since this one started.
    let at_start = memory::read();
    let mut sampled = Instant::now();
    let mut killed_for = None;
    loop {
        // No longer than the forwarder's next dial is away, so a gap the host was down for
        // reaches it as soon as it is back and not when the tenant next speaks.
        let within = forwarder
            .reconnect_due_in()
            .map_or(POLL_INTERVAL, |due| due.min(POLL_INTERVAL));
        output.wait(signals, within);
        output.forward(forwarder);
        forwarder.reconnect_if_due();
        match wait_for_signal(Duration::ZERO) {
            Arrived::Shutdown => return Watched::ShutdownRequested,
            Arrived::ChildDied | Arrived::Nothing => {
                if let Some(exit) = reap_until(tenant) {
                    output.forward(forwarder);
                    let because = killed_for.take().unwrap_or_else(|| {
                        match (at_start, memory::read()) {
                            (Some(before), Some(after)) if Watch::kernel_killed_between(&before, &after) => {
                                format!(
                                    ": the kernel killed it for running out of memory at its ceiling of {} MiB",
                                    ceiling::mib(limit_bytes)
                                )
                            }
                            _ => String::new(),
                        }
                    });
                    return Watched::Exited { exit, because };
                }
            }
        }
        if sampled.elapsed() < ceiling::SAMPLE_INTERVAL || killed_for.is_some() {
            continue;
        }
        sampled = Instant::now();
        let Some(reading) = memory::read() else {
            continue;
        };
        if let Verdict::Thrashing { faults_per_second } = memory.observe(reading, sampled) {
            let because = format!(
                ": killed at its memory ceiling, {} of {} MiB and reading its own code back in at {faults_per_second} faults a second",
                ceiling::mib(reading.current_bytes),
                ceiling::mib(limit_bytes)
            );
            log(&format!("the tenant is thrashing{because}"));
            match memory::kill_everything() {
                Ok(()) => killed_for = Some(because),
                Err(error) => log(&format!("the tenant could not be killed: {error}")),
            }
        }
    }
}

/// How long a watch sleeps with nothing to do between looks at the tenant's memory.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// What the tenant's pipes are widened to, so a burst of output up to this lands without the
/// tenant waiting on this runtime. The kernel's default stays where it refuses that much.
const PIPE_CAPACITY: i32 = 1 << 20;

pub(crate) struct TenantOutput {
    /// `None` once the pipe has hit its end, everything that held its writing side having closed
    /// it, or a read of it has failed outright: it has nothing more to say, and a poll on it
    /// would only ever return at once.
    stdout: Option<std::fs::File>,
    stderr: Option<std::fs::File>,
    /// A read's worth, which is a frame's worth: the host reads no larger a frame.
    buffer: Box<[u8]>,
}

impl TenantOutput {
    fn new(stdout: OwnedFd, stderr: OwnedFd) -> Self {
        Self {
            stdout: Some(std::fs::File::from(stdout)),
            stderr: Some(std::fs::File::from(stderr)),
            buffer: vec![0; guest_contract::logs::MAX_FRAME_PAYLOAD_BYTES].into_boxed_slice(),
        }
    }

    /// Returns once a pipe has something to read, a waited signal is pending, or `within` has
    /// passed, whichever is first.
    fn wait(&self, signals: Option<&SignalFd>, within: Duration) {
        let mut watched = Vec::with_capacity(3);
        for open in [&self.stdout, &self.stderr].into_iter().flatten() {
            watched.push(PollFd::new(open.as_fd(), PollFlags::POLLIN));
        }
        if let Some(signals) = signals {
            watched.push(PollFd::new(signals.as_fd(), PollFlags::POLLIN));
        }
        // Whole milliseconds, rounded up: truncated, the last part of one would be no wait at all.
        let millis = within.as_micros().div_ceil(1_000);
        let _ = poll(
            &mut watched,
            PollTimeout::try_from(millis).unwrap_or(PollTimeout::NONE),
        );
    }

    fn forward(&mut self, forwarder: &mut Forwarder) {
        let Self {
            stdout,
            stderr,
            buffer,
        } = self;
        for (stream, pipe) in [
            (protocol::TenantLogStream::Stdout, stdout),
            (protocol::TenantLogStream::Stderr, stderr),
        ] {
            let Some(open) = pipe else {
                continue;
            };
            // A pipeful a wake, so a tenant that never pauses cannot keep the signals from being
            // looked at; what is left is what the next wake finds ready.
            let mut budget = PIPE_CAPACITY as usize;
            while budget > 0 {
                match std::io::Read::read(open, buffer) {
                    Ok(0) => {
                        *pipe = None;
                        break;
                    }
                    Ok(read) => {
                        forwarder.write(stream, &buffer[..read]);
                        budget = budget.saturating_sub(read);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        *pipe = None;
                        break;
                    }
                }
            }
        }
    }
}

/// A pipe for one of the tenant's streams: its reading side never blocks this runtime, and it
/// holds `PIPE_CAPACITY` where the kernel allows that much.
fn tenant_pipe() -> nix::Result<(OwnedFd, OwnedFd)> {
    let (read, write) = nix::unistd::pipe()?;
    let _ = nix::fcntl::fcntl(
        &read,
        nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_NONBLOCK),
    );
    let _ = nix::fcntl::fcntl(&read, nix::fcntl::FcntlArg::F_SETPIPE_SZ(PIPE_CAPACITY));
    Ok((read, write))
}

pub(crate) struct Tenant {
    pid: Pid,
    output: TenantOutput,
}

fn reap_until(tenant: Pid) -> Option<TenantExit> {
    let mut tenant_exit = None;
    loop {
        match waitpid(None, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) | Err(_) => return tenant_exit,
            Ok(status) => {
                let (pid, exit) = match status {
                    WaitStatus::Exited(pid, code) => (pid, TenantExit::Code(code)),
                    WaitStatus::Signaled(pid, signal, _) => (pid, TenantExit::Signal(signal as i32)),
                    _ => continue,
                };
                if pid == tenant {
                    tenant_exit = Some(exit);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arrived {
    Shutdown,
    ChildDied,
    Nothing,
}

#[allow(unsafe_code)]
fn wait_for_signal(within: Duration) -> Arrived {
    let blocked = waited_signals();
    let timeout = nix::sys::time::TimeSpec::from_duration(within);
    let taken = unsafe { libc::sigtimedwait(blocked.as_ref(), std::ptr::null_mut(), timeout.as_ref()) };
    match Signal::try_from(taken) {
        Ok(Signal::SIGINT | Signal::SIGTERM) => Arrived::Shutdown,
        Ok(Signal::SIGCHLD) => Arrived::ChildDied,
        _ => Arrived::Nothing,
    }
}

fn stop(tenant: Pid) {
    let _ = nix::sys::signal::kill(tenant, Signal::SIGTERM);
    let deadline = Instant::now() + Duration::from_millis(u64::from(SHUTDOWN_GRACE_MS));
    while Instant::now() < deadline {
        if reap_until(tenant).is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    log("the tenant did not stop in time and was killed");
    let _ = nix::sys::signal::kill(tenant, Signal::SIGKILL);
    let _ = waitpid(tenant, None);
}

fn spawn(config: &InstanceConfig, ceiling: &Ceiling) -> Option<Tenant> {
    let procs_file = ceiling.procs_file();
    let root = CString::new(paths::ROOT_MOUNT).ok()?;
    let working_directory = CString::new(config.working_directory.as_str()).ok()?;
    let executable = CString::new(config.program.as_str()).ok()?;
    let mut argv = vec![executable.clone()];
    for argument in &config.arguments {
        argv.push(CString::new(argument.as_str()).ok()?);
    }
    let environment: Vec<CString> = config
        .tenant_environment()
        .into_iter()
        .filter_map(|(name, value)| CString::new(format!("{name}={value}")).ok())
        .collect();

    let (stdout_read, stdout_write) = tenant_pipe().ok()?;
    let (stderr_read, stderr_write) = tenant_pipe().ok()?;

    match unsafe { nix::unistd::fork() } {
        Err(error) => {
            log(&format!("the tenant could not be forked: {error}"));
            None
        }
        Ok(ForkResult::Parent { child }) => {
            drop(stdout_write);
            drop(stderr_write);
            Some(Tenant {
                pid: child,
                output: TenantOutput::new(stdout_read, stderr_read),
            })
        }
        Ok(ForkResult::Child) => {
            let failed = |_| unsafe { libc::_exit(127) };
            let _ = SigSet::all().thread_unblock();
            let _ = nix::unistd::dup2_stdout(&stdout_write).map_err(failed);
            let _ = nix::unistd::dup2_stderr(&stderr_write).map_err(failed);
            // Into the cgroup before the root changes, since the cgroup filesystem is only in
            // this one; and before anything is allocated, since the ceiling is on all of it.
            let _ = memory::join(procs_file).map_err(failed);
            // Into the stacked root before the privileges go: chroot is root's to call, and a
            // process that is 65534 in a root it cannot leave is the whole of the isolation.
            let _ = nix::unistd::chroot(root.as_c_str()).map_err(failed);
            let _ = nix::unistd::chdir(working_directory.as_c_str()).map_err(failed);
            let _ = nix::unistd::setgid(Gid::from_raw(paths::TENANT_GID)).map_err(failed);
            let _ = nix::unistd::setgroups(&[]).map_err(failed);
            let _ = nix::unistd::setuid(Uid::from_raw(paths::TENANT_UID)).map_err(failed);
            let _ = nix::unistd::execve(&executable, &argv, &environment);
            unsafe { libc::_exit(127) }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;
    use std::sync::MutexGuard;

    use guest_contract::logs::{decode_frames, GuestLogFrame};
    use protocol::TenantLogStream;

    use super::*;

    fn one_tenant_at_a_time() -> MutexGuard<'static, ()> {
        crate::child_process_guard()
    }

    /// A tenant that is this process forked, running `run` on its two pipes and exiting well
    /// when that returns; what a real one gets from `spawn` less the root, the cgroup and the
    /// exec.
    fn tenant_that(run: impl FnOnce(&OwnedFd, &OwnedFd)) -> Tenant {
        let (stdout_read, stdout_write) = tenant_pipe().unwrap();
        let (stderr_read, stderr_write) = tenant_pipe().unwrap();
        match unsafe { nix::unistd::fork() }.unwrap() {
            ForkResult::Parent { child } => Tenant {
                pid: child,
                output: TenantOutput::new(stdout_read, stderr_read),
            },
            ForkResult::Child => {
                drop(stdout_read);
                drop(stderr_read);
                run(&stdout_write, &stderr_write);
                exit(0)
            }
        }
    }

    fn say(pipe: &OwnedFd, bytes: &[u8]) {
        let mut written = 0;
        while written < bytes.len() {
            match nix::unistd::write(pipe, &bytes[written..]) {
                Ok(count) => written += count,
                Err(nix::errno::Errno::EINTR) => {}
                Err(_) => unsafe { libc::_exit(111) },
            }
        }
    }

    fn exit(code: i32) -> ! {
        unsafe { libc::_exit(code) }
    }

    /// A forwarder whose host is a file, read back with `frames_in`.
    fn forwarder_into(sink: &Path) -> Forwarder {
        let sink = sink.to_path_buf();
        Forwarder::dialing(Box::new(move || {
            std::fs::File::create(&sink).ok().map(OwnedFd::from)
        }))
    }

    fn frames_in(sink: &Path) -> Vec<GuestLogFrame> {
        let (frames, rest) = decode_frames(&[], &std::fs::read(sink).unwrap()).unwrap();
        assert!(rest.is_empty(), "a frame arrived in part");
        frames
    }

    fn payload_of(frames: &[GuestLogFrame], wanted: TenantLogStream) -> Vec<u8> {
        frames
            .iter()
            .filter_map(|frame| match frame {
                GuestLogFrame::Data { stream, bytes } if *stream == wanted => Some(bytes.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect()
    }

    fn watched(tenant: Tenant, sink: &Path) -> (Watched, Duration) {
        block_signals();
        let mut forwarder = forwarder_into(sink);
        let signals = signals_as_fd();
        let started = Instant::now();
        let ended = watch(
            tenant.pid,
            tenant.output,
            &mut forwarder,
            signals.as_ref(),
            1 << 30,
        );
        (ended, started.elapsed())
    }

    fn sink() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let sink = dir.path().join("host");
        (dir, sink)
    }

    /// What one of the tenant's pipes holds on this kernel.
    fn pipeful() -> usize {
        let (read, _write) = tenant_pipe().unwrap();
        nix::fcntl::fcntl(&read, nix::fcntl::FcntlArg::F_GETPIPE_SZ).unwrap() as usize
    }

    #[test]
    fn a_burst_of_many_pipefuls_lands_as_fast_as_it_is_written() {
        let _one = one_tenant_at_a_time();
        let (_dir, sink) = sink();
        // Thirty-two pipefuls, whatever a pipeful is here: drained a pipeful every 100 ms, as the
        // timer did, they would take over three seconds.
        let chunk = vec![b'x'; guest_contract::logs::MAX_FRAME_PAYLOAD_BYTES];
        let chunks = 32 * pipeful() / chunk.len();
        let tenant = tenant_that(|stdout, _| {
            for _ in 0..chunks {
                say(stdout, &chunk);
            }
        });
        let (ended, took) = watched(tenant, &sink);
        assert_eq!(
            ended,
            Watched::Exited {
                exit: TenantExit::Code(0),
                because: String::new()
            }
        );
        assert_eq!(
            payload_of(&frames_in(&sink), TenantLogStream::Stdout),
            chunk.repeat(chunks)
        );
        assert!(took < Duration::from_secs(1), "{chunks} chunks took {took:?}");
    }

    #[test]
    fn a_tenant_that_exits_is_reaped_promptly_with_the_last_of_what_it_said() {
        let _one = one_tenant_at_a_time();
        let (_dir, sink) = sink();
        let tenant = tenant_that(|stdout, stderr| {
            say(stdout, b"going\n");
            say(stderr, b"gone\n");
            exit(3)
        });
        let (ended, took) = watched(tenant, &sink);
        assert_eq!(
            ended,
            Watched::Exited {
                exit: TenantExit::Code(3),
                because: String::new()
            }
        );
        assert!(took < Duration::from_secs(1), "the exit took {took:?} to notice");
        let frames = frames_in(&sink);
        assert_eq!(payload_of(&frames, TenantLogStream::Stdout), b"going\n");
        assert_eq!(payload_of(&frames, TenantLogStream::Stderr), b"gone\n");
    }

    #[test]
    fn a_tenant_the_kernel_killed_is_reaped_with_the_signal_rather_than_a_code() {
        let _one = one_tenant_at_a_time();
        let (_dir, sink) = sink();
        let tenant = tenant_that(|_, _| loop {
            unsafe { libc::pause() };
        });
        let _ = nix::sys::signal::kill(tenant.pid, Signal::SIGKILL);
        let (ended, _) = watched(tenant, &sink);
        assert_eq!(
            ended,
            Watched::Exited {
                exit: TenantExit::Signal(9),
                because: String::new()
            }
        );
    }

    #[test]
    fn a_restart_is_sent_to_the_host_after_the_last_of_what_the_tenant_said() {
        let _one = one_tenant_at_a_time();
        let (_dir, sink) = sink();
        let tenant = tenant_that(|stdout, stderr| {
            say(stdout, b"going\n");
            say(stderr, b"out of memory\n");
            exit(3)
        });
        block_signals();
        let mut forwarder = forwarder_into(&sink);
        let signals = signals_as_fd();
        let Watched::Exited { exit, because } = watch(
            tenant.pid,
            tenant.output,
            &mut forwarder,
            signals.as_ref(),
            1 << 30,
        ) else {
            panic!("the tenant exited");
        };
        let restart = Budget::default()
            .exited(&crate::supervise::config(|_| {}), 0, exit, &because)
            .unwrap();

        announce(&restart, &mut forwarder);

        let frames = frames_in(&sink);
        assert_eq!(frames.last(), Some(&GuestLogFrame::Restart(restart.clone())));
        assert_eq!(
            payload_of(&frames[..frames.len() - 1], TenantLogStream::Stderr),
            b"out of memory\n"
        );
        assert_eq!(restart.exit, TenantExit::Code(3));
        assert_eq!(
            restart.reason.as_str(),
            "the tenant exited (3); restart 1 of 5 in 500ms"
        );
    }

    #[test]
    fn a_shutdown_signal_ends_the_watch_of_a_tenant_still_running() {
        let _one = one_tenant_at_a_time();
        let (_dir, sink) = sink();
        let tenant = tenant_that(|stdout, _| {
            say(stdout, b"up\n");
            loop {
                unsafe { libc::pause() };
            }
        });
        let pid = tenant.pid;
        // At this thread, since it is the one blocking the signal: the process's other threads
        // would take one sent to the process and end it. Through a usize because musl's
        // pthread_t is a pointer, which cannot cross into the thread that sends.
        let this_thread = unsafe { libc::pthread_self() } as usize;
        let asked = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            unsafe { libc::pthread_kill(this_thread as libc::pthread_t, libc::SIGTERM) }
        });
        let (ended, took) = watched(tenant, &sink);
        assert_eq!(asked.join().unwrap(), 0);
        let _ = nix::sys::signal::kill(pid, Signal::SIGKILL);
        let _ = waitpid(pid, None);
        assert_eq!(ended, Watched::ShutdownRequested);
        assert!(
            took < Duration::from_secs(1),
            "the signal took {took:?} to notice"
        );
        assert_eq!(payload_of(&frames_in(&sink), TenantLogStream::Stdout), b"up\n");
    }

    #[test]
    fn a_gap_reaches_a_host_that_is_back_while_the_tenant_has_nothing_more_to_say() {
        let _one = one_tenant_at_a_time();
        let (_dir, sink) = sink();
        let tenant = tenant_that(|stdout, _| {
            say(stdout, b"up\n");
            loop {
                unsafe { libc::pause() };
            }
        });
        let pid = tenant.pid;
        block_signals();
        // A host away for the tenant's one line, and back for good after that.
        let dials = Rc::new(Cell::new(0));
        let mut forwarder = {
            let dials = Rc::clone(&dials);
            let sink = sink.clone();
            Forwarder::dialing(Box::new(move || {
                dials.set(dials.get() + 1);
                if dials.get() == 1 {
                    return None;
                }
                std::fs::File::create(&sink).ok().map(OwnedFd::from)
            }))
        };
        let signals = signals_as_fd();
        let this_thread = unsafe { libc::pthread_self() } as usize;
        let started = Instant::now();
        let landed = {
            let sink = sink.clone();
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline && !std::fs::metadata(&sink).is_ok_and(|host| host.len() > 0)
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                let landed = started.elapsed();
                unsafe { libc::pthread_kill(this_thread as libc::pthread_t, libc::SIGTERM) };
                landed
            })
        };
        let ended = watch(pid, tenant.output, &mut forwarder, signals.as_ref(), 1 << 30);
        let landed = landed.join().unwrap();
        let _ = nix::sys::signal::kill(pid, Signal::SIGKILL);
        let _ = waitpid(pid, None);
        assert_eq!(ended, Watched::ShutdownRequested);
        assert_eq!(dials.get(), 2);
        assert_eq!(frames_in(&sink), vec![GuestLogFrame::Gap { dropped_bytes: 3 }]);
        assert!(
            landed < Duration::from_secs(1),
            "the gap took {landed:?} to reach the host"
        );
    }

    #[test]
    fn the_pipe_is_widened_where_the_kernel_allows_it() {
        let (read, _write) = tenant_pipe().unwrap();
        let widened = nix::fcntl::fcntl(&read, nix::fcntl::FcntlArg::F_GETPIPE_SZ).unwrap();
        let allowed: i32 = std::fs::read_to_string("/proc/sys/fs/pipe-max-size")
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        if allowed >= PIPE_CAPACITY {
            assert_eq!(widened, PIPE_CAPACITY);
        } else {
            assert!(widened > 0 && widened <= allowed, "{widened} of {allowed}");
        }
    }
}
