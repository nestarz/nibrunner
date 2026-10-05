use std::io;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{ForkResult, Pid};

#[derive(Default)]
pub(crate) struct Reclaimer {
    pending: Option<Pid>,
}

impl Reclaimer {
    fn reap(&mut self) -> io::Result<bool> {
        let Some(pid) = self.pending else {
            return Ok(true);
        };
        match waitpid(pid, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => Ok(false),
            Ok(_) | Err(nix::errno::Errno::ECHILD) => {
                self.pending = None;
                Ok(true)
            }
            Err(error) => Err(error.into()),
        }
    }

    // A reclaim write can wait for storage. A separate process keeps the control channel
    // responsive; retaining its PID prevents repeated timeouts from accumulating helpers.
    pub(crate) fn reclaim(&mut self, file: std::fs::File, bytes: u64, budget: Duration) -> io::Result<()> {
        if !self.reap()? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "a reclaim is still stopping",
            ));
        }
        let amount = bytes.to_string();
        #[cfg(target_os = "linux")]
        let parent = nix::unistd::getpid().as_raw();
        let pid = match unsafe { nix::unistd::fork() }? {
            ForkResult::Child => {
                #[cfg(target_os = "linux")]
                unsafe {
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 || libc::getppid() != parent {
                        libc::_exit(1);
                    }
                }
                let written = unsafe { libc::write(file.as_raw_fd(), amount.as_ptr().cast(), amount.len()) };
                let ok = written == amount.len() as isize
                    || (written < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EAGAIN));
                unsafe { libc::_exit(if ok { 0 } else { 1 }) };
            }
            ForkResult::Parent { child } => {
                self.pending = Some(child);
                child
            }
        };
        let deadline = Instant::now() + budget;
        loop {
            match waitpid(pid, Some(WaitPidFlag::WNOHANG)) {
                Ok(WaitStatus::Exited(_, code)) => {
                    self.pending = None;
                    return if code == 0 {
                        Ok(())
                    } else {
                        Err(io::Error::other("memory reclaim failed"))
                    };
                }
                Ok(WaitStatus::Signaled(..)) => {
                    self.pending = None;
                    return Err(io::Error::other("memory reclaim was interrupted"));
                }
                Ok(_) => {}
                Err(error) => return Err(error.into()),
            }
            if Instant::now() >= deadline {
                let _ = kill(pid, Signal::SIGKILL);
                let _ = self.reap();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "memory reclaim timed out",
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Reclaimer {
    fn drop(&mut self) {
        if let Some(pid) = self.pending {
            let _ = kill(pid, Signal::SIGKILL);
            let _ = self.reap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek};

    #[test]
    fn a_reclaim_writes_only_the_requested_byte_count() {
        let _children = crate::child_process_guard();
        let mut file = tempfile::tempfile().unwrap();
        Reclaimer::default()
            .reclaim(file.try_clone().unwrap(), 65536, Duration::from_secs(1))
            .unwrap();
        file.rewind().unwrap();
        let mut value = String::new();
        file.read_to_string(&mut value).unwrap();
        assert_eq!(value, "65536");
    }

    #[test]
    fn a_blocked_reclaim_times_out_and_is_reaped_before_another_starts() {
        let _children = crate::child_process_guard();
        let (read, write) = nix::unistd::pipe().unwrap();
        let flags = nix::fcntl::OFlag::O_NONBLOCK;
        nix::fcntl::fcntl(&write, nix::fcntl::FcntlArg::F_SETFL(flags)).unwrap();
        while nix::unistd::write(&write, &[0; 4096]).is_ok() {}
        nix::fcntl::fcntl(&write, nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::empty())).unwrap();
        let mut reclaimer = Reclaimer::default();
        let started = Instant::now();
        let error = reclaimer
            .reclaim(write.into(), 65536, Duration::from_millis(25))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(read);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !reclaimer.reap().unwrap() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(reclaimer.pending.is_none());
        reclaimer
            .reclaim(tempfile::tempfile().unwrap(), 4096, Duration::from_secs(1))
            .unwrap();
    }
}
