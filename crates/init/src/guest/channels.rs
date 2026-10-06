use guest_contract::channels::ChannelTransport;
use nix::unistd::{ForkResult, Pid};

use crate::guest::{control, filesystem, log};

pub(crate) struct Channels {
    control: Option<Pid>,
    files: Option<Pid>,
    transport: ChannelTransport,
}

pub(crate) fn start(transport: ChannelTransport) -> Channels {
    Channels {
        control: fork_channel("control", transport, control::serve),
        files: fork_channel("filesystem", transport, filesystem::serve),
        transport,
    }
}

impl Channels {
    pub(crate) fn stop(&self) {
        for pid in [self.control, self.files].into_iter().flatten() {
            let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        }
        if self.transport == ChannelTransport::Vsock {
            control::thaw_quietly();
        }
        for pid in [self.control, self.files].into_iter().flatten() {
            let _ = nix::sys::wait::waitpid(pid, None);
        }
    }
}

fn fork_channel(
    what: &'static str,
    transport: ChannelTransport,
    serve: fn(ChannelTransport) -> !,
) -> Option<Pid> {
    match unsafe { nix::unistd::fork() } {
        Ok(ForkResult::Parent { child }) => Some(child),
        Ok(ForkResult::Child) => {
            // The guest blocks SIGTERM before forking us; left blocked, Channels::stop() would
            // SIGTERM this child and then waitpid on it forever, so the guest never reboots.
            let _ = nix::sys::signal::SigSet::all().thread_unblock();
            serve(transport)
        }
        Err(error) => {
            log(&format!("the {what} channel could not be started: {error}"));
            None
        }
    }
}
