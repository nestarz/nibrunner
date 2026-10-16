use nix::unistd::{ForkResult, Pid};

use guest_contract::channels::ChannelTransport;
use guest_contract::instance_env::InstanceConfig;

use crate::guest::{control, cron, filesystem, log, memory};

pub(crate) struct Channels {
    control: Option<Pid>,
    files: Option<Pid>,
    cron: Option<Pid>,
}

pub(crate) fn start(
    config: &InstanceConfig,
    ceiling: &memory::Ceiling,
    transport: ChannelTransport,
) -> Channels {
    Channels {
        control: fork_channel("control", transport, control::serve),
        files: fork_channel("filesystem", transport, filesystem::serve),
        cron: cron::start(config, ceiling),
    }
}

impl Channels {
    pub(crate) fn stop(&self) {
        if let Some(pid) = self.cron {
            cron::stop(pid);
        }
        for pid in [self.control, self.files].into_iter().flatten() {
            let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        }
        control::thaw_quietly();
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
