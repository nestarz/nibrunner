/// How a microVM's process ended: with a code of its own, or under a signal from outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmExit {
    Code(i32),
    Signal(i32),
}

impl VmExit {
    pub fn describe(self) -> String {
        match self {
            VmExit::Code(code) => format!("exited with exit code {code}"),
            VmExit::Signal(signal) => {
                let name = nix::sys::signal::Signal::try_from(signal)
                    .map(|known| format!(" ({})", known.as_str()))
                    .unwrap_or_default();
                format!("was killed by signal {signal}{name}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VmStatus {
    pub loaded: bool,
    pub active: bool,
    pub frozen: bool,
    pub failed: bool,
    pub started_this_boot: bool,
    pub exit: Option<VmExit>,
}

pub const UNKNOWN_VM: VmStatus = VmStatus {
    loaded: false,
    active: false,
    frozen: false,
    failed: false,
    started_this_boot: false,
    exit: None,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_microvm_this_host_knows_nothing_about_claims_nothing_about_it() {
        let unknown = UNKNOWN_VM;
        assert_eq!(unknown, VmStatus::default());
        assert!(!unknown.loaded);
        assert!(!unknown.active);
        assert!(!unknown.failed);
        assert!(!unknown.started_this_boot);
        assert_eq!(unknown.exit, None);
    }

    #[test]
    fn a_status_that_differs_in_any_field_is_a_different_status() {
        assert_ne!(
            VmStatus {
                exit: Some(VmExit::Code(0)),
                ..UNKNOWN_VM
            },
            UNKNOWN_VM
        );
        assert_ne!(
            VmStatus {
                active: true,
                ..UNKNOWN_VM
            },
            UNKNOWN_VM
        );
        assert_eq!(
            VmStatus {
                loaded: true,
                ..UNKNOWN_VM
            },
            VmStatus {
                loaded: true,
                active: false,
                frozen: false,
                failed: false,
                started_this_boot: false,
                exit: None,
            }
        );
    }

    #[test]
    fn an_exit_says_how_the_process_ended_and_names_a_signal_the_host_knows() {
        assert_eq!(VmExit::Code(137).describe(), "exited with exit code 137");
        assert_eq!(VmExit::Signal(9).describe(), "was killed by signal 9 (SIGKILL)");
        assert_eq!(VmExit::Signal(15).describe(), "was killed by signal 15 (SIGTERM)");
        assert_eq!(VmExit::Signal(200).describe(), "was killed by signal 200");
    }
}
