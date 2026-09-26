pub const FREEZE_REQUEST: &str = "FREEZE\n";
pub const FREEZE_HELD: &str = "OK";
pub const TENANT_FREEZE_REQUEST: &str = "SLEEP";
pub const TENANT_FREEZE_HELD: &str = "OK";
pub const TENANT_CLOCK_REQUEST: &str = "WAKE ";
pub const TENANT_CLOCK_READY: &str = "READY";
pub const TENANT_CLOCK_RELEASE: &str = "GO";
pub const TENANT_CLOCK_RELEASED: &str = "OK";

pub const GUEST_SHUTDOWN_GRACE_MS: u64 = 10_000;

pub const GUEST_LOG_PREFIX: &str = "[nibrun] ";

const KERNEL_PANIC: &str = "Kernel panic - not syncing: ";
const KERNEL_REBOOT: &str = "reboot: ";

/// Why the guest went down, read off its console once its VMM has exited.
///
/// A guest that shut itself down said why first: init's last line before the kernel reported the
/// restart init asked it for. A kernel that panicked said so itself. A console that ends short of
/// either belongs to a VMM that was killed from outside, and its last line — whatever init was
/// saying when the lights went out — is not a reason.
pub fn exit_reason(console: &str) -> Option<String> {
    let mut rebooted = false;
    for line in console
        .lines()
        .map(|line| without_kernel_timestamp(line.trim()))
        .rev()
    {
        if line.starts_with(KERNEL_PANIC) {
            return Some(line.to_string());
        }
        if line.starts_with(KERNEL_REBOOT) {
            rebooted = true;
            continue;
        }
        if let Some(said) = line.strip_prefix(GUEST_LOG_PREFIX) {
            return rebooted.then(|| said.to_string());
        }
    }
    None
}

// The kernel stamps its lines `[   15.736786] `; init's own `[nibrun] ` is not a stamp.
fn without_kernel_timestamp(line: &str) -> &str {
    let Some((stamp, rest)) = line.strip_prefix('[').and_then(|rest| rest.split_once(']')) else {
        return line;
    };
    if stamp.trim().parse::<f64>().is_ok() {
        rest.trim_start()
    } else {
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_guest_that_shut_itself_down_gives_the_reason_its_init_wrote_before_the_kernel_restarted() {
        let console = [
            "[nibrun] starting the tenant as uid 65534 with data at /app/data",
            "[nibrun] the tenant used its 5 restarts without staying up; shutting the guest down",
            "[   15.736786] reboot: Restarting system",
            "2026-08-26T15:46:48.429 [anonymous-instance:main] Vmm is stopping.",
            "",
        ]
        .join("\n");
        assert_eq!(
            exit_reason(&console).as_deref(),
            Some("the tenant used its 5 restarts without staying up; shutting the guest down")
        );
    }

    #[test]
    fn a_console_that_stops_short_of_a_restart_holds_no_reason_however_much_init_said() {
        // A VMM killed from outside leaves the console mid-sentence: the last thing init said is
        // a progress line, not why the guest went down.
        let console = [
            "[    0.000000] Linux version 6.1.180",
            "[nibrun] guest runtime starting",
            "[nibrun] starting /app/probe as uid 65534 in /app, with 198 MiB to spend",
            "",
        ]
        .join("\n");
        assert_eq!(exit_reason(&console), None);
    }

    #[test]
    fn a_kernel_that_panicked_gives_its_panic_line_without_the_timestamp() {
        let console = [
            "[nibrun] starting /app/probe as uid 65534 in /app, with 198 MiB to spend",
            "[   12.345678] Kernel panic - not syncing: Attempted to kill init! exitcode=0x00000100",
            "[   12.345680] Rebooting in 1 seconds..",
            "",
        ]
        .join("\n");
        assert_eq!(
            exit_reason(&console).as_deref(),
            Some("Kernel panic - not syncing: Attempted to kill init! exitcode=0x00000100")
        );
    }

    #[test]
    fn a_restart_init_said_nothing_before_and_an_empty_console_give_no_reason() {
        assert_eq!(
            exit_reason(
                "[    0.000000] Linux version 6.1.180\n[   15.7] reboot: Restarting system\nVmm is stopping.\n"
            ),
            None
        );
        assert_eq!(exit_reason(""), None);
    }

    #[test]
    fn only_the_kernel_stamp_is_taken_off_a_line() {
        assert_eq!(
            without_kernel_timestamp("[   15.736786] reboot: Restarting system"),
            "reboot: Restarting system"
        );
        assert_eq!(
            without_kernel_timestamp("[nibrun] the tenant has stopped"),
            "[nibrun] the tenant has stopped"
        );
        assert_eq!(without_kernel_timestamp("Vmm is stopping."), "Vmm is stopping.");
    }
}
