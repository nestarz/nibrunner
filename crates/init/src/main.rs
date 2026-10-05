#![allow(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic, clippy::expect_used))]

#[cfg_attr(
    not(target_os = "linux"),
    allow(dead_code, reason = "only the Linux guest reads any of it")
)]
mod ceiling;
#[cfg_attr(
    not(target_os = "linux"),
    allow(dead_code, reason = "only the Linux guest reads any of it")
)]
mod supervise;

#[cfg(unix)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod reclaim;

#[cfg(target_os = "linux")]
mod guest;

#[cfg(test)]
fn child_process_guard() -> std::sync::MutexGuard<'static, ()> {
    // PID 1's supervisor tests reap every child, including helpers from other tests.
    static CHILDREN: std::sync::Mutex<()> = std::sync::Mutex::new(());
    CHILDREN.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn main() -> std::process::ExitCode {
    #[cfg(target_os = "linux")]
    {
        guest::run()
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("nibrunner-init is a Linux guest's PID 1 and has nothing to do here");
        std::process::ExitCode::FAILURE
    }
}
