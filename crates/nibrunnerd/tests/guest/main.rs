//! What the machine does, asked of a real microVM.
//!
//! Every test here writes a document to a host built the way `nibrunnerd serve` builds one, and
//! then asks the guest — through the proxy, on the wire — whether the invariant held. The unit
//! suite proves what the daemon decides; these prove what happens once a kernel, a hypervisor and
//! a tenant are involved.
//!
//! Each one asks for a multi-threaded runtime: a host that a panicking test never stopped takes
//! its guests down as it is dropped, and blocking on that needs a runtime with another thread to
//! put the work on.
//!
//! They need Linux, root, `/dev/kvm`, `nft`, `mke2fs`, a guest image from `just guest-image` and
//! the tenant from `just guest-tests`, and they say which of those is missing rather than failing
//! on a machine that was never going to run them. `just guest-tests` is the way in.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

#[cfg(target_os = "linux")]
mod lifecycle;

#[cfg(target_os = "linux")]
pub use nibrunnerd::test_support::machine::RunningHost;

/// A host to test against, or nothing when this machine cannot hold one.
#[cfg(target_os = "linux")]
pub async fn host() -> Option<RunningHost> {
    if !std::env::var("NIBRUNNER_INTEGRATION").is_ok_and(|value| value == "1") {
        return None;
    }
    #[allow(unsafe_code, reason = "asking who this process is has no safe spelling")]
    if unsafe { libc::geteuid() } != 0 {
        panic!("NIBRUNNER_INTEGRATION=1 was set but this is not running as root");
    }
    let started = nibrunnerd::test_support::machine::started().await;
    if started.is_none() {
        eprintln!("no guest image or no tenant: run `just guest-image`, then `just guest-tests`");
    }
    started
}
