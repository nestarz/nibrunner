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
//! the tenant from `just integration-guest`. An enabled run fails if its fixtures are missing;
//! ordinary `just test` skips this suite. `just integration-guest` is the way in.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

#[cfg(target_os = "linux")]
mod cache;
#[cfg(target_os = "linux")]
mod cron;
#[cfg(target_os = "linux")]
mod idle;
#[cfg(target_os = "linux")]
mod isolation;
#[cfg(target_os = "linux")]
mod lifecycle;
#[cfg(target_os = "linux")]
mod raw;
#[cfg(target_os = "linux")]
mod resources;
#[cfg(target_os = "linux")]
mod sleep;
#[cfg(target_os = "linux")]
mod tenant;
#[cfg(target_os = "linux")]
mod volumes;

#[cfg(target_os = "linux")]
pub use nibrunnerd::test_support::machine::RunningHost;

/// A host to test against, or nothing when this machine cannot hold one.
#[cfg(target_os = "linux")]
pub async fn host() -> Option<RunningHost> {
    host_with(|_| {}).await
}

#[cfg(target_os = "linux")]
pub async fn host_with(edit: impl FnOnce(&mut nibrunnerd::config::HostConfig)) -> Option<RunningHost> {
    if !std::env::var("NIBRUNNER_INTEGRATION").is_ok_and(|value| value == "1") {
        return None;
    }
    #[allow(unsafe_code, reason = "asking who this process is has no safe spelling")]
    if unsafe { libc::geteuid() } != 0 {
        panic!("NIBRUNNER_INTEGRATION=1 was set but this is not running as root");
    }
    Some(nibrunnerd::test_support::machine::started_with(edit).await.expect(
        "NIBRUNNER_INTEGRATION=1 requires a guest image and tenant: run `just guest-image`, then `just integration-guest`",
    ))
}
