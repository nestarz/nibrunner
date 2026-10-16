#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic, clippy::expect_used))]

#[macro_use]
mod wire;
mod control;
mod cron;
mod domain;
/// The frames a root-local service exchanges with the host to reserve memory for work outside
/// its microVMs, over `<runtime_dir>/memory.sock`.
pub mod memory;
#[cfg(feature = "schema")]
pub mod schema;

pub use control::*;
pub use cron::*;
pub use domain::*;
pub use wire::*;

#[cfg(test)]
mod tests;
