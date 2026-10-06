#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic, clippy::expect_used))]

#[macro_use]
mod wire;
mod control;
mod domain;
pub mod memory;
#[cfg(feature = "schema")]
pub mod schema;

pub use control::*;
pub use domain::*;
pub use wire::*;

#[cfg(test)]
mod tests;
