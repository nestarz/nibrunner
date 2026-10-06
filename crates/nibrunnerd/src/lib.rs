#![cfg_attr(
    any(test, feature = "testing"),
    allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)
)]

pub mod adapters;
pub mod clock;
pub mod config;
pub mod controllers;
pub mod desired;
pub mod domain;
pub mod host;
pub mod install;
pub mod json_store;
pub mod memory_service;
pub mod ports;
pub mod reload;
pub mod repositories;
pub mod run;
pub mod runtime_policy;
pub mod services;
pub mod start;
pub mod state;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
