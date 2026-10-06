//! Root-local memory reservations for work outside the app runtime.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

pub const SOCKET_NAME: &str = "memory.sock";
pub const MAX_FRAME_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Request {
    Acquire {
        id: String,
        unit: String,
        memory_mib: NonZeroU32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum_mib: Option<NonZeroU32>,
    },
    Release {
        id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Reply {
    Granted { memory_mib: NonZeroU32 },
    Released,
    Waiting { reason: String },
    Rejected { reason: String },
}
