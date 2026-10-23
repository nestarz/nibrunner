use std::net::{IpAddr, SocketAddr};

#[cfg(feature = "schema")]
use std::borrow::Cow;

#[cfg(feature = "schema")]
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

use crate::wire::*;

pub const MAX_PURGE_KEYS: usize = 4;
pub const MAX_PURGE_PEERS: usize = 256;

const MAX_CACHE_NAME_LENGTH: usize = 32;
#[cfg(feature = "schema")]
const CACHE_NAME_PATTERN: &str = "^[a-z][a-z0-9-]{0,31}$";

fn is_cache_name(value: &str) -> bool {
    value.len() <= MAX_CACHE_NAME_LENGTH
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

validated_string!(
    /// What the response cache calls itself in the `Cache-Status` it adds (RFC 9211).
    CacheName,
    "cache name",
    "a lowercase letter, then at most 31 lowercase letters, digits or `-`",
    is_cache_name,
    { "pattern": CACHE_NAME_PATTERN, "maxLength": MAX_CACHE_NAME_LENGTH }
);

const MAX_PURGE_PATH_LENGTH: usize = 256;

fn is_purge_path(value: &str) -> bool {
    value.starts_with('/')
        && value.len() <= MAX_PURGE_PATH_LENGTH
        && value
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b'?' && b != b'#')
}

validated_string!(
    PurgePath,
    "purge path",
    "an absolute path of at most 256 printable characters, without a query or fragment",
    is_purge_path,
    { "pattern": "^/[\\x21\\x22\\x24-\\x3e\\x40-\\x7e]*$", "maxLength": MAX_PURGE_PATH_LENGTH }
);

// 32 bytes are 43 base64 digits and a pad, the last digit carrying four bits and two zeros.
#[cfg(feature = "schema")]
const PURGE_KEY_PATTERN: &str = "^[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=$";

fn is_purge_key(value: &str) -> bool {
    let digit = |b: &u8| b.is_ascii_alphanumeric() || *b == b'+' || *b == b'/';
    let bytes = value.as_bytes();
    bytes.len() == 44
        && bytes[..42].iter().all(digit)
        && b"AEIMQUYcgkosw048".contains(&bytes[42])
        && bytes[43] == b'='
}

validated_string!(
    /// A key purge tokens are signed with, in standard base64.
    PurgeKey,
    "purge key",
    "32 bytes in padded standard base64",
    is_purge_key,
    { "pattern": PURGE_KEY_PATTERN }
);

/// The address an `https://` URL names, when it names an IP address rather than a hostname.
fn peer_address(value: &str) -> Option<SocketAddr> {
    let authority = value.strip_prefix("https://")?;
    if let Ok(address) = authority.parse::<SocketAddr>() {
        return Some(address);
    }
    let ip = match authority.strip_prefix('[') {
        Some(bracketed) => IpAddr::V6(bracketed.strip_suffix(']')?.parse().ok()?),
        None => IpAddr::V4(authority.parse().ok()?),
    };
    Some(SocketAddr::new(ip, 443))
}

validated_string!(
    /// Another host a purge is relayed to, by its address: the hostname the purge is served under
    /// may resolve to any host, and each one is asked.
    CachePeer,
    "cache peer",
    "an https URL naming an IP address and optionally a port, without a path",
    |value| peer_address(value).is_some_and(|address| address.port() != 0),
    { "pattern": "^https://(\\[[0-9A-Fa-f:.]+\\]|[0-9.]+)(:[0-9]{1,5})?$" }
);

impl CachePeer {
    pub fn address(&self) -> SocketAddr {
        peer_address(&self.0).expect("validated on construction")
    }
}

/// Where an app purges what the response cache holds of it, and the hosts a purge reaches.
///
/// `POST https://<hostname><path>` with `authorization: Bearer v1.<owner>.<signature>`, the
/// signature `HMAC-SHA256(key, "mf-cache-purge\0" || owner)` in unpadded base64url under any of
/// `keys`, and `{"tags": [...]}`, removes every stored answer of `owner` carrying one of the tags:
/// here, then on every peer. `owner` is the `app` label of the instances it covers, or the app id
/// of one without that label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CachePurge {
    pub hostname: Hostname,
    pub path: PurgePath,
    /// Two while the key is being rotated, so a token signed under either is taken.
    #[cfg_attr(feature = "schema", schemars(length(min = 1, max = MAX_PURGE_KEYS)))]
    pub keys: Vec<PurgeKey>,
    /// Every other host, each asked to purge too before the purge is answered.
    #[cfg_attr(feature = "schema", schemars(length(max = MAX_PURGE_PEERS)))]
    pub peers: Vec<CachePeer>,
}

/// How the response cache of `[proxy.http.cache]` is shared: what it calls itself to visitors, and
/// how an app purges it across every host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DesiredHttpCache {
    pub name: CacheName,
    pub purge: CachePurge,
}

impl DesiredHttpCache {
    pub(crate) fn check(&self) -> Result<(), InvalidValue> {
        let keys = self.purge.keys.len();
        if !(1..=MAX_PURGE_KEYS).contains(&keys) {
            return Err(InvalidValue::new_public(&format!(
                "httpCache.purge names between 1 and {MAX_PURGE_KEYS} keys, not {keys}"
            )));
        }
        if self.purge.peers.len() > MAX_PURGE_PEERS {
            return Err(InvalidValue::new_public(&format!(
                "httpCache.purge names at most {MAX_PURGE_PEERS} peers"
            )));
        }
        Ok(())
    }
}
