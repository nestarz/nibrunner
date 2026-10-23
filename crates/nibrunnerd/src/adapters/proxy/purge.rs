//! The purge an app asks of the response cache by tag, here and on every other host, under the
//! hostname and path a desired document's `httpCache` names. It is a request rather than state:
//! nothing is written down, and an app whose purge was not confirmed everywhere asks again.

use std::time::Duration;

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{HeaderValue, ALLOW, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE};
use hyper::{Method, Request, Response, StatusCode};
use protocol::{CachePurge, DesiredHttpCache};
use serde::Deserialize;
use sha2::Sha256;

use crate::adapters::proxy::cache::ResponseCache;
use crate::adapters::proxy::forward::ProxyBody;

pub(crate) const MOST_TAGS_PER_PURGE: usize = 500;
// 500 tags of the longest a stored answer may carry, and the JSON around them.
const MOST_BODY_BYTES: usize = 600 * 1024;
const RELAY_TIMEOUT: Duration = Duration::from_secs(10);
const SIGNED_PREFIX: &[u8] = b"mf-cache-purge\0";
const TOKEN_VERSION: &str = "v1.";
/// On a purge relayed from another host, so that it is not relayed again.
pub(crate) const RELAYED: &str = "mf-cache-relay";
const DEFAULT_NAME: &str = "nibrunner";

/// What the document's `httpCache` makes of the response cache: the name it gives itself in
/// `Cache-Status`, and the purge it serves.
pub(crate) struct SharedCache {
    desired: Option<DesiredHttpCache>,
    name: Box<str>,
    purge: Option<Purge>,
}

impl Default for SharedCache {
    fn default() -> Self {
        Self {
            desired: None,
            name: DEFAULT_NAME.into(),
            purge: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("the purge's relay to the other hosts could not be set up: {0}")]
pub(crate) struct RelayUnavailable(#[from] reqwest::Error);

impl SharedCache {
    /// `roots` are the only certificates a peer is trusted under when there are any; a host
    /// trusts its system's otherwise.
    pub(crate) fn of(
        desired: Option<&DesiredHttpCache>,
        roots: &[reqwest::Certificate],
    ) -> Result<Self, RelayUnavailable> {
        let Some(desired) = desired else {
            return Ok(Self::default());
        };
        Ok(Self {
            desired: Some(desired.clone()),
            name: desired.name.as_str().into(),
            purge: Some(Purge::of(&desired.purge, roots)?),
        })
    }

    pub(crate) fn describes(&self, desired: Option<&DesiredHttpCache>) -> bool {
        self.desired.as_ref() == desired
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// The purge, when `hostname` and `path` are where it is served.
    pub(crate) fn purge_at(&self, hostname: &str, path: &str) -> Option<&Purge> {
        self.purge
            .as_ref()
            .filter(|purge| purge.hostname == hostname && purge.path == path)
    }
}

struct Peer {
    listed: String,
    url: String,
    client: reqwest::Client,
}

pub(crate) struct Purge {
    hostname: String,
    path: String,
    keys: Vec<Vec<u8>>,
    peers: Vec<Peer>,
}

/// A purge this host was asked for by an app rather than relayed by another host.
pub(crate) struct Asked {
    pub(crate) owner: String,
}

#[derive(Deserialize)]
struct Tags {
    tags: Vec<String>,
}

fn json(status: StatusCode, body: serde_json::Value) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .body(
            Full::new(Bytes::from(body.to_string()))
                .map_err(|never| match never {})
                .boxed(),
        )
        .expect("a JSON answer is always buildable")
}

fn refused(status: StatusCode, error: &str) -> Response<ProxyBody> {
    json(status, serde_json::json!({ "error": error }))
}

fn signed(key: &[u8], owner: &str) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(SIGNED_PREFIX);
    mac.update(owner.as_bytes());
    mac
}

/// The token an app is given to purge as `owner` under `key`.
#[cfg(test)]
pub(crate) fn token(key: &[u8], owner: &str) -> String {
    let signature = URL_SAFE_NO_PAD.encode(signed(key, owner).finalize().into_bytes());
    format!("{TOKEN_VERSION}{owner}.{signature}")
}

impl Purge {
    fn of(desired: &CachePurge, roots: &[reqwest::Certificate]) -> Result<Self, RelayUnavailable> {
        let hostname = desired.hostname.as_str();
        let peers = desired
            .peers
            .iter()
            .map(|peer| {
                // Reached at its address under the purge's own hostname, which its certificate
                // covers: SNI and `Host` are that name, whichever host public DNS sends it to.
                let mut client = reqwest::Client::builder()
                    .resolve(hostname, peer.address())
                    .timeout(RELAY_TIMEOUT)
                    .https_only(true)
                    .no_proxy();
                if !roots.is_empty() {
                    client = client.tls_certs_only(roots.iter().cloned());
                }
                Ok(Peer {
                    listed: peer.as_str().to_string(),
                    url: format!("https://{hostname}:{}{}", peer.address().port(), desired.path),
                    client: client.build()?,
                })
            })
            .collect::<Result<_, reqwest::Error>>()?;
        Ok(Self {
            hostname: hostname.to_string(),
            path: desired.path.as_str().to_string(),
            keys: desired
                .keys
                .iter()
                .map(|key| {
                    STANDARD
                        .decode(key.as_str())
                        .expect("a purge key is validated as base64 on parsing")
                })
                .collect(),
            peers,
        })
    }

    /// Whose purge `authorization` carries: `Bearer v1.<owner>.<signature>`, signed under any key.
    fn owner_of<'a>(&self, authorization: Option<&'a HeaderValue>) -> Option<&'a str> {
        let (scheme, token) = authorization?.to_str().ok()?.split_once(' ')?;
        let (owner, signature) = token.strip_prefix(TOKEN_VERSION)?.rsplit_once('.')?;
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        let valid = scheme.eq_ignore_ascii_case("bearer")
            && !owner.is_empty()
            && self
                .keys
                .iter()
                .any(|key| signed(key, owner).verify_slice(&signature).is_ok());
        valid.then_some(owner)
    }

    /// The purge answered: 2xx only once this host and every peer have purged, the purge of an
    /// app rather than a relay said so beside it.
    pub(crate) async fn answer<B>(
        &self,
        request: Request<B>,
        cache: Option<&ResponseCache>,
    ) -> (Response<ProxyBody>, Option<Asked>)
    where
        B: hyper::body::Body<Data = Bytes, Error = hyper::Error>,
    {
        if request.method() != Method::POST {
            let mut response = refused(StatusCode::METHOD_NOT_ALLOWED, "a purge is a POST");
            response
                .headers_mut()
                .insert(ALLOW, HeaderValue::from_static("POST"));
            return (response, None);
        }
        let (parts, body) = request.into_parts();
        let authorization = parts.headers.get(AUTHORIZATION);
        let Some(owner) = self.owner_of(authorization) else {
            return (refused(StatusCode::UNAUTHORIZED, "no valid purge token"), None);
        };
        let body = match Limited::new(body, MOST_BODY_BYTES).collect().await {
            Ok(body) => body.to_bytes(),
            Err(_) => {
                return (
                    refused(StatusCode::PAYLOAD_TOO_LARGE, "the body is too large"),
                    None,
                )
            }
        };
        let tags = match serde_json::from_slice::<Tags>(&body) {
            Ok(Tags { tags }) if tags.len() > MOST_TAGS_PER_PURGE => {
                return (
                    refused(StatusCode::PAYLOAD_TOO_LARGE, "a purge names at most 500 tags"),
                    None,
                )
            }
            Ok(Tags { tags }) if tags.iter().all(|tag| !tag.is_empty()) => tags,
            _ => {
                return (
                    refused(
                        StatusCode::BAD_REQUEST,
                        "the body is {\"tags\": [...]}, each a non-empty string",
                    ),
                    None,
                )
            }
        };
        let purged = match cache {
            Some(cache) => cache.purge(owner, &tags).await,
            None => 0,
        };
        if parts.headers.get(RELAYED).is_some_and(|relayed| relayed == "1") {
            return (
                json(
                    StatusCode::OK,
                    serde_json::json!({ "purged": purged, "hosts": 1 }),
                ),
                None,
            );
        }
        let asked = Some(Asked {
            owner: owner.to_string(),
        });
        let token = authorization.expect("a purge with an owner carried its token");
        let relayed = futures::future::join_all(
            self.peers
                .iter()
                .map(|peer| Self::relayed_to(peer, token, body.clone())),
        )
        .await;
        let failed: Vec<&str> = self
            .peers
            .iter()
            .zip(&relayed)
            .filter(|(_, purged)| purged.is_none())
            .map(|(peer, _)| peer.listed.as_str())
            .collect();
        if !failed.is_empty() {
            return (
                json(StatusCode::BAD_GATEWAY, serde_json::json!({ "failed": failed })),
                asked,
            );
        }
        let elsewhere: usize = relayed.into_iter().flatten().sum();
        let answer = serde_json::json!({ "purged": purged + elsewhere, "hosts": 1 + self.peers.len() });
        (json(StatusCode::OK, answer), asked)
    }

    /// How many entries `peer` purged, once it answered 2xx.
    async fn relayed_to(peer: &Peer, token: &HeaderValue, body: Bytes) -> Option<usize> {
        let response = peer
            .client
            .post(&peer.url)
            .header(AUTHORIZATION, token.clone())
            .header(RELAYED, "1")
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await;
        let response = match response {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::warn!(peer = %peer.listed, status = %response.status(), "a peer refused a relayed purge");
                return None;
            }
            Err(error) => {
                tracing::warn!(peer = %peer.listed, %error, "a peer could not be reached with a relayed purge");
                return None;
            }
        };
        let answered: serde_json::Value = response.json().await.unwrap_or_default();
        Some(
            answered["purged"]
                .as_u64()
                .and_then(|purged| usize::try_from(purged).ok())
                .unwrap_or(0),
        )
    }
}
