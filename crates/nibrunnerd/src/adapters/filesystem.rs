//! What a guest holds, listed for something else on this machine.
//!
//! A read surface, like the scrape page: it answers questions and takes no input, so "nothing may
//! tell this daemon what to do except by writing that document" still holds of a daemon that now
//! answers two connections. A listing does wake a sleeping app, because that is what any request
//! reaching one does.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use protocol::{AppId, GuestPath};

use crate::domain::filesystem::client::GuestFilesystemError;
use crate::domain::filesystem::reader;
use crate::host::Host;
use crate::ports::{WakeFailure, WakeRefusal};

const APPS_PREFIX: &str = "/apps/";
const LISTING_SUFFIX: &str = "/files";
const PATH_QUERY: &str = "path=";

const USAGE: &str = "A listing is GET /apps/<appId>/files?path=<path>.";

const SOCKET_MODE: u32 = 0o600;

/// The body every refusal is answered with.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub(crate) struct Refused {
    /// Why the listing was refused, as the sentence an operator will find in a log.
    message: String,
}

#[derive(Debug, thiserror::Error)]
enum Refusal {
    #[error("nothing on this host answers {route}. {USAGE}")]
    NoRoute { route: String },
    #[error("a listing is read with GET, and this asked with {method}. {USAGE}")]
    NotRead { method: String },
    #[error("{value} is not {what}. {USAGE}")]
    Unreadable { value: String, what: &'static str },
    #[error("a listing says which directory it wants. {USAGE}")]
    NoDirectory,
    #[error("this host runs no app called {app_id}")]
    NotHere { app_id: AppId },
    #[error("{app_id} is stopped, so it holds no files to list")]
    Stopped { app_id: AppId },
    #[error("{app_id} could not be woken to be listed: {reason}")]
    NotWoken { app_id: AppId, reason: String },
    #[error("this host has no room to wake {app_id}: it is {shortfall_mib} MiB short")]
    NoRoom { app_id: AppId, shortfall_mib: u64 },
    #[error("{0}")]
    Guest(#[from] GuestFilesystemError),
}

/// A refusal apart from the sentence it carries: the status it is answered with, and what the
/// published document says that status means. The document is written from these, so a refusal
/// this socket can answer with cannot go undescribed — adding a [`Refusal`] does not compile
/// until it names one of these, and adding one of these does not compile until it is described.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RefusalKind {
    NoRoute,
    NotRead,
    Unreadable,
    NoDirectory,
    NotHere,
    Stopped,
    NotWoken,
    NoRoom,
    GuestUnreachable,
    GuestSilent,
    GuestRefused,
    GuestTooLarge,
    GuestMalformed,
}

impl RefusalKind {
    #[cfg(feature = "schema")]
    pub(crate) const DECLARED: &'static [Self] = &[
        Self::NoRoute,
        Self::NotRead,
        Self::Unreadable,
        Self::NoDirectory,
        Self::NotHere,
        Self::Stopped,
        Self::NotWoken,
        Self::NoRoom,
        Self::GuestUnreachable,
        Self::GuestSilent,
        Self::GuestRefused,
        Self::GuestTooLarge,
        Self::GuestMalformed,
    ];

    pub(crate) fn status(self) -> StatusCode {
        match self {
            Self::NoRoute | Self::NotHere => StatusCode::NOT_FOUND,
            Self::NotRead => StatusCode::METHOD_NOT_ALLOWED,
            Self::Unreadable | Self::NoDirectory | Self::GuestTooLarge => StatusCode::BAD_REQUEST,
            Self::Stopped => StatusCode::CONFLICT,
            Self::NotWoken | Self::NoRoom | Self::GuestUnreachable => StatusCode::SERVICE_UNAVAILABLE,
            Self::GuestSilent => StatusCode::GATEWAY_TIMEOUT,
            Self::GuestRefused => StatusCode::FORBIDDEN,
            Self::GuestMalformed => StatusCode::BAD_GATEWAY,
        }
    }

    /// One clause, joined with the others its status carries.
    #[cfg(feature = "schema")]
    pub(crate) fn describes(self) -> &'static str {
        match self {
            Self::NoRoute => "the route is not one this socket answers",
            Self::NotRead => "the request was not a GET",
            Self::Unreadable => "the app id or the path could not be read",
            Self::NoDirectory => "no path was given",
            Self::NotHere => "this host runs no app by that id",
            Self::Stopped => "the app is stopped, so there is no guest to ask",
            Self::NotWoken => "the app could not be woken",
            Self::NoRoom => "the host has no memory left to wake the app",
            Self::GuestUnreachable => "no microVM is running for the app",
            Self::GuestSilent => "the guest took the request and never answered",
            Self::GuestRefused => "the guest would not list that directory",
            Self::GuestTooLarge => "more was asked of the guest than one request carries",
            Self::GuestMalformed => "the guest answered with bytes the host could not read",
        }
    }
}

impl Refusal {
    fn kind(&self) -> RefusalKind {
        match self {
            Self::NoRoute { .. } => RefusalKind::NoRoute,
            Self::NotRead { .. } => RefusalKind::NotRead,
            Self::Unreadable { .. } => RefusalKind::Unreadable,
            Self::NoDirectory => RefusalKind::NoDirectory,
            Self::NotHere { .. } => RefusalKind::NotHere,
            Self::Stopped { .. } => RefusalKind::Stopped,
            Self::NotWoken { .. } => RefusalKind::NotWoken,
            Self::NoRoom { .. } => RefusalKind::NoRoom,
            Self::Guest(error) => match error {
                GuestFilesystemError::Unreachable { .. } => RefusalKind::GuestUnreachable,
                GuestFilesystemError::Silent { .. } => RefusalKind::GuestSilent,
                GuestFilesystemError::Refused { .. } => RefusalKind::GuestRefused,
                GuestFilesystemError::TooLarge { .. } => RefusalKind::GuestTooLarge,
                GuestFilesystemError::Malformed { .. } => RefusalKind::GuestMalformed,
            },
        }
    }

    fn status(&self) -> StatusCode {
        self.kind().status()
    }
}

impl From<(AppId, WakeRefusal)> for Refusal {
    fn from((app_id, refusal): (AppId, WakeRefusal)) -> Self {
        match refusal {
            WakeRefusal::NoRoom { shortfall_mib } => Self::NoRoom {
                app_id,
                shortfall_mib,
            },
            WakeRefusal::Failed {
                kind: WakeFailure::NotOnRequest,
                ..
            } => Self::Stopped { app_id },
            WakeRefusal::Failed { reason, .. } => Self::NotWoken { app_id, reason },
        }
    }
}

/// The app and the directory a request names, or why it names neither.
fn asked_for<B>(request: &Request<B>) -> Result<(AppId, GuestPath), Refusal> {
    if request.method() != Method::GET {
        return Err(Refusal::NotRead {
            method: request.method().to_string(),
        });
    }
    let route = request.uri().path();
    let named = route
        .strip_prefix(APPS_PREFIX)
        .and_then(|rest| rest.strip_suffix(LISTING_SUFFIX))
        .ok_or_else(|| Refusal::NoRoute {
            route: route.to_string(),
        })?;
    let app_id = AppId::parse(named).map_err(|_| Refusal::Unreadable {
        value: named.to_string(),
        what: "an app id",
    })?;
    let asked = request
        .uri()
        .query()
        .and_then(|query| query.split('&').find_map(|pair| pair.strip_prefix(PATH_QUERY)))
        .ok_or(Refusal::NoDirectory)?;
    let decoded = percent_encoding::percent_decode_str(asked)
        .decode_utf8()
        .map_err(|_| Refusal::Unreadable {
            value: asked.to_string(),
            what: "text",
        })?;
    let path = GuestPath::parse(decoded.as_ref()).map_err(|_| Refusal::Unreadable {
        value: decoded.to_string(),
        what: "a path inside a guest",
    })?;
    Ok((app_id, path))
}

/// A listing reaches the guest the way a request does: an app that is asleep, or being written
/// out, is woken first, and asking counts as having been used — so the pass that decides what is
/// quiet does not snapshot a guest out from under the listing it is answering.
async fn list(
    host: &Arc<Host>,
    app_id: &AppId,
    path: &GuestPath,
) -> Result<protocol::DirectoryListing, Refusal> {
    let record = host.state.record(app_id).await.ok_or_else(|| Refusal::NotHere {
        app_id: app_id.clone(),
    })?;
    if !record.desired_running {
        return Err(Refusal::Stopped {
            app_id: app_id.clone(),
        });
    }
    host.state.mark_active(app_id, crate::clock::now_ms()).await;
    if record.is_idle() || host.state.is_snapshotting(app_id).await {
        host.waker
            .wake(app_id)
            .await
            .map_err(|refusal| Refusal::from((app_id.clone(), refusal)))?;
    }
    Ok(reader::list(host, app_id, path).await?)
}

async fn answer(host: &Arc<Host>, request: Request<Incoming>) -> Response<Full<bytes::Bytes>> {
    let listed = match asked_for(&request) {
        Ok((app_id, path)) => list(host, &app_id, &path).await,
        Err(refusal) => Err(refusal),
    };
    match listed {
        Ok(listing) => rendered(StatusCode::OK, &listing),
        Err(refusal) => {
            tracing::debug!(error = %refusal, "a listing was refused");
            rendered(
                refusal.status(),
                &Refused {
                    message: refusal.to_string(),
                },
            )
        }
    }
}

fn rendered<T: serde::Serialize>(status: StatusCode, body: &T) -> Response<Full<bytes::Bytes>> {
    let rendered = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(bytes::Bytes::from(rendered)))
        .expect("a rendered body is always a response")
}

/// Whoever may read the socket may list any app's files, so it is opened for this host's own
/// account and nobody else, and a socket an earlier daemon left behind is cleared rather than
/// refused — the path is this daemon's alone, and nothing else is ever bound to it.
fn bind(socket_path: &Path) -> std::io::Result<tokio::net::UnixListener> {
    if let Some(parent) = socket_path.parent() {
        crate::json_store::make_directory(parent, 0o700)?;
    }
    let _ = std::fs::remove_file(socket_path);
    let listener = tokio::net::UnixListener::bind(socket_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(SOCKET_MODE))?;
    }
    Ok(listener)
}

pub fn serve(host: &Arc<Host>, socket_path: PathBuf) {
    let host = host.clone();
    tokio::spawn(async move {
        let listener = match bind(&socket_path) {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(%error, socket = %socket_path.display(), "guest listings could not be served");
                return;
            }
        };
        tracing::info!(socket = %socket_path.display(), "guest listings are being served");
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let host = host.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| {
                    let host = host.clone();
                    async move { Ok::<_, std::convert::Infallible>(answer(&host, request).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
            });
        }
    });
}

/// This socket as an OpenAPI document, written from the route this file serves, the types its
/// answers carry, and [`RefusalKind::DECLARED`]. `just openapi` writes it and the docs site
/// renders its reference page from it, so the surface cannot be described wrongly by hand.
///
/// No `servers`: OpenAPI has no way to name a unix socket, and inventing a URL for one would be
/// the only untrue line in the file. Where the socket is put is `config.toml`'s to say.
#[cfg(feature = "schema")]
pub fn openapi() -> serde_json::Value {
    use std::collections::BTreeMap;

    let mut generator = schemars::generate::SchemaSettings::draft2020_12()
        .with(|settings| settings.definitions_path = COMPONENT_SCHEMAS.into())
        .into_generator();
    let listing = generator.subschema_for::<protocol::DirectoryListing>();
    let refused = generator.subschema_for::<Refused>();

    let mut answers: BTreeMap<u16, Vec<&'static str>> = BTreeMap::new();
    for kind in RefusalKind::DECLARED {
        answers
            .entry(kind.status().as_u16())
            .or_default()
            .push(kind.describes());
    }
    let mut responses = serde_json::Map::new();
    responses.insert(
        StatusCode::OK.as_u16().to_string(),
        body("What the directory holds.", &listing),
    );
    for (status, clauses) in answers {
        responses.insert(status.to_string(), body(&sentence(&clauses), &refused));
    }

    serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "nibrunner guest filesystem",
            "version": SOCKET_VERSION,
            "description": "What a guest holds, over the unix socket `[filesystem] socket` names.",
        },
        "paths": {
            format!("{APPS_PREFIX}{{appId}}{LISTING_SUFFIX}"): {
                "get": {
                    "summary": "List a directory inside an app's guest.",
                    "x-codeSamples": [example_call()],
                    "description": "A sleeping app is woken first, as a request reaching it would, \
                                    and asking counts as activity.",
                    "parameters": [
                        parameter(Parameter {
                            name: "appId",
                            located: "path",
                            describes: "The app to ask, as `desired.json` names it.",
                            schema: generator.subschema_for::<protocol::AppId>(),
                        }),
                        parameter(Parameter {
                            name: "path",
                            located: "query",
                            describes: "The directory to list, percent-encoded.",
                            schema: generator.subschema_for::<protocol::GuestPath>(),
                        }),
                    ],
                    "responses": responses,
                }
            }
        },
        "components": { "schemas": generator.take_definitions(true) },
    })
}

/// The one call that works, for a document that cannot say where it is served: a unix socket has
/// no URL, so anything generating one from a host and a path generates a lie. The socket is named
/// from the example configuration, so the two cannot disagree about where a host puts it.
#[cfg(feature = "schema")]
fn example_call() -> serde_json::Value {
    let socket = crate::config::HostConfig::example()
        .filesystem
        .map(|filesystem| filesystem.socket)
        .unwrap_or_default();
    serde_json::json!({
        "lang": "bash",
        "label": "curl",
        "source": format!(
            "curl --unix-socket {} \\\n  'http://localhost{APPS_PREFIX}app-7{LISTING_SUFFIX}?{PATH_QUERY}%2Fdata'",
            socket.display()
        ),
    })
}

/// Bumped when this socket answers differently than it did, which no release has yet needed.
#[cfg(feature = "schema")]
const SOCKET_VERSION: &str = "1";

#[cfg(feature = "schema")]
const COMPONENT_SCHEMAS: &str = "#/components/schemas/";

#[cfg(feature = "schema")]
struct Parameter {
    name: &'static str,
    located: &'static str,
    describes: &'static str,
    schema: schemars::Schema,
}

#[cfg(feature = "schema")]
fn parameter(parameter: Parameter) -> serde_json::Value {
    serde_json::json!({
        "name": parameter.name,
        "in": parameter.located,
        "required": true,
        "description": parameter.describes,
        "schema": parameter.schema,
    })
}

#[cfg(feature = "schema")]
fn body(describes: &str, schema: &schemars::Schema) -> serde_json::Value {
    serde_json::json!({
        "description": describes,
        "content": { "application/json": { "schema": schema } },
    })
}

/// The clauses one status is answered for, as one sentence a reference page can print.
#[cfg(feature = "schema")]
fn sentence(clauses: &[&str]) -> String {
    let mut said = clauses.join(", or ");
    said[..1].make_ascii_uppercase();
    said.push('.');
    said
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    fn asking(uri: &str) -> Request<()> {
        Request::builder()
            .uri(uri)
            .body(())
            .expect("a request built from a constant")
    }

    #[test]
    fn a_route_names_the_app_and_the_directory_it_asks_about() {
        let (app_id, path) = asked_for(&asking("/apps/app-1/files?path=%2Fdata")).unwrap();
        assert_eq!(app_id.as_str(), "app-1");
        assert_eq!(path.as_str(), "/data");
    }

    #[test]
    fn a_directory_whose_name_needs_escaping_arrives_as_it_was_written() {
        let (_, path) = asked_for(&asking(
            "/apps/app-1/files?path=%2Fdata%2Fmy%20notes%20%26%20drafts",
        ))
        .unwrap();
        assert_eq!(path.as_str(), "/data/my notes & drafts");
    }

    #[test]
    fn the_root_of_a_guest_is_a_directory_like_any_other() {
        let (_, path) = asked_for(&asking("/apps/app-1/files?path=%2F")).unwrap();
        assert_eq!(path.as_str(), "/");
    }

    #[test]
    fn a_request_that_is_not_a_listing_is_told_what_one_looks_like() {
        for route in ["/", "/apps/app-1", "/apps/app-1/files/data", "/metrics"] {
            let refusal = asked_for(&asking(route)).unwrap_err();
            assert_eq!(refusal.status(), StatusCode::NOT_FOUND, "{route}");
            assert!(refusal.to_string().contains("GET /apps/"), "{route}");
        }
    }

    #[test]
    fn a_listing_asked_for_with_no_directory_says_so_rather_than_guessing_at_one() {
        let refusal = asked_for(&asking("/apps/app-1/files")).unwrap_err();
        assert_eq!(refusal.status(), StatusCode::BAD_REQUEST);
        assert!(
            refusal.to_string().contains("which directory it wants"),
            "{refusal}"
        );
    }

    #[test]
    fn a_path_that_climbs_out_of_the_guest_is_refused_where_it_is_read() {
        for asked in ["%2Fdata%2F..%2F..%2Fetc", "relative", "%2Fdata%2F%22quoted%22"] {
            let refusal = asked_for(&asking(&format!("/apps/app-1/files?path={asked}"))).unwrap_err();
            assert_eq!(refusal.status(), StatusCode::BAD_REQUEST, "{asked}");
            assert!(
                refusal.to_string().contains("a path inside a guest"),
                "{asked}: {refusal}"
            );
        }
    }

    #[test]
    fn something_that_is_not_an_app_id_is_refused_before_this_host_is_asked_about_it() {
        for named in ["app.1", "_app", ""] {
            let refusal = asked_for(&asking(&format!("/apps/{named}/files?path=%2F"))).unwrap_err();
            assert_eq!(refusal.status(), StatusCode::BAD_REQUEST, "{named}");
            assert!(refusal.to_string().contains("an app id"), "{named}: {refusal}");
        }
    }

    #[test]
    fn a_listing_is_read_rather_than_written() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/apps/app-1/files?path=%2F")
            .body(())
            .unwrap();
        let refusal = asked_for(&request).unwrap_err();
        assert_eq!(refusal.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn an_app_this_host_does_not_run_is_named_rather_than_dialled() {
        let host = test_host().await;
        let refusal = list(host.arc(), &app_id(), &GuestPath::parse("/").unwrap())
            .await
            .unwrap_err();
        assert_eq!(refusal.status(), StatusCode::NOT_FOUND);
        assert!(refusal.to_string().contains(app_id().as_str()), "{refusal}");
    }

    #[tokio::test]
    async fn a_stopped_app_holds_no_guest_to_ask() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| record.desired_running = false))
            .await;
        let refusal = list(host.arc(), &app_id(), &GuestPath::parse("/").unwrap())
            .await
            .unwrap_err();
        assert_eq!(refusal.status(), StatusCode::CONFLICT);
        assert!(refusal.to_string().contains("stopped"), "{refusal}");
    }

    #[tokio::test]
    async fn asking_about_an_app_counts_as_having_used_it() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let _ = list(host.arc(), &app_id(), &GuestPath::parse("/").unwrap()).await;
        assert!(
            host.state
                .snapshot()
                .await
                .last_active_at_ms
                .contains_key(&app_id()),
            "a listing moves the app's last activity forward, so it is not slept on mid-answer"
        );
    }

    #[tokio::test]
    async fn a_guest_that_is_not_running_says_so_rather_than_answering_with_nothing() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let refusal = list(host.arc(), &app_id(), &GuestPath::parse("/").unwrap())
            .await
            .unwrap_err();
        assert_eq!(refusal.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(refusal.to_string().contains("no runtime is running"), "{refusal}");
    }

    async fn asked_over(socket_path: &Path, uri: &str) -> (StatusCode, serde_json::Value) {
        let stream = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let response = sender
            .send_request(
                Request::builder()
                    .uri(uri)
                    .header("host", "localhost")
                    .body(Full::<bytes::Bytes>::new(bytes::Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn a_listing_asked_for_over_the_socket_is_answered_over_it() {
        let host = test_host().await;
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("filesystem.sock");
        serve(host.arc(), socket_path.clone());
        for _ in 0..200 {
            if socket_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let (status, body) = asked_over(&socket_path, "/apps/app-1/files?path=%2Fdata").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            body["message"].as_str().unwrap().contains("no app called app-1"),
            "{body}"
        );

        let (status, body) = asked_over(&socket_path, "/metrics").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["message"].as_str().unwrap().contains("GET /apps/"), "{body}");
    }

    /// One of every refusal this socket can build, so the catalogue the document is written from
    /// can be held to them. A variant added without a kind does not compile; one added without a
    /// row here fails the test below.
    fn every_refusal() -> Vec<Refusal> {
        let app_id = app_id();
        let guest = |error: GuestFilesystemError| Refusal::Guest(error);
        vec![
            Refusal::NoRoute {
                route: "/".to_string(),
            },
            Refusal::NotRead {
                method: "POST".to_string(),
            },
            Refusal::Unreadable {
                value: String::new(),
                what: "an app id",
            },
            Refusal::NoDirectory,
            Refusal::NotHere {
                app_id: app_id.clone(),
            },
            Refusal::Stopped {
                app_id: app_id.clone(),
            },
            Refusal::NotWoken {
                app_id: app_id.clone(),
                reason: String::new(),
            },
            Refusal::NoRoom {
                app_id: app_id.clone(),
                shortfall_mib: 512,
            },
            guest(GuestFilesystemError::Unreachable {
                app_id: app_id.clone(),
            }),
            guest(GuestFilesystemError::Silent {
                app_id: app_id.clone(),
            }),
            guest(GuestFilesystemError::Refused {
                app_id: app_id.clone(),
                refusal: "no",
            }),
            guest(GuestFilesystemError::TooLarge { app_id }),
            guest(GuestFilesystemError::Malformed {
                reason: String::new(),
            }),
        ]
    }

    #[test]
    fn every_refusal_this_socket_can_answer_with_is_one_the_document_declares() {
        let built: std::collections::BTreeSet<RefusalKind> =
            every_refusal().iter().map(Refusal::kind).collect();
        let declared: std::collections::BTreeSet<RefusalKind> =
            RefusalKind::DECLARED.iter().copied().collect();
        assert_eq!(
            built, declared,
            "the catalogue and the refusals have drifted apart"
        );
    }

    #[test]
    fn no_refusal_is_described_twice_or_left_undescribed() {
        for kind in RefusalKind::DECLARED {
            assert!(!kind.describes().is_empty(), "{kind:?}");
        }
        let said: std::collections::BTreeSet<&str> = RefusalKind::DECLARED
            .iter()
            .map(|kind| kind.describes())
            .collect();
        assert_eq!(
            said.len(),
            RefusalKind::DECLARED.len(),
            "two kinds say the same thing"
        );
    }

    #[cfg(feature = "schema")]
    #[test]
    fn the_document_answers_for_every_status_this_socket_returns() {
        let document = openapi();
        let responses = &document["paths"]["/apps/{appId}/files"]["get"]["responses"];
        for refusal in every_refusal() {
            let status = refusal.status().as_u16().to_string();
            assert!(
                !responses[&status].is_null(),
                "{status} is answered but not documented: {refusal}"
            );
        }
        assert!(!responses["200"].is_null(), "a listing that worked is documented");
    }

    #[cfg(feature = "schema")]
    #[test]
    fn nothing_the_document_points_at_is_missing_from_it() {
        let document = openapi();
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("the document carries its schemas");
        let rendered = document.to_string();
        let mut referenced = rendered.split("\"#/components/schemas/").skip(1).peekable();
        assert!(referenced.peek().is_some(), "the document refers to its schemas");
        for tail in referenced {
            let name = tail.split('"').next().unwrap_or_default();
            assert!(schemas.contains_key(name), "{name} is pointed at but not carried");
        }
    }

    #[tokio::test]
    async fn a_socket_an_earlier_daemon_left_behind_is_bound_over() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("nested").join("filesystem.sock");
        drop(bind(&socket_path).unwrap());
        assert!(socket_path.exists());
        let listener = bind(&socket_path).unwrap();
        assert!(listener.local_addr().is_ok());

        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&socket_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, SOCKET_MODE, "only this host's own account may ask");
    }
}
