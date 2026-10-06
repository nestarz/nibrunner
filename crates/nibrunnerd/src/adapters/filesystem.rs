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

impl Refusal {
    fn status(&self) -> StatusCode {
        match self {
            Self::NoRoute { .. } => StatusCode::NOT_FOUND,
            Self::NotRead { .. } => StatusCode::METHOD_NOT_ALLOWED,
            Self::Unreadable { .. } | Self::NoDirectory => StatusCode::BAD_REQUEST,
            Self::NotHere { .. } => StatusCode::NOT_FOUND,
            Self::Stopped { .. } => StatusCode::CONFLICT,
            Self::NotWoken { .. } | Self::NoRoom { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Guest(error) => match error {
                GuestFilesystemError::Unreachable { .. } => StatusCode::SERVICE_UNAVAILABLE,
                GuestFilesystemError::Silent { .. } => StatusCode::GATEWAY_TIMEOUT,
                GuestFilesystemError::Refused { .. } => StatusCode::FORBIDDEN,
                GuestFilesystemError::TooLarge { .. } => StatusCode::BAD_REQUEST,
                GuestFilesystemError::Malformed { .. } => StatusCode::BAD_GATEWAY,
            },
        }
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
    if record.needs_wake() || host.state.is_snapshotting(app_id).await {
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
                &serde_json::json!({ "message": refusal.to_string() }),
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
