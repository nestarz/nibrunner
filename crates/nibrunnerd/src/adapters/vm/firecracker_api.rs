use std::path::{Path, PathBuf};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde::Serialize;

use crate::ports::VmError;

const VM_PATH: &str = "/vm";
const SNAPSHOT_CREATE_PATH: &str = "/snapshot/create";
const SNAPSHOT_LOAD_PATH: &str = "/snapshot/load";

const MAX_DETAIL_LENGTH: usize = 200;

const CALL_TIMEOUT: Duration = Duration::from_secs(60);

const BIND_POLL_INTERVAL: Duration = Duration::from_millis(2);
const BIND_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Serialize)]
struct VmState {
    state: &'static str,
}

#[derive(Serialize)]
struct CreateSnapshot<'a> {
    snapshot_path: &'a str,
    mem_file_path: &'a str,
    snapshot_type: &'static str,
}

#[derive(Serialize)]
struct MemBackend<'a> {
    backend_type: &'static str,
    backend_path: &'a str,
}

#[derive(Serialize)]
struct LoadSnapshot<'a> {
    snapshot_path: &'a str,
    mem_backend: MemBackend<'a>,
    clock_realtime: bool,
}

pub struct FirecrackerApi {
    socket_path: PathBuf,
}

impl FirecrackerApi {
    pub fn at(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    async fn call<B: Serialize>(&self, method: Method, path: &str, body: &B) -> Result<(), VmError> {
        let unreachable = |reason: String| VmError::Unreachable {
            socket_path: self.socket_path.display().to_string(),
            reason,
        };
        let rendered = serde_json::to_vec(body).map_err(|error| unreachable(error.to_string()))?;
        let stream = tokio::net::UnixStream::connect(&self.socket_path)
            .await
            .map_err(|error| unreachable(error.to_string()))?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|error| unreachable(error.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Full::new(bytes::Bytes::from(rendered)))
            .map_err(|error| unreachable(error.to_string()))?;
        let response = tokio::time::timeout(CALL_TIMEOUT, sender.send_request(request))
            .await
            .map_err(|_| {
                unreachable(format!(
                    "{path} did not answer within {}s",
                    CALL_TIMEOUT.as_secs()
                ))
            })?
            .map_err(|error| unreachable(error.to_string()))?;
        let status = response.status();
        if status == hyper::StatusCode::NO_CONTENT {
            return Ok(());
        }
        let detail = response
            .into_body()
            .collect()
            .await
            .map(|body| String::from_utf8_lossy(&body.to_bytes()).trim().to_string())
            .unwrap_or_default();
        Err(VmError::Rejected {
            path: path.to_string(),
            status: status.as_u16(),
            detail: protocol::truncate_chars(detail, MAX_DETAIL_LENGTH),
        })
    }

    async fn until_bound<B: Serialize>(&self, method: Method, path: &str, body: &B) -> Result<(), VmError> {
        let deadline = std::time::Instant::now() + BIND_TIMEOUT;
        loop {
            match self.call(method.clone(), path, body).await {
                Err(VmError::Unreachable { .. }) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(BIND_POLL_INTERVAL).await;
                }
                outcome => return outcome,
            }
        }
    }

    pub async fn pause(&self) -> Result<(), VmError> {
        self.call(Method::PATCH, VM_PATH, &VmState { state: "Paused" })
            .await
    }

    pub async fn resume(&self) -> Result<(), VmError> {
        self.call(Method::PATCH, VM_PATH, &VmState { state: "Resumed" })
            .await
    }

    pub async fn create_snapshot(&self, state_path: &Path, memory_path: &Path) -> Result<(), VmError> {
        self.call(
            Method::PUT,
            SNAPSHOT_CREATE_PATH,
            &CreateSnapshot {
                snapshot_path: &state_path.display().to_string(),
                mem_file_path: &memory_path.display().to_string(),
                snapshot_type: "Full",
            },
        )
        .await
    }

    pub async fn load_snapshot(&self, state_path: &Path, memory_path: &Path) -> Result<(), VmError> {
        self.until_bound(
            Method::PUT,
            SNAPSHOT_LOAD_PATH,
            &LoadSnapshot {
                snapshot_path: &state_path.display().to_string(),
                mem_backend: MemBackend {
                    backend_type: "File",
                    backend_path: &memory_path.display().to_string(),
                },
                clock_realtime: false,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct FakeVmm {
        seen: Arc<Mutex<Vec<(String, String, String)>>>,
        status: hyper::StatusCode,
        body: &'static str,
    }

    impl FakeVmm {
        async fn listening(
            directory: &Path,
            status: hyper::StatusCode,
            body: &'static str,
        ) -> (PathBuf, Arc<Mutex<Vec<(String, String, String)>>>) {
            let socket_path = directory.join("firecracker.sock");
            let seen = Arc::new(Mutex::new(Vec::new()));
            let vmm = FakeVmm {
                seen: seen.clone(),
                status,
                body,
            };
            let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let seen = vmm.seen.clone();
                    let status = vmm.status;
                    let body = vmm.body;
                    tokio::spawn(async move {
                        let service = hyper::service::service_fn(
                            move |request: hyper::Request<hyper::body::Incoming>| {
                                let seen = seen.clone();
                                async move {
                                    let method = request.method().to_string();
                                    let path = request.uri().path().to_string();
                                    let payload = request
                                        .into_body()
                                        .collect()
                                        .await
                                        .map(|body| String::from_utf8_lossy(&body.to_bytes()).into_owned())
                                        .unwrap_or_default();
                                    seen.lock().unwrap().push((method, path, payload));
                                    Ok::<_, std::convert::Infallible>(
                                        hyper::Response::builder()
                                            .status(status)
                                            .body(Full::new(bytes::Bytes::from(body)))
                                            .unwrap(),
                                    )
                                }
                            },
                        );
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
            });
            (socket_path, seen)
        }
    }

    #[tokio::test]
    async fn pausing_and_resuming_are_both_a_patch_on_the_vm_path() {
        let directory = tempfile::tempdir().unwrap();
        let (socket_path, seen) =
            FakeVmm::listening(directory.path(), hyper::StatusCode::NO_CONTENT, "").await;
        let api = FirecrackerApi::at(&socket_path);
        api.pause().await.unwrap();
        api.resume().await.unwrap();
        let calls = seen.lock().unwrap().clone();
        assert_eq!(
            calls[0],
            ("PATCH".into(), "/vm".into(), r#"{"state":"Paused"}"#.into())
        );
        assert_eq!(
            calls[1],
            ("PATCH".into(), "/vm".into(), r#"{"state":"Resumed"}"#.into())
        );
    }

    #[tokio::test]
    async fn a_snapshot_is_created_full_and_loaded_through_mem_backend_for_guest_clock_sync() {
        let directory = tempfile::tempdir().unwrap();
        let (socket_path, seen) =
            FakeVmm::listening(directory.path(), hyper::StatusCode::NO_CONTENT, "").await;
        let api = FirecrackerApi::at(&socket_path);
        api.create_snapshot(Path::new("/snap/vmstate"), Path::new("/snap/memory"))
            .await
            .unwrap();
        api.load_snapshot(Path::new("/snap/vmstate"), Path::new("/snap/memory"))
            .await
            .unwrap();
        let calls = seen.lock().unwrap().clone();
        assert_eq!(calls[0].0, "PUT");
        assert_eq!(calls[0].1, "/snapshot/create");
        assert_eq!(
            calls[0].2,
            r#"{"snapshot_path":"/snap/vmstate","mem_file_path":"/snap/memory","snapshot_type":"Full"}"#
        );
        assert_eq!(calls[1].1, "/snapshot/load");
        assert!(!calls[1].2.contains("mem_file_path"));
        assert_eq!(
            calls[1].2,
            r#"{"snapshot_path":"/snap/vmstate","mem_backend":{"backend_type":"File","backend_path":"/snap/memory"},"clock_realtime":false}"#
        );
    }

    #[tokio::test]
    async fn a_refusal_carries_the_account_the_vmm_gave_of_itself() {
        let directory = tempfile::tempdir().unwrap();
        let (socket_path, _) = FakeVmm::listening(
            directory.path(),
            hyper::StatusCode::BAD_REQUEST,
            r#"{"fault_message":"Load snapshot error: Cannot restore"}"#,
        )
        .await;
        let error = FirecrackerApi::at(&socket_path).pause().await.unwrap_err();
        match error {
            VmError::Rejected { path, status, detail } => {
                assert_eq!(path, "/vm");
                assert_eq!(status, 400);
                assert!(detail.contains("Cannot restore"));
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_socket_nothing_answers_is_unreachable_rather_than_a_refusal() {
        let directory = tempfile::tempdir().unwrap();
        let api = FirecrackerApi::at(directory.path().join("absent.sock"));
        assert!(matches!(api.pause().await, Err(VmError::Unreachable { .. })));
    }

    #[tokio::test]
    async fn anything_but_no_content_is_read_as_a_refusal_rather_than_as_success() {
        let directory = tempfile::tempdir().unwrap();
        let (socket_path, _) =
            FakeVmm::listening(directory.path(), hyper::StatusCode::OK, "not what was asked").await;
        let error = FirecrackerApi::at(&socket_path).resume().await.unwrap_err();
        assert!(matches!(error, VmError::Rejected { status: 200, .. }), "{error}");
    }

    const A_LONG_FAULT: &str = concat!(
        "the hypervisor described its own state at length ",
        "the hypervisor described its own state at length ",
        "the hypervisor described its own state at length ",
        "the hypervisor described its own state at length ",
        "the hypervisor described its own state at length "
    );

    #[tokio::test]
    async fn a_vmm_that_says_a_great_deal_about_itself_is_cut_down_before_it_reaches_a_report() {
        assert!(A_LONG_FAULT.len() > MAX_DETAIL_LENGTH);
        let directory = tempfile::tempdir().unwrap();
        let (socket_path, _) =
            FakeVmm::listening(directory.path(), hyper::StatusCode::BAD_REQUEST, A_LONG_FAULT).await;
        let error = FirecrackerApi::at(&socket_path).pause().await.unwrap_err();
        match error {
            VmError::Rejected { detail, .. } => {
                assert_eq!(detail.chars().count(), MAX_DETAIL_LENGTH);
                assert!(A_LONG_FAULT.starts_with(&detail));
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_load_waits_for_the_hypervisor_to_bind_rather_than_racing_it() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("firecracker.sock");
        let binding = tokio::spawn({
            let directory = directory.path().to_path_buf();
            async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                FakeVmm::listening(&directory, hyper::StatusCode::NO_CONTENT, "").await
            }
        });
        FirecrackerApi::at(&socket_path)
            .load_snapshot(Path::new("/snap/vmstate"), Path::new("/snap/memory"))
            .await
            .unwrap();
        let (_, seen) = binding.await.unwrap();
        assert_eq!(seen.lock().unwrap()[0].1, "/snapshot/load");
    }

    #[tokio::test]
    async fn a_load_a_bound_hypervisor_refuses_is_handed_up_rather_than_retried_until_it_times_out() {
        let directory = tempfile::tempdir().unwrap();
        let (socket_path, seen) = FakeVmm::listening(
            directory.path(),
            hyper::StatusCode::BAD_REQUEST,
            r#"{"fault_message":"Cannot restore"}"#,
        )
        .await;
        let started = std::time::Instant::now();
        let error = FirecrackerApi::at(&socket_path)
            .load_snapshot(Path::new("/snap/vmstate"), Path::new("/snap/memory"))
            .await
            .unwrap_err();
        assert!(matches!(error, VmError::Rejected { .. }), "{error}");
        assert!(started.elapsed() < BIND_TIMEOUT);
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_socket_this_host_names_is_the_one_it_says_it_could_not_reach() {
        let directory = tempfile::tempdir().unwrap();
        let absent = directory.path().join("absent.sock");
        let error = FirecrackerApi::at(&absent).pause().await.unwrap_err();
        assert!(error.message().contains(&absent.display().to_string()), "{error}");
    }
}
