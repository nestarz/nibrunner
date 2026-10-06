use std::path::Path;

use protocol::{HostCapacity, HostId, HostReportedState, HostState, HostVersions};

use crate::clock::now_timestamp;
use crate::domain::report::build_report::{build_reported_state, ReportInputs};
use crate::domain::report::capacity::{
    allocatable_capacity, committed_resources, read_filesystem_space, read_vcpu_count,
};
use crate::host::Host;

pub async fn host_id_of(host: &Host) -> HostId {
    host.known_host_id()
        .await
        .unwrap_or_else(|| HostId::parse("host-local").expect("a constant identifier"))
}

pub async fn build(host: &Host, versions: HostVersions) -> HostReportedState {
    let snapshot = host.state.snapshot().await;
    let records: Vec<_> = snapshot.records.values().cloned().collect();
    let space = read_filesystem_space(&host.config.state_dir).unwrap_or_default();
    let capacity = HostCapacity {
        vcpu_count: read_vcpu_count(),
        memory_mib: host.guest_memory_mib,
        cache_bytes: space.total_bytes,
    };
    let allocatable = allocatable_capacity(&capacity, &committed_resources(&records), space.available_bytes);

    let mut report = build_reported_state(ReportInputs {
        host_id: host_id_of(host).await,
        reported_at: now_timestamp(),
        state: if snapshot.converged {
            HostState::Ready
        } else {
            HostState::Registering
        },
        capacity,
        allocatable,
        versions,
        records: &records,
        last_active_at_ms: &snapshot.last_active_at_ms,
        volumes: snapshot.volume_reports.clone(),
        checkpoints: snapshot.checkpoint_reports.clone(),
        exports: snapshot.export_reports.clone(),
        accepted_digest: snapshot.accepted_digest.clone(),
        accepted_revision: snapshot.accepted_revision.clone(),
        message: snapshot.desired_refusal.clone(),
    });
    let ids = records
        .iter()
        .map(|record| record.app_id.clone())
        .collect::<Vec<_>>();
    let mut memory = host.vms.memory(&ids).await;
    for instance in &mut report.instances {
        instance.memory = memory.remove(&instance.app_id);
    }
    report
}

/// Whether a report says what the one before it said.
///
/// `reportedAt` moves every time a report is built, and the capacity figures are readings of a
/// disk that moves under every write — this host's own report and logs included — so neither is
/// a change worth waking a watcher for. Everything else counts, so a field added to the report
/// is compared without being remembered here. What this excuses is carried by the heartbeat the
/// reporter writes on anyway, which is why free space can be up to that far behind.
pub fn says_the_same(held: &HostReportedState, built: &HostReportedState) -> bool {
    let mut instances = built.instances.clone();
    for instance in &mut instances {
        if let Some(previous) = held
            .instances
            .iter()
            .find(|previous| previous.app_id == instance.app_id)
        {
            if let (Some(memory), Some(previous)) = (&mut instance.memory, &previous.memory) {
                let oom_kills = memory.oom_kills;
                *memory = previous.clone();
                memory.oom_kills = oom_kills;
            }
        }
    }
    *held
        == HostReportedState {
            reported_at: held.reported_at.clone(),
            capacity: held.capacity,
            allocatable: held.allocatable,
            instances,
            ..built.clone()
        }
}

pub fn write(path: &Path, report: &HostReportedState) -> bool {
    match crate::json_store::write_json(path, report) {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(error = %error.message(), "this host could not write down what it observed");
            false
        }
    }
}

pub fn reported_state_file(host: &Host) -> std::path::PathBuf {
    host.config.in_state_dir("reported.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use protocol::InstanceState;

    fn versions() -> HostVersions {
        crate::domain::report::versions::compiled_versions("v1.16.1", "6.1.180-test")
    }

    #[tokio::test]
    async fn memory_samples_use_the_heartbeat_but_a_new_oom_is_reported_immediately() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let mut previous = build(&host, versions()).await;
        previous.instances[0].memory = Some(protocol::ReportedMemory {
            cgroup: None,
            proportional_set_bytes: None,
            anonymous_set_bytes: None,
            limits: None,
            measured_at: now_timestamp(),
            current_bytes: 256 << 20,
            peak_bytes: Some(300 << 20),
            swap_bytes: 0,
            high_events: 0,
            oom_kills: 0,
            pressure_some_us: 0,
            pressure_full_us: 0,
        });
        let mut next = previous.clone();
        let memory = next.instances[0].memory.as_mut().unwrap();
        memory.current_bytes += 4096;
        memory.pressure_some_us += 100;
        assert!(says_the_same(&previous, &next));
        next.instances[0].memory.as_mut().unwrap().oom_kills += 1;
        assert!(!says_the_same(&previous, &next));
        next.instances[0].memory = None;
        assert!(!says_the_same(&previous, &next));
    }

    #[tokio::test]
    async fn a_report_says_what_the_host_holds_and_what_is_left_of_it() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        host.state.modify(|snapshot| snapshot.converged = true).await;

        let report = build(&host, versions()).await;
        assert_eq!(report.state, HostState::Ready);
        assert_eq!(report.instances.len(), 1);
        assert_eq!(report.instances[0].app_id, app_id());
        assert_eq!(report.instances[0].state, InstanceState::Running);
        assert_eq!(report.capacity.memory_mib, host.guest_memory_mib);
        assert_eq!(
            report.allocatable.memory_mib,
            host.guest_memory_mib - u64::from(protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib)
        );
    }

    #[tokio::test]
    async fn a_refused_document_this_host_is_still_holding_is_named_at_the_top_of_the_report() {
        let host = test_host().await;
        host.state
            .modify(|snapshot| {
                snapshot.desired_refusal = Some(protocol::StateMessage::new(
                    "desired.json holds a document this host cannot read: missing field `instances`",
                ))
            })
            .await;

        let message = build(&host, versions())
            .await
            .message
            .expect("the refusal reaches the report");
        assert!(message.as_str().contains("missing field `instances`"));
    }

    #[tokio::test]
    async fn a_report_says_which_document_this_host_took_up() {
        let host = test_host().await;
        assert_eq!(
            build(&host, versions()).await.accepted_digest,
            None,
            "a host that has been handed nothing is on no document"
        );

        let document = accepted_document(desired_state(|state| {
            state.revision = protocol::Revision::parse("deploy-4821").unwrap();
        }));
        host.remember_accepted_document(&document).await;
        let report = build(&host, versions()).await;
        assert_eq!(report.accepted_digest, Some(document.digest));
        assert_eq!(
            report.accepted_revision,
            Some(protocol::Revision::parse("deploy-4821").unwrap()),
            "what whoever wrote the document called it is handed back to them"
        );
    }

    #[tokio::test]
    async fn a_host_with_no_id_of_its_own_still_has_one_to_report_under() {
        let host = test_host().await;
        assert_eq!(host_id_of(&host).await.as_str(), "host-local");
        host.remember_host_id("host-7").await;
        assert_eq!(host_id_of(&host).await.as_str(), "host-7");

        host.remember_host_id("host-9").await;
        assert_eq!(host_id_of(&host).await.as_str(), "host-7");
    }

    #[tokio::test]
    async fn what_is_written_is_the_document_it_will_be_read_as() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let report = build(&host, versions()).await;
        let path = reported_state_file(&host);
        assert!(write(&path, &report));
        let read_back: HostReportedState = crate::json_store::read_json(&path)
            .unwrap()
            .expect("the report is there");
        assert_eq!(read_back, report);
        assert_eq!(read_back.state, HostState::Registering);
    }

    #[tokio::test]
    async fn a_report_that_cannot_be_written_is_logged_rather_than_thrown() {
        let host = test_host().await;
        let path = reported_state_file(&host);
        std::fs::create_dir_all(&path).unwrap();
        assert!(!write(&path, &build(&host, versions()).await));
        assert!(path.is_dir());
    }
}
