use protocol::{
    AppId, HostCapacity, HostId, HostReportedState, HostState, HostVersions, ReportedCheckpoint,
    ReportedExport, ReportedInstance, ReportedVolume, Revision, Sha256Digest, StateMessage, Timestamp,
};
use std::collections::BTreeMap;

use crate::domain::report::InstanceRecord;

pub fn to_reported_instance(record: &InstanceRecord, last_active_at_ms: Option<i64>) -> ReportedInstance {
    ReportedInstance {
        app_id: record.app_id.clone(),
        deployment_id: record.deployment_id.clone(),
        state: record.state,
        host_port: Some(record.host_port),
        guest_ipv4: Some(record.guest_ipv4.clone()),
        layer_digests: record.layer_digests.clone(),
        restart_count: record.restart_count,
        last_restart: record.last_restart.clone(),
        started_at: record.started_at.clone(),
        converged_at: record.converged_at.clone(),
        last_active_at: last_active_at_ms.map(Timestamp::from_epoch_ms),
        expired_at: record.expired_at_ms.map(Timestamp::from_epoch_ms),
        last_exit_code: record.last_exit_code,
        message: record.message.clone(),
    }
}

pub struct ReportInputs<'a> {
    pub host_id: HostId,
    pub reported_at: Timestamp,
    pub state: HostState,
    pub capacity: HostCapacity,
    pub allocatable: HostCapacity,
    pub versions: HostVersions,
    pub records: &'a [InstanceRecord],
    pub last_active_at_ms: &'a BTreeMap<AppId, i64>,
    pub volumes: Vec<ReportedVolume>,
    pub checkpoints: Vec<ReportedCheckpoint>,
    pub exports: Vec<ReportedExport>,
    pub accepted_digest: Option<Sha256Digest>,
    pub accepted_revision: Option<Revision>,
    pub message: Option<StateMessage>,
}

pub fn build_reported_state(inputs: ReportInputs<'_>) -> HostReportedState {
    HostReportedState {
        host_id: inputs.host_id,
        reported_at: inputs.reported_at,
        state: inputs.state,
        capacity: inputs.capacity,
        allocatable: inputs.allocatable,
        versions: inputs.versions,
        volumes: inputs.volumes,
        instances: inputs
            .records
            .iter()
            .map(|record| to_reported_instance(record, inputs.last_active_at_ms.get(&record.app_id).copied()))
            .collect(),
        checkpoints: inputs.checkpoints,
        exports: inputs.exports,
        accepted_digest: inputs.accepted_digest,
        accepted_revision: inputs.accepted_revision,
        message: inputs.message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use protocol::AppId;

    fn assembled(
        records: &[InstanceRecord],
        checkpoints: Vec<ReportedCheckpoint>,
        exports: Vec<ReportedExport>,
    ) -> HostReportedState {
        let capacity = HostCapacity {
            vcpu_count: 4,
            memory_mib: 8192,
            cache_bytes: 1000,
        };
        build_reported_state(ReportInputs {
            host_id: host_id(),
            reported_at: observed_at(),
            state: HostState::Ready,
            capacity,
            allocatable: capacity,
            versions: HostVersions {
                agent: "sha".into(),
                guest_image: "6.1".into(),
                zerofs: "none".into(),
                firecracker: "v1.16.1".into(),
            },
            records,
            last_active_at_ms: &BTreeMap::new(),
            volumes: vec![],
            checkpoints,
            exports,
            accepted_digest: None,
            accepted_revision: None,
            message: None,
        })
    }

    #[test]
    fn the_report_always_names_the_host_side_port_and_omits_what_it_does_not_know() {
        let instance = to_reported_instance(&instance_record(|_| {}), None);
        let written = serde_json::to_value(&instance).unwrap();
        assert_eq!(written["hostPort"], u32::from(instance.host_port.unwrap()));
        for absent in ["startedAt", "lastExitCode", "message"] {
            assert!(written.get(absent).is_none(), "{absent} should be absent");
        }
        let exited = to_reported_instance(&instance_record(|record| record.last_exit_code = Some(0)), None);
        assert_eq!(serde_json::to_value(&exited).unwrap()["lastExitCode"], 0);
    }

    #[test]
    fn every_record_the_host_holds_reaches_the_report_in_the_order_it_was_given() {
        let records: Vec<InstanceRecord> = ["app-one", "app-two", "app-three"]
            .iter()
            .map(|name| instance_record(|record| record.app_id = AppId::parse(*name).unwrap()))
            .collect();
        let report = assembled(&records, vec![], vec![]);
        assert_eq!(
            report
                .instances
                .iter()
                .map(|instance| instance.app_id.as_str().to_string())
                .collect::<Vec<_>>(),
            vec![
                "app-one".to_string(),
                "app-two".to_string(),
                "app-three".to_string()
            ]
        );
        assert!(assembled(&[], vec![], vec![]).instances.is_empty());
    }

    #[test]
    fn what_the_host_observed_of_its_checkpoints_and_exports_is_passed_on_untouched() {
        let checkpoint = ReportedCheckpoint {
            checkpoint_id: checkpoint_id(),
            volume_id: volume_id(),
            state: protocol::CheckpointState::Ready,
            reference: None,
            ready_at: Some(observed_at()),
            message: None,
        };
        let export = ReportedExport {
            export_id: export_id(),
            checkpoint_id: Some(checkpoint_id()),
            state: protocol::ExportState::Failed,
            size_bytes: None,
            ready_at: None,
            message: Some(protocol::StateMessage::new(
                "the volume would not freeze".to_string(),
            )),
        };
        let report = assembled(&[], vec![checkpoint.clone()], vec![export.clone()]);
        assert_eq!(report.checkpoints, vec![checkpoint]);
        assert_eq!(report.exports, vec![export]);
    }

    #[test]
    fn the_assembled_report_is_the_document_it_will_be_sent_as() {
        let report = assembled(&[instance_record(|_| {})], vec![], vec![]);
        let written = serde_json::to_value(&report).unwrap();
        let parsed: HostReportedState = serde_json::from_value(written).unwrap();
        assert_eq!(parsed, report);
    }
}
