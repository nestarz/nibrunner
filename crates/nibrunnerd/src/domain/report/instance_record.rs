use protocol::{
    AppHostname, AppId, DeploymentId, GuestPort, HealthCheck, HostPort, HttpPort, InstanceResources,
    InstanceState, Ipv4Address, PortName, ReportedRestart, RestartPolicy, Sha256Digest, StateMessage,
    Timestamp, VolumeId, DEFAULT_RESTART_POLICY,
};
use serde::{Deserialize, Serialize};

use crate::domain::backoff::{AttemptWindow, NO_START_ATTEMPTS};
use crate::domain::health::{GraceInputs, HealthTracker};

/// A port beyond the HTTP one, and the host port the slot handed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordPort {
    pub name: PortName,
    pub host_port: HostPort,
    pub guest_port: GuestPort,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceRecord {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub volume_id: VolumeId,
    pub hostnames: Vec<AppHostname>,
    pub host_port: HostPort,
    pub http_port: HttpPort,
    /// Everything this app answers on besides `http_port`. Absent is every record written
    /// before an app could ask for a second port.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<RecordPort>,
    pub guest_ipv4: Ipv4Address,
    pub layer_digests: Vec<Sha256Digest>,
    pub state: InstanceState,
    pub health: HealthTracker,
    pub health_check: HealthCheck,
    pub resources: InstanceResources,
    /// The policy the status loop judges an exit by. Absent is a record written before the status
    /// loop needed it; it reads as the default until the first pass copies the document's.
    #[serde(default = "default_restart_policy")]
    pub restart_policy: RestartPolicy,
    pub desired_running: bool,
    pub on_request: bool,
    #[serde(default)]
    pub expiry: Option<protocol::ExpiryPolicy>,
    #[serde(default)]
    pub expiry_since_ms: Option<i64>,
    #[serde(default)]
    pub expired_at_ms: Option<i64>,
    #[serde(default)]
    pub start_attempts: AttemptWindow,
    /// Restarts of the tenant by the supervisor inside its guest, since the host last booted the
    /// app afresh: a cold boot starts the count over, a restore keeps it. The host's own boots
    /// are `start_attempts`.
    pub restart_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_restart: Option<ReportedRestart>,
    #[serde(default)]
    pub stop_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converged_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<StateMessage>,
}

fn default_restart_policy() -> RestartPolicy {
    DEFAULT_RESTART_POLICY
}

#[derive(Debug, Clone)]
pub struct RecordFields {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub volume_id: VolumeId,
    pub hostnames: Vec<AppHostname>,
    pub host_port: HostPort,
    pub http_port: HttpPort,
    pub ports: Vec<RecordPort>,
    pub guest_ipv4: Ipv4Address,
    pub layer_digests: Vec<Sha256Digest>,
    pub health_check: HealthCheck,
    pub resources: InstanceResources,
    pub restart_policy: RestartPolicy,
    pub desired_running: bool,
    pub on_request: bool,
    pub expiry: Option<protocol::ExpiryPolicy>,
}

impl InstanceRecord {
    pub fn new(fields: RecordFields, state: InstanceState, health: HealthTracker) -> Self {
        Self {
            app_id: fields.app_id,
            deployment_id: fields.deployment_id,
            volume_id: fields.volume_id,
            hostnames: fields.hostnames,
            host_port: fields.host_port,
            http_port: fields.http_port,
            ports: fields.ports,
            guest_ipv4: fields.guest_ipv4,
            layer_digests: fields.layer_digests,
            state,
            health,
            health_check: fields.health_check,
            resources: fields.resources,
            restart_policy: fields.restart_policy,
            desired_running: fields.desired_running,
            on_request: fields.on_request,
            expiry: fields.expiry,
            expiry_since_ms: fields.expiry.map(|_| crate::clock::now_ms()),
            expired_at_ms: None,
            start_attempts: NO_START_ATTEMPTS,
            restart_count: 0,
            last_restart: None,
            stop_requested: false,
            started_at: None,
            converged_at: None,
            last_exit_code: None,
            message: None,
        }
    }

    pub fn adopt(&mut self, fields: RecordFields) {
        self.apply_expiry(fields.expiry, &fields.deployment_id, crate::clock::now_ms());
        self.deployment_id = fields.deployment_id;
        self.volume_id = fields.volume_id;
        self.hostnames = fields.hostnames;
        self.host_port = fields.host_port;
        self.http_port = fields.http_port;
        self.ports = fields.ports;
        self.guest_ipv4 = fields.guest_ipv4;
        self.layer_digests = fields.layer_digests;
        self.health_check = fields.health_check;
        self.resources = fields.resources;
        self.restart_policy = fields.restart_policy;
        self.desired_running = fields.desired_running;
        self.on_request = fields.on_request;
    }

    pub fn apply_expiry(
        &mut self,
        policy: Option<protocol::ExpiryPolicy>,
        deployment: &DeploymentId,
        now: i64,
    ) {
        let changed = self.deployment_id != *deployment;
        if changed || policy.is_none() {
            self.expiry_since_ms = None;
            self.expired_at_ms = None;
            if self.state == InstanceState::Expired {
                self.state = InstanceState::Idle;
            }
        }
        if policy.is_some() && self.expiry_since_ms.is_none() {
            self.expiry_since_ms = Some(now);
        }
        self.expiry = policy;
    }

    pub fn is_idle(&self) -> bool {
        self.state == InstanceState::Idle
    }

    pub fn needs_wake(&self) -> bool {
        matches!(self.state, InstanceState::Idle | InstanceState::Frozen)
    }

    pub fn grace_inputs(&self, now_ms: i64) -> GraceInputs<'_> {
        GraceInputs {
            health_check: &self.health_check,
            started_at_ms: self.started_at.as_ref().map(protocol::Timestamp::epoch_ms),
            now_ms,
        }
    }
}

pub fn read_instance_records(value: Option<serde_json::Value>) -> Vec<InstanceRecord> {
    let Some(serde_json::Value::Array(entries)) = value else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter_map(|entry| serde_json::from_value(entry).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{instance_record, record_fields};

    const REDEPLOYED_AT_MS: i64 = 1_760_000_000_000;

    fn redeployed() -> RecordFields {
        let mut fields = record_fields();
        fields.deployment_id = DeploymentId::parse("dep-2").unwrap();
        fields.volume_id = VolumeId::parse("vol-2").unwrap();
        fields.hostnames = vec![];
        fields.host_port = HostPort::new(23_456).unwrap();
        fields.http_port = HttpPort::new(9_001).unwrap();
        fields.guest_ipv4 = Ipv4Address::parse("10.9.9.9").unwrap();
        fields.layer_digests = vec![Sha256Digest::parse("a".repeat(64)).unwrap()];
        fields.resources = InstanceResources {
            vcpu_count: 4,
            memory_mib: 4_096,
        };
        fields.restart_policy = RestartPolicy {
            max_restarts: 9,
            reset_after_ms: 5_000,
            ..DEFAULT_RESTART_POLICY
        };
        fields.desired_running = false;
        fields.on_request = true;
        fields
    }

    #[test]
    fn a_record_round_trips_through_the_notes_this_daemon_writes() {
        let record = instance_record(|_| {});
        let written = serde_json::to_value(vec![record.clone()]).unwrap();
        assert_eq!(read_instance_records(Some(written)), vec![record]);
    }

    #[test]
    fn a_note_missing_a_field_this_daemon_needs_is_discarded_rather_than_guessed_at() {
        let mut written = serde_json::to_value(instance_record(|_| {})).unwrap();
        written.as_object_mut().unwrap().remove("httpPort");
        assert_eq!(
            read_instance_records(Some(serde_json::Value::Array(vec![written]))),
            vec![]
        );
        assert_eq!(read_instance_records(None), vec![]);
        assert_eq!(read_instance_records(Some(serde_json::json!({}))), vec![]);
    }

    #[test]
    fn a_note_that_predates_a_field_reads_as_the_no_it_meant() {
        let mut written = serde_json::to_value(instance_record(|_| {})).unwrap();
        let object = written.as_object_mut().unwrap();
        object.remove("startAttempts");
        object.remove("restartPolicy");
        let records = read_instance_records(Some(serde_json::Value::Array(vec![written])));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].start_attempts, NO_START_ATTEMPTS);
        assert_eq!(records[0].restart_policy, DEFAULT_RESTART_POLICY);
    }

    #[test]
    fn a_record_this_host_has_just_made_has_nothing_to_say_about_the_app_yet() {
        let record = InstanceRecord::new(
            record_fields(),
            InstanceState::Pending,
            crate::domain::health::initial_tracker(),
        );
        assert_eq!(record.state, InstanceState::Pending);
        assert_eq!(record.start_attempts, NO_START_ATTEMPTS);
        assert_eq!(record.restart_count, 0);
        assert_eq!(record.last_restart, None);
        assert!(!record.stop_requested);
        assert_eq!(record.started_at, None);
        assert_eq!(record.last_exit_code, None);
        assert_eq!(record.message, None);
    }

    #[test]
    fn a_new_deployment_is_taken_on_by_the_record_that_was_already_there() {
        let mut record = instance_record(|_| {});
        record.adopt(redeployed());

        let wanted = redeployed();
        assert_eq!(record.deployment_id, wanted.deployment_id);
        assert_eq!(record.volume_id, wanted.volume_id);
        assert_eq!(record.hostnames, wanted.hostnames);
        assert_eq!(record.host_port, wanted.host_port);
        assert_eq!(record.http_port, wanted.http_port);
        assert_eq!(record.guest_ipv4, wanted.guest_ipv4);
        assert_eq!(record.layer_digests, wanted.layer_digests);
        assert_eq!(record.resources, wanted.resources);
        assert_eq!(record.restart_policy, wanted.restart_policy);
        assert!(!record.desired_running);
        assert!(record.on_request);
    }

    #[test]
    fn adopting_a_deployment_does_not_wipe_what_the_host_learned_about_the_app() {
        let mut record = instance_record(|record| {
            record.state = InstanceState::Unhealthy;
            record.restart_count = 4;
            record.last_restart = Some(crate::test_support::reported_restart(|_| {}));
            record.stop_requested = true;
            record.started_at = Some(Timestamp::from_epoch_ms(REDEPLOYED_AT_MS));
            record.last_exit_code = Some(137);
            record.message = Some(StateMessage::new("it stopped on its own".to_string()));
            record.health.ever_healthy = true;
            record.start_attempts = crate::domain::backoff::AttemptWindow {
                attempts: 2,
                last_attempt_at_ms: Some(REDEPLOYED_AT_MS),
            };
        });
        let before = record.clone();
        record.adopt(redeployed());

        assert_eq!(record.state, before.state);
        assert_eq!(record.health, before.health);
        assert_eq!(record.start_attempts, before.start_attempts);
        assert_eq!(record.restart_count, before.restart_count);
        assert_eq!(record.last_restart, before.last_restart);
        assert_eq!(record.stop_requested, before.stop_requested);
        assert_eq!(record.started_at, before.started_at);
        assert_eq!(record.last_exit_code, before.last_exit_code);
        assert_eq!(record.message, before.message);
        assert_eq!(record.app_id, before.app_id);
    }

    #[test]
    fn only_an_idle_record_reads_as_idle() {
        for state in protocol::INSTANCE_STATES {
            let record = instance_record(|record| record.state = state);
            assert_eq!(record.is_idle(), state == InstanceState::Idle, "{state:?}");
        }
    }

    #[test]
    fn an_app_that_never_started_is_still_inside_its_grace_period() {
        let never_started = instance_record(|_| {});
        assert_eq!(never_started.grace_inputs(REDEPLOYED_AT_MS).started_at_ms, None);
        assert!(crate::domain::health::is_within_grace_period(
            &never_started.grace_inputs(REDEPLOYED_AT_MS)
        ));

        let started =
            instance_record(|record| record.started_at = Some(Timestamp::from_epoch_ms(REDEPLOYED_AT_MS)));
        let grace = started.grace_inputs(REDEPLOYED_AT_MS);
        assert_eq!(grace.started_at_ms, Some(REDEPLOYED_AT_MS));
        assert_eq!(grace.now_ms, REDEPLOYED_AT_MS);
        assert_eq!(grace.health_check, &started.health_check);
        assert!(crate::domain::health::is_within_grace_period(&grace));
        assert!(!crate::domain::health::is_within_grace_period(
            &started.grace_inputs(REDEPLOYED_AT_MS + started.health_check.probe().grace_period_ms as i64 + 1)
        ));
    }
}
