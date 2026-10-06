use std::collections::{BTreeMap, BTreeSet};

use futures::StreamExt;
use protocol::{AppId, DesiredVolume, HostDesiredState, ReportedVolume, StateMessage, VolumeId, VolumeState};

use crate::domain::metrics::passes::Trigger;
use crate::domain::metrics::resources::Operation;
use crate::domain::reconcile::plan::{ObservedState, ObservedVolume, ReconcilePlan, VolumePlan};
use crate::domain::report::InstanceRecord;
use crate::host::Host;
use crate::state::merge_volume_reports;

// How many volumes a pass provisions at once. Bringing a device back is `nbd-client` run,
// refused, the device taken down and the client run again: a second or so of waiting on the
// kernel and on the server per device, none of it shared between devices, and a ZeroFS restart
// leaves every device on the host needing it. One at a time had a full host back in a minute;
// eight have it back in seconds without sixty clients at the one socket the moment it opens.
pub const PROVISION_CONCURRENCY: usize = 8;

pub fn volume_owners(
    desired: &HostDesiredState,
    records: &BTreeMap<AppId, InstanceRecord>,
) -> BTreeMap<VolumeId, AppId> {
    let mut owners: BTreeMap<VolumeId, AppId> = records
        .values()
        .map(|record| (record.volume_id.clone(), record.app_id.clone()))
        .collect();
    for volume in &desired.volumes {
        owners.insert(volume.volume_id.clone(), volume.app_id.clone());
    }
    owners
}

pub async fn observe_volumes(host: &Host, owners: &BTreeMap<VolumeId, AppId>) -> Vec<ObservedVolume> {
    host.volumes
        .observe(owners)
        .await
        .into_iter()
        .filter_map(|backing| {
            let app_id = owners.get(&backing.volume_id)?.clone();
            Some(ObservedVolume {
                volume_id: backing.volume_id,
                app_id,
                attached: backing.attached,
                formatted: backing.formatted,
                size_bytes: backing.size_bytes,
                storage_prefix: backing.storage_prefix,
                device_path: backing.device_path,
            })
        })
        .collect()
}

/// A volume as the control plane hears of it. `last_refusal` is what the last pass said when it
/// could not provision this volume: an attached device with no filesystem is still failed, and
/// the reason stays on the report rather than going blank between one attempt and the next. A
/// device this host holds for the volume that no longer answers (what a ZeroFS restart leaves
/// under every guest) is failed too, and says so, until the pass has re-attached it; only a
/// volume this host holds no device for is detached.
pub fn to_reported_volume(observed: &ObservedVolume, last_refusal: Option<&StateMessage>) -> ReportedVolume {
    let (state, message) = match (observed.attached, observed.formatted, &observed.device_path) {
        (true, true, _) => (VolumeState::Ready, None),
        (true, false, _) => (VolumeState::Failed, last_refusal.cloned()),
        (false, _, Some(device_path)) => (
            VolumeState::Failed,
            Some(StateMessage::new(format!(
                "{device_path} is not answering and is being re-attached"
            ))),
        ),
        (false, _, None) => (VolumeState::Detached, None),
    };
    ReportedVolume {
        volume_id: observed.volume_id.clone(),
        app_id: observed.app_id.clone(),
        state,
        size_bytes: observed.size_bytes,
        storage_prefix: Some(observed.storage_prefix.clone()),
        device_path: observed.device_path.clone(),
        message,
    }
}

/// What the last pass left on record against each volume it could not provision.
fn last_refusals(reports: &[ReportedVolume]) -> BTreeMap<VolumeId, StateMessage> {
    reports
        .iter()
        .filter(|report| report.state == VolumeState::Failed)
        .filter_map(|report| Some((report.volume_id.clone(), report.message.clone()?)))
        .collect()
}

/// Whether the host already holds this volume's device, answering reads, with no filesystem on
/// it: what a refused seed leaves.
fn is_bare(observed: &ObservedState, volume_id: &VolumeId) -> bool {
    observed
        .volumes
        .iter()
        .any(|volume| &volume.volume_id == volume_id && volume.attached && !volume.formatted)
}

pub async fn apply_volumes(
    host: &Host,
    plan: &ReconcilePlan,
    observed: &ObservedState,
    desired: &HostDesiredState,
    trigger: Trigger,
) {
    let last_refusals = last_refusals(&host.state.snapshot().await.volume_reports);

    let still_named: BTreeSet<VolumeId> = desired
        .volumes
        .iter()
        .map(|volume| volume.volume_id.clone())
        .collect();
    host.state.forget_deleted_volumes(&still_named).await;

    // What was observed goes on record before anything is done about it. Re-attaching a device
    // is seconds, and a host of many guests re-attaches them a few at a time, so a report that
    // waited for the last of them would say every device was well for the whole of the outage.
    let existing: Vec<ReportedVolume> = observed
        .volumes
        .iter()
        .map(|volume| to_reported_volume(volume, last_refusals.get(&volume.volume_id)))
        .collect();
    host.state
        .modify(|snapshot| {
            let deleted = snapshot.deleted_volumes.values().cloned().collect();
            snapshot.volume_reports = merge_volume_reports(existing, deleted);
        })
        .await;

    let provisions: Vec<_> = plan
        .volumes
        .iter()
        .filter_map(|action| match action {
            // Provisioning a bare volume fetches its seed again before it can fail again, so one
            // whose seed was refused is tried again when the document moves or the daemon comes
            // back, not by every tick against a document that still names the wrong thing. A
            // volume whose device is gone is still re-attached on a tick: that costs nothing and
            // is what brings a guest back after a ZeroFS restart.
            VolumePlan::Provision { desired }
                if trigger == Trigger::Tick && is_bare(observed, &desired.volume_id) =>
            {
                None
            }
            VolumePlan::Provision { desired } => Some(provision_volume(host, desired, &last_refusals)),
            VolumePlan::Blocked { desired, blocked_by } => {
                tracing::warn!(
                    volume_id = %desired.volume_id,
                    blocked_by = ?blocked_by.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    "volume removal deferred: still held"
                );
                None
            }
            _ => None,
        })
        .collect();
    futures::stream::iter(provisions)
        .buffer_unordered(PROVISION_CONCURRENCY)
        .collect::<()>()
        .await;
}

/// The attachment goes with the slot rather than with the runtime, because a stopped app keeps both and
/// only an app that is leaving gives them up. Taking it here is also what stops the slot the
/// cursor hands out next from inheriting a live device.
async fn give_back_slot(host: &Host, app_id: &AppId) {
    let interface_name = {
        let mut allocator = host.allocator.lock().await;
        let held = allocator.lookup(app_id).map(|slot| slot.interface_name);
        allocator.release(app_id);
        held
    };
    if let Some(interface_name) = interface_name {
        if let Err(error) = host.vms.delete_attachment(&interface_name).await {
            tracing::warn!(%interface_name, error = %error.message(), "a network attachment outlived the app it was made for");
        }
    }
}

async fn provision_volume(
    host: &Host,
    desired: &DesiredVolume,
    last_refusals: &BTreeMap<VolumeId, StateMessage>,
) {
    let provisioning = std::time::Instant::now();
    let provisioned = host.volumes.provision(desired).await;
    host.metrics.resources.done(
        Operation::VolumeProvision,
        provisioned.is_ok(),
        provisioning.elapsed(),
    );
    let report = match provisioned {
        Ok(attached) => ReportedVolume {
            volume_id: attached.volume_id,
            app_id: desired.app_id.clone(),
            state: VolumeState::Ready,
            size_bytes: attached.size_bytes,
            storage_prefix: Some(attached.storage_prefix),
            device_path: Some(attached.device_path),
            message: None,
        },
        Err(error) => {
            let refusal = StateMessage::new(error.message());
            // A volume that cannot be provisioned is tried again every pass, and the same
            // refusal every pass is one line in the log rather than one per pass.
            if last_refusals.get(&desired.volume_id) != Some(&refusal) {
                tracing::error!(volume_id = %desired.volume_id, error = %error.message(), "volume provisioning failed");
            }
            ReportedVolume {
                volume_id: desired.volume_id.clone(),
                app_id: desired.app_id.clone(),
                state: VolumeState::Failed,
                size_bytes: desired.size_bytes,
                storage_prefix: None,
                device_path: None,
                message: Some(refusal),
            }
        }
    };
    host.state
        .modify(|snapshot| {
            snapshot.volume_reports =
                merge_volume_reports(std::mem::take(&mut snapshot.volume_reports), vec![report]);
        })
        .await;
}

pub async fn apply_teardowns(host: &Host, plan: &ReconcilePlan) {
    for action in &plan.volumes {
        match action {
            VolumePlan::Teardown { desired } => tear_down_volume(host, desired).await,
            VolumePlan::Detach { volume_id, app_id } => detach_volume(host, volume_id, app_id).await,
            _ => {}
        }
    }
}

async fn tear_down_volume(host: &Host, desired: &protocol::DesiredVolume) {
    let tearing_down = std::time::Instant::now();
    let torn_down = host.volumes.teardown(&desired.volume_id, &desired.app_id).await;
    host.metrics.resources.done(
        Operation::VolumeTeardown,
        torn_down.is_ok(),
        tearing_down.elapsed(),
    );
    match torn_down {
        Ok(()) => {
            if host.state.record(&desired.app_id).await.is_some_and(|r| {
                r.expired_at_ms.is_some() && r.volume_id == desired.volume_id && r.hostnames.is_empty()
            }) {
                host.state.drop_record(&desired.app_id).await;
            }
            give_back_slot(host, &desired.app_id).await;
            let report = ReportedVolume {
                volume_id: desired.volume_id.clone(),
                app_id: desired.app_id.clone(),
                state: VolumeState::Deleted,
                size_bytes: 0,
                storage_prefix: None,
                device_path: None,
                message: None,
            };
            host.state.remember_deleted_volume(report.clone()).await;
            host.state
                .modify(|snapshot| {
                    snapshot.volume_reports =
                        merge_volume_reports(std::mem::take(&mut snapshot.volume_reports), vec![report]);
                })
                .await;
        }
        Err(error) => {
            tracing::error!(volume_id = %desired.volume_id, error = %error.message(), "volume teardown failed");
        }
    }
}

async fn detach_volume(host: &Host, volume_id: &VolumeId, app_id: &AppId) {
    match host.volumes.detach(volume_id, app_id).await {
        Ok(()) => {
            give_back_slot(host, app_id).await;
            tracing::info!(%volume_id, %app_id, "volume detached: the document no longer names it");
            // The pass reported it as observed, on a device this host no longer holds for it.
            // Nothing owns it after this, so the next pass drops it.
            host.state
                .modify(|snapshot| {
                    for report in snapshot
                        .volume_reports
                        .iter_mut()
                        .filter(|report| &report.volume_id == volume_id)
                    {
                        report.state = VolumeState::Detached;
                        report.device_path = None;
                    }
                })
                .await;
        }
        Err(error) => {
            tracing::error!(%volume_id, error = %error.message(), "volume detach failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::volumes::{MockVolumeBackend, VolumeError};
    use crate::domain::reconcile::plan::plan_reconcile;
    use crate::test_support::*;
    use protocol::DesiredPresence;

    async fn host_over(volumes: MockVolumeBackend) -> TestHost {
        host_sharing(std::sync::Arc::new(volumes)).await
    }

    async fn host_sharing(volumes: std::sync::Arc<dyn crate::adapters::volumes::VolumeBackend>) -> TestHost {
        let mut host = test_host().await;
        std::sync::Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .volumes = volumes;
        host
    }

    fn absent() -> protocol::DesiredVolume {
        desired_volume(|volume| volume.desired_state = DesiredPresence::Absent)
    }

    fn deleted_note() -> ReportedVolume {
        reported_volume(|report| report.state = VolumeState::Deleted)
    }

    #[test]
    fn a_volume_being_deleted_is_still_owned_once_its_record_is_gone() {
        let desired = desired_state(|state| state.volumes = vec![desired_volume(|_| {})]);
        let owners = volume_owners(&desired, &BTreeMap::new());
        assert_eq!(owners.get(&volume_id()), Some(&app_id()));

        let records = BTreeMap::from([(app_id(), instance_record(|_| {}))]);
        let held = volume_owners(&desired_state(|_| {}), &records);
        assert_eq!(held.get(&volume_id()), Some(&app_id()));
    }

    #[tokio::test]
    async fn a_backing_with_no_app_is_left_out_rather_than_reported_under_a_guess() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        assert!(observe_volumes(&host, &BTreeMap::new()).await.is_empty());
        let owners = BTreeMap::from([(volume_id(), app_id())]);
        let observed = observe_volumes(&host, &owners).await;
        assert_eq!(observed.len(), 1);
        assert!(observed[0].formatted);
        assert_eq!(to_reported_volume(&observed[0], None).state, VolumeState::Ready);
    }

    #[tokio::test]
    async fn a_teardown_gives_the_slot_back_and_says_the_volume_is_gone() {
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        let plan = ReconcilePlan {
            volumes: vec![VolumePlan::Teardown {
                desired: desired_volume(|volume| volume.desired_state = protocol::DesiredPresence::Absent),
            }],
            ..Default::default()
        };
        apply_teardowns(&host, &plan).await;
        assert!(host.slot_of(&app_id()).await.is_none());
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.volume_reports[0].state, VolumeState::Deleted);
        assert!(snapshot.deleted_volumes.contains_key(&volume_id()));
        assert!(host.volumes.observe(&Default::default()).await.is_empty());
        assert_eq!(
            host.vms.removed_attachments(),
            vec!["nbr0".to_string()],
            "the slot went back with its tap still on the host"
        );
    }

    #[tokio::test]
    async fn an_app_that_never_held_a_slot_has_no_tap_to_take() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        let plan = ReconcilePlan {
            volumes: vec![VolumePlan::Teardown {
                desired: desired_volume(|volume| volume.desired_state = protocol::DesiredPresence::Absent),
            }],
            ..Default::default()
        };
        apply_teardowns(&host, &plan).await;
        assert!(
            host.vms.removed_attachments().is_empty(),
            "a device nothing was ever given is not one to go looking for"
        );
    }

    #[tokio::test]
    async fn a_volume_that_could_not_be_provisioned_is_reported_failed_with_the_reason() {
        let host = test_host().await;
        host.volumes
            .provision(&desired_volume(|volume| {
                volume.size_bytes = VOLUME_SIZE_BYTES * 4
            }))
            .await
            .unwrap();
        let plan = ReconcilePlan {
            volumes: vec![VolumePlan::Provision {
                desired: desired_volume(|_| {}),
            }],
            ..Default::default()
        };
        apply_volumes(
            &host,
            &plan,
            &ObservedState::default(),
            &desired_state(|_| {}),
            Trigger::Change,
        )
        .await;
        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports[0].state, VolumeState::Failed);
        assert!(reports[0]
            .message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("cannot be resized down"));
    }

    #[test]
    fn the_document_says_who_owns_a_volume_when_the_record_this_host_kept_disagrees() {
        let newer = AppId::parse("app-2").unwrap();
        let desired = desired_state(|state| {
            state.volumes = vec![desired_volume(|volume| {
                volume.app_id = AppId::parse("app-2").unwrap()
            })]
        });
        let records = BTreeMap::from([(app_id(), instance_record(|_| {}))]);
        assert_eq!(volume_owners(&desired, &records).get(&volume_id()), Some(&newer));
    }

    #[test]
    fn a_volume_this_host_holds_no_device_for_is_reported_detached_rather_than_ready() {
        let report = to_reported_volume(
            &observed_volume(|volume| {
                volume.attached = false;
                volume.device_path = None;
            }),
            None,
        );
        assert_eq!(report.state, VolumeState::Detached);
        assert_eq!(report.message, None);
        assert_eq!(report.device_path, None);
        assert_eq!(
            report.storage_prefix.map(|prefix| prefix.as_str().to_string()),
            Some(HOST_STORAGE_PREFIX.to_string())
        );
    }

    #[test]
    fn a_device_this_host_holds_that_does_not_answer_is_reported_failed_and_says_it_is_being_re_attached() {
        let dead = observed_volume(|volume| {
            volume.attached = false;
            volume.formatted = false;
        });

        let report = to_reported_volume(&dead, None);
        assert_eq!(report.state, VolumeState::Failed);
        assert_eq!(
            report.message.as_ref().map(StateMessage::as_str),
            Some("/dev/nbd0 is not answering and is being re-attached")
        );
        assert_eq!(report.device_path.as_deref(), Some("/dev/nbd0"));

        let refused_before = StateMessage::new("the initial contents could not be laid out");
        assert_eq!(
            to_reported_volume(&dead, Some(&refused_before)).message,
            report.message,
            "what the last pass could not do is not what is wrong with the device now"
        );
    }

    #[test]
    fn an_attached_volume_with_no_filesystem_is_reported_failed_with_the_reason_it_last_was() {
        let refusal = StateMessage::new("the initial contents could not be laid out: wrong digest");
        let bare = observed_volume(|volume| volume.formatted = false);

        let report = to_reported_volume(&bare, Some(&refusal));
        assert_eq!(report.state, VolumeState::Failed);
        assert_eq!(report.message.as_ref(), Some(&refusal));
        assert_eq!(report.device_path.as_deref(), Some("/dev/nbd0"));

        let unexplained = to_reported_volume(&bare, None);
        assert_eq!(unexplained.state, VolumeState::Failed);
        assert_eq!(unexplained.message, None);

        let ready = to_reported_volume(&observed_volume(|_| {}), Some(&refusal));
        assert_eq!(ready.state, VolumeState::Ready);
        assert_eq!(
            ready.message, None,
            "a volume that is ready has nothing to explain"
        );
    }

    #[tokio::test]
    async fn a_refusal_stays_on_the_report_of_a_volume_a_pass_left_bare() {
        let host = test_host().await;
        let refusal = reported_volume(|report| {
            report.state = VolumeState::Failed;
            report.message = Some(StateMessage::new("the initial contents could not be laid out"));
        });
        host.state
            .modify(|snapshot| snapshot.volume_reports = vec![refusal.clone()])
            .await;
        let observed =
            observed_state(|state| state.volumes = vec![observed_volume(|volume| volume.formatted = false)]);

        apply_volumes(
            &host,
            &ReconcilePlan::default(),
            &observed,
            &desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
            Trigger::Change,
        )
        .await;

        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports[0].state, VolumeState::Failed);
        assert_eq!(reports[0].message, refusal.message);
    }

    #[tokio::test]
    async fn a_seed_that_is_refused_is_tried_again_every_pass_and_stays_failed_until_it_takes() {
        let host = test_host().await;
        // The store holds the app's layer; the document names a digest of something else.
        let wrong = desired_volume(|volume| {
            volume.initial_contents = Some(initial_contents(b"not what the store holds", "/app/data"))
        });
        let desired = desired_state(|state| state.volumes = vec![wrong.clone()]);
        let plan = ReconcilePlan {
            volumes: vec![VolumePlan::Provision {
                desired: wrong.clone(),
            }],
            ..Default::default()
        };
        let owners = volume_owners(&desired, &BTreeMap::new());

        apply_volumes(&host, &plan, &ObservedState::default(), &desired, Trigger::Change).await;
        let first = host.state.snapshot().await.volume_reports;
        assert_eq!(first[0].state, VolumeState::Failed);
        let reason = first[0].message.clone().expect("the refusal");
        assert!(reason.as_str().contains("hashes to"), "{}", reason.as_str());

        // The file is there and sized, so the next pass finds the volume attached and bare.
        let observed = observe_volumes(&host, &owners).await;
        assert_eq!(observed.len(), 1);
        assert!(observed[0].attached);
        assert!(!observed[0].formatted);
        let again = plan_reconcile(
            &desired,
            &observed_state(|state| state.volumes = observed.clone()),
        );
        assert_eq!(again.volumes, plan.volumes, "the next pass provisions it again");

        apply_volumes(
            &host,
            &again,
            &observed_state(|state| state.volumes = observed),
            &desired,
            Trigger::Change,
        )
        .await;
        let second = host.state.snapshot().await.volume_reports;
        assert_eq!(second[0].state, VolumeState::Failed);
        assert_eq!(second[0].message, Some(reason), "the reason is kept, not blanked");
        let page = crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        );
        assert!(
            page.contains(
                "nibrunner_storage_operation_seconds_count{operation=\"volume_provision\",outcome=\"failed\"} 2\n"
            ),
            "{page}"
        );
    }

    #[tokio::test]
    async fn a_volume_the_plan_provisions_is_reported_with_the_device_the_guest_will_read() {
        let host = test_host().await;
        let plan = ReconcilePlan {
            volumes: vec![VolumePlan::Provision {
                desired: desired_volume(|_| {}),
            }],
            ..Default::default()
        };

        apply_volumes(
            &host,
            &plan,
            &ObservedState::default(),
            &desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
            Trigger::Change,
        )
        .await;

        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, VolumeState::Ready);
        assert_eq!(reports[0].size_bytes, VOLUME_SIZE_BYTES);
        assert!(reports[0].device_path.is_some());
        assert!(reports[0].storage_prefix.is_some());
        let page = crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        );
        assert!(page.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"volume_provision\",outcome=\"ok\"} 1\n"
        ));
    }

    #[tokio::test]
    async fn a_volume_still_held_by_an_instance_is_left_where_it_is_rather_than_torn_down() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        let plan = ReconcilePlan {
            volumes: vec![VolumePlan::Blocked {
                desired: absent(),
                blocked_by: vec![app_id()],
            }],
            ..Default::default()
        };
        let observed = observed_state(|state| state.volumes = vec![observed_volume(|_| {})]);

        apply_volumes(
            &host,
            &plan,
            &observed,
            &desired_state(|state| state.volumes = vec![absent()]),
            Trigger::Change,
        )
        .await;

        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports[0].state, VolumeState::Ready);
        assert!(!host.volumes.observe(&Default::default()).await.is_empty());
    }

    #[tokio::test]
    async fn a_note_about_a_deleted_volume_is_dropped_once_the_document_stops_naming_it() {
        let host = test_host().await;
        host.state.remember_deleted_volume(deleted_note()).await;

        apply_volumes(
            &host,
            &ReconcilePlan::default(),
            &ObservedState::default(),
            &desired_state(|_| {}),
            Trigger::Change,
        )
        .await;

        let snapshot = host.state.snapshot().await;
        assert!(snapshot.deleted_volumes.is_empty());
        assert!(snapshot.volume_reports.is_empty());
    }

    #[tokio::test]
    async fn and_kept_while_it_still_does_so_the_control_plane_hears_the_deletion() {
        let host = test_host().await;
        host.state.remember_deleted_volume(deleted_note()).await;

        apply_volumes(
            &host,
            &ReconcilePlan::default(),
            &ObservedState::default(),
            &desired_state(|state| state.volumes = vec![absent()]),
            Trigger::Change,
        )
        .await;

        let snapshot = host.state.snapshot().await;
        assert!(snapshot.deleted_volumes.contains_key(&volume_id()));
        assert_eq!(snapshot.volume_reports[0].state, VolumeState::Deleted);
    }

    fn provisioning() -> ReconcilePlan {
        ReconcilePlan {
            volumes: vec![VolumePlan::Provision {
                desired: desired_volume(|_| {}),
            }],
            ..Default::default()
        }
    }

    fn dead_device() -> ObservedState {
        observed_state(|state| {
            state.volumes = vec![observed_volume(|volume| {
                volume.attached = false;
                volume.formatted = false;
            })]
        })
    }

    #[tokio::test]
    async fn a_tick_leaves_a_bare_volume_alone_rather_than_fetching_its_seed_again() {
        let mut volumes = MockVolumeBackend::new();
        volumes.expect_provision().never();
        let host = host_over(volumes).await;
        let refusal = StateMessage::new("the initial contents could not be laid out");
        host.state
            .modify(|snapshot| {
                snapshot.volume_reports = vec![reported_volume(|report| {
                    report.state = VolumeState::Failed;
                    report.message = Some(refusal.clone());
                })]
            })
            .await;
        let bare =
            observed_state(|state| state.volumes = vec![observed_volume(|volume| volume.formatted = false)]);

        apply_volumes(
            &host,
            &provisioning(),
            &bare,
            &desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
            Trigger::Tick,
        )
        .await;

        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports[0].state, VolumeState::Failed);
        assert_eq!(
            reports[0].message.as_ref(),
            Some(&refusal),
            "the volume is still failed for the reason it was, and the app stays down for it"
        );
    }

    #[tokio::test]
    async fn but_a_tick_still_re_attaches_a_volume_whose_device_is_gone() {
        let mut volumes = MockVolumeBackend::new();
        volumes.expect_provision().times(1).returning(|desired| {
            Ok(crate::adapters::volumes::AttachedVolume {
                volume_id: desired.volume_id.clone(),
                device_path: "/dev/nbd0".to_string(),
                size_bytes: desired.size_bytes,
                storage_prefix: protocol::ObjectKey::parse(HOST_STORAGE_PREFIX).unwrap(),
            })
        });
        let host = host_over(volumes).await;

        apply_volumes(
            &host,
            &provisioning(),
            &dead_device(),
            &desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
            Trigger::Tick,
        )
        .await;

        assert_eq!(
            host.state.snapshot().await.volume_reports[0].state,
            VolumeState::Ready
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_device_that_stopped_answering_is_reported_failed_while_it_is_re_attached_and_ready_once_it_is()
    {
        let mut host = test_host().await;
        let state = host.state.clone();
        let while_re_attaching = std::sync::Arc::new(std::sync::Mutex::new(None));
        let noted = while_re_attaching.clone();
        let mut volumes = MockVolumeBackend::new();
        volumes.expect_provision().times(1).returning(move |desired| {
            let snapshot =
                tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(state.snapshot()));
            *noted.lock().unwrap() = snapshot.volume_reports.into_iter().next();
            Ok(crate::adapters::volumes::AttachedVolume {
                volume_id: desired.volume_id.clone(),
                device_path: "/dev/nbd0".to_string(),
                size_bytes: desired.size_bytes,
                storage_prefix: protocol::ObjectKey::parse(HOST_STORAGE_PREFIX).unwrap(),
            })
        });
        std::sync::Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .volumes = std::sync::Arc::new(volumes);

        apply_volumes(
            &host,
            &provisioning(),
            &dead_device(),
            &desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
            Trigger::Tick,
        )
        .await;

        let seen = while_re_attaching
            .lock()
            .unwrap()
            .clone()
            .expect("the record while the device was re-attached");
        assert_eq!(seen.state, VolumeState::Failed);
        assert_eq!(
            seen.message.as_ref().map(StateMessage::as_str),
            Some("/dev/nbd0 is not answering and is being re-attached")
        );
        let mut report = crate::domain::metrics::tests::report();
        report.volumes = vec![seen];
        let page =
            crate::domain::metrics::tests::page(&report, &host.metrics, &host.state.snapshot().await, 0);
        assert!(
            page.contains("nibrunner_volume_state{volume=\"vol-1\",app=\"app-1\",state=\"failed\"} 1\n"),
            "{page}"
        );

        let after = host.state.snapshot().await.volume_reports;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].state, VolumeState::Ready);
        assert_eq!(
            after[0].message, None,
            "a device that answers again has nothing to explain"
        );
    }

    #[tokio::test]
    async fn a_device_that_stopped_answering_under_a_volume_nothing_re_attaches_this_pass_still_says_so() {
        let host = test_host().await;

        apply_volumes(
            &host,
            &ReconcilePlan::default(),
            &dead_device(),
            &desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
            Trigger::Tick,
        )
        .await;

        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports[0].state, VolumeState::Failed);
        assert!(reports[0]
            .message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("is not answering"));
    }

    fn nth(n: usize) -> DesiredVolume {
        desired_volume(|volume| {
            volume.volume_id = VolumeId::parse(format!("vol-{n}")).unwrap();
            volume.app_id = AppId::parse(format!("app-{n}")).unwrap();
        })
    }

    fn attached(desired: &DesiredVolume) -> crate::adapters::volumes::AttachedVolume {
        crate::adapters::volumes::AttachedVolume {
            volume_id: desired.volume_id.clone(),
            device_path: format!("/dev/nbd-{}", desired.app_id),
            size_bytes: desired.size_bytes,
            storage_prefix: protocol::ObjectKey::parse(HOST_STORAGE_PREFIX).unwrap(),
        }
    }

    /// A tick over this many volumes whose devices are all gone at once, as a ZeroFS restart
    /// leaves them, on the given host.
    fn a_tick_over(host: &TestHost, count: usize) -> tokio::task::JoinHandle<()> {
        let host = host.arc().clone();
        tokio::spawn(async move {
            let plan = ReconcilePlan {
                volumes: (1..=count)
                    .map(|n| VolumePlan::Provision { desired: nth(n) })
                    .collect(),
                ..Default::default()
            };
            let yanked = observed_state(|state| {
                state.volumes = (1..=count)
                    .map(|n| {
                        observed_volume(|volume| {
                            volume.volume_id = nth(n).volume_id;
                            volume.app_id = nth(n).app_id;
                            volume.attached = false;
                            volume.formatted = false;
                        })
                    })
                    .collect()
            });
            let desired = desired_state(|state| state.volumes = (1..=count).map(nth).collect());
            apply_volumes(&host, &plan, &yanked, &desired, Trigger::Tick).await;
        })
    }

    async fn ready(host: &TestHost) -> usize {
        host.state
            .snapshot()
            .await
            .volume_reports
            .iter()
            .filter(|report| report.state == VolumeState::Ready)
            .count()
    }

    #[tokio::test]
    async fn a_batch_of_yanked_volumes_is_re_attached_eight_at_a_time_and_every_one_ends_ready() {
        let count = PROVISION_CONCURRENCY * 2;
        let mut volumes = MockVolumeBackend::new();
        volumes
            .expect_provision()
            .times(count)
            .returning(|desired| Ok(attached(desired)));
        let (held, gate) = mocks::volumes_holding_provisions(volumes);
        let host = host_sharing(held).await;
        let pass = a_tick_over(&host, count);

        gate.held_up(PROVISION_CONCURRENCY).await;
        // Nothing has been let through, so anything more at the gate now is a ninth beside eight.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(gate.in_flight(), PROVISION_CONCURRENCY);
        let meanwhile = host.state.snapshot().await.volume_reports;
        assert_eq!(meanwhile.len(), count);
        assert!(
            meanwhile.iter().all(|report| report.state == VolumeState::Failed
                && report
                    .message
                    .as_ref()
                    .is_some_and(|message| message.as_str().contains("is not answering"))),
            "every device reads failed from the moment the pass saw it: {meanwhile:?}"
        );

        gate.let_through(PROVISION_CONCURRENCY);
        let landed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while ready(&host).await != PROVISION_CONCURRENCY {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(
            landed.is_ok(),
            "the first eight went on record as they landed rather than with the last"
        );

        gate.let_through(count - PROVISION_CONCURRENCY);
        pass.await.unwrap();

        assert_eq!(gate.most_in_flight(), PROVISION_CONCURRENCY);
        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports.len(), count);
        for n in 1..=count {
            let volume = nth(n);
            let report = reports
                .iter()
                .find(|report| report.volume_id == volume.volume_id)
                .unwrap();
            assert_eq!(report.state, VolumeState::Ready, "{}", volume.volume_id);
            assert_eq!(report.app_id, volume.app_id);
            assert_eq!(report.device_path, Some(attached(&volume).device_path));
        }
    }

    #[tokio::test]
    async fn a_volume_that_refuses_its_re_attach_holds_up_none_of_the_others() {
        let count = PROVISION_CONCURRENCY + 1;
        let refused = nth(1).volume_id;
        let mut volumes = MockVolumeBackend::new();
        volumes.expect_provision().times(count).returning({
            let refused = refused.clone();
            move |desired| {
                if desired.volume_id == refused {
                    Err(VolumeError::Unusable(format!(
                        "nbd-client /dev/nbd-{} exited 1",
                        desired.app_id
                    )))
                } else {
                    Ok(attached(desired))
                }
            }
        });
        let (held, gate) = mocks::volumes_holding_provisions(volumes);
        let host = host_sharing(held).await;
        let pass = a_tick_over(&host, count);

        gate.held_up(PROVISION_CONCURRENCY).await;
        gate.let_through(count);
        pass.await.unwrap();

        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports.len(), count);
        for report in &reports {
            if report.volume_id == refused {
                assert_eq!(report.state, VolumeState::Failed);
                assert_eq!(
                    report.message.as_ref().unwrap().as_str(),
                    "the volume could not be made ready: nbd-client /dev/nbd-app-1 exited 1"
                );
            } else {
                assert_eq!(report.state, VolumeState::Ready, "{}", report.volume_id);
                assert_eq!(report.message, None);
            }
        }
        let page = crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        );
        assert!(
            page.contains(&format!(
                "nibrunner_storage_operation_seconds_count{{operation=\"volume_provision\",outcome=\"ok\"}} {}\n",
                count - 1
            )),
            "{page}"
        );
        assert!(
            page.contains(
                "nibrunner_storage_operation_seconds_count{operation=\"volume_provision\",outcome=\"failed\"} 1\n"
            ),
            "{page}"
        );
    }

    #[tokio::test]
    async fn and_a_change_provisions_a_bare_volume_again_since_the_document_may_have_put_it_right() {
        let mut volumes = MockVolumeBackend::new();
        volumes.expect_provision().times(1).returning(|_| {
            Err(VolumeError::ContentsUnusable(
                "still the wrong digest".to_string(),
            ))
        });
        let host = host_over(volumes).await;
        let bare =
            observed_state(|state| state.volumes = vec![observed_volume(|volume| volume.formatted = false)]);

        apply_volumes(
            &host,
            &provisioning(),
            &bare,
            &desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
            Trigger::Change,
        )
        .await;

        let reports = host.state.snapshot().await.volume_reports;
        assert_eq!(reports[0].state, VolumeState::Failed);
        assert!(reports[0]
            .message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("still the wrong digest"));
    }

    #[tokio::test]
    async fn a_teardown_that_failed_keeps_the_slot_and_says_nothing_was_deleted() {
        let mut volumes = MockVolumeBackend::new();
        volumes
            .expect_teardown()
            .times(1)
            .returning(|_, _| Err(VolumeError::Unusable("the file is still open".to_string())));
        let host = host_over(volumes).await;
        host.slot_for(&app_id()).await.unwrap();
        let plan = ReconcilePlan {
            volumes: vec![VolumePlan::Teardown { desired: absent() }],
            ..Default::default()
        };

        apply_teardowns(&host, &plan).await;

        assert!(host.slot_of(&app_id()).await.is_some());
        let snapshot = host.state.snapshot().await;
        assert!(snapshot.deleted_volumes.is_empty());
        assert!(snapshot.volume_reports.is_empty());
    }

    fn detaching() -> ReconcilePlan {
        ReconcilePlan {
            volumes: vec![VolumePlan::Detach {
                volume_id: volume_id(),
                app_id: app_id(),
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_detach_gives_the_slot_back_and_keeps_the_data() {
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state
            .modify(|snapshot| {
                snapshot.volume_reports = vec![reported_volume(|report| {
                    report.device_path = Some("/dev/nbd0".to_string())
                })]
            })
            .await;

        apply_teardowns(&host, &detaching()).await;

        assert!(host.slot_of(&app_id()).await.is_none());
        assert_eq!(
            host.vms.removed_attachments(),
            vec!["nbr0".to_string()],
            "the slot went back with its tap"
        );
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.volume_reports[0].state, VolumeState::Detached);
        assert_eq!(snapshot.volume_reports[0].device_path, None);
        assert!(
            snapshot.deleted_volumes.is_empty(),
            "nothing was deleted, so nothing says so"
        );
        let owners = BTreeMap::from([(volume_id(), app_id())]);
        assert_eq!(host.volumes.observe(&owners).await.len(), 1, "the data is kept");
    }

    #[tokio::test]
    async fn a_detach_that_failed_keeps_the_slot_so_its_device_is_not_handed_on() {
        let mut volumes = MockVolumeBackend::new();
        volumes
            .expect_detach()
            .times(1)
            .returning(|_, _| Err(VolumeError::Unusable("the device would not let go".to_string())));
        let host = host_over(volumes).await;
        host.slot_for(&app_id()).await.unwrap();

        apply_teardowns(&host, &detaching()).await;

        assert!(host.slot_of(&app_id()).await.is_some());
        assert!(host.vms.removed_attachments().is_empty());
    }

    #[tokio::test]
    async fn a_plan_with_nothing_to_tear_down_asks_the_backend_for_nothing() {
        let mut volumes = MockVolumeBackend::new();
        volumes.expect_teardown().never();
        volumes.expect_detach().never();
        let host = host_over(volumes).await;

        apply_teardowns(
            &host,
            &ReconcilePlan {
                volumes: vec![VolumePlan::None {
                    volume_id: volume_id(),
                }],
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn a_backing_the_host_holds_but_no_document_owns_is_left_out_of_what_is_reported() {
        let mut volumes = MockVolumeBackend::new();
        volumes.expect_observe().returning(|_| {
            vec![crate::adapters::volumes::ObservedBacking {
                volume_id: protocol::VolumeId::parse("vol-9").unwrap(),
                size_bytes: VOLUME_SIZE_BYTES,
                attached: true,
                formatted: true,
                device_path: None,
                storage_prefix: protocol::ObjectKey::parse(HOST_STORAGE_PREFIX).unwrap(),
            }]
        });
        let host = host_over(volumes).await;

        let owners = BTreeMap::from([(volume_id(), app_id())]);
        assert!(observe_volumes(&host, &owners).await.is_empty());
    }
}
