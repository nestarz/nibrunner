use protocol::{
    AppId, CheckpointId, Crontab, DesiredExport, ExportId, ExportState, ReportedExport, StateMessage,
    Timestamp,
};

use crate::domain::exports::bundle::{dump_volume, write_bundle};
use crate::domain::exports::freeze::frozen;
use crate::domain::exports::reader::ReaderDevice;
use crate::domain::metrics::passes::Trigger;
use crate::domain::metrics::resources::Operation;
use crate::domain::reconcile::plan::{ExportPlan, ObservedExport, ReconcilePlan};
use crate::host::Host;

const EXPORT_PREFIX: &str = "export-";

pub fn export_checkpoint_id(export_id: &ExportId) -> Option<CheckpointId> {
    CheckpointId::parse(format!("{EXPORT_PREFIX}{export_id}")).ok()
}

pub fn is_export_checkpoint(checkpoint_id: &CheckpointId) -> bool {
    checkpoint_id.as_str().starts_with(EXPORT_PREFIX)
}

pub async fn observe_exports(host: &Host) -> Vec<ObservedExport> {
    host.state
        .snapshot()
        .await
        .export_reports
        .into_iter()
        .map(|report| ObservedExport {
            export_id: report.export_id,
            written: report.state == ExportState::Ready,
        })
        .collect()
}

pub async fn apply_exports(host: &Host, plan: &ReconcilePlan, trigger: Trigger) {
    reap(host, plan).await;

    let mut reports = host.state.snapshot().await.export_reports;
    for action in &plan.exports {
        match action {
            // Writing a bundle freezes the tenant and dumps its volume, so one that failed is
            // tried again when the document moves or the daemon comes back, not by every tick
            // against a store that keeps refusing.
            ExportPlan::Write { .. } if trigger == Trigger::Tick => {}
            ExportPlan::Write { desired } => {
                let report = write(host, desired).await;
                reports.retain(|held| held.export_id != report.export_id);
                reports.push(report);
            }
            ExportPlan::Forget { export_id } => reports.retain(|held| &held.export_id != export_id),
            ExportPlan::None { .. } => {}
        }
    }
    host.state
        .modify(|snapshot| snapshot.export_reports = reports)
        .await;
}

async fn write(host: &Host, desired: &DesiredExport) -> ReportedExport {
    let Some(checkpoint_id) = export_checkpoint_id(&desired.export_id) else {
        return failed(
            desired,
            None,
            "its id does not make a checkpoint name".to_string(),
        );
    };
    let staging_dir = host.config.export_staging_dir.join(desired.export_id.as_str());

    let writing = std::time::Instant::now();
    let written = write_inner(host, desired, &checkpoint_id, &staging_dir).await;
    host.metrics
        .resources
        .done(Operation::ExportWrite, written.is_ok(), writing.elapsed());

    let _ = std::fs::remove_dir_all(&staging_dir);

    match written {
        Ok(size_bytes) => {
            tracing::info!(
                export_id = %desired.export_id,
                object_key = %desired.object_key,
                size_bytes,
                "export written"
            );
            ReportedExport {
                export_id: desired.export_id.clone(),
                checkpoint_id: Some(checkpoint_id),
                state: ExportState::Ready,
                size_bytes: Some(size_bytes),
                ready_at: Some(Timestamp::now()),
                message: None,
            }
        }
        Err(reason) => failed(desired, Some(checkpoint_id), reason),
    }
}

/// A paused guest cannot settle its filesystem for the cut, so a frozen app is resumed first; the
/// next pass may freeze it again once it is quiet.
async fn thaw_for_export(host: &Host, app_id: &protocol::AppId) -> Result<(), String> {
    let frozen = host
        .vms
        .statuses(std::slice::from_ref(app_id))
        .await
        .get(app_id)
        .is_some_and(|status| status.frozen);
    if !frozen {
        return Ok(());
    }
    host.vms.thaw(app_id).await.map_err(|error| error.message())?;
    host.state
        .update_record(app_id, |record| {
            if record.state == protocol::InstanceState::Frozen {
                record.state = protocol::InstanceState::Starting;
            }
        })
        .await;
    host.state.probe_at_once(app_id).await;
    Ok(())
}

async fn write_inner(
    host: &Host,
    desired: &DesiredExport,
    checkpoint_id: &CheckpointId,
    staging_dir: &std::path::Path,
) -> Result<u64, String> {
    let crontab = registered_crontab(host, &desired.app_id).await?;
    let vsock_path = crate::domain::filesystem::reader::guest_vsock_path(host, &desired.app_id);
    thaw_for_export(host, &desired.app_id).await?;
    let lease = frozen(&desired.app_id, &vsock_path)
        .await
        .map_err(|error| error.message())?;
    // A checkpoint is of what the store holds, and what the store holds trails what the guest has
    // written by however long the backend waits between flushes — thirty seconds, for zerofs. The
    // freeze settles the guest's filesystem and nothing carried that as far as the store, so the
    // cut could be of a moment before the freeze rather than inside it: up to half a minute of a
    // tenant's writes missing from their own bundle, and on a volume young enough not to have been
    // flushed yet, no device in the checkpoint to read at all.
    host.volumes.flush().await.map_err(|error| error.message())?;
    host.volumes
        .create_checkpoint(checkpoint_id)
        .await
        .map_err(|error| error.message())?;
    if let Err(error) = lease.assert_held() {
        let _ = host.volumes.delete_checkpoint(checkpoint_id).await;
        return Err(error.message());
    }
    drop(lease);
    tracing::info!(
        app_id = %desired.app_id,
        %checkpoint_id,
        "export checkpoint cut while the tenant was frozen"
    );

    let read = read_into_staging(host, desired, checkpoint_id, staging_dir).await;
    if let Err(error) = host.volumes.delete_checkpoint(checkpoint_id).await {
        tracing::error!(
            %checkpoint_id,
            error = %error.message(),
            "export checkpoint not deleted; storage reclamation stays paused"
        );
    }
    read?;

    let bundle = write_bundle(desired.environment.as_ref(), crontab.as_ref(), staging_dir)
        .map_err(|error| error.message())?;
    host.exports
        .upload(&bundle.path, &desired.object_key)
        .await
        .map_err(|error| error.message())?;
    Ok(bundle.size_bytes)
}

async fn registered_crontab(host: &Host, app_id: &AppId) -> Result<Option<Crontab>, String> {
    Ok(host
        .cron
        .tables()
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|table| &table.app_id == app_id && !table.jobs.is_empty())
        .and_then(|table| table.crontab))
}

async fn read_into_staging(
    host: &Host,
    desired: &DesiredExport,
    checkpoint_id: &CheckpointId,
    staging_dir: &std::path::Path,
) -> Result<(), String> {
    let servers = host
        .checkpoint_servers
        .as_ref()
        .ok_or_else(|| "this host serves no checkpoints".to_string())?;
    let server = servers
        .start(checkpoint_id)
        .await
        .map_err(|error| error.message())?;
    let reader = match ReaderDevice::attach(
        &host.nbd,
        server.socket_path(),
        &desired.volume_id,
        host.config.max_apps,
    )
    .await
    {
        Ok(reader) => reader,
        Err(error) => {
            // A server that died after it began listening leaves a socket that refuses, so the
            // refusal on this end says nothing and the reason is only ever on that one.
            let refused = match server.complained() {
                Some(said) => format!("{}: the checkpoint server last said: {said}", error.message()),
                None => error.message(),
            };
            server.stop().await;
            return Err(refused);
        }
    };
    let dumped = dump_volume(&host.commands, reader.path(), staging_dir).await;
    reader.detach().await;
    server.stop().await;
    dumped.map_err(|error| error.message())
}

async fn reap(host: &Host, plan: &ReconcilePlan) {
    let in_flight: Vec<CheckpointId> = plan
        .exports
        .iter()
        .filter_map(|action| match action {
            ExportPlan::Write { desired } => export_checkpoint_id(&desired.export_id),
            ExportPlan::Forget { .. } | ExportPlan::None { .. } => None,
        })
        .collect();

    let orphans: Vec<CheckpointId> = host
        .volumes
        .observe_checkpoints()
        .await
        .into_iter()
        .filter(is_export_checkpoint)
        .filter(|held| !in_flight.contains(held))
        .collect();
    if orphans.is_empty() {
        return;
    }
    let _ = std::fs::remove_dir_all(&host.config.export_staging_dir);
    let _ = host
        .nbd
        .detach(&nft_render::export_reader_device_path(host.config.max_apps))
        .await;
    for checkpoint_id in &orphans {
        if let Err(error) = host.volumes.delete_checkpoint(checkpoint_id).await {
            tracing::error!(
                %checkpoint_id,
                error = %error.message(),
                "an export checkpoint left behind could not be reaped"
            );
        }
    }
    tracing::warn!(
        checkpoints = orphans.len(),
        "export checkpoints left behind were reaped"
    );
}

fn failed(desired: &DesiredExport, checkpoint_id: Option<CheckpointId>, reason: String) -> ReportedExport {
    tracing::warn!(export_id = %desired.export_id, reason = %reason, "export not written");
    ReportedExport {
        export_id: desired.export_id.clone(),
        checkpoint_id,
        state: ExportState::Failed,
        size_bytes: None,
        ready_at: None,
        message: Some(StateMessage::new(reason)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::reconcile::plan::plan_reconcile;
    use crate::test_support::*;
    use protocol::{DesiredPresence, ObjectKey, VolumeId};

    fn wanted(state: DesiredPresence) -> DesiredExport {
        DesiredExport {
            export_id: ExportId::parse("exp-1").unwrap(),
            app_id: app_id(),
            volume_id: VolumeId::parse("vol-1").unwrap(),
            object_key: ObjectKey::parse("exports/exp-1/bundle.tar.gz").unwrap(),
            environment: None,
            desired_state: state,
        }
    }

    async fn asked_for(host: &crate::host::Host, exports: Vec<DesiredExport>) -> ReconcilePlan {
        let desired = desired_state(|state| state.exports = exports);
        host.cache.lock().await.accept(desired.clone());
        let observed = observe_exports(host).await;
        plan_reconcile(&desired, &observed_state(|state| state.exports = observed))
    }

    #[tokio::test]
    async fn the_exported_crontab_comes_from_the_apps_durable_registration() {
        let mut host = test_host().await;
        let second_app = AppId::parse("app-2").unwrap();
        host.cron
            .synchronize(&[(app_id(), deployment_id()), (second_app.clone(), deployment_id())])
            .await
            .unwrap();
        let after = "2026-10-01T00:00:00Z".parse().unwrap();
        let text = "# cleanup\r\nTOKEN=\"a secret\"\r\n@hourly echo \"$TOKEN\"  ";
        host.cron
            .replace(&app_id(), &deployment_id(), text, after)
            .await
            .unwrap();
        host.cron
            .replace(&second_app, &deployment_id(), "@daily echo another app", after)
            .await
            .unwrap();

        let restored = crate::domain::cron::registry::CronRegistry::new(
            host.repositories.cron.clone(),
            host.config.cron.max_jobs_per_app,
            host.config.cron.time_zone,
        );
        std::sync::Arc::get_mut(&mut host.host).unwrap().cron = std::sync::Arc::new(restored);
        assert_eq!(
            registered_crontab(host.arc(), &app_id())
                .await
                .unwrap()
                .unwrap()
                .expose(),
            text
        );
    }

    #[tokio::test]
    async fn an_app_with_no_registered_jobs_exports_no_crontab() {
        let host = test_host().await;
        assert!(registered_crontab(host.arc(), &app_id()).await.unwrap().is_none());

        host.cron
            .synchronize(&[(app_id(), deployment_id())])
            .await
            .unwrap();
        assert!(registered_crontab(host.arc(), &app_id()).await.unwrap().is_none());

        for text in ["", "# no jobs\n", "TOKEN=unused\n"] {
            host.cron
                .replace(
                    &app_id(),
                    &deployment_id(),
                    text,
                    "2026-10-01T00:00:00Z".parse().unwrap(),
                )
                .await
                .unwrap();
            assert!(registered_crontab(host.arc(), &app_id()).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn an_unreadable_cron_registry_fails_the_export_before_cutting_a_checkpoint() {
        let mut host = test_host().await;
        let mut repository = crate::repositories::cron_repository::MockCronRepository::new();
        repository.expect_all().returning(|| {
            Err(crate::domain::store::StoreError::Unreadable(
                "registry unavailable".into(),
            ))
        });
        let mut volumes = crate::adapters::volumes::MockVolumeBackend::new();
        volumes.expect_observe_checkpoints().returning(Vec::new);
        volumes.expect_flush().never();
        volumes.expect_create_checkpoint().never();
        let held = std::sync::Arc::get_mut(&mut host.host).unwrap();
        held.cron = std::sync::Arc::new(crate::domain::cron::registry::CronRegistry::new(
            std::sync::Arc::new(repository),
            held.config.cron.max_jobs_per_app,
            held.config.cron.time_zone,
        ));
        held.volumes = std::sync::Arc::new(volumes);
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Present)]).await;

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        let reported = &host.state.snapshot().await.export_reports[0];
        assert_eq!(reported.state, ExportState::Failed);
        assert!(reported
            .message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("registry unavailable"));
        assert!(host.exports_written().is_empty());
    }

    #[test]
    fn an_orphan_says_who_owned_it_without_anything_having_written_that_down() {
        let export_id = ExportId::parse("exp-1").unwrap();
        let checkpoint_id = export_checkpoint_id(&export_id).unwrap();
        assert_eq!(checkpoint_id.as_str(), "export-exp-1");
        assert!(is_export_checkpoint(&checkpoint_id));
        assert!(!is_export_checkpoint(&CheckpointId::parse("nightly-1").unwrap()));
    }

    #[tokio::test]
    async fn a_host_that_cannot_pin_a_view_reports_the_export_failed_rather_than_reading_the_live_disk() {
        let host = test_host().await;
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Present)]).await;

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        let reports = host.state.snapshot().await.export_reports;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, ExportState::Failed);
        assert!(host.exports_written().is_empty());
    }

    #[tokio::test]
    async fn forgetting_an_export_drops_the_note_and_nothing_else() {
        let host = test_host().await;
        host.state
            .modify(|snapshot| {
                snapshot.export_reports = vec![ReportedExport {
                    export_id: ExportId::parse("exp-1").unwrap(),
                    checkpoint_id: None,
                    state: ExportState::Ready,
                    size_bytes: Some(10),
                    ready_at: None,
                    message: None,
                }];
            })
            .await;
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Absent)]).await;

        apply_exports(host.arc(), &plan, Trigger::Change).await;
        assert!(host.state.snapshot().await.export_reports.is_empty());
    }

    #[tokio::test]
    async fn an_export_omitted_from_the_document_disappears_from_the_next_report() {
        for state in [ExportState::Ready, ExportState::Failed] {
            let host = test_host().await;
            let retained = ReportedExport {
                export_id: ExportId::parse("exp-2").unwrap(),
                ..note(ExportState::Ready)
            };
            holding(host.arc(), vec![note(state), retained.clone()]).await;
            let desired = desired_state(|desired| {
                desired.exports = vec![DesiredExport {
                    export_id: retained.export_id.clone(),
                    ..wanted(DesiredPresence::Present)
                }];
            });
            host.cache.lock().await.accept(desired.clone());
            let observed = crate::domain::reconcile::observe(host.arc(), &desired).await;
            let plan = plan_reconcile(&desired, &observed);

            apply_exports(host.arc(), &plan, Trigger::Tick).await;

            let versions = crate::domain::report::versions::compiled_versions("v1.16.1", "6.1.180-test");
            let reported = crate::domain::report::writer::build(host.arc(), versions).await;
            assert_eq!(reported.exports, vec![retained]);
            assert!(host.exports_written().is_empty());
        }
    }

    #[tokio::test]
    async fn an_export_this_host_has_already_written_is_not_written_twice() {
        let host = test_host().await;
        let desired = desired_state(|state| state.exports = vec![wanted(DesiredPresence::Present)]);
        host.state
            .modify(|snapshot| {
                snapshot.export_reports = vec![ReportedExport {
                    export_id: ExportId::parse("exp-1").unwrap(),
                    checkpoint_id: None,
                    state: ExportState::Ready,
                    size_bytes: Some(10),
                    ready_at: None,
                    message: None,
                }];
            })
            .await;

        let observed = observe_exports(host.arc()).await;
        assert_eq!(observed.len(), 1);
        assert!(observed[0].written);

        let plan = plan_reconcile(
            &desired,
            &observed_state(|state| state.exports = observed.clone()),
        );
        assert!(matches!(plan.exports[0], ExportPlan::None { .. }));
    }

    async fn host_over(volumes: crate::adapters::volumes::MockVolumeBackend) -> TestHost {
        let mut host = test_host().await;
        std::sync::Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .volumes = std::sync::Arc::new(volumes);
        host
    }

    fn note(state: ExportState) -> ReportedExport {
        ReportedExport {
            export_id: ExportId::parse("exp-1").unwrap(),
            checkpoint_id: None,
            state,
            size_bytes: Some(10),
            ready_at: None,
            message: None,
        }
    }

    async fn holding(host: &crate::host::Host, reports: Vec<ReportedExport>) {
        host.state
            .modify(|snapshot| snapshot.export_reports = reports)
            .await;
    }

    #[tokio::test]
    async fn an_export_id_that_cannot_name_a_checkpoint_is_refused_before_the_tenant_is_frozen() {
        let unusable = ExportId::parse("e".repeat(63)).unwrap();
        assert!(export_checkpoint_id(&unusable).is_none());

        let host = test_host().await;
        let plan = ReconcilePlan {
            exports: vec![ExportPlan::Write {
                desired: DesiredExport {
                    export_id: unusable,
                    ..wanted(DesiredPresence::Present)
                },
            }],
            ..Default::default()
        };

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        let reports = host.state.snapshot().await.export_reports;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, ExportState::Failed);
        assert_eq!(reports[0].checkpoint_id, None);
        assert!(reports[0]
            .message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("does not make a checkpoint name"));
        assert!(host.exports_written().is_empty());
    }

    #[tokio::test]
    async fn a_bundle_the_document_still_wants_leaves_one_note_rather_than_two() {
        let host = test_host().await;
        holding(host.arc(), vec![note(ExportState::Failed)]).await;
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Present)]).await;

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        let reports = host.state.snapshot().await.export_reports;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, ExportState::Failed);
    }

    #[tokio::test]
    async fn a_bundle_that_failed_is_left_for_the_document_to_ask_for_again_rather_than_tried_every_tick() {
        let host = test_host().await;
        let failed = ReportedExport {
            message: Some(StateMessage::new("the store refused it")),
            ..note(ExportState::Failed)
        };
        holding(host.arc(), vec![failed.clone()]).await;
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Present)]).await;
        assert!(matches!(plan.exports[0], ExportPlan::Write { .. }));

        apply_exports(host.arc(), &plan, Trigger::Tick).await;

        assert_eq!(
            host.state.snapshot().await.export_reports,
            vec![failed],
            "the note, and the reason on it, stand as they were"
        );
        assert!(host.exports_written().is_empty());
        let page = crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        );
        assert!(
            !page.contains("operation=\"export_write\",outcome=\"failed\"} 1\n"),
            "nothing was tried, so nothing was counted: {page}"
        );
    }

    #[tokio::test]
    async fn an_export_already_written_keeps_the_note_that_says_so() {
        let host = test_host().await;
        holding(host.arc(), vec![note(ExportState::Ready)]).await;
        let plan = ReconcilePlan {
            exports: vec![ExportPlan::None {
                export_id: ExportId::parse("exp-1").unwrap(),
            }],
            ..Default::default()
        };

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        let reports = host.state.snapshot().await.export_reports;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, ExportState::Ready);
        assert!(host.exports_written().is_empty());
    }

    #[tokio::test]
    async fn a_bundle_that_failed_is_not_a_bundle_this_host_has_written() {
        let host = test_host().await;
        holding(host.arc(), vec![note(ExportState::Failed)]).await;

        let observed = observe_exports(host.arc()).await;
        assert_eq!(observed.len(), 1);
        assert!(!observed[0].written);
    }

    #[tokio::test]
    async fn an_export_checkpoint_nothing_is_writing_any_more_is_reaped() {
        let mut volumes = crate::adapters::volumes::MockVolumeBackend::new();
        volumes.expect_observe_checkpoints().returning(|| {
            vec![
                CheckpointId::parse("export-exp-9").unwrap(),
                CheckpointId::parse("nightly-1").unwrap(),
            ]
        });
        volumes
            .expect_delete_checkpoint()
            .times(1)
            .withf(|held| held.as_str() == "export-exp-9")
            .returning(|_| Ok(()));
        let host = host_over(volumes).await;

        apply_exports(host.arc(), &ReconcilePlan::default(), Trigger::Change).await;
    }

    #[tokio::test]
    async fn the_store_is_flushed_before_the_cut_so_the_checkpoint_holds_the_frozen_filesystem() {
        let mut sequence = mockall::Sequence::new();
        let mut volumes = crate::adapters::volumes::MockVolumeBackend::new();
        volumes.expect_observe_checkpoints().returning(Vec::new);
        volumes
            .expect_flush()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|| Ok(()));
        // Refused here only to end the pass: what is under test is that the flush came first.
        volumes
            .expect_create_checkpoint()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| {
                Err(crate::adapters::volumes::VolumeError::NoCheckpoints {
                    what: "a volume kept as a file on this host's own disk",
                })
            });
        let host = host_over(volumes).await;
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Present)]).await;

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        assert_eq!(
            host.state.snapshot().await.export_reports[0].state,
            ExportState::Failed
        );
    }

    #[tokio::test]
    async fn a_store_that_would_not_flush_is_not_checkpointed_over() {
        let mut volumes = crate::adapters::volumes::MockVolumeBackend::new();
        volumes.expect_observe_checkpoints().returning(Vec::new);
        volumes.expect_flush().returning(|| {
            Err(crate::adapters::volumes::VolumeError::Unusable(
                "the store would not flush".to_string(),
            ))
        });
        // A cut over a store that would not settle is of a moment nobody asked for.
        volumes.expect_create_checkpoint().never();
        let host = host_over(volumes).await;
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Present)]).await;

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        let reported = &host.state.snapshot().await.export_reports[0];
        assert_eq!(reported.state, ExportState::Failed);
        assert!(
            reported
                .message
                .as_ref()
                .unwrap()
                .as_str()
                .contains("would not flush"),
            "{:?}",
            reported.message
        );
    }

    #[tokio::test]
    async fn the_checkpoint_for_an_export_this_pass_is_writing_is_left_where_it_is() {
        let mut volumes = crate::adapters::volumes::MockVolumeBackend::new();
        volumes
            .expect_observe_checkpoints()
            .returning(|| vec![CheckpointId::parse("export-exp-1").unwrap()]);
        volumes.expect_flush().returning(|| Ok(()));
        volumes.expect_create_checkpoint().returning(|_| {
            Err(crate::adapters::volumes::VolumeError::NoCheckpoints {
                what: "a volume kept as a file on this host's own disk",
            })
        });
        volumes.expect_delete_checkpoint().never();
        let host = host_over(volumes).await;
        let plan = asked_for(host.arc(), vec![wanted(DesiredPresence::Present)]).await;

        apply_exports(host.arc(), &plan, Trigger::Change).await;

        assert_eq!(
            host.state.snapshot().await.export_reports[0].state,
            ExportState::Failed
        );
    }
}
