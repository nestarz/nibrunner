use std::collections::BTreeSet;
use std::sync::Arc;

use protocol::{
    AppId, DesiredInstance, DesiredInstanceState, InstanceState, StateMessage, TenantExit, VolumeId,
    VolumeState,
};

use crate::adapters::vm::{VmExit, VmStatus, UNKNOWN_VM};
use crate::clock::{now_ms, now_timestamp};
use crate::domain::activation::SleepReason;
use crate::domain::backoff::{is_ready_to_retry, next_attempt_window, BackoffPolicy, NO_START_ATTEMPTS};
use crate::domain::health::{
    apply_probe, asks_the_port, describe_instance_failure, describe_unhealthy_instance,
    evaluate_instance_state, initial_tracker, next_probe_delay_ms, HealthTracker, LifecycleInputs,
};
use crate::domain::metrics::converge;
use crate::domain::metrics::health::Failure;
use crate::domain::metrics::resources::Operation;
use crate::domain::metrics::sleep_wake::SleepOutcome;
use crate::domain::reconcile::plan::{InstancePlan, ReconcilePlan};
use crate::domain::report::instance_record::{InstanceRecord, RecordFields};
use crate::host::Host;
use crate::ports::{BootRequest, SuspendRequest, VmError, WakeFailure, WakeOutcome, WakeRefusal, Writable};

/// The host ports a slot hands this app's extra ports, in the order the document named them.
///
/// Index 0 of the slot is the HTTP port, so these start at 1. A port the slot has no room for is
/// left out rather than folded onto another app's: the document is refused before it reaches here.
fn record_ports(
    desired: &DesiredInstance,
    slot: &nft_render::AppSlot,
) -> Vec<crate::domain::report::instance_record::RecordPort> {
    desired
        .config
        .ports
        .iter()
        .enumerate()
        .filter_map(|(index, port)| {
            Some(crate::domain::report::instance_record::RecordPort {
                name: port.name.clone(),
                host_port: slot.host_port_at(u32::try_from(index).ok()? + 1)?,
                guest_port: port.guest_port,
            })
        })
        .collect()
}

fn record_fields(desired: &DesiredInstance, slot: &nft_render::AppSlot) -> RecordFields {
    RecordFields {
        app_id: desired.app_id.clone(),
        deployment_id: desired.deployment_id.clone(),
        volume_id: desired.volume_id.clone(),
        hostnames: desired.hostnames.clone(),
        host_port: slot.host_port,
        http_port: desired.config.http_port,
        ports: record_ports(desired, slot),
        guest_ipv4: slot.guest_ipv4.clone(),
        layer_digests: desired
            .layers
            .iter()
            .map(|layer| layer.digest().clone())
            .collect(),
        health_check: desired.config.health_check.clone(),
        resources: desired.config.resources,
        restart_policy: desired.config.restart_policy.clone(),
        desired_running: desired.desired_state != DesiredInstanceState::Stopped,
        on_request: desired.desired_state == DesiredInstanceState::OnRequest,
        expiry: desired.expiry.clone(),
    }
}

async fn settled(host: &Host, app_id: &AppId, reason: &str) {
    if let Err(error) = host.volumes.flush().await {
        tracing::warn!(%app_id, reason, error = %error.message(), "stopping a guest whose disk would not flush");
    }
}

pub async fn stop_instance(host: &Host, app_id: &AppId, reason: &str) {
    host.state
        .update_record(app_id, |record| {
            record.state = InstanceState::Stopping;
            record.stop_requested = true;
        })
        .await;
    settled(host, app_id, reason).await;
    match host.vms.stop(app_id).await {
        Ok(()) => tracing::info!(%app_id, reason, "instance stopped"),
        Err(error) => tracing::error!(%app_id, reason, error = %error.message(), "instance stop failed"),
    }
    host.state
        .update_record(app_id, |record| {
            record.state = InstanceState::Stopped;
            record.start_attempts = NO_START_ATTEMPTS;
        })
        .await;
}

/// Puts the microVM to sleep and says how that went; nothing when there was no microVM to put
/// down, or no slot for it to come back to and it was stopped instead.
pub(super) async fn suspend_owned(
    host: Arc<Host>,
    app: AppId,
    why: SleepReason,
    transition: tokio::sync::OwnedMutexGuard<()>,
) -> Option<SleepOutcome> {
    // Cancelling a pass cannot cancel Firecracker's snapshot write or free its reserved memory.
    tokio::spawn(async move {
        let _transition = crate::state::InstanceTransition::new(host.state.clone(), app.clone(), transition);
        let outcome = suspend_instance(&host, &app, why).await;
        host.state.mark_snapshotting(&app, false).await;
        outcome
    })
    .await
    .unwrap_or_else(|error| {
        tracing::error!(%error, "snapshot task did not complete");
        None
    })
}

pub async fn suspend_instance(host: &Host, app_id: &AppId, why: SleepReason) -> Option<SleepOutcome> {
    let reason = why.as_str();
    let record = host.state.record(app_id).await?;
    if record.expired_at_ms.is_some() {
        return None;
    }
    let Some(slot) = host.slot_of(app_id).await else {
        stop_instance(host, app_id, reason).await;
        return None;
    };

    // Full snapshots fault in the guest's complete memory, even when its paused working set is small.
    // Do not reclaim recursively: the caller may already be reclaiming another app's reservation.
    let _memory = match host
        .reserve_memory_once(
            app_id,
            record.resources,
            crate::domain::memory_admission::MemoryOperation::Snapshot,
        )
        .await
    {
        Ok(reservation) => reservation,
        Err(shortfall_mib) => {
            tracing::info!(%app_id, shortfall_mib, "snapshot waits for memory");
            host.metrics.sleep_wake.slept(
                app_id,
                why,
                SleepOutcome::Refused,
                std::time::Duration::ZERO,
                std::time::Duration::ZERO,
            );
            return Some(SleepOutcome::Refused);
        }
    };

    host.state.mark_snapshotting(app_id, true).await;
    // The port goes to the activator before the guest is paused, not once the batch is done: a
    // host port still pointing at a paused guest connects to nothing for as long as the snapshot
    // takes, where one the activator holds keeps the request until the guest is restored. That
    // is the whole ruleset loaded again, the only way the firewall has of dropping one forward;
    // the sleeps running side by side render the same ruleset once each has marked itself, and
    // loading the ruleset already in place costs nothing.
    crate::domain::reconcile::network::apply_network(host).await;
    // That handover reaches only the connections still to be opened, so what the proxy already
    // holds into this guest goes down with it.
    host.router.close_connections_to(app_id).await;
    let started = std::time::Instant::now();
    settled(host, app_id, reason).await;
    let flushed = started.elapsed();
    let outcome = host
        .vms
        .sleep(SuspendRequest {
            app_id: app_id.clone(),
            deployment_id: record.deployment_id.clone(),
            slot,
        })
        .await;
    let snapshotted = started.elapsed() - flushed;
    let outcome = match outcome {
        Ok(()) => {
            host.state
                .update_record(app_id, |record| {
                    record.state = InstanceState::Idle;
                    record.stop_requested = true;
                    record.start_attempts = NO_START_ATTEMPTS;
                    record.message = None;
                })
                .await;
            tracing::info!(
                %app_id,
                reason,
                flushed_ms = flushed.as_millis(),
                snapshotted_ms = snapshotted.as_millis(),
                "app put to sleep"
            );
            SleepOutcome::Slept
        }
        Err(VmError::SleepRefused { reason: refusal }) => {
            tracing::warn!(%app_id, reason, refusal, "this microVM may not be snapshotted, so it stays up");
            SleepOutcome::Refused
        }
        Err(error) => {
            tracing::warn!(%app_id, reason, error = %error.message(), "this microVM would not sleep; leaving it up");
            SleepOutcome::Failed
        }
    };
    host.metrics
        .sleep_wake
        .slept(app_id, why, outcome, flushed, snapshotted);
    host.state.mark_snapshotting(app_id, false).await;
    if outcome != SleepOutcome::Slept {
        // The guest is still up, and its port was given away for a sleep that did not happen.
        crate::domain::reconcile::network::apply_network(host).await;
    }
    Some(outcome)
}

// An attempt is a boot, whether the last one failed on the way up or came up and exited later,
// and the window is the policy's: a backoff between attempts, a budget of them, and a reset after
// `resetAfterMs`. A guest that exited keeps its window unless it stayed up for that long, in
// which case the status loop gave the window back when it saw the exit, and the boot after it is
// a first boot again.
//
// A start is refused for two very different reasons, and an instance waiting out its backoff read
// exactly like one that is never starting again: failed, with the message from the last time it
// worked. Only one of them is something an operator can do anything about.
enum StartRefused {
    OutOfRestarts { attempted: u32, allowed: u32 },
    UntilTheBackoffIsDone,
}

fn start_refused(
    existing: Option<&InstanceRecord>,
    now_ms: i64,
    desired: &DesiredInstance,
) -> Option<StartRefused> {
    let existing = existing?;
    let policy = &desired.config.restart_policy;
    if existing.start_attempts.attempts > policy.max_restarts {
        return Some(StartRefused::OutOfRestarts {
            attempted: existing.start_attempts.attempts,
            allowed: policy.max_restarts,
        });
    }
    (!is_ready_to_retry(&existing.start_attempts, now_ms, &BackoffPolicy::from(policy)))
        .then_some(StartRefused::UntilTheBackoffIsDone)
}

// The attempts are what the decision is made on, and restartCount is not that number — it counts
// the tenant's restarts inside the guest. So the sentence carries both, because an instance out
// of restarts is otherwise indistinguishable from one that has never been restarted at all.
async fn say_it_is_out_of_restarts(host: &Host, app_id: &AppId, attempted: u32, allowed: u32) {
    let said = StateMessage::new(format!(
        "out of restarts: {attempted} starts attempted against a budget of {allowed}, and this instance will not be started again until it is deployed afresh"
    ));
    if say(host, app_id, said).await {
        host.metrics.health.failed(app_id, Failure::OutOfRestarts);
    }
}

/// Puts a sentence on the record, and says whether it is news. The status loop passes every
/// second, and a record written every second is a write per second that says what the last one
/// did — and a failure counted every second is one failure counted for ever.
async fn say(host: &Host, app_id: &AppId, said: StateMessage) -> bool {
    let mut news = false;
    host.state
        .update_record(app_id, |record| {
            news = record.message.as_ref() != Some(&said);
            if news {
                record.message = Some(said);
            }
        })
        .await;
    news
}

/// Why the volume this instance would boot onto cannot be booted onto, as the pass over the
/// volumes just reported it; nothing for a volume that is ready, or that this host has no report
/// of yet, which the attach below answers for itself.
async fn volume_refusal(host: &Host, volume_id: &VolumeId) -> Option<StateMessage> {
    host.state
        .snapshot()
        .await
        .volume_reports
        .into_iter()
        .find(|report| &report.volume_id == volume_id && report.state == VolumeState::Failed)
        .map(|report| {
            report
                .message
                .unwrap_or_else(|| StateMessage::new(format!("{volume_id} could not be made ready")))
        })
}

/// The device the volume is attached at, or why it could not be.
async fn attach_volume(
    host: &Host,
    desired: &DesiredInstance,
    volume_id: &VolumeId,
) -> Result<String, String> {
    let attaching = std::time::Instant::now();
    let attached = host.volumes.attach(volume_id, &desired.app_id).await;
    host.metrics
        .resources
        .done(Operation::VolumeAttach, attached.is_ok(), attaching.elapsed());
    attached
        .map(|attached| attached.device_path)
        .map_err(|error| error.message())
}

async fn writable_for(host: &Host, desired: &DesiredInstance) -> Result<Writable, String> {
    match (&desired.volume_id, desired.scratch) {
        (Some(volume_id), _) => {
            let device_path = attach_volume(host, desired, volume_id).await?;
            let attached_at = now_ms();
            converge::stamp(&host.state, &desired.app_id, |deploy| {
                deploy.volume_ready_at_ms = Some(attached_at);
            })
            .await;
            Ok(Writable::Volume { device_path })
        }
        (None, Some(scratch)) => Ok(Writable::Scratch(scratch)),
        (None, None) => Err(format!(
            "{} names neither a volume nor a scratch, so its root has nowhere to be written",
            desired.app_id
        )),
    }
}

pub async fn sleep_instance(host: &Host, desired: &DesiredInstance) {
    let Ok(slot) = host.slot_for(&desired.app_id).await else {
        tracing::error!(app_id = %desired.app_id, "this host has no slot left to answer for the app");
        return;
    };
    let fields = record_fields(desired, &slot);
    let existing = host.state.record(&desired.app_id).await;
    match existing {
        Some(mut record) => {
            record.adopt(fields);
            host.state.put_record(record).await;
        }
        None => {
            host.state
                .put_record(InstanceRecord::new(
                    fields,
                    InstanceState::Idle,
                    initial_tracker(),
                ))
                .await;
            tracing::info!(app_id = %desired.app_id, host_port = %slot.host_port, "app is waiting to be asked for");
        }
    }
}

/// A stop leaves a record behind, and it is the record that keeps the app's hostnames answering
/// that it is stopped rather than that it is not here; an app that arrives already stopped is
/// owed the same one.
pub async fn hold_instance(host: &Host, desired: &DesiredInstance) {
    let Ok(slot) = host.slot_for(&desired.app_id).await else {
        tracing::error!(app_id = %desired.app_id, "this host has no slot left to answer for the app");
        return;
    };
    host.state
        .put_record(InstanceRecord::new(
            record_fields(desired, &slot),
            InstanceState::Stopped,
            initial_tracker(),
        ))
        .await;
    tracing::info!(app_id = %desired.app_id, host_port = %slot.host_port, "app is held stopped");
}

/// A start this host has no memory for is left pending with the shortfall on it, and planned
/// again every pass until an app sleeps or stops. Nothing is spent on it: what a guest that
/// exited had spent is given back, as a stop gives it back, and a record pending with no
/// attempt spent is how the capacity sums tell one waiting from a boot in flight. Its
/// `started_at` goes too, or the status loop would read it as that guest having failed rather
/// than this one waiting.
pub async fn wait_for_room(host: &Host, desired: &DesiredInstance, shortfall_mib: u64) {
    wait_to_start(
        host,
        desired,
        StateMessage::new(format!(
            "waiting to be started: its host is {shortfall_mib} MiB short of the memory it needs"
        )),
    )
    .await;
}

pub(super) async fn wait_for_start_slot(host: &Host, desired: &DesiredInstance) {
    wait_to_start(
        host,
        desired,
        StateMessage::new("waiting to be started: another guest is still becoming ready"),
    )
    .await;
}

async fn wait_to_start(host: &Host, desired: &DesiredInstance, message: StateMessage) {
    let Ok(slot) = host.slot_for(&desired.app_id).await else {
        tracing::error!(app_id = %desired.app_id, "this host has no slot left to answer for the app");
        return;
    };
    let fields = record_fields(desired, &slot);
    let mut waiting =
        host.state.record(&desired.app_id).await.unwrap_or_else(|| {
            InstanceRecord::new(fields.clone(), InstanceState::Pending, initial_tracker())
        });
    waiting.adopt(fields);
    waiting.state = InstanceState::Pending;
    waiting.stop_requested = false;
    waiting.started_at = None;
    waiting.start_attempts = NO_START_ATTEMPTS;
    waiting.message = Some(message);
    host.state.put_record(waiting).await;
}

pub async fn start_instance(host: &Host, desired: &DesiredInstance) {
    let now = now_ms();
    let existing = host.state.record(&desired.app_id).await;
    if existing.as_ref().is_some_and(|r| {
        r.expired_at_ms.is_some() && r.deployment_id == desired.deployment_id && desired.expiry.is_some()
    }) {
        return;
    }
    if let Some(refusal) = start_refused(existing.as_ref(), now, desired) {
        if let StartRefused::OutOfRestarts { attempted, allowed } = refusal {
            say_it_is_out_of_restarts(host, &desired.app_id, attempted, allowed).await;
        }
        return;
    }
    let Ok(slot) = host.slot_for(&desired.app_id).await else {
        if say(
            host,
            &desired.app_id,
            StateMessage::new("this host has no slot left"),
        )
        .await
        {
            host.metrics.health.failed(&desired.app_id, Failure::NoSlot);
        }
        return;
    };

    // Before anything is fetched or attached, and before it counts as an attempt: a microVM this
    // host could not have put within reach is a cost paid for an app nobody could have reached,
    // and the document that asked for it is wrong in a way no retry mends. So the window is left
    // alone, the reason stays on the record for every pass that finds it, and it is said once.
    if let Some(refusal) = crate::domain::reconcile::ingress::ingress_refusal(desired, &host.config) {
        let mut refused = existing.unwrap_or_else(|| {
            InstanceRecord::new(
                record_fields(desired, &slot),
                InstanceState::Failed,
                initial_tracker(),
            )
        });
        refused.adopt(record_fields(desired, &slot));
        refused.state = InstanceState::Failed;
        refused.stop_requested = false;
        host.state.put_record(refused).await;
        if say(host, &desired.app_id, StateMessage::new(refusal.clone())).await {
            host.metrics.health.failed(&desired.app_id, Failure::Refused);
            tracing::error!(app_id = %desired.app_id, refusal, "instance refused before it was started");
        }
        return;
    }

    let mut attempted = match existing.clone() {
        Some(record) => record,
        None => InstanceRecord::new(
            record_fields(desired, &slot),
            InstanceState::Pending,
            initial_tracker(),
        ),
    };
    attempted.adopt(record_fields(desired, &slot));
    attempted.stop_requested = false;

    // A volume the pass over the volumes could not make ready (a seed the document names wrongly
    // leaves it attached but bare) is nothing to boot onto: the guest would only die failing to
    // mount it. The fault is the volume's, so no start attempt is spent on it, and the app is
    // started the pass the volume is put right.
    let refused_volume = match &desired.volume_id {
        Some(volume_id) => volume_refusal(host, volume_id).await,
        None => None,
    };
    if let Some(refusal) = refused_volume {
        attempted.state = InstanceState::Failed;
        host.state.put_record(attempted).await;
        if say(host, &desired.app_id, refusal.clone()).await {
            host.metrics.health.failed(&desired.app_id, Failure::Volume);
            tracing::error!(app_id = %desired.app_id, reason = refusal.as_str(), "instance start failed");
        }
        return;
    }

    attempted.start_attempts = next_attempt_window(
        &existing
            .as_ref()
            .map(|record| record.start_attempts)
            .unwrap_or(NO_START_ATTEMPTS),
        now,
        desired.config.restart_policy.reset_after_ms,
    );
    host.state.put_record(attempted.clone()).await;
    host.state.mark_active(&desired.app_id, now).await;

    let booted = match host.payloads.prepare(&desired.layers).await {
        Err(error) => Err((Failure::Layers, error.message())),
        Ok(payload) => {
            let writable = match writable_for(host, desired).await {
                Ok(writable) => writable,
                Err(reason) => {
                    host.state
                        .update_record(&desired.app_id, |record| {
                            record.state = InstanceState::Failed;
                            record.message = Some(StateMessage::new(reason.clone()));
                        })
                        .await;
                    host.metrics.health.failed(&desired.app_id, Failure::Volume);
                    tracing::error!(app_id = %desired.app_id, error = %reason, "instance start failed");
                    return;
                }
            };
            let boot = host
                .vms
                .boot(BootRequest {
                    desired: desired.clone(),
                    slot,
                    writable,
                    payload,
                })
                .await;
            if matches!(boot, Err(VmError::StartBusy)) {
                host.state
                    .update_record(&desired.app_id, |record| {
                        record.state = InstanceState::Pending;
                        record.start_attempts = existing
                            .as_ref()
                            .map_or(NO_START_ATTEMPTS, |prior| prior.start_attempts);
                        record.message = Some(StateMessage::new(VmError::StartBusy.message()));
                    })
                    .await;
                return;
            }
            boot.map_err(|error| (Failure::Boot, error.message()))
        }
    };

    match booted {
        Ok(()) => {
            let started_at = now_timestamp();
            let booted_at = started_at.epoch_ms();
            host.state
                .update_record(&desired.app_id, |record| {
                    record.started_at = Some(started_at);
                    record.state = InstanceState::Starting;
                    record.health = initial_tracker();
                    // A fresh guest starts its tenant for the first time: what the last one
                    // counted was that guest's, and the host's own boots are the attempts.
                    record.restart_count = 0;
                    record.last_restart = None;
                    record.message = None;
                })
                .await;
            converge::stamp(&host.state, &desired.app_id, |deploy| {
                deploy.booted_at_ms = Some(booted_at);
            })
            .await;
            host.state.probe_at_once(&desired.app_id).await;
            tracing::info!(app_id = %desired.app_id, host_port = %attempted.host_port, guest_ipv4 = %attempted.guest_ipv4, "instance started");
        }
        Err((failure, reason)) => {
            host.state
                .update_record(&desired.app_id, |record| {
                    record.state = InstanceState::Failed;
                    record.message = Some(StateMessage::new(reason.clone()));
                })
                .await;
            host.metrics.health.failed(&desired.app_id, failure);
            tracing::error!(app_id = %desired.app_id, attempt = attempted.start_attempts.attempts, reason, "instance start failed");
        }
    }
}

pub async fn resume_instance(host: &Host, desired: &DesiredInstance) -> Result<WakeOutcome, WakeRefusal> {
    let would_not_start = |reason: String| WakeRefusal::Failed {
        kind: WakeFailure::WouldNotStart,
        reason,
    };
    if host
        .state
        .record(&desired.app_id)
        .await
        .is_some_and(|r| r.expired_at_ms.is_some())
    {
        return Err(would_not_start("This revision has expired.".into()));
    }
    let Ok(slot) = host.slot_for(&desired.app_id).await else {
        return Err(would_not_start("this host has no slot left".to_string()));
    };

    let status = host.vms.statuses(std::slice::from_ref(&desired.app_id)).await;
    let status = status.get(&desired.app_id).copied().unwrap_or(UNKNOWN_VM);
    if status.active && !status.frozen {
        return Ok(WakeOutcome::AlreadyRunning);
    }

    // The snapshot carries the guest's page cache, so a guest restored onto a device that no
    // longer answers serves reads from memory and only finds out when it writes. The device is
    // made to answer first — re-attached if it does not — or the wake is refused. A scratch is
    // the hypervisor's own and was never let go of.
    if let Some(volume_id) = &desired.volume_id {
        if let Err(reason) = attach_volume(host, desired, volume_id).await {
            host.state
                .update_record(&desired.app_id, |record| {
                    record.message = Some(StateMessage::new(reason.clone()));
                })
                .await;
            tracing::error!(app_id = %desired.app_id, reason, "wake refused: the volume is not usable");
            return Err(WakeRefusal::Failed {
                kind: WakeFailure::VolumeUnusable,
                reason,
            });
        }
    }

    host.state.mark_active(&desired.app_id, now_ms()).await;

    if status.active && status.frozen {
        host.vms
            .thaw(&desired.app_id)
            .await
            .map_err(|error| would_not_start(error.message()))?;
        host.state
            .update_record(&desired.app_id, |record| {
                record.state = InstanceState::Starting;
                record.started_at = Some(now_timestamp());
                record.stop_requested = false;
                record.message = None;
            })
            .await;
        host.state.mark_active(&desired.app_id, now_ms()).await;
        host.state.probe_at_once(&desired.app_id).await;
        return Ok(WakeOutcome::Thawed);
    }
    let request = SuspendRequest {
        app_id: desired.app_id.clone(),
        deployment_id: desired.deployment_id.clone(),
        slot,
    };
    match host.vms.wake(request).await {
        Ok(()) => {
            let started_at = now_timestamp();
            host.state
                .update_record(&desired.app_id, |record| {
                    record.state = InstanceState::Starting;
                    record.started_at = Some(started_at);
                    record.stop_requested = false;
                    record.message = None;
                })
                .await;
            host.state.probe_at_once(&desired.app_id).await;
            Ok(WakeOutcome::Restored)
        }
        Err(VmError::SnapshotUnusable { reason }) => {
            tracing::info!(app_id = %desired.app_id, reason, "nothing to wake this app from; booting it instead");
            start_instance(host, desired).await;
            Ok(WakeOutcome::ColdBoot)
        }
        Err(error) => {
            let reason = error.message();
            host.state
                .update_record(&desired.app_id, |record| {
                    record.message = Some(StateMessage::new(reason.clone()));
                })
                .await;
            Err(would_not_start(reason))
        }
    }
}

/// How the tenant of an instance given no restarts ended, read off the console of a guest that
/// went down by itself. Read once: the state it settles the record in is kept, and a guest that
/// said nothing of it is failed, as one with a budget would be.
async fn ran_once(host: &Host, record: &InstanceRecord, status: &VmStatus) -> Option<TenantExit> {
    let went_down_by_itself = !status.active && record.started_at.is_some() && !record.stop_requested;
    let settled = matches!(record.state, InstanceState::Exited | InstanceState::Failed);
    if record.restart_policy.max_restarts != 0 || !went_down_by_itself || settled {
        return None;
    }
    host.vms
        .guest_verdict(&record.app_id)
        .await
        .as_deref()
        .and_then(guest_contract::control::ran_once_exit)
}

/// The sentence an unhealthy instance carries, and nothing for any other state.
fn unwell(state: InstanceState, health: &HealthTracker, record: &InstanceRecord) -> Option<StateMessage> {
    (state == InstanceState::Unhealthy)
        .then(|| StateMessage::new(describe_unhealthy_instance(health, &record.health_check)))
}

async fn verdict(
    host: &Host,
    state: InstanceState,
    status: &VmStatus,
    health: &HealthTracker,
    record: &InstanceRecord,
) -> Option<StateMessage> {
    if state != InstanceState::Failed {
        return unwell(state, health, record);
    }
    let guest_verdict = if status.active || record.started_at.is_none() {
        None
    } else {
        host.vms.guest_verdict(&record.app_id).await
    };
    Some(StateMessage::new(describe_instance_failure(
        status,
        health,
        &record.health_check,
        record.http_port,
        guest_verdict.as_deref(),
    )))
}

pub async fn refresh_states(host: &Arc<Host>) {
    let app_ids: Vec<_> = host
        .state
        .records()
        .await
        .into_iter()
        .map(|record| record.app_id)
        .collect();
    let mut settling = Vec::new();
    for app_id in app_ids {
        let host = host.clone();
        settling.push(tokio::spawn(async move {
            let Some(_transition) = host.state.try_transition(&app_id) else {
                return;
            };
            let snapshot = host.state.snapshot().await;
            if snapshot.snapshotting.contains(&app_id) {
                return;
            }
            let Some(record) = snapshot.records.get(&app_id).cloned() else {
                return;
            };
            let statuses = host.vms.statuses(std::slice::from_ref(&app_id)).await;
            let status = statuses.get(&app_id).copied().unwrap_or(UNKNOWN_VM);
            let now = now_ms();
            let due = now >= snapshot.next_probe_at_ms.get(&app_id).copied().unwrap_or(0);
            settle(&host, record, status, due, now).await;
        }));
    }
    for task in settling {
        let _ = task.await;
    }
}

async fn settle(host: &Arc<Host>, record: InstanceRecord, status: VmStatus, due: bool, now_ms: i64) {
    if record.expired_at_ms.is_some() {
        return;
    }
    let health = if status.active && !status.frozen && due {
        let probed = std::time::Instant::now();
        let outcome = if asks_the_port(&record.health, &record.health_check) {
            crate::domain::health::probe::probe_instance(
                &record.guest_ipv4,
                record.http_port,
                &record.health_check,
            )
            .await
        } else {
            // A boot-completed tenant found listening is asked nothing more: that its microVM is
            // still up is the whole of what this host reads about it after that.
            Ok(())
        };
        host.metrics
            .health
            .probed(&record.app_id, outcome.is_ok(), probed.elapsed());
        let delay = next_probe_delay_ms(&record.health, &record.grace_inputs(now_ms));
        host.state
            .modify(|snapshot| {
                snapshot
                    .next_probe_at_ms
                    .insert(record.app_id.clone(), now_ms + delay as i64);
            })
            .await;
        apply_probe(
            &record.health,
            outcome,
            &now_timestamp(),
            record.health_check.probe().healthy_threshold,
        )
    } else {
        record.health.clone()
    };

    let ran_once = ran_once(host, &record, &status).await;
    let state = evaluate_instance_state(&LifecycleInputs {
        unit: &status,
        tracker: &health,
        health_check: &record.health_check,
        desired_running: record.desired_running,
        on_request: record.on_request,
        stop_requested: record.stop_requested,
        snapshotting: false,
        started_at_ms: record.started_at.as_ref().map(protocol::Timestamp::epoch_ms),
        now_ms,
        current: record.state,
        ran_once,
    });

    if state == record.state {
        // An instance that stays unhealthy keeps its sentence current — the count grows and how
        // the probes fail can change — but only a probe changes it, and the status loop passes
        // every second.
        let said = unwell(state, &health, &record);
        host.state
            .update_record(&record.app_id, |latest| {
                latest.health = health;
                if said.is_some() && latest.message != said {
                    latest.message = said;
                }
            })
            .await;
        return;
    }
    match state {
        InstanceState::Failed if status.active => {
            host.metrics.health.failed(&record.app_id, Failure::NeverAnswered)
        }
        InstanceState::Failed => host.metrics.health.failed(&record.app_id, Failure::Exited),
        InstanceState::Unhealthy => host.metrics.health.went_unhealthy(&record.app_id),
        _ => {}
    }
    let message = match ran_once {
        Some(exit) => Some(StateMessage::new(guest_contract::control::ran_once(exit))),
        None => verdict(host, state, &status, &health, &record).await,
    };
    let exited = state == InstanceState::Failed && !status.active;
    // A guest that stayed up for the policy's resetAfterMs has earned its app a fresh window: the
    // boot after it owes no backoff and has the whole budget. One that exited sooner keeps its
    // window, and the boot after it is the next attempt in it.
    let stayed_up = exited
        && record
            .started_at
            .as_ref()
            .is_some_and(|at| now_ms - at.epoch_ms() >= record.restart_policy.reset_after_ms as i64);
    host.state
        .update_record(&record.app_id, |latest| {
            if latest.expired_at_ms.is_some() {
                return;
            }
            latest.health = health;
            latest.state = state;
            if let Some(exit) = ran_once {
                latest.last_exit_code = Some(exit.status());
            } else if let Some(exit) = status.exit.filter(|_| !status.active) {
                // A microVM killed under a signal has no exit code, and the code of an earlier
                // exit is not this one's.
                latest.last_exit_code = match exit {
                    VmExit::Code(code) => Some(code),
                    VmExit::Signal(_) => None,
                };
            }
            if stayed_up {
                latest.start_attempts = NO_START_ATTEMPTS;
            }
            latest.message = message;
        })
        .await;
    let answered_ms = (state == InstanceState::Running)
        .then(|| record.started_at.as_ref().map(|at| now_ms - at.epoch_ms()))
        .flatten();
    tracing::info!(
        app_id = %record.app_id,
        from = record.state.as_str(),
        to = state.as_str(),
        answered_ms,
        "instance state changed"
    );
    host.state.signal_report();
}

fn starting(plan: &ReconcilePlan) -> impl Iterator<Item = &DesiredInstance> {
    plan.instances.iter().filter_map(|action| match action {
        InstancePlan::Start { desired }
        | InstancePlan::Replace { desired }
        | InstancePlan::Recover { desired } => Some(desired),
        _ => None,
    })
}

pub fn layers_to_start(plan: &ReconcilePlan) -> Vec<protocol::DesiredLayer> {
    layers_of(starting(plan))
}

fn layers_of<'a>(instances: impl Iterator<Item = &'a DesiredInstance>) -> Vec<protocol::DesiredLayer> {
    let mut seen = BTreeSet::new();
    instances
        .flat_map(|desired| &desired.layers)
        .filter(|layer| seen.insert((*layer).clone()))
        .cloned()
        .collect()
}

pub async fn prefetch_layers(host: &Host, plan: &ReconcilePlan) {
    // An app the start below will refuse is not fetched for either: nothing it needs is worth
    // having, and a pass that finds the same refusal standing would otherwise look it up again.
    let mut waiting: Vec<&DesiredInstance> = starting(plan)
        .filter(|desired| crate::domain::reconcile::ingress::ingress_refusal(desired, &host.config).is_none())
        .collect();
    let mut prepared = BTreeSet::new();
    for layer in layers_of(waiting.iter().copied()) {
        let fetching = std::time::Instant::now();
        match host.payloads.prepare(std::slice::from_ref(&layer)).await {
            Ok(payload) if payload.fetched_bytes > 0 => {
                host.metrics
                    .resources
                    .layer_fetched(payload.fetched_bytes, fetching.elapsed());
                prepared.insert(layer);
            }
            Ok(_) => {
                host.metrics.resources.layer_cached();
                prepared.insert(layer);
            }
            Err(error) => {
                host.metrics
                    .resources
                    .done(Operation::LayerFetch, false, fetching.elapsed());
                tracing::warn!(digest = %layer.digest(), error = %error.message(), "layer prefetch failed");
            }
        }
        let ready_at = now_ms();
        let (ready, still_waiting): (Vec<_>, Vec<_>) = waiting
            .into_iter()
            .partition(|desired| desired.layers.iter().all(|layer| prepared.contains(layer)));
        waiting = still_waiting;
        for desired in ready {
            converge::stamp(&host.state, &desired.app_id, |deploy| {
                deploy.layers_ready_at_ms = Some(ready_at);
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[test]
    fn one_image_is_fetched_per_layer_however_many_apps_deploy_it() {
        let same = layer(|_| {});
        let plan = ReconcilePlan {
            instances: vec![
                InstancePlan::Start {
                    desired: desired_instance(|instance| instance.layers = vec![base_layer(), same.clone()]),
                },
                InstancePlan::Replace {
                    desired: desired_instance(|instance| {
                        instance.app_id = AppId::parse("app-2").unwrap();
                        instance.layers = vec![base_layer(), same.clone()];
                    }),
                },
                InstancePlan::Stop {
                    app_id: app_id(),
                    reason: crate::domain::reconcile::InstanceStopReason::Idle,
                },
                InstancePlan::Sleep {
                    desired: desired_instance(|_| {}),
                },
            ],
            ..Default::default()
        };
        let wanted = layers_to_start(&plan);
        assert_eq!(wanted, vec![base_layer(), same.clone()]);
        let whole = protocol::DesiredLayer::Filesystem {
            object: same.stored_object().unwrap().clone(),
        };
        let same_bytes_twice = layers_to_start(&ReconcilePlan {
            instances: vec![InstancePlan::Start {
                desired: desired_instance(|instance| instance.layers = vec![whole.clone(), same.clone()]),
            }],
            ..Default::default()
        });
        assert_eq!(
            same_bytes_twice,
            vec![whole, same],
            "one digest as two kinds is two images"
        );
        assert!(layers_to_start(&ReconcilePlan {
            instances: vec![InstancePlan::Sleep {
                desired: desired_instance(|_| {})
            }],
            ..Default::default()
        })
        .is_empty());
    }

    #[test]
    fn a_start_is_refused_while_its_backoff_still_has_time_to_run() {
        let desired = desired_instance(|_| {});
        assert!(start_refused(None, 0, &desired).is_none());
        let spent = instance_record(|record| {
            record.start_attempts = crate::domain::backoff::AttemptWindow {
                attempts: 3,
                last_attempt_at_ms: Some(0),
            };
        });
        assert!(matches!(
            start_refused(Some(&spent), 0, &desired),
            Some(StartRefused::UntilTheBackoffIsDone)
        ));
        assert!(start_refused(Some(&spent), 10_000, &desired).is_none());
    }

    #[test]
    fn an_instance_out_of_restarts_is_refused_for_that_and_not_for_its_backoff() {
        let desired = desired_instance(|_| {});
        let allowed = desired.config.restart_policy.max_restarts;
        let exhausted = instance_record(|record| {
            record.start_attempts = crate::domain::backoff::AttemptWindow {
                attempts: allowed + 1,
                last_attempt_at_ms: Some(0),
            };
        });
        // Long past any backoff, so the only thing still refusing it is the budget.
        assert!(matches!(
            start_refused(Some(&exhausted), 10_000_000, &desired),
            Some(StartRefused::OutOfRestarts { attempted, allowed: budget })
                if attempted == allowed + 1 && budget == allowed
        ));
    }

    #[tokio::test]
    async fn a_busy_host_does_not_spend_the_apps_restart_budget() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.vms.boot_error(Some(VmError::StartBusy));
        for _ in 0..10 {
            start_instance(&host, &desired_instance(|_| {})).await;
            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, InstanceState::Pending);
            assert_eq!(record.start_attempts, NO_START_ATTEMPTS);
        }
        host.vms.boot_error(None);
        start_instance(&host, &desired_instance(|_| {})).await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Starting
        );
    }

    #[tokio::test]
    async fn an_instance_that_will_not_start_again_says_so_rather_than_keeping_its_last_good_message() {
        let host = test_host().await;
        let desired = desired_instance(|_| {});
        let allowed = desired.config.restart_policy.max_restarts;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Failed;
                record.start_attempts = crate::domain::backoff::AttemptWindow {
                    attempts: allowed + 1,
                    last_attempt_at_ms: Some(0),
                };
                // What the report used to carry: a sentence from the last time it worked.
                record.message = Some(StateMessage::new("starting the tenant as uid 65534"));
            }))
            .await;

        start_instance(&host, &desired).await;

        let said = host.state.record(&app_id()).await.unwrap().message.unwrap();
        assert!(said.as_str().contains("out of restarts"), "{}", said.as_str());
        assert!(
            said.as_str().contains(&format!("budget of {allowed}")),
            "the budget the decision was made against is missing: {}",
            said.as_str()
        );
        assert!(
            said.as_str()
                .contains(&format!("{} starts attempted", allowed + 1)),
            "the attempts the decision was made on are missing: {}",
            said.as_str()
        );

        start_instance(&host, &desired).await;
        assert_eq!(
            host.metrics.health.of(&app_id()).failures,
            failures(&[("out_of_restarts", 1)]),
            "said once, counted once, however many passes say it again"
        );
    }

    #[tokio::test]
    async fn a_start_the_document_made_impossible_is_refused_once_and_takes_nothing_from_the_budget() {
        let (said, _listening) = Said::listening();
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        let port = |name: &str, number: u16| protocol::InstancePort {
            name: protocol::PortName::parse(name).unwrap(),
            guest_port: protocol::GuestPort::new(number).unwrap(),
        };
        // Two ports beside the HTTP one, on a host whose [proxy.raw] allows each guest one.
        let asking_too_much =
            desired_instance(|instance| instance.config.ports = vec![port("ssh", 22), port("git", 9418)]);
        let budget = asking_too_much.config.restart_policy.max_restarts;

        // Pass after pass over a document that stands still, past what the budget would allow,
        // each fetching ahead of its starts the way a pass does.
        for _ in 0..budget + 3 {
            let plan = ReconcilePlan {
                instances: vec![InstancePlan::Start {
                    desired: asking_too_much.clone(),
                }],
                ..Default::default()
            };
            prefetch_layers(&host, &plan).await;
            start_instance(&host, &asking_too_much).await;
        }

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(
            record.start_attempts, NO_START_ATTEMPTS,
            "a refusal is the document's fault, not a start that failed"
        );
        let reason = record.message.unwrap();
        assert!(
            reason.as_str().contains("allows 1"),
            "the refusal, not a budget, is what the record says: {}",
            reason.as_str()
        );
        assert!(host.vms.calls().is_empty());
        assert!(
            !cached_image(&host).exists(),
            "nothing is fetched for an app nobody could have reached"
        );
        let page = metrics_page(&host).await;
        assert!(page.contains("nibrunner_layers_cached_total 0\n"), "{page}");
        assert!(page.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"layer_fetch\",outcome=\"ok\"} 0\n"
        ));
        assert_eq!(
            host.metrics.health.of(&app_id()).failures,
            failures(&[("refused", 1)]),
            "counted once, however many passes find it"
        );
        let refusals = said
            .lines()
            .into_iter()
            .filter(|line| line.contains("refused before it was started"))
            .count();
        assert_eq!(refusals, 1, "said once: {:?}", said.lines());
    }

    fn failures(counted: &[(&str, u64)]) -> [u64; 8] {
        let mut failures = [0; 8];
        for (index, reason) in [
            "refused",
            "no_slot",
            "layers",
            "volume",
            "boot",
            "exited",
            "never_answered",
            "out_of_restarts",
        ]
        .iter()
        .enumerate()
        {
            failures[index] = counted
                .iter()
                .find(|(each, _)| each == reason)
                .map_or(0, |(_, count)| *count);
        }
        failures
    }

    fn spent_window() -> crate::domain::backoff::AttemptWindow {
        crate::domain::backoff::AttemptWindow {
            attempts: 3,
            last_attempt_at_ms: Some(now_ms()),
        }
    }

    fn cached_image(host: &crate::host::Host) -> std::path::PathBuf {
        crate::adapters::vm::layers::layer_image_path(&host.config.artifact_cache_dir(), &layer(|_| {}))
    }

    fn refuse_artifacts(host: &mut TestHost, reason: &str) {
        let cache_dir = host.config.artifact_cache_dir();
        let refusing = mocks::artifacts_refusing(crate::ports::ArtifactError::Transfer(reason.to_string()));
        Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .payloads = crate::adapters::vm::layers::LayerImages::new(refusing, cache_dir);
    }

    fn on_request() -> DesiredInstance {
        desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest)
    }

    #[tokio::test]
    async fn an_app_this_host_has_never_served_is_left_a_record_waiting_to_be_asked_for() {
        let host = test_host().await;

        sleep_instance(&host, &on_request()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Idle);
        assert!(record.on_request);
        assert!(record.desired_running);
        assert!(host.slot_of(&app_id()).await.is_some());
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn one_this_host_already_holds_takes_the_new_release_without_leaving_the_state_it_is_in() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Stopped;
                record.restart_count = 3;
            }))
            .await;
        let newer = desired_instance(|instance| {
            instance.desired_state = DesiredInstanceState::OnRequest;
            instance.deployment_id = protocol::DeploymentId::parse("dep-2").unwrap();
        });

        sleep_instance(&host, &newer).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Stopped);
        assert_eq!(record.deployment_id.as_str(), "dep-2");
        assert_eq!(record.restart_count, 3);
        assert!(record.on_request);
    }

    #[tokio::test]
    async fn an_app_that_arrives_already_stopped_is_left_a_record_that_answers_for_it() {
        let host = test_host().await;
        let stopped = desired_instance(|instance| {
            instance.desired_state = DesiredInstanceState::Stopped;
            instance.hostnames = vec![app_hostname()];
        });

        hold_instance(&host, &stopped).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Stopped);
        assert!(!record.desired_running);
        assert!(!record.on_request);
        assert_eq!(record.started_at, None);
        assert_eq!(record.hostnames, vec![app_hostname()]);
        let slot = host.slot_of(&app_id()).await.expect("the slot it answers on");
        assert_eq!(record.host_port, slot.host_port);
        assert!(host.vms.calls().is_empty());
        let routes = crate::domain::report::routes::renderable_routes(&host.state.records().await);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hostnames, vec![app_hostname()]);
        assert_eq!(routes[0].host_port, slot.host_port);
    }

    #[tokio::test]
    async fn a_stop_leaves_the_record_stopped_with_the_attempts_it_spent_given_back() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| record.start_attempts = spent_window()))
            .await;

        stop_instance(&host, &app_id(), "not-desired").await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Stopped);
        assert!(record.stop_requested);
        assert_eq!(record.start_attempts, NO_START_ATTEMPTS);
        assert_eq!(host.vms.calls(), vec![crate::ports::VmCall::Stop]);
    }

    #[tokio::test]
    async fn a_stop_for_an_app_this_host_has_no_record_for_still_takes_the_microvm_down() {
        let host = test_host().await;

        stop_instance(&host, &app_id(), "not-desired").await;

        assert_eq!(host.vms.calls(), vec![crate::ports::VmCall::Stop]);
        assert!(host.state.record(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn an_app_this_host_has_no_record_for_is_not_put_to_sleep() {
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();

        suspend_instance(&host, &app_id(), SleepReason::Quiet).await;

        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn a_snapshot_without_memory_leaves_the_frozen_vm_and_its_routes_untouched() {
        let mut host = test_host().await;
        Arc::get_mut(&mut host.host).unwrap().config.memory_admission =
            Some(crate::config::MemoryAdmission {
                pool: None,
                mode: crate::config::MemoryAdmissionMode::Adaptive,
                headroom_mib: 1024.try_into().unwrap(),
                reclaim: true,
                freeze_after_ms: None,
            });
        host.slot_for(&app_id()).await.unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Frozen;
                record.resources.memory_mib = 1_000_000;
            }))
            .await;
        for index in 0..4 {
            host.state
                .put_record(instance_record(|record| {
                    record.app_id = AppId::parse(format!("neighbour-{index}")).unwrap();
                }))
                .await;
        }
        let outcome = suspend_instance(&host, &app_id(), SleepReason::MemoryPressure).await;
        assert_eq!(outcome, Some(SleepOutcome::Refused));
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Frozen
        );
        assert!(!host.state.is_snapshotting(&app_id()).await);
        assert!(host.vms.calls().is_empty());
        assert!(host.commands.calls().is_empty());
    }

    #[tokio::test]
    async fn cancelling_a_snapshot_caller_keeps_its_reservation_and_transition_until_completion() {
        let _serial = crate::test_support::ONE_HOST_AT_A_TIME.lock().await;
        let mut host = test_host().await;
        let (held, _) = crate::test_support::mocks::vmm_holding_sleeps();
        Arc::get_mut(&mut host.host).unwrap().vms = held.clone();
        host.slot_for(&app_id()).await.unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Frozen;
                record.on_request = true;
            }))
            .await;
        let transition = host.state.transition(&app_id()).await;
        let running = host.host.clone();
        let caller =
            tokio::spawn(
                async move { suspend_owned(running, app_id(), SleepReason::Quiet, transition).await },
            );
        held.held_up(1).await;
        let reserved = host.state.memory_generation();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(host.state.memory_generation(), reserved);
        assert!(host.state.try_transition(&app_id()).is_none());
        assert!(host.state.is_snapshotting(&app_id()).await);
        held.let_through(1);
        let _next = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            host.state.transition(&app_id()),
        )
        .await
        .unwrap();
        assert_eq!(host.state.memory_generation(), reserved + 1);
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Idle
        );
        assert!(!host.state.is_snapshotting(&app_id()).await);
    }

    #[tokio::test]
    async fn a_start_inside_the_backoff_it_earned_does_not_touch_the_microvm() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Failed;
                record.start_attempts = spent_window();
            }))
            .await;

        start_instance(&host, &desired_instance(|_| {})).await;

        assert!(host.vms.calls().is_empty());
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Failed
        );
        assert!(host.slot_of(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn an_image_this_host_cannot_fetch_fails_the_instance_rather_than_booting_something_else() {
        let mut host = test_host().await;
        refuse_artifacts(&mut host, "the object store is down");
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();

        start_instance(&host, &desired_instance(|_| {})).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert!(record
            .message
            .unwrap()
            .as_str()
            .contains("the object store is down"));
        assert!(host.vms.calls().is_empty());
        assert_eq!(
            host.metrics.health.of(&app_id()).failures,
            failures(&[("layers", 1)])
        );
    }

    #[tokio::test]
    async fn an_app_whose_volume_has_no_filesystem_is_not_booted_and_says_why_the_volume_has_none() {
        let host = test_host().await;
        // What the pass over the volumes leaves when a seed is refused: a report that says failed,
        // with the reason.
        let refusal = StateMessage::new("the initial contents could not be laid out: wrong digest");
        host.state
            .modify(|snapshot| {
                snapshot.volume_reports = vec![reported_volume(|report| {
                    report.state = protocol::VolumeState::Failed;
                    report.message = Some(refusal.clone());
                })]
            })
            .await;

        start_instance(&host, &desired_instance(|_| {})).await;

        assert!(
            host.vms.calls().is_empty(),
            "nothing was booted onto a bare volume"
        );
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(record.message.as_ref(), Some(&refusal));
        assert_eq!(record.deployment_id, deployment_id());
        assert!(record.started_at.is_none());
        assert_eq!(
            record.start_attempts, NO_START_ATTEMPTS,
            "a volume that is not ready is not the app's failure to pay for"
        );
        assert_eq!(
            host.metrics.health.of(&app_id()).failures,
            failures(&[("volume", 1)])
        );

        start_instance(&host, &desired_instance(|_| {})).await;
        assert_eq!(
            host.metrics.health.of(&app_id()).failures,
            failures(&[("volume", 1)]),
            "said once, counted once, however many passes say it again"
        );

        // The status loop reads the record as it is left, not as an app asleep and well.
        refresh_states(host.arc()).await;
        let settled = host.state.record(&app_id()).await.unwrap();
        assert_eq!(settled.state, InstanceState::Failed);
        assert_eq!(settled.message.as_ref(), Some(&refusal));
    }

    fn scratch_instance() -> DesiredInstance {
        desired_instance(|instance| {
            instance.volume_id = None;
            instance.scratch = Some(protocol::Scratch::Disk {
                mib: std::num::NonZeroU32::new(64).unwrap(),
            });
        })
    }

    #[tokio::test]
    async fn an_app_with_a_scratch_boots_without_a_volume_and_without_a_record_of_one() {
        let host = test_host().await;

        start_instance(&host, &scratch_instance()).await;

        assert_eq!(host.vms.calls(), vec![crate::ports::VmCall::Boot]);
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert_eq!(record.volume_id, None);
    }

    #[tokio::test]
    async fn a_failed_volume_report_does_not_hold_back_an_app_that_writes_to_a_scratch() {
        let host = test_host().await;
        host.state
            .modify(|snapshot| {
                snapshot.volume_reports = vec![reported_volume(|report| {
                    report.state = protocol::VolumeState::Failed;
                })]
            })
            .await;

        start_instance(&host, &scratch_instance()).await;

        assert_eq!(host.vms.calls(), vec![crate::ports::VmCall::Boot]);
    }

    #[tokio::test]
    async fn an_app_that_names_neither_a_volume_nor_a_scratch_is_refused_rather_than_booted() {
        let host = test_host().await;

        start_instance(&host, &desired_instance(|instance| instance.volume_id = None)).await;

        assert!(host.vms.calls().is_empty());
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert!(record
            .message
            .unwrap()
            .as_str()
            .contains("neither a volume nor a scratch"));
    }

    #[tokio::test]
    async fn a_volume_reported_ready_again_lets_the_app_that_was_refused_for_it_start() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Failed;
                record.message = Some(StateMessage::new("the initial contents could not be laid out"));
            }))
            .await;
        host.state
            .modify(|snapshot| snapshot.volume_reports = vec![reported_volume(|_| {})])
            .await;

        start_instance(&host, &desired_instance(|_| {})).await;

        assert_eq!(host.vms.calls(), vec![crate::ports::VmCall::Boot]);
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert_eq!(record.message, None);
    }

    #[tokio::test]
    async fn an_on_request_first_start_that_failed_stays_failed_through_the_status_loop() {
        let mut host = test_host().await;
        refuse_artifacts(&mut host, "the object store 403'd the executable layer");
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();

        // The very first start of an on-request app, and its layer will not fetch.
        start_instance(&host, &on_request()).await;
        let failed = host.state.record(&app_id()).await.unwrap();
        assert_eq!(failed.state, InstanceState::Failed);
        assert!(failed.started_at.is_none(), "the microVM never came up");
        assert!(failed.message.as_ref().unwrap().as_str().contains("403"));

        // The status loop used to read the never-booted on-request record as Idle with no
        // message: an app asleep and well. It must keep the failure it observed.
        refresh_states(host.arc()).await;

        let settled = host.state.record(&app_id()).await.unwrap();
        assert_eq!(
            settled.state,
            InstanceState::Failed,
            "a failed first start is not an app that is merely asleep"
        );
        assert!(
            settled.message.as_ref().unwrap().as_str().contains("403"),
            "the reason the start failed is kept: {:?}",
            settled.message
        );
    }

    #[tokio::test]
    async fn a_cold_boot_starts_the_count_of_the_guests_restarts_over() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.restart_count = 5;
                record.last_restart = Some(reported_restart(|_| {}));
                record.stop_requested = false;
            }))
            .await;

        start_instance(&host, &desired_instance(|_| {})).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert_eq!(record.restart_count, 0);
        assert_eq!(record.last_restart, None);
        assert!(record.started_at.is_some());
        assert!(!record.stop_requested);
        assert_eq!(
            record.start_attempts.attempts, 1,
            "the host's own boots are counted apart"
        );
    }

    #[tokio::test]
    async fn a_boot_that_failed_keeps_what_the_last_guest_counted() {
        let mut host = test_host().await;
        refuse_artifacts(&mut host, "the object store is down");
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.restart_count = 5;
                record.last_restart = Some(reported_restart(|_| {}));
            }))
            .await;

        start_instance(&host, &desired_instance(|_| {})).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(record.restart_count, 5);
        assert_eq!(record.last_restart, Some(reported_restart(|_| {})));
    }

    #[tokio::test]
    async fn a_wake_that_failed_for_its_own_reason_says_so_rather_than_booting_the_app_cold() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.vms
            .refuse_wake(VmError::Host("the snapshot file is unreadable".to_string()));
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
            }))
            .await;

        let refusal = resume_instance(&host, &on_request()).await.unwrap_err();

        let WakeRefusal::Failed { kind, reason } = refusal else {
            panic!("a wake that failed is not a host short of memory: {refusal:?}");
        };
        assert_eq!(kind, WakeFailure::WouldNotStart);
        assert!(reason.contains("the snapshot file is unreadable"), "{reason}");
        assert_eq!(host.vms.calls(), vec![crate::ports::VmCall::Wake]);
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Idle);
        assert!(record
            .message
            .unwrap()
            .as_str()
            .contains("the snapshot file is unreadable"));
    }

    #[tokio::test]
    async fn the_image_of_everything_the_plan_starts_is_in_the_cache_before_any_of_it_boots() {
        let host = test_host().await;

        prefetch_layers(
            &host,
            &ReconcilePlan {
                instances: vec![InstancePlan::Start {
                    desired: desired_instance(|_| {}),
                }],
                ..Default::default()
            },
        )
        .await;

        assert!(cached_image(&host).exists());
        let page = metrics_page(&host).await;
        assert!(page.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"layer_fetch\",outcome=\"ok\"} 1\n"
        ));
        assert!(page.contains(&format!(
            "nibrunner_layer_fetch_bytes_total {}\n",
            ARTIFACT_BYTES.len()
        )));
        assert!(page.contains("nibrunner_layers_cached_total 0\n"));

        prefetch_layers(
            &host,
            &ReconcilePlan {
                instances: vec![InstancePlan::Start {
                    desired: desired_instance(|_| {}),
                }],
                ..Default::default()
            },
        )
        .await;
        let page = metrics_page(&host).await;
        assert!(
            page.contains("nibrunner_layers_cached_total 1\n"),
            "the second pass found it"
        );
        assert!(page.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"layer_fetch\",outcome=\"ok\"} 1\n"
        ));
    }

    async fn metrics_page(host: &TestHost) -> String {
        crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        )
    }

    #[tokio::test]
    async fn an_image_that_could_not_be_fetched_leaves_the_pass_running() {
        let mut host = test_host().await;
        refuse_artifacts(&mut host, "the object store is down");

        prefetch_layers(
            &host,
            &ReconcilePlan {
                instances: vec![InstancePlan::Start {
                    desired: desired_instance(|_| {}),
                }],
                ..Default::default()
            },
        )
        .await;

        assert!(!cached_image(&host).exists());
        assert!(metrics_page(&host).await.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"layer_fetch\",outcome=\"failed\"} 1\n"
        ));
    }

    #[tokio::test]
    async fn a_microvm_that_went_away_leaves_the_code_it_exited_with_on_the_record() {
        let host = test_host().await;
        host.vms.set_status(VmStatus {
            loaded: true,
            active: false,
            frozen: false,
            failed: false,
            started_this_boot: true,
            exit: Some(VmExit::Code(137)),
        });
        host.state
            .put_record(instance_record(|record| record.started_at = Some(observed_at())))
            .await;

        refresh_states(host.arc()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(record.last_exit_code, Some(137));
        assert!(record.message.unwrap().as_str().contains("exit code 137"));
        assert_eq!(
            host.metrics.health.of(&app_id()).failures,
            failures(&[("exited", 1)])
        );

        refresh_states(host.arc()).await;
        assert_eq!(
            host.metrics.health.of(&app_id()).failures,
            failures(&[("exited", 1)]),
            "a state that has not moved is not a failure that happened again"
        );
    }

    #[tokio::test]
    async fn a_microvm_killed_from_outside_says_which_signal_and_leaves_no_code_behind() {
        let host = test_host().await;
        host.vms.set_status(VmStatus {
            loaded: true,
            active: false,
            frozen: false,
            failed: true,
            started_this_boot: true,
            exit: Some(VmExit::Signal(libc::SIGKILL)),
        });
        host.state
            .put_record(instance_record(|record| {
                record.started_at = Some(observed_at());
                // The code of an earlier exit, which is not this one's.
                record.last_exit_code = Some(137);
            }))
            .await;

        refresh_states(host.arc()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(record.last_exit_code, None);
        assert_eq!(
            record.message.unwrap().as_str(),
            "the microVM was killed by signal 9 (SIGKILL)"
        );
    }

    #[tokio::test]
    async fn only_a_guest_that_stayed_up_for_reset_after_ms_gives_its_attempts_back_when_it_exits() {
        let reset_after_ms = protocol::DEFAULT_RESTART_POLICY.reset_after_ms as i64;
        let exited_after = |uptime_ms: i64| async move {
            let host = test_host().await;
            host.vms.set_status(VmStatus {
                loaded: true,
                active: false,
                frozen: false,
                failed: false,
                started_this_boot: true,
                exit: Some(VmExit::Code(1)),
            });
            host.state
                .put_record(instance_record(|record| {
                    record.started_at = Some(protocol::Timestamp::from_epoch_ms(now_ms() - uptime_ms));
                    record.start_attempts = spent_window();
                }))
                .await;
            refresh_states(host.arc()).await;
            host.state.record(&app_id()).await.unwrap()
        };

        let short_lived = exited_after(reset_after_ms - 1_000).await;
        assert_eq!(short_lived.state, InstanceState::Failed);
        assert_eq!(short_lived.start_attempts.attempts, spent_window().attempts);

        let long_lived = exited_after(reset_after_ms).await;
        assert_eq!(long_lived.state, InstanceState::Failed);
        assert_eq!(long_lived.start_attempts, NO_START_ATTEMPTS);
    }

    #[tokio::test]
    async fn a_probe_is_counted_on_the_app_with_what_it_found() {
        let host = test_host().await;
        host.vms.set_status(up());
        let listening = listening().await;
        host.state
            .put_record(instance_record(|record| {
                record.health_check = protocol::HealthCheck::BootCompleted;
                record.state = InstanceState::Starting;
                record.started_at = Some(now_timestamp());
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = listening;
            }))
            .await;

        refresh_states(host.arc()).await;

        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Running
        );
        let health = host.metrics.health.of(&app_id());
        assert_eq!((health.probes_healthy, health.probes_unhealthy), (1, 0));
    }

    // The boot completes ~100 ms before the tenant binds its port; a record read as `running` in
    // that window had the host port forwarded to a guest that refused the connection.
    #[tokio::test]
    async fn a_boot_completed_guest_is_starting_until_its_port_accepts_and_running_for_good_after() {
        let host = test_host().await;
        host.vms.set_status(up());
        let nobody_listening = protocol::HttpPort::new(1).unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.health_check = protocol::HealthCheck::BootCompleted;
                record.state = InstanceState::Starting;
                record.started_at = Some(now_timestamp());
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = nobody_listening;
            }))
            .await;

        let record = probed(&host).await;
        assert_eq!(
            record.state,
            InstanceState::Starting,
            "up, but nothing listening yet"
        );
        assert_eq!(record.health.consecutive_failures, 1);
        let asked_again_at = host.state.snapshot().await.next_probe_at_ms[&app_id()];
        assert!(
            asked_again_at <= now_ms() + crate::domain::health::STARTUP_PROBE_INTERVAL_MS as i64,
            "asked again on the settling cadence"
        );

        let listening = listening().await;
        host.state
            .update_record(&app_id(), |record| record.http_port = listening)
            .await;
        let record = probed(&host).await;
        assert_eq!(record.state, InstanceState::Running);
        assert!(record.health.ever_healthy);

        host.state
            .update_record(&app_id(), |record| record.http_port = nobody_listening)
            .await;
        let record = probed(&host).await;
        assert_eq!(
            record.state,
            InstanceState::Running,
            "the port was a start gate, not a health check"
        );
        assert_eq!(record.health.consecutive_failures, 0, "it is never asked again");
        assert_eq!(record.message, None);
    }

    fn up() -> VmStatus {
        VmStatus {
            loaded: true,
            active: true,
            frozen: false,
            failed: false,
            started_this_boot: true,
            exit: None,
        }
    }

    /// A port on the loopback that accepts, for as long as the test runs.
    async fn listening() -> protocol::HttpPort {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = protocol::HttpPort::new(listener.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move { while listener.accept().await.is_ok() {} });
        port
    }

    /// A record of an app that was well, checked on the loopback so a test can be the tenant.
    fn well_until_now(http_port: protocol::HttpPort) -> InstanceRecord {
        instance_record(|record| {
            record.state = InstanceState::Running;
            record.health.ever_healthy = true;
            record.guest_ipv4 = crate::domain::health::probe::loopback();
            record.http_port = http_port;
            record.started_at = Some(protocol::Timestamp::from_epoch_ms(
                now_ms() - 2 * TCP_HEALTH_CHECK.probe().grace_period_ms as i64,
            ));
        })
    }

    async fn probed(host: &TestHost) -> InstanceRecord {
        host.state.probe_at_once(&app_id()).await;
        refresh_states(host.arc()).await;
        host.state.record(&app_id()).await.unwrap()
    }

    #[tokio::test]
    async fn health_refresh_skips_a_lifecycle_transition_and_reads_the_new_state_afterward() {
        let host = test_host().await;
        host.vms.set_status(up());
        let mut record = well_until_now(protocol::HttpPort::new(1).unwrap());
        record.health.consecutive_failures = TCP_HEALTH_CHECK.probe().unhealthy_threshold - 1;
        host.state.put_record(record.clone()).await;
        let transition = host.state.transition(&app_id()).await;
        tokio::time::timeout(std::time::Duration::from_millis(100), refresh_states(host.arc()))
            .await
            .expect("health refresh skips an occupied transition");
        assert_eq!(host.state.record(&app_id()).await.unwrap(), record);
        assert_eq!(host.metrics.health.of(&app_id()).went_unhealthy, 0);
        host.vms.set_status(VmStatus {
            active: false,
            ..up()
        });
        host.state
            .update_record(&app_id(), |record| {
                record.on_request = true;
                record.stop_requested = true;
                record.state = InstanceState::Idle;
            })
            .await;
        drop(transition);
        refresh_states(host.arc()).await;
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Idle);
        assert_eq!(
            record.health.consecutive_failures,
            TCP_HEALTH_CHECK.probe().unhealthy_threshold - 1
        );
        assert_eq!(host.metrics.health.of(&app_id()).went_unhealthy, 0);
    }

    #[tokio::test]
    async fn an_app_that_stopped_answering_says_why_until_it_answers_again() {
        let host = test_host().await;
        host.vms.set_status(up());
        let nobody_listening = protocol::HttpPort::new(1).unwrap();
        host.state.put_record(well_until_now(nobody_listening)).await;
        let threshold = TCP_HEALTH_CHECK.probe().unhealthy_threshold;

        for _ in 1..threshold {
            let record = probed(&host).await;
            assert_eq!(record.state, InstanceState::Running);
            assert_eq!(record.message, None, "not unhealthy yet, so nothing to explain");
        }
        let record = probed(&host).await;
        assert_eq!(record.state, InstanceState::Unhealthy);
        let said = record.message.expect("an unhealthy instance says why");
        assert_eq!(
            said.as_str(),
            format!("{threshold} tcp probes failed in a row; the last tcp connect refused")
        );
        assert_eq!(host.metrics.health.of(&app_id()).went_unhealthy, 1);

        // The status loop passes every second, and a probe is due every five: a pass without one
        // has nothing new to say.
        refresh_states(host.arc()).await;
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Unhealthy);
        assert_eq!(record.message, Some(said));

        let record = probed(&host).await;
        assert_eq!(
            record.message.unwrap().as_str(),
            format!(
                "{} tcp probes failed in a row; the last tcp connect refused",
                threshold + 1
            ),
            "another failed probe is counted into the sentence"
        );
        assert_eq!(host.metrics.health.of(&app_id()).went_unhealthy, 1);

        // The tenant is back: the port accepts again.
        let answering = listening().await;
        host.state
            .update_record(&app_id(), |record| record.http_port = answering)
            .await;
        let record = probed(&host).await;
        assert_eq!(record.state, InstanceState::Running);
        assert_eq!(record.message, None, "well again, so nothing left to explain");
        assert_eq!(record.health.last_failure, None);
    }

    #[tokio::test]
    async fn a_state_that_has_not_moved_keeps_the_message_that_explained_it() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Pending;
                record.message = Some(StateMessage::new("waiting for its volume"));
            }))
            .await;

        refresh_states(host.arc()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Pending);
        assert_eq!(record.message.unwrap().as_str(), "waiting for its volume");
        assert_eq!(record.last_exit_code, None);
    }
}
