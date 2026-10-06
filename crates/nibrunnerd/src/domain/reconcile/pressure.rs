use std::collections::BTreeMap;

use protocol::{ActivationPolicy, AppId, InstanceState, MemoryPriority, SleepPolicy};

use crate::domain::activation::{ActivitySignals, SleepReason};
use crate::domain::metrics::sleep_wake::SleepOutcome;
use crate::domain::report::InstanceRecord;
use crate::host::Host;

const BYTES_PER_MIB: u64 = 1_048_576;
const QUIET_MS: i64 = 30_000;
const COOLDOWN_MS: i64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimPurpose {
    Pressure,
    Deployment,
    Wake,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PressureState {
    recovering: bool,
    attempted_at: BTreeMap<AppId, i64>,
}

#[derive(Debug, Clone, Copy)]
struct Reading {
    available_bytes: u64,
    stalls: Option<(f64, f64)>,
}

impl Reading {
    fn read() -> Option<Self> {
        Some(Self {
            available_bytes: crate::domain::report::capacity::read_memory_available_bytes()?,
            stalls: std::fs::read_to_string("/proc/pressure/memory")
                .ok()
                .and_then(|text| pressure(&text)),
        })
    }
}

fn pressure(text: &str) -> Option<(f64, f64)> {
    let average = |name: &str| {
        let line = text
            .lines()
            .find(|line| line.split_whitespace().next() == Some(name))?;
        let value: f64 = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("avg10="))?
            .parse()
            .ok()?;
        (value.is_finite() && (0.0..=100.0).contains(&value)).then_some(value)
    };
    Some((average("some")?, average("full")?))
}

impl PressureState {
    fn observe(&mut self, reading: Reading, headroom_mib: u32) -> bool {
        let low = u64::from(headroom_mib) * BYTES_PER_MIB;
        let high = low.saturating_add((low / 4).max(256 * BYTES_PER_MIB));
        let stalled = reading
            .stalls
            .is_some_and(|(some, full)| some >= 10.0 || full >= 1.0);
        let recovered = reading.available_bytes >= high
            && reading
                .stalls
                .is_none_or(|(some, full)| some <= 2.0 && full <= 0.2);
        self.recovering = if self.recovering {
            !recovered
        } else {
            reading.available_bytes < low || stalled
        };
        self.recovering
    }
}

fn eligible(
    record: &InstanceRecord,
    policy: &ActivationPolicy,
    signals: &ActivitySignals,
    attempted_at: Option<i64>,
    purpose: ReclaimPurpose,
    priority: MemoryPriority,
    now: i64,
) -> bool {
    record.on_request
        && record.desired_running
        && matches!(record.state, InstanceState::Running | InstanceState::Frozen)
        && matches!(policy.sleep_when, SleepPolicy::TrafficIdle { .. })
        && !(purpose == ReclaimPurpose::Deployment && priority == MemoryPriority::Production)
        && signals.requests_open == 0
        && (record.state == InstanceState::Frozen || signals.measured_lately(now))
        && signals
            .last_active_at_ms
            .is_some_and(|at| now.saturating_sub(at) >= QUIET_MS)
        && signals
            .started_at_ms
            .is_some_and(|at| now.saturating_sub(at) >= COOLDOWN_MS)
        && attempted_at.is_none_or(|at| now.saturating_sub(at) >= COOLDOWN_MS)
}

fn priority(host: &Host, id: &AppId) -> MemoryPriority {
    host.runtime_policy
        .vm_budget(id)
        .and_then(|budget| budget.memory)
        .map_or(MemoryPriority::Standard, |memory| memory.priority)
}

fn eviction_order(priority: MemoryPriority) -> u8 {
    match priority {
        MemoryPriority::Build => 0,
        MemoryPriority::Preview => 1,
        MemoryPriority::Standard => 2,
        MemoryPriority::Production => 3,
    }
}

/// The caller holds the host reclaim lock. App locks are tried, never waited on, so a start
/// holding its own transition cannot deadlock with another start asking to reclaim capacity.
pub(crate) async fn reclaim_one(host: &Host, purpose: ReclaimPurpose, exclude: Option<&AppId>) -> bool {
    let policies: BTreeMap<_, _> = host
        .cache
        .lock()
        .await
        .latest()
        .map(|desired| {
            desired
                .instances
                .iter()
                .map(|instance| (instance.app_id.clone(), instance.activation()))
                .collect()
        })
        .unwrap_or_default();
    let now = crate::clock::now_ms();
    let snapshot = host.state.snapshot().await;
    let mut candidates: Vec<_> = snapshot
        .records
        .values()
        .filter_map(|record| {
            let id = &record.app_id;
            if exclude == Some(id) || snapshot.snapshotting.contains(id) {
                return None;
            }
            let policy = policies.get(id)?;
            let signals = super::idle::signals(&snapshot, record, host.metrics.proxy.open_requests_for(id));
            let priority = priority(host, id);
            eligible(
                record,
                policy,
                &signals,
                snapshot.memory_pressure.attempted_at.get(id).copied(),
                purpose,
                priority,
                now,
            )
            .then(|| {
                (
                    eviction_order(priority),
                    record.state != InstanceState::Frozen,
                    signals.last_active_at_ms,
                    id.clone(),
                )
            })
        })
        .collect();
    candidates.sort();
    for (_, _, _, app) in candidates {
        let Some(_transition) = host.state.try_transition(&app) else {
            continue;
        };
        let policy = host.cache.lock().await.latest().and_then(|desired| {
            desired
                .instances
                .iter()
                .find(|instance| instance.app_id == app)
                .map(|instance| instance.activation())
        });
        let Some(policy) = policy else {
            continue;
        };
        let now = crate::clock::now_ms();
        let mut snapshot = host.state.locked_snapshot().await;
        let Some(record) = snapshot.records.get(&app) else {
            continue;
        };
        let signals = super::idle::signals(&snapshot, record, host.metrics.proxy.open_requests_for(&app));
        if snapshot.snapshotting.contains(&app)
            || !eligible(
                record,
                &policy,
                &signals,
                snapshot.memory_pressure.attempted_at.get(&app).copied(),
                purpose,
                priority(host, &app),
                now,
            )
        {
            continue;
        }
        snapshot.memory_pressure.attempted_at.insert(app.clone(), now);
        snapshot.snapshotting.insert(app.clone());
        drop(snapshot);
        tracing::info!(app_id = %app, ?purpose, "reclaiming a quiet workload's memory");
        host.metrics
            .sleep_wake
            .sleep_due(SleepReason::MemoryPressure, std::time::Duration::ZERO);
        let outcome = super::instances::suspend_instance(host, &app, SleepReason::MemoryPressure).await;
        host.state.mark_snapshotting(&app, false).await;
        if outcome == Some(SleepOutcome::Slept) {
            return true;
        }
    }
    false
}

pub(crate) async fn apply(host: &Host) {
    let Some(config) = host
        .config
        .memory_admission
        .as_ref()
        .filter(|config| config.reclaim)
    else {
        return;
    };
    let Some(reading) = Reading::read() else {
        return;
    };
    let Ok(_reclaiming) = host.state.reclaim.try_lock() else {
        return;
    };
    let needed = host
        .state
        .modify(|snapshot| {
            snapshot
                .memory_pressure
                .attempted_at
                .retain(|id, _| snapshot.records.contains_key(id));
            snapshot
                .memory_pressure
                .observe(reading, config.headroom_mib.get())
        })
        .await;
    if needed {
        reclaim_one(host, ReclaimPurpose::Pressure, None).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use protocol::{DesiredInstanceState, Timestamp};

    const NOW: i64 = 120_000;

    fn idle() -> ActivationPolicy {
        ActivationPolicy {
            sleep_when: SleepPolicy::TrafficIdle {
                timeout_ms: 300_000.try_into().unwrap(),
            },
        }
    }

    fn quiet() -> ActivitySignals {
        ActivitySignals {
            last_active_at_ms: Some(60_000),
            measured_at_ms: Some(NOW),
            started_at_ms: Some(0),
            requests_open: 0,
        }
    }

    fn running() -> InstanceRecord {
        instance_record(|record| {
            record.on_request = true;
            record.desired_running = true;
            record.state = InstanceState::Running;
        })
    }

    #[test]
    fn reclaim_stops_only_after_physical_headroom_and_pressure_have_recovered() {
        let mut state = PressureState::default();
        let read = |available_mib, stalls| Reading {
            available_bytes: available_mib * BYTES_PER_MIB,
            stalls,
        };
        assert!(!state.observe(read(1024, Some((0.0, 0.0))), 1024));
        assert!(state.observe(read(1023, Some((0.0, 0.0))), 1024));
        assert!(state.observe(read(1279, Some((0.0, 0.0))), 1024));
        assert!(!state.observe(read(1280, Some((0.0, 0.0))), 1024));
        assert!(state.observe(read(2048, Some((1.0, 1.0))), 1024));
        assert!(state.observe(read(2048, Some((1.0, 0.3))), 1024));
        assert!(!state.observe(read(2048, Some((1.0, 0.2))), 1024));
    }

    #[test]
    fn only_complete_finite_pressure_readings_are_used() {
        assert_eq!(
            pressure("some avg10=3.20 avg60=2.00 total=400\nfull avg10=0.25 total=20"),
            Some((3.2, 0.25))
        );
        assert_eq!(pressure("some avg10=NaN\nfull avg10=0"), None);
        assert_eq!(pressure("some avg10=0"), None);
        assert_eq!(pressure("some avg10=101\nfull avg10=0"), None);
    }

    #[test]
    fn active_streams_recent_wakes_and_unknown_activity_are_protected() {
        let allowed = |signals: &ActivitySignals| {
            eligible(
                &running(),
                &idle(),
                signals,
                None,
                ReclaimPurpose::Pressure,
                MemoryPriority::Preview,
                NOW,
            )
        };
        assert!(allowed(&quiet()));
        assert!(!allowed(&ActivitySignals {
            requests_open: 1,
            ..quiet()
        }));
        assert!(!allowed(&ActivitySignals {
            last_active_at_ms: Some(NOW - 1),
            ..quiet()
        }));
        assert!(!allowed(&ActivitySignals {
            started_at_ms: Some(NOW - 1),
            ..quiet()
        }));
        assert!(!allowed(&ActivitySignals {
            measured_at_ms: None,
            ..quiet()
        }));
        assert!(!allowed(&ActivitySignals {
            measured_at_ms: Some(NOW + 1),
            ..quiet()
        }));
        assert!(!allowed(&ActivitySignals {
            measured_at_ms: Some(0),
            ..quiet()
        }));
    }

    #[test]
    fn deployments_preserve_production_and_repeated_failed_reclaims_cool_down() {
        assert!(!eligible(
            &running(),
            &idle(),
            &quiet(),
            None,
            ReclaimPurpose::Deployment,
            MemoryPriority::Production,
            NOW
        ));
        assert!(eligible(
            &running(),
            &idle(),
            &quiet(),
            None,
            ReclaimPurpose::Wake,
            MemoryPriority::Production,
            NOW
        ));
        assert!(!eligible(
            &running(),
            &idle(),
            &quiet(),
            Some(NOW - 1),
            ReclaimPurpose::Pressure,
            MemoryPriority::Preview,
            NOW
        ));
        assert!(!eligible(
            &running(),
            &ActivationPolicy {
                sleep_when: SleepPolicy::Never
            },
            &quiet(),
            None,
            ReclaimPurpose::Pressure,
            MemoryPriority::Preview,
            NOW
        ));
        let mut record = running();
        record.on_request = false;
        assert!(!eligible(
            &record,
            &idle(),
            &quiet(),
            None,
            ReclaimPurpose::Pressure,
            MemoryPriority::Preview,
            NOW
        ));
    }

    async fn workloads() -> TestHost {
        let host = test_host().await;
        let now = crate::clock::now_ms();
        let mut instances = Vec::new();
        for (id, priority) in [
            ("production", MemoryPriority::Production),
            ("preview", MemoryPriority::Preview),
        ] {
            let app = AppId::parse(id).unwrap();
            host.slot_for(&app).await.unwrap();
            host.state
                .put_record(instance_record(|record| {
                    record.app_id = app.clone();
                    record.state = InstanceState::Running;
                    record.on_request = true;
                    record.started_at = Some(Timestamp::from_epoch_ms(now - 120_000));
                }))
                .await;
            measured_quiet_since(&host.state, &app, now - 90_000).await;
            instances.push(desired_instance(|instance| {
                instance.app_id = app;
                instance.desired_state = DesiredInstanceState::OnRequest;
                instance.limits = Some(protocol::InstanceLimits {
                    concurrent: 16.try_into().unwrap(),
                    cpu_percent: 100.try_into().unwrap(),
                    memory_mib: 512.try_into().unwrap(),
                    memory: Some(protocol::MemoryPolicy {
                        target_mib: 128.try_into().unwrap(),
                        swap_mib: 256,
                        priority,
                    }),
                });
            }));
        }
        host.runtime_policy.replace_instances(&instances);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = instances));
        host
    }

    #[tokio::test]
    async fn pressure_sleeps_a_preview_before_an_equally_idle_production_app() {
        let host = workloads().await;
        let _reclaiming = host.state.reclaim.lock().await;
        assert!(reclaim_one(&host, ReclaimPurpose::Pressure, None).await);
        assert_eq!(
            host.state
                .record(&AppId::parse("preview").unwrap())
                .await
                .unwrap()
                .state,
            InstanceState::Idle
        );
        assert_eq!(
            host.state
                .record(&AppId::parse("production").unwrap())
                .await
                .unwrap()
                .state,
            InstanceState::Running
        );
        assert!(!reclaim_one(&host, ReclaimPurpose::Deployment, None).await);
    }

    #[tokio::test]
    async fn a_reclaim_does_not_wait_for_or_interrupt_an_app_transition() {
        let host = workloads().await;
        let preview = AppId::parse("preview").unwrap();
        let _transition = host.state.transition(&preview).await;
        let _reclaiming = host.state.reclaim.lock().await;
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            reclaim_one(&host, ReclaimPurpose::Deployment, None),
        )
        .await;
        assert!(!result.expect("the held transition must not block reclamation"));
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn an_open_request_protects_a_quiet_preview_until_it_finishes() {
        let host = workloads().await;
        let preview = AppId::parse("preview").unwrap();
        let request = host.metrics.proxy.open(&preview);
        let _reclaiming = host.state.reclaim.lock().await;
        assert!(!reclaim_one(&host, ReclaimPurpose::Deployment, None).await);
        assert!(host.vms.calls().is_empty());
        drop(request);
        assert!(reclaim_one(&host, ReclaimPurpose::Deployment, None).await);
    }

    #[tokio::test]
    async fn deployment_admission_reclaims_a_preview_and_reserves_the_freed_capacity() {
        let mut host = workloads().await;
        let inner = std::sync::Arc::get_mut(&mut host.host).unwrap();
        inner.guest_memory_mib = u64::from(protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib) * 2;
        inner.config.memory_admission = Some(crate::config::MemoryAdmission {
            mode: crate::config::MemoryAdmissionMode::Observe,
            freeze_after_ms: None,
            reclaim: true,
            headroom_mib: 1024.try_into().unwrap(),
        });
        let candidate = AppId::parse("candidate").unwrap();
        let _transition = host.state.transition(&candidate).await;
        let reservation = host
            .reserve_memory(
                &candidate,
                protocol::DEFAULT_INSTANCE_RESOURCES,
                ReclaimPurpose::Deployment,
            )
            .await;
        assert!(reservation.is_ok());
        assert_eq!(
            host.state
                .record(&AppId::parse("preview").unwrap())
                .await
                .unwrap()
                .state,
            InstanceState::Idle
        );
        assert_eq!(
            host.state
                .record(&AppId::parse("production").unwrap())
                .await
                .unwrap()
                .state,
            InstanceState::Running
        );
        assert!(host
            .reserve_memory(
                &AppId::parse("another").unwrap(),
                protocol::DEFAULT_INSTANCE_RESOURCES,
                ReclaimPurpose::Deployment
            )
            .await
            .is_err());
    }
}
