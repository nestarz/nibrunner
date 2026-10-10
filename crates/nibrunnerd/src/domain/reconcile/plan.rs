use std::collections::{BTreeMap, BTreeSet};

use protocol::{
    AppId, CheckpointId, DeploymentId, DesiredCheckpoint, DesiredExport, DesiredInstance,
    DesiredInstanceState, DesiredPresence, DesiredVolume, ExportId, HostDesiredState, ObjectKey, VolumeId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedInstance {
    pub app_id: AppId,
    pub volume_id: Option<VolumeId>,
    pub deployment_id: Option<DeploymentId>,
    pub present: bool,
    pub running: bool,
    pub exited: bool,
    /// Whether the record is down from a start this host refused before spending an attempt on
    /// it: its volume could not be made ready, its document asked for what this host cannot
    /// give, or the host had no memory left for it. Nothing has been served under it, so it is
    /// not one asleep, nor one that spent its budget: it is started the pass the refusal lifts,
    /// on request or not.
    pub refused: bool,
    pub expired: bool,
    /// Whether its program was run once and has ended: it is done, and only a new deployment
    /// runs it again.
    pub ran_once: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedVolume {
    pub volume_id: VolumeId,
    pub app_id: AppId,
    pub attached: bool,
    /// Whether the device carries a filesystem. A refused seed leaves a volume attached and bare,
    /// and only a volume that is both is one an app can be booted onto.
    pub formatted: bool,
    pub size_bytes: u64,
    pub storage_prefix: ObjectKey,
    pub device_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub volume_id: VolumeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedExport {
    pub export_id: ExportId,
    pub written: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObservedState {
    pub instances: Vec<ObservedInstance>,
    pub volumes: Vec<ObservedVolume>,
    pub checkpoints: Vec<ObservedCheckpoint>,
    pub exports: Vec<ObservedExport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceStopReason {
    DesiredStopped,
    NotDesired,
    Superseded,
    Idle,
    VolumeLost,
}

impl InstanceStopReason {
    pub fn as_str(self) -> &'static str {
        match self {
            InstanceStopReason::DesiredStopped => "desired-stopped",
            InstanceStopReason::NotDesired => "not-desired",
            InstanceStopReason::Superseded => "superseded",
            InstanceStopReason::Idle => "idle",
            InstanceStopReason::VolumeLost => "volume-lost",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InstancePlan {
    Start {
        desired: DesiredInstance,
    },
    Replace {
        desired: DesiredInstance,
    },
    /// Cold-restart an instance whose data volume backing was pulled out from under it (a ZeroFS
    /// restart yanks the NBD device the guest was reading). The host re-attaches the device and
    /// the guest is booted afresh onto it, since a running guest cannot pick a yanked disk back up
    /// in place.
    Recover {
        desired: DesiredInstance,
    },
    Sleep {
        desired: DesiredInstance,
    },
    /// A stopped record on a slot of its own for an app the document wants down that this host
    /// has never held, with nothing booted under it: what a stop leaves behind, so that its
    /// hostnames say it is stopped rather than that it is not here.
    Hold {
        desired: DesiredInstance,
    },
    Stop {
        app_id: AppId,
        reason: InstanceStopReason,
    },
    Forget {
        app_id: AppId,
    },
    None {
        app_id: AppId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumePlan {
    Provision {
        desired: DesiredVolume,
    },
    Teardown {
        desired: DesiredVolume,
    },
    Blocked {
        desired: DesiredVolume,
        blocked_by: Vec<AppId>,
    },
    /// Let go of a volume the document no longer names: its device is taken down and the slot
    /// its app held given back, and its data is kept. `absent` is the word for deleting that.
    Detach {
        volume_id: VolumeId,
        app_id: AppId,
    },
    None {
        volume_id: VolumeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointPlan {
    Create { desired: DesiredCheckpoint },
    Delete { desired: DesiredCheckpoint },
    None { checkpoint_id: CheckpointId },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExportPlan {
    Write { desired: DesiredExport },
    Forget { export_id: ExportId },
    None { export_id: ExportId },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReconcilePlan {
    pub instances: Vec<InstancePlan>,
    pub volumes: Vec<VolumePlan>,
    pub checkpoints: Vec<CheckpointPlan>,
    pub exports: Vec<ExportPlan>,
}

impl ReconcilePlan {
    pub fn has_deferred_work(&self) -> bool {
        self.volumes
            .iter()
            .any(|action| matches!(action, VolumePlan::Blocked { .. }))
    }
}

pub fn plan_reconcile(desired: &HostDesiredState, observed: &ObservedState) -> ReconcilePlan {
    ReconcilePlan {
        instances: plan_instances(desired, observed),
        volumes: plan_volumes(desired, observed),
        checkpoints: plan_checkpoints(desired, observed),
        exports: plan_exports(desired, observed),
    }
}

fn plan_instance(
    wanted: &DesiredInstance,
    current: Option<&ObservedInstance>,
    volume_lost: bool,
) -> InstancePlan {
    // A guest whose disk was yanked keeps a dead virtio-blk device even after the host re-attaches
    // the NBD device, so the only way back is a fresh boot onto the recovered device.
    let recover = || InstancePlan::Recover {
        desired: wanted.clone(),
    };
    if wanted.desired_state == DesiredInstanceState::Stopped {
        let Some(current) = current.filter(|instance| instance.present) else {
            return InstancePlan::Hold {
                desired: wanted.clone(),
            };
        };
        return if current.running {
            InstancePlan::Stop {
                app_id: wanted.app_id.clone(),
                reason: InstanceStopReason::DesiredStopped,
            }
        } else {
            InstancePlan::None {
                app_id: wanted.app_id.clone(),
            }
        };
    }
    if wanted.desired_state == DesiredInstanceState::OnRequest {
        let Some(current) = current.filter(|instance| instance.present) else {
            return InstancePlan::Start {
                desired: wanted.clone(),
            };
        };
        if current.deployment_id.as_ref() != Some(&wanted.deployment_id) {
            return InstancePlan::Replace {
                desired: wanted.clone(),
            };
        }
        if current.ran_once {
            return InstancePlan::None {
                app_id: wanted.app_id.clone(),
            };
        }
        if current.expired {
            return if wanted.expiry.is_some() {
                InstancePlan::None {
                    app_id: wanted.app_id.clone(),
                }
            } else {
                InstancePlan::Replace {
                    desired: wanted.clone(),
                }
            };
        }
        // An idle on-request app has no live guest to recover: its device reading as gone is only
        // the outage, and it is left asleep to be woken (and re-attached) when it is next asked for.
        // One refused before it was ever brought up is not asleep: a request would only find it
        // failed, so it is started the way an app this host has never served is.
        return if current.running {
            if volume_lost {
                recover()
            } else {
                InstancePlan::None {
                    app_id: wanted.app_id.clone(),
                }
            }
        } else if current.refused {
            InstancePlan::Start {
                desired: wanted.clone(),
            }
        } else {
            InstancePlan::Sleep {
                desired: wanted.clone(),
            }
        };
    }
    let Some(current) = current.filter(|instance| instance.present) else {
        return InstancePlan::Start {
            desired: wanted.clone(),
        };
    };
    if current.deployment_id.as_ref() != Some(&wanted.deployment_id) {
        return InstancePlan::Replace {
            desired: wanted.clone(),
        };
    }
    if current.ran_once {
        return InstancePlan::None {
            app_id: wanted.app_id.clone(),
        };
    }
    if current.running {
        return if volume_lost {
            recover()
        } else {
            InstancePlan::None {
                app_id: wanted.app_id.clone(),
            }
        };
    }
    // A guest that exited on its own is booted again — the start applies the app's restart
    // policy, so a crash loop is bounded — unless its disk was pulled out from under it, when the
    // boot has to be the clean teardown and re-attach a recovery does first. A cold record is a
    // plain start either way.
    if current.exited && volume_lost {
        return recover();
    }
    InstancePlan::Start {
        desired: wanted.clone(),
    }
}

fn plan_instances(desired: &HostDesiredState, observed: &ObservedState) -> Vec<InstancePlan> {
    let observed_by_id: BTreeMap<&AppId, &ObservedInstance> = observed
        .instances
        .iter()
        .map(|instance| (&instance.app_id, instance))
        .collect();
    // A volume this host serves but can no longer read (the NBD device is dead because ZeroFS was
    // restarted under it) is a backing that has been pulled out from under whatever holds it.
    let lost_volumes: BTreeSet<&VolumeId> = observed
        .volumes
        .iter()
        .filter(|volume| !volume.attached)
        .map(|volume| &volume.volume_id)
        .collect();
    let desired_ids: BTreeSet<&AppId> = desired
        .instances
        .iter()
        .map(|instance| &instance.app_id)
        .collect();
    let mut plans: Vec<InstancePlan> = desired
        .instances
        .iter()
        .map(|wanted| {
            plan_instance(
                wanted,
                observed_by_id.get(&wanted.app_id).copied(),
                wanted
                    .volume_id
                    .as_ref()
                    .is_some_and(|id| lost_volumes.contains(id)),
            )
        })
        .collect();
    plans.extend(
        observed
            .instances
            .iter()
            .filter(|current| !desired_ids.contains(&current.app_id))
            .map(|current| {
                if current.running {
                    InstancePlan::Stop {
                        app_id: current.app_id.clone(),
                        reason: InstanceStopReason::NotDesired,
                    }
                } else {
                    InstancePlan::Forget {
                        app_id: current.app_id.clone(),
                    }
                }
            }),
    );
    plans
}

fn plan_volumes(desired: &HostDesiredState, observed: &ObservedState) -> Vec<VolumePlan> {
    let observed_by_id: BTreeMap<&VolumeId, &ObservedVolume> = observed
        .volumes
        .iter()
        .map(|volume| (&volume.volume_id, volume))
        .collect();
    let mut used_by: BTreeMap<VolumeId, Vec<AppId>> = BTreeMap::new();
    for instance in &observed.instances {
        if !instance.present {
            continue;
        }
        let Some(volume_id) = &instance.volume_id else {
            continue;
        };
        used_by
            .entry(volume_id.clone())
            .or_default()
            .push(instance.app_id.clone());
    }
    for volume_id in desired
        .instances
        .iter()
        .filter_map(|instance| instance.volume_id.as_ref())
    {
        used_by.entry(volume_id.clone()).or_default();
    }
    // A volume the document no longer names goes the way an instance it no longer names does,
    // once nothing needs it: a running guest keeps its disk until the stop has landed, and an app
    // the document still names keeps its slot whatever it says of the app — an instance without a
    // volume entry is the instance's own trouble, not a reason to move it.
    let wanted_apps: BTreeSet<&AppId> = desired
        .instances
        .iter()
        .map(|instance| &instance.app_id)
        .collect();
    let kept_for: BTreeSet<&VolumeId> = observed
        .instances
        .iter()
        .filter(|instance| instance.running || wanted_apps.contains(&instance.app_id))
        .filter_map(|instance| instance.volume_id.as_ref())
        .collect();
    let desired_ids: BTreeSet<&VolumeId> = desired.volumes.iter().map(|wanted| &wanted.volume_id).collect();

    let mut plans: Vec<VolumePlan> = desired
        .volumes
        .iter()
        .map(|wanted| {
            let current = observed_by_id.get(&wanted.volume_id).copied();
            if wanted.desired_state == DesiredPresence::Absent {
                let holders = used_by.get(&wanted.volume_id).cloned().unwrap_or_default();
                if !holders.is_empty() {
                    return VolumePlan::Blocked {
                        desired: wanted.clone(),
                        blocked_by: holders,
                    };
                }
                return match current {
                    Some(_) => VolumePlan::Teardown {
                        desired: wanted.clone(),
                    },
                    None => VolumePlan::None {
                        volume_id: wanted.volume_id.clone(),
                    },
                };
            }
            // An attached device with no filesystem on it is provisioned again rather than left
            // alone: a seed that was refused is retried, and a document that has since put the
            // seed right takes effect, where taking "attached" for "ready" booted apps onto a
            // volume nothing could mount.
            match current {
                Some(volume)
                    if volume.attached && volume.formatted && volume.size_bytes >= wanted.size_bytes =>
                {
                    VolumePlan::None {
                        volume_id: wanted.volume_id.clone(),
                    }
                }
                _ => VolumePlan::Provision {
                    desired: wanted.clone(),
                },
            }
        })
        .collect();
    // A terminal retention decision authorizes deleting its unreferenced disk once the
    // document drops both entries. Keep planning this even if an earlier delete reached disk
    // but the daemon crashed before forgetting the record.
    let mut expired_disks = BTreeSet::new();
    for instance in &observed.instances {
        let Some(id) = &instance.volume_id else {
            continue;
        };
        if !instance.expired
            || instance.running
            || wanted_apps.contains(&instance.app_id)
            || desired_ids.contains(id)
            || kept_for.contains(id)
            || desired.instances.iter().any(|i| i.volume_id.as_ref() == Some(id))
            || observed
                .instances
                .iter()
                .any(|i| i.app_id != instance.app_id && i.volume_id.as_ref() == Some(id))
        {
            continue;
        }
        expired_disks.insert(id.clone());
        plans.push(VolumePlan::Teardown {
            desired: DesiredVolume {
                volume_id: id.clone(),
                app_id: instance.app_id.clone(),
                size_bytes: observed_by_id.get(id).map_or(0, |v| v.size_bytes),
                desired_state: DesiredPresence::Absent,
                initial_contents: None,
            },
        });
    }
    plans.extend(
        observed
            .volumes
            .iter()
            .filter(|current| {
                current.attached
                    && !desired_ids.contains(&current.volume_id)
                    && !expired_disks.contains(&current.volume_id)
            })
            .filter(|current| !kept_for.contains(&current.volume_id))
            .map(|current| VolumePlan::Detach {
                volume_id: current.volume_id.clone(),
                app_id: current.app_id.clone(),
            }),
    );
    plans
}

fn plan_exports(desired: &HostDesiredState, observed: &ObservedState) -> Vec<ExportPlan> {
    let written_ids: BTreeSet<&ExportId> = observed
        .exports
        .iter()
        .filter(|current| current.written)
        .map(|current| &current.export_id)
        .collect();
    let desired_ids: BTreeSet<&ExportId> = desired.exports.iter().map(|wanted| &wanted.export_id).collect();
    let mut plans: Vec<ExportPlan> = desired
        .exports
        .iter()
        .map(|wanted| {
            if wanted.desired_state == DesiredPresence::Absent {
                return ExportPlan::Forget {
                    export_id: wanted.export_id.clone(),
                };
            }
            if written_ids.contains(&wanted.export_id) {
                ExportPlan::None {
                    export_id: wanted.export_id.clone(),
                }
            } else {
                ExportPlan::Write {
                    desired: wanted.clone(),
                }
            }
        })
        .collect();
    plans.extend(
        observed
            .exports
            .iter()
            .filter(|current| !desired_ids.contains(&current.export_id))
            .map(|current| ExportPlan::Forget {
                export_id: current.export_id.clone(),
            }),
    );
    plans
}

fn plan_checkpoints(desired: &HostDesiredState, observed: &ObservedState) -> Vec<CheckpointPlan> {
    let observed_ids: BTreeSet<&CheckpointId> = observed
        .checkpoints
        .iter()
        .map(|checkpoint| &checkpoint.checkpoint_id)
        .collect();
    desired
        .checkpoints
        .iter()
        .map(|wanted| {
            let exists = observed_ids.contains(&wanted.checkpoint_id);
            match (wanted.desired_state, exists) {
                (DesiredPresence::Present, true) => CheckpointPlan::None {
                    checkpoint_id: wanted.checkpoint_id.clone(),
                },
                (DesiredPresence::Present, false) => CheckpointPlan::Create {
                    desired: wanted.clone(),
                },
                (DesiredPresence::Absent, true) => CheckpointPlan::Delete {
                    desired: wanted.clone(),
                },
                (DesiredPresence::Absent, false) => CheckpointPlan::None {
                    checkpoint_id: wanted.checkpoint_id.clone(),
                },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    fn plan(desired: HostDesiredState, observed: ObservedState) -> ReconcilePlan {
        plan_reconcile(&desired, &observed)
    }

    mod instances_are_authoritative {
        use super::*;

        #[test]
        fn a_desired_instance_nothing_is_running_is_started() {
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                ObservedState::default(),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Start {
                    desired: desired_instance(|_| {})
                }]
            );
        }

        #[test]
        fn a_running_instance_the_control_plane_does_not_mention_is_stopped() {
            let result = plan(
                desired_state(|_| {}),
                observed_state(|state| state.instances = vec![observed_instance(|_| {})]),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Stop {
                    app_id: app_id(),
                    reason: InstanceStopReason::NotDesired
                }]
            );
        }

        #[test]
        fn a_stopped_instance_the_control_plane_does_not_mention_is_forgotten() {
            let result = plan(
                desired_state(|_| {}),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.volume_id = None;
                        instance.deployment_id = None;
                        instance.running = false;
                        instance.exited = true;
                    })]
                }),
            );
            assert_eq!(result.instances, vec![InstancePlan::Forget { app_id: app_id() }]);
        }

        #[test]
        fn a_deployment_change_replaces_rather_than_restarts() {
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.deployment_id = Some(DeploymentId::parse("dep-0").unwrap())
                    })]
                }),
            );
            assert!(matches!(result.instances[0], InstancePlan::Replace { .. }));
        }

        #[test]
        fn a_microvm_with_no_record_is_treated_as_a_mismatch_not_as_converged() {
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.volume_id = None;
                        instance.deployment_id = None;
                    })]
                }),
            );
            assert!(matches!(result.instances[0], InstancePlan::Replace { .. }));
        }

        #[test]
        fn a_vm_that_exited_on_its_own_is_booted_again() {
            // The start it is handed applies the app's restart policy, so this is what keeps a
            // running app up, not what loops a crashing one.
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = true;
                    })]
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Start {
                    desired: desired_instance(|_| {})
                }]
            );
        }

        #[test]
        fn a_vm_that_has_not_run_since_the_host_booted_is_started() {
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = false;
                    })]
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Start {
                    desired: desired_instance(|_| {})
                }]
            );
        }

        #[test]
        fn desired_state_stopped_stops_a_running_instance_and_leaves_a_stopped_one_alone() {
            let stopped =
                || desired_instance(|instance| instance.desired_state = DesiredInstanceState::Stopped);
            let running = plan(
                desired_state(|state| state.instances = vec![stopped()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.volume_id = None;
                        instance.deployment_id = None;
                    })]
                }),
            );
            assert_eq!(
                running.instances[0],
                InstancePlan::Stop {
                    app_id: app_id(),
                    reason: InstanceStopReason::DesiredStopped
                }
            );
            let already = plan(
                desired_state(|state| state.instances = vec![stopped()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = true;
                    })]
                }),
            );
            assert_eq!(already.instances, vec![InstancePlan::None { app_id: app_id() }]);
        }

        #[test]
        fn a_stopped_app_this_host_has_never_held_is_held_rather_than_left_off_the_report() {
            // What was measured: an app added to the document already stopped was never written
            // down, so its hostnames answered that it was not here rather than that it was down.
            let stopped =
                || desired_instance(|instance| instance.desired_state = DesiredInstanceState::Stopped);
            let result = plan(
                desired_state(|state| state.instances = vec![stopped()]),
                ObservedState::default(),
            );
            assert_eq!(result.instances, vec![InstancePlan::Hold { desired: stopped() }]);
        }
    }

    mod an_app_that_runs_on_request {
        use super::*;

        fn on_request() -> DesiredInstance {
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest)
        }

        #[test]
        fn one_this_host_has_never_served_is_started_rather_than_put_to_sleep() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                ObservedState::default(),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Start {
                    desired: on_request()
                }]
            );
        }

        #[test]
        fn one_already_sleeping_stays_that_way_however_many_times_this_runs() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = false;
                    })]
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Sleep {
                    desired: on_request()
                }]
            );
        }

        #[test]
        fn one_whose_guest_exited_is_left_for_the_next_request_rather_than_booted_again() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = true;
                    })]
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Sleep {
                    desired: on_request()
                }]
            );
        }

        #[test]
        fn one_refused_before_it_was_ever_brought_up_is_started_rather_than_left_for_a_request() {
            // What a refused seed leaves once the volume is put right: a failed record with no
            // attempt spent, and nothing running under it. A request would only find it failed.
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = false;
                        instance.refused = true;
                    })]
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Start {
                    desired: on_request()
                }]
            );
        }

        #[test]
        fn a_deploy_onto_one_that_is_asleep_replaces_it_rather_than_leaving_it_asleep() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = false;
                        instance.deployment_id = Some(DeploymentId::parse("dep-0").unwrap());
                    })]
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Replace {
                    desired: on_request()
                }]
            );
        }

        #[test]
        fn one_that_is_up_and_serving_the_release_it_should_be_is_left_alone() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| state.instances = vec![observed_instance(|_| {})]),
            );
            assert_eq!(result.instances, vec![InstancePlan::None { app_id: app_id() }]);
        }

        #[test]
        fn one_that_is_up_on_an_older_release_is_replaced() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.deployment_id = Some(DeploymentId::parse("dep-0").unwrap())
                    })]
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Replace {
                    desired: on_request()
                }]
            );
        }
    }

    mod a_program_run_once {
        use super::*;

        fn ended() -> ObservedInstance {
            observed_instance(|instance| {
                instance.running = false;
                instance.exited = true;
                instance.ran_once = true;
            })
        }

        #[test]
        fn that_has_ended_is_not_started_again() {
            for wanted in [DesiredInstanceState::Running, DesiredInstanceState::OnRequest] {
                let result = plan(
                    desired_state(|state| {
                        state.instances = vec![desired_instance(|instance| instance.desired_state = wanted)]
                    }),
                    observed_state(|state| state.instances = vec![ended()]),
                );
                assert_eq!(
                    result.instances,
                    vec![InstancePlan::None { app_id: app_id() }],
                    "{wanted:?}"
                );
            }
        }

        #[test]
        fn that_has_ended_is_run_again_by_a_new_deployment() {
            let redeployed = desired_instance(|instance| {
                instance.deployment_id = DeploymentId::parse("dep-2").unwrap();
            });
            let result = plan(
                desired_state(|state| state.instances = vec![redeployed.clone()]),
                observed_state(|state| state.instances = vec![ended()]),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Replace { desired: redeployed }]
            );
        }
    }

    mod a_volume_pulled_out_from_under_a_running_guest {
        use super::*;

        fn detached_volume() -> ObservedVolume {
            observed_volume(|volume| volume.attached = false)
        }

        fn on_request() -> DesiredInstance {
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest)
        }

        #[test]
        fn a_running_guest_whose_disk_is_gone_is_recovered_rather_than_left_running_broken() {
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|_| {})];
                    state.volumes = vec![detached_volume()];
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Recover {
                    desired: desired_instance(|_| {})
                }]
            );
        }

        #[test]
        fn one_whose_disk_is_still_there_is_left_exactly_as_it_was() {
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|_| {})];
                    state.volumes = vec![observed_volume(|_| {})];
                }),
            );
            assert_eq!(result.instances, vec![InstancePlan::None { app_id: app_id() }]);
        }

        #[test]
        fn a_guest_that_already_exited_under_the_outage_is_recovered_rather_than_merely_started() {
            // An exited guest is booted again anyway; the yanked disk is what makes that boot a
            // recovery — the clean teardown and re-attach — rather than a plain start onto a dead
            // device.
            let exited = observed_state(|state| {
                state.instances = vec![observed_instance(|instance| {
                    instance.running = false;
                    instance.exited = true;
                })];
                state.volumes = vec![detached_volume()];
            });
            let result = plan(
                desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
                exited,
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Recover {
                    desired: desired_instance(|_| {})
                }]
            );
        }

        #[test]
        fn an_idle_on_request_app_is_left_asleep_since_it_has_no_live_guest_to_recover() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = false;
                    })];
                    state.volumes = vec![detached_volume()];
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Sleep {
                    desired: on_request()
                }]
            );
        }

        #[test]
        fn a_woken_on_request_guest_with_a_dead_disk_is_recovered_like_any_other() {
            let result = plan(
                desired_state(|state| state.instances = vec![on_request()]),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|_| {})];
                    state.volumes = vec![detached_volume()];
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Recover {
                    desired: on_request()
                }]
            );
        }

        #[test]
        fn only_the_guest_whose_own_volume_died_is_recovered() {
            let other_app = AppId::parse("app-2").unwrap();
            let result = plan(
                desired_state(|state| {
                    state.instances = vec![
                        desired_instance(|_| {}),
                        desired_instance(|instance| {
                            instance.app_id = AppId::parse("app-2").unwrap();
                            instance.volume_id = Some(VolumeId::parse("vol-2").unwrap());
                        }),
                    ]
                }),
                observed_state(|state| {
                    state.instances = vec![
                        observed_instance(|_| {}),
                        observed_instance(|instance| {
                            instance.app_id = AppId::parse("app-2").unwrap();
                            instance.volume_id = Some(VolumeId::parse("vol-2").unwrap());
                        }),
                    ];
                    // Only the first app's volume is gone; the second one's is fine.
                    state.volumes = vec![
                        detached_volume(),
                        observed_volume(|volume| volume.volume_id = VolumeId::parse("vol-2").unwrap()),
                    ];
                }),
            );
            assert!(matches!(result.instances[0], InstancePlan::Recover { .. }));
            assert_eq!(result.instances[1], InstancePlan::None { app_id: other_app });
        }

        #[test]
        fn a_disk_that_died_under_an_app_asked_to_stop_still_only_stops_it() {
            // Recovery is for apps meant to be up; a lost disk never boots one the document wants down.
            let result = plan(
                desired_state(|state| {
                    state.instances = vec![desired_instance(|instance| {
                        instance.desired_state = DesiredInstanceState::Stopped
                    })]
                }),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|_| {})];
                    state.volumes = vec![detached_volume()];
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Stop {
                    app_id: app_id(),
                    reason: InstanceStopReason::DesiredStopped
                }]
            );
        }
    }

    mod scratch {
        use super::*;

        fn scratch_instance() -> DesiredInstance {
            desired_instance(|instance| {
                instance.volume_id = None;
                instance.scratch = Some(protocol::Scratch::Memory {
                    mib: std::num::NonZeroU32::new(64).unwrap(),
                });
            })
        }

        #[test]
        fn an_instance_with_a_scratch_is_started_with_no_volume_planned_for_it() {
            let result = plan(
                desired_state(|state| {
                    state.volumes = vec![];
                    state.instances = vec![scratch_instance()];
                }),
                observed_state(|_| {}),
            );
            assert!(result.volumes.is_empty(), "{:?}", result.volumes);
            assert_eq!(
                result.instances,
                vec![InstancePlan::Start {
                    desired: scratch_instance()
                }]
            );
        }

        #[test]
        fn a_dead_disk_is_no_reason_to_recover_an_instance_that_writes_to_a_scratch() {
            let result = plan(
                desired_state(|state| {
                    state.volumes = vec![];
                    state.instances = vec![scratch_instance()];
                }),
                observed_state(|state| {
                    state.instances = vec![observed_instance(|instance| instance.volume_id = None)];
                    state.volumes = vec![observed_volume(|volume| volume.attached = false)];
                }),
            );
            assert_eq!(result.instances, vec![InstancePlan::None { app_id: app_id() }]);
        }
    }

    mod volumes_are_not_authoritative {
        use super::*;

        fn absent() -> DesiredVolume {
            desired_volume(|volume| volume.desired_state = DesiredPresence::Absent)
        }

        fn detach() -> VolumePlan {
            VolumePlan::Detach {
                volume_id: volume_id(),
                app_id: app_id(),
            }
        }

        #[test]
        fn an_attached_volume_the_document_no_longer_names_is_detached_rather_than_left_on_its_slot() {
            // What was measured: an entry deleted outright, rather than set absent, left the device
            // attached and the slot taken for good, and the host filled up on volumes nobody named.
            let result = plan(
                desired_state(|_| {}),
                observed_state(|state| state.volumes = vec![observed_volume(|_| {})]),
            );
            assert_eq!(result.volumes, vec![detach()]);
        }

        #[test]
        fn one_a_running_guest_still_reads_is_left_under_it_until_the_guest_has_stopped() {
            let result = plan(
                desired_state(|_| {}),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|_| {})];
                    state.instances = vec![observed_instance(|_| {})];
                }),
            );
            assert_eq!(
                result.instances,
                vec![InstancePlan::Stop {
                    app_id: app_id(),
                    reason: InstanceStopReason::NotDesired
                }]
            );
            assert_eq!(result.volumes, vec![]);
        }

        #[test]
        fn one_whose_guest_has_stopped_goes_the_pass_its_record_is_forgotten() {
            let result = plan(
                desired_state(|_| {}),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|_| {})];
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = true;
                    })];
                }),
            );
            assert_eq!(result.instances, vec![InstancePlan::Forget { app_id: app_id() }]);
            assert_eq!(result.volumes, vec![detach()]);
        }

        #[test]
        fn one_whose_app_the_document_still_names_is_left_alone_whatever_it_says_of_the_app() {
            // An instance without a volume entry is that instance's own trouble; its volume and
            // its slot are not taken away under it, asleep or stopped.
            for wanted in [DesiredInstanceState::OnRequest, DesiredInstanceState::Stopped] {
                let result = plan(
                    desired_state(|state| {
                        state.instances = vec![desired_instance(|instance| instance.desired_state = wanted)]
                    }),
                    observed_state(|state| {
                        state.volumes = vec![observed_volume(|_| {})];
                        state.instances = vec![observed_instance(|instance| instance.running = false)];
                    }),
                );
                assert_eq!(result.volumes, vec![], "{wanted:?}");
            }
        }

        #[test]
        fn one_this_host_does_not_hold_a_device_for_has_nothing_to_take_down() {
            let result = plan(
                desired_state(|_| {}),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|volume| volume.attached = false)]
                }),
            );
            assert_eq!(result.volumes, vec![]);
        }

        #[test]
        fn removal_requires_an_explicit_absent() {
            let result = plan(
                desired_state(|state| state.volumes = vec![absent()]),
                observed_state(|state| state.volumes = vec![observed_volume(|_| {})]),
            );
            assert_eq!(
                result.volumes,
                vec![VolumePlan::Teardown { desired: absent() }],
                "absent deletes; only a vanished entry merely detaches"
            );
        }

        #[test]
        fn a_volume_still_held_by_an_instance_is_blocked_rather_than_destroyed() {
            let result = plan(
                desired_state(|state| state.volumes = vec![absent()]),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|_| {})];
                    state.instances = vec![observed_instance(|instance| {
                        instance.running = false;
                        instance.exited = true;
                    })];
                }),
            );
            assert_eq!(
                result.volumes[0],
                VolumePlan::Blocked {
                    desired: absent(),
                    blocked_by: vec![app_id()]
                }
            );
            assert!(result.has_deferred_work());
        }

        #[test]
        fn a_plan_that_finished_everything_leaves_nothing_to_re_run_for() {
            let result = plan(
                desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
                observed_state(|state| state.volumes = vec![observed_volume(|_| {})]),
            );
            assert!(matches!(result.volumes[0], VolumePlan::None { .. }));
            assert!(!result.has_deferred_work());
        }

        #[test]
        fn a_volume_the_document_wants_gone_that_is_already_gone_is_nothing_to_do() {
            let result = plan(
                desired_state(|state| state.volumes = vec![absent()]),
                ObservedState::default(),
            );
            assert_eq!(
                result.volumes,
                vec![VolumePlan::None {
                    volume_id: volume_id()
                }]
            );
            assert!(!result.has_deferred_work());
        }

        #[test]
        fn a_microvm_that_is_no_longer_there_does_not_hold_the_volume_it_used_to() {
            let result = plan(
                desired_state(|state| state.volumes = vec![absent()]),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|_| {})];
                    state.instances = vec![observed_instance(|instance| {
                        instance.present = false;
                        instance.running = false;
                    })];
                }),
            );
            assert!(matches!(result.volumes[0], VolumePlan::Teardown { .. }));
        }

        #[test]
        fn an_instance_with_no_volume_of_its_own_holds_nobody_elses() {
            let result = plan(
                desired_state(|state| state.volumes = vec![absent()]),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|_| {})];
                    state.instances = vec![observed_instance(|instance| instance.volume_id = None)];
                }),
            );
            assert!(matches!(result.volumes[0], VolumePlan::Teardown { .. }));
        }

        #[test]
        fn an_unattached_volume_is_provisioned_and_a_grown_one_re_provisioned() {
            let missing = plan(
                desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
                ObservedState::default(),
            );
            assert!(matches!(missing.volumes[0], VolumePlan::Provision { .. }));
            let small = plan(
                desired_state(|state| {
                    state.volumes = vec![desired_volume(|volume| volume.size_bytes = VOLUME_SIZE_BYTES * 2)]
                }),
                observed_state(|state| state.volumes = vec![observed_volume(|_| {})]),
            );
            assert!(matches!(small.volumes[0], VolumePlan::Provision { .. }));
            let detached = plan(
                desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|volume| volume.attached = false)]
                }),
            );
            assert!(matches!(detached.volumes[0], VolumePlan::Provision { .. }));
        }

        #[test]
        fn an_attached_volume_with_no_filesystem_on_it_is_provisioned_again_every_pass() {
            // What a refused seed leaves: the device answers reads, so it is attached, but the
            // format never happened. Left alone it would be reported ready and booted onto.
            let bare = plan(
                desired_state(|state| state.volumes = vec![desired_volume(|_| {})]),
                observed_state(|state| {
                    state.volumes = vec![observed_volume(|volume| volume.formatted = false)]
                }),
            );
            assert_eq!(
                bare.volumes,
                vec![VolumePlan::Provision {
                    desired: desired_volume(|_| {})
                }]
            );
        }
    }

    #[test]
    fn checkpoints_create_only_what_is_missing_and_delete_only_what_exists() {
        let held = observed_state(|state| {
            state.checkpoints = vec![ObservedCheckpoint {
                checkpoint_id: checkpoint_id(),
                volume_id: volume_id(),
            }]
        });
        let create = plan(
            desired_state(|state| state.checkpoints = vec![desired_checkpoint(|_| {})]),
            ObservedState::default(),
        );
        assert!(matches!(create.checkpoints[0], CheckpointPlan::Create { .. }));
        let already = plan(
            desired_state(|state| state.checkpoints = vec![desired_checkpoint(|_| {})]),
            held.clone(),
        );
        assert!(matches!(already.checkpoints[0], CheckpointPlan::None { .. }));
        let remove = plan(
            desired_state(|state| {
                state.checkpoints = vec![desired_checkpoint(|checkpoint| {
                    checkpoint.desired_state = DesiredPresence::Absent
                })]
            }),
            held,
        );
        assert!(matches!(remove.checkpoints[0], CheckpointPlan::Delete { .. }));
        let gone = plan(
            desired_state(|state| {
                state.checkpoints = vec![desired_checkpoint(|checkpoint| {
                    checkpoint.desired_state = DesiredPresence::Absent
                })]
            }),
            ObservedState::default(),
        );
        assert!(matches!(gone.checkpoints[0], CheckpointPlan::None { .. }));
    }

    mod exports {
        use super::*;

        #[test]
        fn a_bundle_this_host_has_not_written_is_written_and_one_already_written_never_twice() {
            let fresh = plan(
                desired_state(|state| state.exports = vec![desired_export(|_| {})]),
                ObservedState::default(),
            );
            assert_eq!(
                fresh.exports,
                vec![ExportPlan::Write {
                    desired: desired_export(|_| {})
                }]
            );
            let written = plan(
                desired_state(|state| state.exports = vec![desired_export(|_| {})]),
                observed_state(|state| {
                    state.exports = vec![ObservedExport {
                        export_id: export_id(),
                        written: true,
                    }]
                }),
            );
            assert!(matches!(written.exports[0], ExportPlan::None { .. }));
        }

        #[test]
        fn a_bundle_that_failed_is_retried() {
            let result = plan(
                desired_state(|state| state.exports = vec![desired_export(|_| {})]),
                observed_state(|state| {
                    state.exports = vec![ObservedExport {
                        export_id: export_id(),
                        written: false,
                    }]
                }),
            );
            assert!(matches!(result.exports[0], ExportPlan::Write { .. }));
        }

        #[test]
        fn absent_forgets_the_record_rather_than_deleting_an_object_it_cannot_reach() {
            let result = plan(
                desired_state(|state| {
                    state.exports = vec![desired_export(|export| {
                        export.desired_state = DesiredPresence::Absent
                    })]
                }),
                observed_state(|state| {
                    state.exports = vec![ObservedExport {
                        export_id: export_id(),
                        written: true,
                    }]
                }),
            );
            assert_eq!(
                result.exports,
                vec![ExportPlan::Forget {
                    export_id: export_id()
                }]
            );
        }

        #[test]
        fn a_record_desired_state_does_not_mention_at_all_is_forgotten_too() {
            let result = plan(
                desired_state(|_| {}),
                observed_state(|state| {
                    state.exports = vec![ObservedExport {
                        export_id: export_id(),
                        written: true,
                    }]
                }),
            );
            assert_eq!(
                result.exports,
                vec![ExportPlan::Forget {
                    export_id: export_id()
                }]
            );
        }
    }

    #[test]
    fn every_reason_an_instance_is_stopped_is_one_the_logs_can_name() {
        assert_eq!(InstanceStopReason::DesiredStopped.as_str(), "desired-stopped");
        assert_eq!(InstanceStopReason::NotDesired.as_str(), "not-desired");
        assert_eq!(InstanceStopReason::Superseded.as_str(), "superseded");
        assert_eq!(InstanceStopReason::Idle.as_str(), "idle");
        assert_eq!(InstanceStopReason::VolumeLost.as_str(), "volume-lost");
    }

    #[test]
    fn a_plan_with_nothing_in_it_has_nothing_left_for_the_next_pass() {
        assert!(!ReconcilePlan::default().has_deferred_work());
        assert!(!plan(desired_state(|_| {}), ObservedState::default()).has_deferred_work());
    }

    #[test]
    fn a_host_serving_several_apps_gets_one_action_for_each_of_them() {
        let other = AppId::parse("app-2").unwrap();
        let result = plan(
            desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
            observed_state(|state| {
                state.instances = vec![
                    observed_instance(|_| {}),
                    observed_instance(|instance| instance.app_id = AppId::parse("app-2").unwrap()),
                ]
            }),
        );
        assert_eq!(
            result.instances,
            vec![
                InstancePlan::None { app_id: app_id() },
                InstancePlan::Stop {
                    app_id: other,
                    reason: InstanceStopReason::NotDesired
                },
            ]
        );
    }
}
