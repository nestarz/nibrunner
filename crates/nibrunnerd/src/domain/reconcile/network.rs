use std::sync::Arc;

use nft_render::{FirewallState, ForwardedInstance, ForwardedPort};
use protocol::InstanceState;

use crate::adapters::proxy::stream_activator::StreamBinding;
use crate::adapters::proxy::RouteTable;
use crate::domain::report::routes::renderable_routes;
use crate::domain::report::InstanceRecord;
use crate::host::Host;

pub async fn forwarded_instances(host: &Host) -> Vec<ForwardedInstance> {
    let snapshot = host.state.snapshot().await;
    let mut forwarded = Vec::new();
    for record in snapshot.records.values() {
        // A guest being written out reads as Running until the snapshot lands, and is paused for
        // most of that. Its port is the activator's meanwhile, so that a request finding it is
        // held for the restore rather than connected to nothing.
        if record.state != InstanceState::Running || snapshot.snapshotting.contains(&record.app_id) {
            continue;
        }
        let Some(slot) = host.slot_of(&record.app_id).await else {
            continue;
        };
        let mut ports = vec![ForwardedPort {
            host_port: slot.host_port,
            guest_port: protocol::GuestPort::new(record.http_port.get()).expect("a port is never zero"),
            raw: false,
        }];
        ports.extend(record.ports.iter().map(|port| ForwardedPort {
            host_port: port.host_port,
            guest_port: port.guest_port,
            raw: true,
        }));
        forwarded.push(ForwardedInstance {
            app_id: record.app_id.clone(),
            ports,
            host_ipv4: slot.host_ipv4,
            guest_ipv4: slot.guest_ipv4,
        });
    }
    forwarded
}

pub async fn apply_network(host: &Host) {
    let state = FirewallState {
        instances: forwarded_instances(host).await,
        denied_egress_addresses_v4: host.config.denied_egress_addresses_v4.clone(),
        denied_egress_addresses_v6: host.config.denied_egress_addresses_v6.clone(),
    };
    match host.firewall.apply(&state).await {
        Ok(()) => host.state.modify(|snapshot| snapshot.isolated = true).await,
        Err(error) => {
            host.state.modify(|snapshot| snapshot.isolated = false).await;
            tracing::error!(error = %error.message(), "firewall apply failed");
        }
    }
}

/// Only what a record names.
///
/// A slot reserves eight host ports so that the limit on how many an app may ask for can be
/// raised without moving anybody's, and a reserved port that ended up listening would undo the
/// reason there is a limit at all. So this reads the records rather than the slots.
pub fn port_bindings(records: &[InstanceRecord]) -> Vec<StreamBinding> {
    records
        .iter()
        .filter(|record| record.expired_at_ms.is_none())
        .flat_map(|record| {
            record.ports.iter().map(|port| StreamBinding {
                app_id: record.app_id.clone(),
                name: port.name.clone(),
                host_port: port.host_port,
                guest_port: port.guest_port,
            })
        })
        .collect()
}

pub async fn apply_activators(host: &Arc<Host>) {
    let records = host.state.snapshot().await.records;
    let slots: Vec<_> = host
        .slots()
        .await
        .into_iter()
        .filter(|slot| {
            !records
                .get(&slot.app_id)
                .is_some_and(|r| r.expired_at_ms.is_some())
        })
        .map(|slot| (slot.app_id, slot.host_port))
        .collect();
    host.activator.serve(&slots).await;

    // Every raw port is bound both ways: a port carries whatever arrives on it, and which of the
    // two a sleeping guest is woken by is not the host's to guess.
    let bindings = port_bindings(&host.state.records().await);
    if let Some(streams) = host.stream_activator.clone() {
        streams.serve(&bindings).await;
    }
    if let Some(datagrams) = host.datagram_activator.clone() {
        datagrams.serve(&bindings).await;
    }
}

pub async fn apply_routes(host: &Host) {
    let table = RouteTable::from_targets(&renderable_routes(&host.state.records().await));
    host.router.apply(table).await;
}

// A tap is only ever made for a slot, so one no slot claims belongs to an app that is gone. That
// is a daemon old enough to have never taken one back, or one that died between releasing a slot
// and reclaiming its device. Read from the host rather than from anything this process remembers,
// and run once the slots are restored and before anything is handed one.
pub async fn reclaim_stranded_taps(host: &Host) {
    let claimed: std::collections::BTreeSet<String> = {
        let allocator = host.allocator.lock().await;
        allocator
            .assignments()
            .iter()
            .map(|(app_id, slot)| nft_render::describe_slot(*slot, app_id.clone()).tap_name)
            .collect()
    };
    let stranded: Vec<String> = host
        .vms
        .tap_names()
        .await
        .into_iter()
        .filter(|name| !claimed.contains(name))
        .collect();
    if stranded.is_empty() {
        return;
    }
    tracing::info!(
        stranded = stranded.len(),
        held = claimed.len(),
        "taps outlived the apps they were made for and are being taken back"
    );
    for name in stranded {
        if let Err(error) = host.vms.delete_tap(&name).await {
            tracing::warn!(tap_name = %name, error = %error.message(), "a stranded tap could not be taken back");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use protocol::INSTANCE_STATES;

    async fn forwards_for(records: Vec<crate::domain::report::InstanceRecord>) -> Vec<ForwardedInstance> {
        let host = crate::test_support::test_host().await;
        for record in records {
            host.slot_for(&record.app_id).await.unwrap();
            host.state.put_record(record).await;
        }
        forwarded_instances(&host).await
    }

    #[tokio::test]
    async fn a_tap_no_slot_claims_is_taken_back_and_one_a_slot_holds_is_left_alone() {
        let host = test_host().await;
        let slot = host.slot_for(&app_id()).await.unwrap();
        host.vms.set_present_taps(vec![
            slot.tap_name.clone(),
            "nbr41".to_string(),
            "nbr42".to_string(),
        ]);

        reclaim_stranded_taps(&host).await;

        assert_eq!(
            host.vms.removed_taps(),
            vec!["nbr41".to_string(), "nbr42".to_string()],
            "only the devices no app is holding go"
        );
        assert!(
            !host.vms.removed_taps().contains(&slot.tap_name),
            "the tap a live slot holds is not one to take"
        );
    }

    #[tokio::test]
    async fn a_host_whose_taps_are_all_spoken_for_takes_nothing_back() {
        let host = test_host().await;
        let slot = host.slot_for(&app_id()).await.unwrap();
        host.vms.set_present_taps(vec![slot.tap_name]);
        reclaim_stranded_taps(&host).await;
        assert!(host.vms.removed_taps().is_empty());
    }

    #[tokio::test]
    async fn the_forward_is_what_decides_whether_a_port_reaches_the_guest() {
        for state in INSTANCE_STATES
            .iter()
            .filter(|state| **state != InstanceState::Running)
        {
            let forwarded = forwards_for(vec![instance_record(|record| record.state = *state)]).await;
            assert!(forwarded.is_empty(), "{state:?} should not be forwarded");
        }
        let running = forwards_for(vec![instance_record(|_| {})]).await;
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].app_id, app_id());
        assert_eq!(running[0].guest_ipv4.as_str(), "10.201.0.2");
        assert_eq!(running[0].ports.len(), 1, "the HTTP port and nothing else");
        assert_eq!(
            running[0].ports[0].guest_port.get(),
            protocol::DEFAULT_HTTP_PORT.get()
        );
    }

    #[test]
    fn a_slot_binds_the_ports_its_record_named_and_none_of_the_reserve() {
        use crate::domain::report::instance_record::RecordPort;

        let slot = nft_render::describe_slot(0, app_id());
        let named = |index: u32, port: u16, name: &str| RecordPort {
            name: protocol::PortName::parse(name).unwrap(),
            host_port: slot.host_port_at(index).unwrap(),
            guest_port: protocol::GuestPort::new(port).unwrap(),
        };
        let record = instance_record(|record| {
            record.ports = vec![named(1, 22, "ssh"), named(2, 53, "dns")];
        });

        let bound = port_bindings(std::slice::from_ref(&record));
        assert_eq!(
            bound.len(),
            2,
            "the slot reserves {} ports and the record named two",
            nft_render::PORTS_PER_SLOT
        );
        assert_eq!(bound[0].host_port, slot.host_port_at(1).unwrap());
        assert_eq!(bound[0].guest_port.get(), 22);
        assert_eq!(bound[1].host_port, slot.host_port_at(2).unwrap());
        assert_eq!(bound[1].guest_port.get(), 53);
        assert!(
            port_bindings(&[instance_record(|_| {})]).is_empty(),
            "an app that named no port beside its HTTP one binds nothing"
        );
    }

    #[tokio::test]
    async fn a_record_that_names_a_second_port_forwards_both_and_only_the_raw_one_both_ways() {
        use crate::domain::report::instance_record::RecordPort;

        let forwarded = forwards_for(vec![instance_record(|record| {
            record.ports = vec![RecordPort {
                name: protocol::PortName::parse("ssh").unwrap(),
                host_port: protocol::HostPort::new(21_001).unwrap(),
                guest_port: protocol::GuestPort::new(22).unwrap(),
            }];
        })])
        .await;

        assert_eq!(forwarded.len(), 1);
        let carried: Vec<(u16, u16, bool)> = forwarded[0]
            .ports
            .iter()
            .map(|port| (port.host_port.get(), port.guest_port.get(), port.raw))
            .collect();
        assert_eq!(
            carried,
            vec![
                (21_000, protocol::DEFAULT_HTTP_PORT.get(), false),
                (21_001, 22, true)
            ],
            "the HTTP port first and tcp only, then what the record named, carried whatever arrives"
        );
    }

    #[tokio::test]
    async fn a_guest_being_written_out_is_not_forwarded_though_its_record_still_reads_running() {
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();
        host.state.put_record(instance_record(|_| {})).await;

        host.state.mark_snapshotting(&app_id(), true).await;
        assert!(forwarded_instances(&host).await.is_empty());

        host.state.mark_snapshotting(&app_id(), false).await;
        assert_eq!(forwarded_instances(&host).await.len(), 1);
    }

    #[tokio::test]
    async fn a_host_holding_two_apps_and_running_one_forwards_only_that_one() {
        let forwarded = forwards_for(vec![
            instance_record(|record| record.state = InstanceState::Stopped),
            instance_record(|record| record.app_id = protocol::AppId::parse("app-2").unwrap()),
        ])
        .await;
        assert_eq!(forwarded.len(), 1);
    }

    #[tokio::test]
    async fn an_app_with_no_slot_is_not_forwarded_however_healthy_its_record_looks() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        assert!(forwarded_instances(&host).await.is_empty());
    }

    #[tokio::test]
    async fn a_ruleset_that_loaded_is_what_lets_this_host_start_anything() {
        let host = test_host().await;
        apply_network(&host).await;
        assert!(host.state.snapshot().await.isolated);
    }

    #[tokio::test]
    async fn one_that_would_not_load_leaves_the_host_saying_it_is_not_isolated() {
        let mut host = test_host().await;
        let (commands, _log) = crate::test_support::mocks::commands_answering(|_| {
            Err(crate::ports::CommandError::Unstartable {
                executable: "nft".to_string(),
                reason: "it is not installed".to_string(),
            })
        });
        Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .firewall = Arc::new(crate::adapters::net::firewall::HostFirewall::new(commands));
        host.state.modify(|snapshot| snapshot.isolated = true).await;

        apply_network(&host).await;

        assert!(!host.state.snapshot().await.isolated);
    }

    #[tokio::test]
    async fn the_apps_this_host_holds_slots_for_are_the_ones_it_answers_the_door_for() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();

        apply_activators(host.arc()).await;
        assert_eq!(host.activator.listening_for().await, vec![app_id()]);

        host.allocator.lock().await.release(&app_id());
        apply_activators(host.arc()).await;
        assert!(host.activator.listening_for().await.is_empty());
    }

    #[tokio::test]
    async fn the_routes_a_pass_publishes_are_the_hostnames_of_the_records_it_holds() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;

        apply_routes(&host).await;
        assert_eq!(
            host.router
                .routes()
                .await
                .port_for(app_hostname().hostname.as_str()),
            Some(instance_record(|_| {}).host_port)
        );

        host.state.drop_record(&app_id()).await;
        apply_routes(&host).await;
        assert!(host.router.routes().await.is_empty());
    }
}
