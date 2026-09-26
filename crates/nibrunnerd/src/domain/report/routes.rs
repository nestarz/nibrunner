use protocol::{AppHostname, AppId, DeploymentId, HostPort};

use crate::domain::report::InstanceRecord;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub hostnames: Vec<AppHostname>,
    pub host_port: HostPort,
}

pub fn renderable_routes(records: &[InstanceRecord]) -> Vec<RouteTarget> {
    records
        .iter()
        .filter(|record| !record.hostnames.is_empty())
        .map(|record| RouteTarget {
            app_id: record.app_id.clone(),
            deployment_id: record.deployment_id.clone(),
            hostnames: record.hostnames.clone(),
            host_port: record.host_port,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_hostname, instance_record};
    use protocol::{AppHostnameKind, Hostname, InstanceState, INSTANCE_STATES};

    fn named(name: &str, port: u16) -> InstanceRecord {
        instance_record(|record| {
            record.app_id = AppId::parse(name).unwrap();
            record.host_port = HostPort::new(port).unwrap();
            record.hostnames = vec![AppHostname {
                hostname: Hostname::parse(format!("{name}.apps.example.com")).unwrap(),
                kind: AppHostnameKind::Platform,
            }];
        })
    }

    #[test]
    fn a_host_answers_for_the_apps_it_holds_not_the_ones_that_happen_to_be_up() {
        for state in INSTANCE_STATES
            .iter()
            .filter(|state| **state != InstanceState::Running)
        {
            assert_eq!(
                renderable_routes(&[instance_record(|record| record.state = *state)]).len(),
                1
            );
        }
        assert!(renderable_routes(&[instance_record(|record| record.hostnames = vec![])]).is_empty());
        let running = renderable_routes(&[instance_record(|_| {})]);
        let stopped = renderable_routes(&[instance_record(|record| record.state = InstanceState::Stopped)]);
        assert_eq!(running[0].host_port, stopped[0].host_port);
        assert_eq!(running[0].hostnames, vec![app_hostname()]);
    }

    #[test]
    fn each_app_keeps_its_own_port_and_the_order_the_records_were_given_in() {
        let records = vec![named("app-one", 20_001), named("app-two", 20_002)];
        let routes = renderable_routes(&records);
        assert_eq!(
            routes
                .iter()
                .map(|route| (route.app_id.as_str().to_string(), route.host_port))
                .collect::<Vec<_>>(),
            vec![
                ("app-one".to_string(), HostPort::new(20_001).unwrap()),
                ("app-two".to_string(), HostPort::new(20_002).unwrap()),
            ]
        );
        assert_eq!(
            routes[1].hostnames[0].hostname.as_str(),
            "app-two.apps.example.com"
        );
    }

    #[test]
    fn an_app_with_no_hostname_of_its_own_does_not_displace_the_ones_that_have_one() {
        let mut records = vec![
            named("app-one", 20_001),
            instance_record(|record| {
                record.app_id = AppId::parse("app-quiet").unwrap();
                record.hostnames = vec![];
            }),
            named("app-two", 20_002),
        ];
        assert_eq!(
            renderable_routes(&records)
                .iter()
                .map(|route| route.app_id.as_str().to_string())
                .collect::<Vec<_>>(),
            vec!["app-one".to_string(), "app-two".to_string()]
        );

        records.clear();
        assert!(renderable_routes(&records).is_empty());
    }
}
