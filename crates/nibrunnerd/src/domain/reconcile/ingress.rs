use protocol::DesiredInstance;

use crate::config::HostConfig;

/// Why this host cannot put the instance within reach, or nothing.
///
/// A host answers for what it was configured to answer for, and a document may ask for more than
/// that. Saying so is the whole of the check: an app left running behind a front door that was
/// never opened is reported healthy and reached by nobody, which is the one failure a level-
/// triggered daemon cannot converge its way out of.
pub fn ingress_refusal(desired: &DesiredInstance, config: &HostConfig) -> Option<String> {
    if let Err(invalid) = desired.config.validate_ports() {
        return Some(invalid.to_string());
    }

    if !desired.hostnames.is_empty() && config.proxy.http.is_none() {
        return Some(format!(
            "it answers for {} but this host runs no proxy, so nothing would reach it",
            desired.hostnames.len()
        ));
    }

    let named = desired.config.ports.len();
    let allowed = config.proxy.raw.as_ref().map_or(0, |raw| raw.max_ports_per_guest);
    if named > allowed {
        return Some(match allowed {
            0 => format!(
                "it asks for {named} port beside its HTTP one and this host names no [proxy.raw] to bind them on"
            ),
            allowed => format!(
                "it asks for {named} ports beside its HTTP one and [proxy.raw] allows {allowed}"
            ),
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HttpListener, ProxyConfig, RawPorts};
    use crate::test_support::*;
    use protocol::{GuestPort, InstancePort, PortName};

    fn ssh_port() -> InstancePort {
        InstancePort {
            name: PortName::parse("ssh").unwrap(),
            guest_port: GuestPort::new(22).unwrap(),
        }
    }

    fn config_with(proxy: ProxyConfig) -> HostConfig {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let mut config = HostConfig::under(directory.path());
        config.proxy = proxy;
        std::mem::forget(directory);
        config
    }

    fn here() -> std::net::IpAddr {
        std::net::Ipv4Addr::LOCALHOST.into()
    }

    /// A host that serves HTTP and offers no port beside it.
    fn serving_http() -> ProxyConfig {
        ProxyConfig {
            http: Some(HttpListener {
                listen_address: here(),
                port: 8080,
                tls: None,
                redirect_from_port: None,
            }),
            raw: None,
        }
    }

    /// The same host, allowing each guest this many ports beside its HTTP one.
    fn allowing(max_ports_per_guest: usize) -> ProxyConfig {
        ProxyConfig {
            raw: Some(RawPorts {
                listen_address: here(),
                max_ports_per_guest,
            }),
            ..serving_http()
        }
    }

    fn and_one_more() -> ProxyConfig {
        allowing(1)
    }

    fn serving_nothing() -> ProxyConfig {
        ProxyConfig::default()
    }

    fn answering_for_a_name() -> protocol::DesiredInstance {
        desired_instance(|instance| instance.hostnames = vec![app_hostname()])
    }

    #[test]
    fn an_app_with_a_hostname_on_a_host_that_runs_no_proxy_is_refused_by_name() {
        let refusal = ingress_refusal(&answering_for_a_name(), &config_with(serving_nothing()))
            .expect("nothing would have reached it");
        assert!(refusal.contains("no proxy"), "{refusal}");

        assert_eq!(
            ingress_refusal(
                &desired_instance(|instance| instance.hostnames = vec![]),
                &config_with(serving_nothing()),
            ),
            None,
            "an app that answers for no name asks the proxy for nothing"
        );
        assert_eq!(
            ingress_refusal(&answering_for_a_name(), &config_with(serving_http())),
            None
        );
    }

    #[test]
    fn a_stream_port_on_a_host_that_offers_none_is_refused_by_name() {
        let wanting = desired_instance(|instance| instance.config.ports = vec![ssh_port()]);
        let refusal =
            ingress_refusal(&wanting, &config_with(serving_http())).expect("nothing would have bound it");
        assert!(refusal.contains("[proxy.raw]"), "{refusal}");
        assert_eq!(ingress_refusal(&wanting, &config_with(and_one_more())), None);
    }

    #[test]
    fn a_document_naming_more_ports_than_the_host_allows_is_refused_and_told_how_many() {
        let mut second = ssh_port();
        second.name = PortName::parse("other").unwrap();
        second.guest_port = GuestPort::new(9000).unwrap();
        let greedy = desired_instance(|instance| instance.config.ports = vec![ssh_port(), second]);

        let refusal = ingress_refusal(&greedy, &config_with(and_one_more()))
            .expect("two ports beside the HTTP one is one more than this host allows");
        assert!(refusal.contains("allows 1"), "{refusal}");

        // The same document on a host that allows two is not a document to refuse.
        assert_eq!(ingress_refusal(&greedy, &config_with(allowing(2))), None);
    }

    #[test]
    fn a_port_named_or_numbered_twice_is_refused() {
        let duplicate_name =
            desired_instance(|instance| instance.config.ports = vec![ssh_port(), ssh_port()]);
        assert!(ingress_refusal(&duplicate_name, &config_with(and_one_more())).is_some());

        let shadowing_http = desired_instance(|instance| {
            instance.config.ports = vec![InstancePort {
                name: PortName::parse("again").unwrap(),
                guest_port: GuestPort::new(instance.config.http_port.get()).unwrap(),
            }];
        });
        let refusal = ingress_refusal(&shadowing_http, &config_with(and_one_more()))
            .expect("one guest port cannot answer two ways");
        assert!(refusal.contains("claimed twice"), "{refusal}");
    }
}
