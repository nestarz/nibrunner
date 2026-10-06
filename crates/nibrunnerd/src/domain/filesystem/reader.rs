use protocol::{AppId, GuestPath};

use crate::domain::filesystem::client::{GuestFilesystem, GuestFilesystemError};
use crate::host::Host;
use crate::ports::GuestReading;

pub fn guest_endpoint(host: &Host, app_id: &AppId) -> guest_contract::channels::ChannelEndpoint {
    host.vms.channel_transport().endpoint(
        &host.config.vm_dir().join(app_id.as_str()),
        guest_contract::channels::Channel::Filesystem,
    )
}

pub async fn list(
    host: &Host,
    app_id: &AppId,
    path: &GuestPath,
) -> Result<protocol::DirectoryListing, GuestFilesystemError> {
    let mut guest = GuestFilesystem::dial(app_id, &guest_endpoint(host, app_id)).await?;
    guest.list(path).await
}

pub async fn measure(host: &Host, app_id: &AppId) -> GuestReading {
    let Ok(mut guest) = GuestFilesystem::dial(app_id, &guest_endpoint(host, app_id)).await else {
        return GuestReading::default();
    };
    GuestReading {
        filesystem: guest.usage().await.ok(),
        compute: guest.compute().await.ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[tokio::test]
    async fn the_runtime_selects_its_transport_without_changing_the_app_directory() {
        let mut host = test_host().await;
        let mut runtime = crate::ports::MockVmm::new();
        runtime
            .expect_channel_transport()
            .return_const(guest_contract::channels::ChannelTransport::Unix);
        std::sync::Arc::get_mut(&mut host.host).unwrap().vms = std::sync::Arc::new(runtime);
        let endpoint = guest_endpoint(host.arc(), &app_id());
        assert_eq!(
            endpoint.path,
            host.config
                .vm_dir()
                .join(app_id().as_str())
                .join("filesystem.sock")
        );
        assert_eq!(endpoint.vsock_port, None);
    }

    #[tokio::test]
    async fn each_guest_is_reached_on_a_socket_inside_its_own_microvm_directory() {
        let host = test_host().await;
        let neighbour = AppId::parse("app-2").unwrap();
        let path = guest_endpoint(host.arc(), &app_id()).path;

        assert!(path.starts_with(host.config.vm_dir()));
        assert!(path.ends_with(guest_contract::vsock::GUEST_VSOCK_FILENAME));
        assert_eq!(path.parent().unwrap().file_name().unwrap(), app_id().as_str());
        assert_ne!(path, guest_endpoint(host.arc(), &neighbour).path);
    }

    #[tokio::test]
    async fn a_guest_that_is_not_running_measures_as_nothing_rather_than_as_zero() {
        let host = test_host().await;
        assert_eq!(measure(host.arc(), &app_id()).await, GuestReading::default());
    }

    #[tokio::test]
    async fn a_guest_that_is_not_running_cannot_be_listed() {
        let host = test_host().await;
        let Err(error) = list(host.arc(), &app_id(), &GuestPath::parse("/").unwrap()).await else {
            panic!("a guest that is not running listed something anyway");
        };
        assert!(error.message().contains("no runtime is running"), "{error}");
        assert!(error.message().contains(app_id().as_str()), "{error}");
    }
}
