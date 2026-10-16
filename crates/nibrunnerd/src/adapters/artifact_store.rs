use std::sync::Arc;

use async_trait::async_trait;
use object_store::{ObjectStore, ObjectStoreExt};
use protocol::{DownloadUrl, ObjectKey};

use crate::ports::{ArtifactError, ArtifactStore};

/// A program is held whole in memory to be checked, so a URL cannot make the daemon hold more.
const MAX_DOWNLOAD_BYTES: usize = 512 * 1024 * 1024;

pub struct ObjectArtifactStore {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl ObjectArtifactStore {
    pub fn open(url: &str) -> Result<Self, ArtifactError> {
        if let Some(rest) = url.strip_prefix("s3://") {
            crate::install_crypto_provider();
            let (bucket, prefix) = match rest.split_once('/') {
                Some((bucket, prefix)) => (bucket, Some(prefix.trim_end_matches('/').to_string())),
                None => (rest, None),
            };
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|error| ArtifactError::Transfer(error.to_string()))?;
            return Ok(Self {
                store: Arc::new(store),
                prefix: prefix.filter(|prefix| !prefix.is_empty()),
            });
        }
        let directory = std::path::PathBuf::from(url);
        crate::json_store::make_directory(&directory, 0o700)
            .map_err(|error| ArtifactError::Transfer(error.to_string()))?;
        let store = object_store::local::LocalFileSystem::new_with_prefix(&directory)
            .map_err(|error| ArtifactError::Transfer(error.to_string()))?;
        Ok(Self {
            store: Arc::new(store),
            prefix: None,
        })
    }

    fn path_for(&self, object_key: &ObjectKey) -> object_store::path::Path {
        match &self.prefix {
            None => object_store::path::Path::from(object_key.as_str()),
            Some(prefix) => object_store::path::Path::from(format!("{prefix}/{object_key}")),
        }
    }
}

#[async_trait]
impl ArtifactStore for ObjectArtifactStore {
    async fn read(&self, object_key: &ObjectKey) -> Result<Vec<u8>, ArtifactError> {
        let result = self
            .store
            .get(&self.path_for(object_key))
            .await
            .map_err(|error| ArtifactError::Transfer(error.to_string()))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|error| ArtifactError::Transfer(error.to_string()))?;
        Ok(bytes.to_vec())
    }

    async fn download(&self, url: &DownloadUrl) -> Result<Vec<u8>, ArtifactError> {
        crate::install_crypto_provider();
        let transferred = |error: reqwest::Error| ArtifactError::Transfer(error.to_string());
        let mut response = reqwest::get(url.as_str())
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(transferred)?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transferred)? {
            if body.len() + chunk.len() > MAX_DOWNLOAD_BYTES {
                return Err(ArtifactError::Transfer(format!(
                    "{url} is larger than the {MAX_DOWNLOAD_BYTES} bytes a downloaded layer may be",
                    url = url.as_str()
                )));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_directory_is_a_bucket_a_single_machine_already_has() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjectArtifactStore::open(&directory.path().display().to_string()).unwrap();
        std::fs::create_dir_all(directory.path().join("artifacts")).unwrap();
        std::fs::write(directory.path().join("artifacts/one"), b"a binary").unwrap();
        let key = ObjectKey::parse("artifacts/one").unwrap();
        assert_eq!(store.read(&key).await.unwrap(), b"a binary");

        let missing = ObjectKey::parse("artifacts/absent").unwrap();
        assert!(matches!(
            store.read(&missing).await,
            Err(ArtifactError::Transfer(_))
        ));
    }

    #[test]
    fn a_bucket_url_carries_its_prefix_into_every_key() {
        let key = ObjectKey::parse("artifacts/one").unwrap();
        let bare = ObjectArtifactStore {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: None,
        };
        assert_eq!(bare.path_for(&key).to_string(), "artifacts/one");
        let nested = ObjectArtifactStore {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: Some("hosts/host-1".into()),
        };
        assert_eq!(nested.path_for(&key).to_string(), "hosts/host-1/artifacts/one");
    }

    #[test]
    fn a_directory_that_is_not_there_yet_is_made_rather_than_refused() {
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("one").join("two");
        ObjectArtifactStore::open(&nested.display().to_string()).unwrap();
        assert!(nested.is_dir());
    }

    #[test]
    fn a_directory_that_cannot_be_made_is_a_transfer_failure_rather_than_a_panic() {
        let directory = tempfile::tempdir().unwrap();
        let occupied = directory.path().join("store");
        std::fs::write(&occupied, b"a file, not a directory").unwrap();
        let opened = ObjectArtifactStore::open(&occupied.display().to_string());
        assert!(matches!(opened, Err(ArtifactError::Transfer(_))));
    }

    #[test]
    fn a_bucket_url_is_split_into_the_bucket_and_the_prefix_under_it() {
        crate::install_crypto_provider();
        for (url, expected) in [
            ("s3://nibrun", None),
            ("s3://nibrun/", None),
            ("s3://nibrun/artifacts", Some("artifacts".to_string())),
            ("s3://nibrun/artifacts/", Some("artifacts".to_string())),
            ("s3://nibrun/hosts/host-1", Some("hosts/host-1".to_string())),
        ] {
            let Ok(store) = ObjectArtifactStore::open(url) else {
                continue;
            };
            assert_eq!(store.prefix, expected, "{url}");
        }
    }

    #[tokio::test]
    async fn a_prefix_is_where_the_bytes_are_looked_for_rather_than_the_bucket_root() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjectArtifactStore {
            store: Arc::new(object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap()),
            prefix: Some("hosts/host-1".into()),
        };
        std::fs::create_dir_all(directory.path().join("hosts/host-1/artifacts")).unwrap();
        std::fs::write(directory.path().join("hosts/host-1/artifacts/one"), b"a binary").unwrap();
        let key = ObjectKey::parse("artifacts/one").unwrap();
        assert_eq!(store.read(&key).await.unwrap(), b"a binary");
    }
}
