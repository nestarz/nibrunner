use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use protocol::ObjectKey;

use crate::ports::{ArtifactBody, ArtifactError, ArtifactStore};

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
    async fn read(&self, object_key: &ObjectKey) -> Result<ArtifactBody, ArtifactError> {
        let transferred = |error: object_store::Error| ArtifactError::Transfer(error.to_string());
        let result = self
            .store
            .get(&self.path_for(object_key))
            .await
            .map_err(transferred)?;
        Ok(result.into_stream().map_err(transferred).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{land_within, ArtifactStoreExt};

    async fn whole(body: ArtifactBody) -> Vec<u8> {
        body.map_ok(|chunk| chunk.to_vec()).try_concat().await.unwrap()
    }

    fn arriving_in(chunks: &[&'static [u8]]) -> ArtifactBody {
        let chunks: Vec<_> = chunks
            .iter()
            .map(|chunk| Ok(bytes::Bytes::from_static(chunk)))
            .collect();
        futures::stream::iter(chunks).boxed()
    }

    #[tokio::test]
    async fn a_body_lands_in_its_file_chunk_by_chunk_and_is_hashed_on_the_way() {
        use sha2::Digest;
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("object");
        let landed = land_within(arriving_in(&[b"a bin", b"ary"]), &destination, 8)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), b"a binary");
        assert_eq!(landed.bytes, 8);
        assert_eq!(landed.sha256, hex::encode(sha2::Sha256::digest(b"a binary")));
    }

    #[tokio::test]
    async fn a_body_past_the_cap_is_refused_part_way_and_leaves_no_file() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("object");
        let error = land_within(arriving_in(&[b"a bin", b"ary"]), &destination, 7)
            .await
            .unwrap_err();
        assert!(
            error.message().contains("larger than an artifact may be"),
            "{error}"
        );
        assert!(!destination.exists());
    }

    #[tokio::test]
    async fn an_object_that_hashes_to_something_else_leaves_no_file_where_it_was_landed() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjectArtifactStore::open(&directory.path().display().to_string()).unwrap();
        std::fs::create_dir_all(directory.path().join("artifacts")).unwrap();
        std::fs::write(directory.path().join("artifacts/one"), b"a binary").unwrap();
        let object = protocol::StoredObject {
            object_key: ObjectKey::parse("artifacts/one").unwrap(),
            digest: protocol::Sha256Digest::parse("0".repeat(64)).unwrap(),
        };
        let destination = directory.path().join("landed");
        let error = store.land_verified(&object, &destination).await.unwrap_err();
        assert!(matches!(error, ArtifactError::DigestMismatch { .. }), "{error}");
        assert!(!destination.exists());
    }

    #[tokio::test]
    async fn a_directory_is_a_bucket_a_single_machine_already_has() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjectArtifactStore::open(&directory.path().display().to_string()).unwrap();
        std::fs::create_dir_all(directory.path().join("artifacts")).unwrap();
        std::fs::write(directory.path().join("artifacts/one"), b"a binary").unwrap();
        let key = ObjectKey::parse("artifacts/one").unwrap();
        assert_eq!(whole(store.read(&key).await.unwrap()).await, b"a binary");

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
        assert_eq!(whole(store.read(&key).await.unwrap()).await, b"a binary");
    }
}
