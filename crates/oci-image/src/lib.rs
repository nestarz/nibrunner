#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic))]

mod filesystem;

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use serde::Deserialize;
use sha2::{Digest, Sha256};

const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";
const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const DOCKER_INDEX: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const MAX_INDEX_DEPTH: usize = 8;
const MAX_METADATA_BYTES: usize = 4 * 1024 * 1024;
const MAX_ENTRIES: usize = 100_000;
const MAX_LAYERS: usize = 128;
const MAX_DECODED_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the OCI image could not be read: {0}")]
    Io(#[from] std::io::Error),
    #[error("the OCI image metadata is unreadable: {0}")]
    Json(#[from] serde_json::Error),
    #[error("the OCI image is unusable: {0}")]
    Invalid(String),
}

pub struct Filesystem {
    pub tar: Vec<u8>,
    pub data_bytes: u64,
    pub entries: u64,
    /// The image configuration's `Env`, `NAME=value` entries in the order the image gives them.
    pub env: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Layout {
    image_layout_version: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Index {
    schema_version: u32,
    manifests: Vec<Descriptor>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Descriptor {
    media_type: String,
    digest: String,
    size: u64,
    platform: Option<Platform>,
}

#[derive(Deserialize)]
struct Platform {
    architecture: String,
    os: String,
}

impl Platform {
    fn supported(&self) -> bool {
        self.os == "linux" && self.architecture == "amd64"
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    schema_version: u32,
    config: Descriptor,
    layers: Vec<Descriptor>,
}

#[derive(Deserialize)]
struct Config {
    #[serde(flatten)]
    platform: Platform,
    rootfs: Rootfs,
    #[serde(default)]
    config: Option<RunConfig>,
}

/// The one part of the image's run configuration a host applies; `Entrypoint`, `Cmd`,
/// `WorkingDir` and `User` are the document's to state.
#[derive(Deserialize)]
struct RunConfig {
    #[serde(rename = "Env", default)]
    env: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct Rootfs {
    #[serde(rename = "type")]
    kind: String,
    diff_ids: Vec<String>,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

fn metadata<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(invalid("metadata exceeds 4 MiB"));
    }
    Ok(serde_json::from_slice(bytes)?)
}

fn archive_files(bytes: &[u8]) -> Result<BTreeMap<String, &[u8]>, Error> {
    let mut files = BTreeMap::new();
    for (count, entry) in tar::Archive::new(Cursor::new(bytes)).entries()?.enumerate() {
        if count >= MAX_ENTRIES {
            return Err(invalid("the archive has too many entries"));
        }
        let entry = entry?;
        let path = filesystem::path(&entry.path()?)?;
        if entry.header().entry_type().is_dir() {
            continue;
        }
        if !entry.header().entry_type().is_file() {
            return Err(invalid("archive metadata and blobs must be regular files"));
        }
        let start =
            usize::try_from(entry.raw_file_position()).map_err(|_| invalid("blob offset overflow"))?;
        let size = usize::try_from(entry.size()).map_err(|_| invalid("blob size overflow"))?;
        let end = start
            .checked_add(size)
            .ok_or_else(|| invalid("blob size overflow"))?;
        let body = bytes
            .get(start..end)
            .ok_or_else(|| invalid("a blob is truncated"))?;
        if files.insert(path, body).is_some() {
            return Err(invalid("the archive repeats a path"));
        }
    }
    Ok(files)
}

fn checked_digest(digest: &str) -> Result<&str, Error> {
    let hash = digest
        .strip_prefix("sha256:")
        .ok_or_else(|| invalid("only SHA256 blobs are supported"))?;
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("a SHA256 digest is malformed"));
    }
    Ok(hash)
}

fn blob<'a>(files: &BTreeMap<String, &'a [u8]>, descriptor: &Descriptor) -> Result<&'a [u8], Error> {
    let hash = checked_digest(&descriptor.digest)?;
    let bytes = files
        .get(&format!("blobs/sha256/{hash}"))
        .ok_or_else(|| invalid(format!("{} is missing", descriptor.digest)))?;
    if bytes.len() as u64 != descriptor.size || hex::encode(Sha256::digest(bytes)) != hash {
        return Err(invalid(format!(
            "{} does not match its size and digest",
            descriptor.digest
        )));
    }
    Ok(bytes)
}

fn manifests(
    files: &BTreeMap<String, &[u8]>,
    index: Index,
    depth: usize,
    found: &mut Vec<Manifest>,
    remaining: &mut usize,
) -> Result<(), Error> {
    if index.schema_version != 2 || depth > MAX_INDEX_DEPTH || index.manifests.len() > MAX_ENTRIES {
        return Err(invalid("the image index version, size or nesting is unsupported"));
    }
    for descriptor in index.manifests {
        *remaining = remaining
            .checked_sub(1)
            .ok_or_else(|| invalid("the indexes reference too many descriptors"))?;
        if descriptor
            .platform
            .as_ref()
            .is_some_and(|platform| !platform.supported())
        {
            continue;
        }
        match descriptor.media_type.as_str() {
            OCI_INDEX | DOCKER_INDEX => manifests(
                files,
                metadata(blob(files, &descriptor)?)?,
                depth + 1,
                found,
                remaining,
            )?,
            OCI_MANIFEST | DOCKER_MANIFEST => {
                let manifest: Manifest = metadata(blob(files, &descriptor)?)?;
                if manifest.config.media_type != "application/vnd.oci.image.config.v1+json"
                    && manifest.config.media_type != "application/vnd.docker.container.image.v1+json"
                {
                    continue;
                }
                let config: Config = metadata(blob(files, &manifest.config)?)?;
                if config.platform.supported() {
                    found.push(manifest);
                    if found.len() > 1 {
                        return Err(invalid("the archive contains more than one Linux amd64 image"));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn decode(bytes: &[u8], media_type: &str, remaining: u64) -> Result<Vec<u8>, Error> {
    let reader: Box<dyn Read + '_> = match media_type {
        "application/vnd.oci.image.layer.v1.tar" => Box::new(Cursor::new(bytes)),
        "application/vnd.oci.image.layer.v1.tar+gzip"
        | "application/vnd.docker.image.rootfs.diff.tar.gzip" => {
            Box::new(flate2::read::MultiGzDecoder::new(bytes))
        }
        "application/vnd.oci.image.layer.v1.tar+zstd" => Box::new(zstd::stream::read::Decoder::new(bytes)?),
        _ => return Err(invalid(format!("layer media type {media_type} is unsupported"))),
    };
    let mut decoded = Vec::new();
    reader.take(remaining + 1).read_to_end(&mut decoded)?;
    if decoded.len() as u64 > remaining {
        return Err(invalid("decoded image layers exceed 1 GiB"));
    }
    Ok(decoded)
}

pub fn flatten(archive: &[u8]) -> Result<Filesystem, Error> {
    let files = archive_files(archive)?;
    let layout: Layout = metadata(
        files
            .get("oci-layout")
            .ok_or_else(|| invalid("oci-layout is missing"))?,
    )?;
    if layout.image_layout_version != "1.0.0" {
        return Err(invalid("the OCI layout version is unsupported"));
    }
    let index = metadata(
        files
            .get("index.json")
            .ok_or_else(|| invalid("index.json is missing"))?,
    )?;
    let mut found = Vec::new();
    let mut remaining_descriptors = MAX_ENTRIES;
    manifests(&files, index, 0, &mut found, &mut remaining_descriptors)?;
    let manifest = found
        .pop()
        .ok_or_else(|| invalid("the archive has no Linux amd64 image"))?;
    let config: Config = metadata(blob(&files, &manifest.config)?)?;
    if manifest.schema_version != 2
        || config.rootfs.kind != "layers"
        || config.rootfs.diff_ids.len() != manifest.layers.len()
        || manifest.layers.len() > MAX_LAYERS
    {
        return Err(invalid("the image manifest and rootfs layers disagree"));
    }
    let mut filesystem = filesystem::Filesystem::new();
    let mut remaining = MAX_DECODED_BYTES;
    for (layer, diff_id) in manifest.layers.iter().zip(config.rootfs.diff_ids) {
        let decoded = decode(blob(&files, layer)?, &layer.media_type, remaining)?;
        remaining -= decoded.len() as u64;
        if hex::encode(Sha256::digest(&decoded)) != checked_digest(&diff_id)? {
            return Err(invalid("an unpacked layer does not match its diff ID"));
        }
        filesystem.apply(&decoded)?;
    }
    Ok(Filesystem {
        env: config.config.and_then(|run| run.env).unwrap_or_default(),
        ..filesystem.finish()?
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::io::Write;

    struct Image {
        blobs: BTreeMap<String, Vec<u8>>,
        index: Value,
    }

    impl Image {
        fn descriptor(&mut self, bytes: Vec<u8>, media_type: &str) -> Value {
            let digest = hex::encode(Sha256::digest(&bytes));
            let descriptor =
                json!({"mediaType": media_type, "digest": format!("sha256:{digest}"), "size": bytes.len()});
            self.blobs.insert(format!("blobs/sha256/{digest}"), bytes);
            descriptor
        }

        fn new(compression: &str, platform: &str, diff_id: Option<&str>) -> Self {
            Self::configured(compression, platform, diff_id, Value::Null)
        }

        fn configured(compression: &str, platform: &str, diff_id: Option<&str>, run: Value) -> Self {
            let mut layer = tar::Builder::new(Vec::new());
            let mut header = tar::Header::new_gnu();
            header.set_size(7);
            header.set_mode(0o755);
            layer
                .append_data(&mut header, "app", b"program".as_slice())
                .unwrap();
            let layer = layer.into_inner().unwrap();
            let digest = format!("sha256:{}", hex::encode(Sha256::digest(&layer)));
            let (encoded, media_type) = match compression {
                "gzip" => {
                    let mut encoder =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(&layer).unwrap();
                    (
                        encoder.finish().unwrap(),
                        "application/vnd.oci.image.layer.v1.tar+gzip",
                    )
                }
                "zstd" => (
                    zstd::stream::encode_all(layer.as_slice(), 0).unwrap(),
                    "application/vnd.oci.image.layer.v1.tar+zstd",
                ),
                _ => (layer, "application/vnd.oci.image.layer.v1.tar"),
            };
            let mut image = Self {
                blobs: BTreeMap::new(),
                index: Value::Null,
            };
            let layer = image.descriptor(encoded, media_type);
            let config = json!({"os": "linux", "architecture": platform, "config": run, "rootfs": {"type": "layers", "diff_ids": [diff_id.unwrap_or(&digest)]}});
            let config = image.descriptor(
                serde_json::to_vec(&config).unwrap(),
                "application/vnd.oci.image.config.v1+json",
            );
            let manifest = json!({"schemaVersion": 2, "config": config, "layers": [layer]});
            let descriptor = image.descriptor(serde_json::to_vec(&manifest).unwrap(), OCI_MANIFEST);
            image.index = json!({"schemaVersion": 2, "manifests": [descriptor]});
            image
        }

        fn archive(&self) -> Vec<u8> {
            let mut tar = tar::Builder::new(Vec::new());
            let metadata = BTreeMap::from([
                (
                    "oci-layout".to_owned(),
                    br#"{"imageLayoutVersion":"1.0.0"}"#.to_vec(),
                ),
                ("index.json".to_owned(), serde_json::to_vec(&self.index).unwrap()),
            ]);
            for (path, bytes) in metadata.iter().chain(self.blobs.iter()) {
                let mut header = tar::Header::new_gnu();
                header.set_size(bytes.len() as u64);
                header.set_mode(0o644);
                tar.append_data(&mut header, path, bytes.as_slice()).unwrap();
            }
            tar.into_inner().unwrap()
        }
    }

    #[test]
    fn an_images_env_is_read_in_its_order_and_nothing_else_of_its_run_configuration() {
        let run = json!({
            "Env": ["PATH=/usr/local/bin:/usr/bin:/bin", "NODE_VERSION=24.1.0", "EMPTY="],
            "Entrypoint": ["docker-entrypoint.sh"], "Cmd": ["node"], "WorkingDir": "/srv", "User": "node",
        });
        let filesystem = flatten(&Image::configured("plain", "amd64", None, run).archive()).unwrap();
        assert_eq!(
            filesystem.env,
            [
                "PATH=/usr/local/bin:/usr/bin:/bin",
                "NODE_VERSION=24.1.0",
                "EMPTY="
            ]
        );
        let plain = flatten(&Image::new("plain", "amd64", None).archive()).unwrap();
        assert!(plain.env.is_empty(), "a null config is no Env");
        let without =
            flatten(&Image::configured("plain", "amd64", None, json!({"Cmd": ["sh"]})).archive()).unwrap();
        assert!(without.env.is_empty());
    }

    #[test]
    fn plain_gzip_and_zstd_layers_produce_the_same_filesystem() {
        let plain = flatten(&Image::new("plain", "amd64", None).archive()).unwrap();
        for compression in ["gzip", "zstd"] {
            let filesystem = flatten(&Image::new(compression, "amd64", None).archive()).unwrap();
            assert_eq!(filesystem.tar, plain.tar);
            assert_eq!(filesystem.data_bytes, 7);
            assert_eq!(filesystem.entries, 2);
        }
    }

    #[test]
    fn a_tampered_blob_or_descriptor_size_is_rejected() {
        let mut image = Image::new("gzip", "amd64", None);
        image.blobs.values_mut().next().unwrap()[0] ^= 1;
        assert!(flatten(&image.archive()).is_err());
        let mut image = Image::new("plain", "amd64", None);
        image.index["manifests"][0]["size"] = json!(0);
        assert!(flatten(&image.archive()).is_err());
    }

    #[test]
    fn a_layer_with_a_wrong_uncompressed_digest_is_rejected() {
        let wrong = format!("sha256:{}", "0".repeat(64));
        assert!(flatten(&Image::new("gzip", "amd64", Some(&wrong)).archive()).is_err());
    }

    #[test]
    fn unsupported_platforms_and_ambiguous_images_are_rejected() {
        assert!(flatten(&Image::new("plain", "arm64", None).archive()).is_err());
        let mut image = Image::new("plain", "amd64", None);
        let descriptor = image.index["manifests"][0].clone();
        image.index["manifests"].as_array_mut().unwrap().push(descriptor);
        assert!(flatten(&image.archive()).is_err());
    }

    #[test]
    fn nested_indexes_select_the_supported_image() {
        let mut image = Image::new("plain", "amd64", None);
        let index = serde_json::to_vec(&image.index).unwrap();
        let descriptor = image.descriptor(index, OCI_INDEX);
        image.index = json!({"schemaVersion": 2, "manifests": [descriptor, {"mediaType": OCI_MANIFEST, "digest": "sha256:missing", "size": 0, "platform": {"os": "linux", "architecture": "arm64"}}]});
        assert!(flatten(&image.archive()).is_ok());
    }

    #[test]
    fn missing_layout_markers_and_duplicate_blob_paths_are_rejected() {
        assert!(flatten(&vec![0; 1024]).is_err());
        let image = Image::new("plain", "amd64", None);
        let mut archive = image.archive();
        archive.truncate(archive.len() - 1024);
        let mut extra = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_size(0);
        header.set_mode(0o644);
        extra
            .append_data(&mut header, "oci-layout", std::io::empty())
            .unwrap();
        archive.extend(extra.into_inner().unwrap());
        assert!(flatten(&archive).is_err());
    }

    #[test]
    fn compressed_layers_stop_at_the_decoded_byte_limit() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&[0; 4096]).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(decode(&compressed, "application/vnd.oci.image.layer.v1.tar+gzip", 1024).is_err());
    }
}
