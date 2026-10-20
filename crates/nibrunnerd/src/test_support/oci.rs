use std::os::unix::fs::MetadataExt;

use serde_json::json;
use sha2::{Digest, Sha256};

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn descriptor(bytes: &[u8], media_type: &str) -> serde_json::Value {
    json!({ "mediaType": media_type, "digest": digest(bytes), "size": bytes.len() })
}

fn append(archive: &mut tar::Builder<Vec<u8>>, path: &str, bytes: &[u8], mode: u32) {
    let directory = tempfile::tempdir().unwrap();
    let owner = std::fs::metadata(directory.path()).unwrap();
    let mut header = tar::Header::new_gnu();
    header.set_uid(u64::from(owner.uid()));
    header.set_gid(u64::from(owner.gid()));
    header.set_size(bytes.len() as u64);
    header.set_mode(mode);
    header.set_mtime(0);
    if path.ends_with('/') {
        header.set_entry_type(tar::EntryType::Directory);
    }
    header.set_cksum();
    archive.append_data(&mut header, path, bytes).unwrap();
}

/// The `Env` every image made here carries.
pub const IMAGE_ENV: [&str; 2] = ["PATH=/usr/local/bin:/usr/bin:/bin", "IMAGE_VERSION=1.2.3"];

pub fn archive(program: &[u8]) -> Vec<u8> {
    let mut filesystem = tar::Builder::new(Vec::new());
    append(&mut filesystem, "./", b"", 0o755);
    append(&mut filesystem, "app/", b"", 0o755);
    append(&mut filesystem, "app/tenant", program, 0o755);
    append(&mut filesystem, "app/from-image", b"the OCI filesystem", 0o640);
    let filesystem = filesystem.into_inner().unwrap();
    from_filesystem(&filesystem)
}

pub fn from_filesystem(filesystem: &[u8]) -> Vec<u8> {
    let config = serde_json::to_vec(&json!({
        "architecture": "amd64", "os": "linux",
        "rootfs": { "type": "layers", "diff_ids": [digest(filesystem)] },
        "config": { "Cmd": ["/this/command/is/not/used"], "User": "root", "Env": IMAGE_ENV }
    }))
    .unwrap();
    let manifest = serde_json::to_vec(&json!({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": descriptor(&config, "application/vnd.oci.image.config.v1+json"),
        "layers": [descriptor(filesystem, "application/vnd.oci.image.layer.v1.tar")]
    }))
    .unwrap();
    let mut chosen = descriptor(&manifest, "application/vnd.oci.image.manifest.v1+json");
    chosen["platform"] = json!({"architecture": "amd64", "os": "linux"});
    let index = serde_json::to_vec(&json!({ "schemaVersion": 2, "manifests": [chosen] })).unwrap();
    let mut archive = tar::Builder::new(Vec::new());
    append(
        &mut archive,
        "oci-layout",
        br#"{"imageLayoutVersion":"1.0.0"}"#,
        0o644,
    );
    append(&mut archive, "index.json", &index, 0o644);
    for bytes in [config.as_slice(), manifest.as_slice(), filesystem] {
        append(
            &mut archive,
            &format!("blobs/sha256/{}", hex::encode(Sha256::digest(bytes))),
            bytes,
            0o644,
        );
    }
    archive.into_inner().unwrap()
}
