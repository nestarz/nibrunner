use std::io::Read;
use std::path::{Path, PathBuf};

const FIRECRACKER_VERSION: &str = "v1.16.2";
const FIRECRACKER_URL: &str = "https://github.com/firecracker-microvm/firecracker/releases/download/v1.16.2/firecracker-v1.16.2-x86_64.tgz";
const FIRECRACKER_SHA256: &str = "32e3cdcd4081f91fe2b024a266f57dcb3b4e5fec5033e0cb22467ad7f7820bda";
const FIRECRACKER_MEMBER: &str = "release-v1.16.2-x86_64/firecracker-v1.16.2-x86_64";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=NIBRUNNER_FIRECRACKER_BINARY");
    println!("cargo:rustc-env=NIBRUNNER_FIRECRACKER_VERSION={FIRECRACKER_VERSION}");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    let embedded = out_dir.join("firecracker");

    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let resolved = if target_arch == "x86_64" {
        // Loud here rather than quiet, because the daemon a VMM-less build produces starts, reads
        // its config, converges and serves — and only the first tenant to boot ever finds out. The
        // warnings above this say which of the three ways it failed.
        Some(resolve(&out_dir).expect(
            "firecracker could not be resolved, so this build would carry no VMM; point NIBRUNNER_FIRECRACKER_BINARY at one to build without fetching it"
        ))
    } else {
        println!(
            "cargo:warning=firecracker {FIRECRACKER_VERSION} ships for x86_64 only; this {target_arch} build carries no VMM"
        );
        None
    };

    match resolved {
        Some(binary) => {
            std::fs::write(&embedded, binary).expect("the build directory is writable");
            println!("cargo:rustc-env=NIBRUNNER_FIRECRACKER_EMBEDDED=1");
        }
        // `include_bytes!` wants a file whichever way this went, and an empty one is why it could
        // never be what catches a build that fetched nothing.
        None => std::fs::write(&embedded, []).expect("the build directory is writable"),
    }
    println!(
        "cargo:rustc-env=NIBRUNNER_FIRECRACKER_PATH={}",
        embedded.display()
    );
}

fn resolve(out_dir: &Path) -> Option<Vec<u8>> {
    if let Ok(path) = std::env::var("NIBRUNNER_FIRECRACKER_BINARY") {
        return std::fs::read(path).ok();
    }
    let cached = cache_path();
    if let Some(cached) = &cached {
        if let Ok(bytes) = std::fs::read(cached) {
            if digest_of(&bytes) == FIRECRACKER_SHA256 {
                return extract(&bytes, out_dir);
            }
        }
    }
    let bytes = download()?;
    if digest_of(&bytes) != FIRECRACKER_SHA256 {
        println!("cargo:warning=the firecracker tarball did not hash to the version this build pins");
        return None;
    }
    if let Some(cached) = &cached {
        let _ = std::fs::create_dir_all(cached.parent()?);
        let _ = std::fs::write(cached, &bytes);
    }
    extract(&bytes, out_dir)
}

fn cache_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(
        PathBuf::from(home)
            .join(".cache/nibrunner")
            .join(format!("firecracker-{FIRECRACKER_VERSION}.tgz")),
    )
}

fn download() -> Option<Vec<u8>> {
    let mut body = Vec::new();
    ureq::get(FIRECRACKER_URL)
        .call()
        .ok()?
        .body_mut()
        .as_reader()
        .read_to_end(&mut body)
        .ok()?;
    Some(body)
}

fn digest_of(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(bytes))
}

fn extract(tarball: &[u8], _out_dir: &Path) -> Option<Vec<u8>> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(tarball));
    for entry in archive.entries().ok()? {
        let mut entry = entry.ok()?;
        if entry.path().ok()?.to_string_lossy() == FIRECRACKER_MEMBER {
            let mut binary = Vec::new();
            entry.read_to_end(&mut binary).ok()?;
            return Some(binary);
        }
    }
    println!("cargo:warning=the firecracker tarball does not hold {FIRECRACKER_MEMBER}");
    None
}
