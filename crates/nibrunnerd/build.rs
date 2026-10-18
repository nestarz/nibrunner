use std::io::Read;
use std::path::{Path, PathBuf};

const FIRECRACKER_VERSION: &str = "v1.17.0";
const FIRECRACKER_URL: &str = "https://github.com/firecracker-microvm/firecracker/releases/download/v1.17.0/firecracker-v1.17.0-x86_64.tgz";
const FIRECRACKER_SHA256: &str = "06094a1108ae9e82aa4c23a775aa92758f53f1175d422270d9d6162cb9ade558";
const FIRECRACKER_MEMBER: &str = "release-v1.17.0-x86_64/firecracker-v1.17.0-x86_64";
const JAILER_MEMBER: &str = "release-v1.17.0-x86_64/jailer-v1.17.0-x86_64";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=NIBRUNNER_FIRECRACKER_BINARY");
    println!("cargo:rerun-if-env-changed=NIBRUNNER_JAILER_BINARY");
    println!("cargo:rustc-env=NIBRUNNER_FIRECRACKER_VERSION={FIRECRACKER_VERSION}");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    let embedded = out_dir.join("firecracker");
    let embedded_jailer = out_dir.join("jailer");

    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let resolved = if target_arch == "x86_64" {
        // Loud here rather than quiet, because the daemon a VMM-less build produces starts, reads
        // its config, converges and serves — and only the first tenant to boot ever finds out. The
        // warnings above this say which of the three ways it failed.
        Some(resolve().expect(
            "firecracker and its matching jailer could not be resolved; set both NIBRUNNER_FIRECRACKER_BINARY and NIBRUNNER_JAILER_BINARY to build without fetching them"
        ))
    } else {
        println!(
            "cargo:warning=firecracker {FIRECRACKER_VERSION} ships for x86_64 only; this {target_arch} build carries no VMM"
        );
        None
    };

    match resolved {
        Some(binaries) => {
            std::fs::write(&embedded, binaries.firecracker).expect("the build directory is writable");
            std::fs::write(&embedded_jailer, binaries.jailer).expect("the build directory is writable");
            println!("cargo:rustc-env=NIBRUNNER_FIRECRACKER_EMBEDDED=1");
        }
        // `include_bytes!` wants a file whichever way this went, and an empty one is why it could
        // never be what catches a build that fetched nothing.
        None => {
            std::fs::write(&embedded, []).expect("the build directory is writable");
            std::fs::write(&embedded_jailer, []).expect("the build directory is writable");
        }
    }
    println!(
        "cargo:rustc-env=NIBRUNNER_FIRECRACKER_PATH={}",
        embedded.display()
    );
    println!(
        "cargo:rustc-env=NIBRUNNER_JAILER_PATH={}",
        embedded_jailer.display()
    );
}

struct Binaries {
    firecracker: Vec<u8>,
    jailer: Vec<u8>,
}

fn resolve() -> Option<Binaries> {
    match (
        std::env::var("NIBRUNNER_FIRECRACKER_BINARY"),
        std::env::var("NIBRUNNER_JAILER_BINARY"),
    ) {
        (Ok(firecracker), Ok(jailer)) => {
            println!("cargo:rerun-if-changed={firecracker}");
            println!("cargo:rerun-if-changed={jailer}");
            return Some(Binaries {
                firecracker: std::fs::read(firecracker).ok()?,
                jailer: std::fs::read(jailer).ok()?,
            });
        }
        (Err(_), Err(_)) => {}
        _ => {
            println!("cargo:warning=offline builds must set both NIBRUNNER_FIRECRACKER_BINARY and NIBRUNNER_JAILER_BINARY to matching binaries");
            return None;
        }
    }
    let cached = cache_path();
    if let Some(cached) = &cached {
        if let Ok(bytes) = std::fs::read(cached) {
            if digest_of(&bytes) == FIRECRACKER_SHA256 {
                return extract(&bytes);
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
    extract(&bytes)
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

fn extract(tarball: &[u8]) -> Option<Binaries> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(tarball));
    let mut firecracker = None;
    let mut jailer = None;
    for entry in archive.entries().ok()? {
        let mut entry = entry.ok()?;
        let path = entry.path().ok()?.into_owned();
        let binary = if path == Path::new(FIRECRACKER_MEMBER) {
            &mut firecracker
        } else if path == Path::new(JAILER_MEMBER) {
            &mut jailer
        } else {
            continue;
        };
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).ok()?;
        *binary = Some(bytes);
    }
    Some(Binaries {
        firecracker: firecracker.or_else(|| {
            println!("cargo:warning=the firecracker tarball does not hold {FIRECRACKER_MEMBER}");
            None
        })?,
        jailer: jailer.or_else(|| {
            println!("cargo:warning=the firecracker tarball does not hold {JAILER_MEMBER}");
            None
        })?,
    })
}
