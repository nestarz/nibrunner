# nibrunner. `just` with no target lists these.
default:
    @just --list

# The whole workspace, for the machine you are on.
build:
    cargo build --workspace

# How the host binary is linked: an x86_64 Linux box has a musl toolchain of its own (`musl-tools`),
# anything else crosses to it through `zig` and `cargo-zigbuild`.
cargo-musl := if os() + "-" + arch() == "linux-x86_64" { "cargo build" } else { "cargo zigbuild" }

# One static x86_64 Linux binary, what a host runs.
build-release:
    {{cargo-musl}} -p nibrunnerd --bin nibrunnerd --target x86_64-unknown-linux-musl --release
    @ls -la target/x86_64-unknown-linux-musl/release/nibrunnerd

# Builds guest/rootfs.ext4 and rewrites the manifest to describe it. Linux, root, docker, e2fsprogs.
guest-image:
    cargo build -p nibrunner-init --target x86_64-unknown-linux-musl --release
    sudo guest/build-image.sh

# Checks the guest image the way the daemon checks it before booting anything.
verify-guest-image:
    #!/usr/bin/env bash
    set -euo pipefail
    sums=$(mktemp)
    trap 'rm -f "$sums"' EXIT
    jq -r '.artifacts[] | select(.name == "vmlinux" or .name == "rootfs.ext4") | "\(.sha256)  \(.name)"' \
        guest/manifest.json > "$sums"
    # Two, because the manifest as committed describes no rootfs and would otherwise pass on none.
    test "$(wc -l < "$sums")" -eq 2
    cd guest && sha256sum -c "$sums"

# Everything a release ships, in one directory, next to the sums a host should hash to.
stage-release dist:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{dist}}"
    install -m 0755 target/x86_64-unknown-linux-musl/release/nibrunnerd "{{dist}}/nibrunnerd-linux-x64"
    install -m 0755 docs/dist/app "{{dist}}/nibrunner-docs-linux-x64"
    install -m 0644 guest/vmlinux guest/rootfs.ext4 guest/manifest.json "{{dist}}"
    # Written from inside the directory, so it names what `sha256sum -c` will be run next to. The
    # docs site is not in it: the installer fetches only what a host runs, and `sha256sum -c` fails
    # on a listed file that is not there.
    cd "{{dist}}" && sha256sum nibrunnerd-linux-x64 vmlinux rootfs.ext4 manifest.json > checksums.txt

# The next release tag, as `YEAR.MONTH.PATCH`, with the patch counter starting at 0 each month.
release-version:
    #!/usr/bin/env bash
    set -euo pipefail
    month="$(date -u +%Y.%-m)"
    # Use the highest surviving patch in case a tag was deleted. Prereleases do not advance it.
    last="$(git tag --list "v$month.*" | sed -nE 's/^v[0-9]{4}\.[0-9]{1,2}\.(0|[1-9][0-9]*)$/\1/p' | sort -n | tail -1)"
    echo "v$month.$(( ${last:--1} + 1 ))"

[positional-arguments]
prepare-release tag:
    #!/usr/bin/env bash
    set -euo pipefail
    tag=$1
    if [[ ! "$tag" =~ ^v[1-9][0-9]{3}\.(1[0-2]|[1-9])\.(0|[1-9][0-9]*)$ ]]; then
        echo "release tags must use YEAR.MONTH.PATCH, such as v2026.9.0" >&2
        exit 1
    fi
    if git show-ref --verify --quiet "refs/tags/$tag"; then
        echo "$tag already exists" >&2
        exit 1
    fi
    {{just_executable()}} set-release-version "${tag#v}"
    cargo update --workspace
    touch CHANGELOG.md
    git cliff --unreleased --tag "$tag" --prepend CHANGELOG.md

[private]
[positional-arguments]
set-release-version version:
    #!/usr/bin/env python3
    import pathlib
    import sys
    import tomllib

    manifest = pathlib.Path("Cargo.toml")
    content = manifest.read_text()
    current = tomllib.loads(content)["workspace"]["package"]["version"]
    manifest.write_text(content.replace(f'version = "{current}"', f'version = "{sys.argv[1]}"', 1))

release-notes:
    git cliff --current --strip all

# Everything that needs no kernel: the planner, the codecs, the ruleset, the reconcile.
test:
    cargo test --workspace

# The tests that need a kernel. Root, Linux, and `nft`, `mke2fs` and `/dev/net/tun` on the box.
# `just integration --build-only` builds them, which needs none of that.
[arg("build-only", long, value="--no-run", help="Build the tests without running them.")]
integration build-only="" *args:
    NIBRUNNER_INTEGRATION=1 cargo test --workspace --test integration {{build-only}} {{args}} -- --test-threads 1 --nocapture

# The tests that boot a real microVM, one at a time: they take the machine's nftables table and its
# tap names, which no two hosts can hold at once. Needs everything `integration` needs, plus
# `just guest-image` first — and the tenant they boot, which this builds.
[arg("build-only", long, value="--no-run", help="Build the tests without running them.")]
integration-guest build-only="" *args:
    cargo build -p nibrunner-test-tenant --target x86_64-unknown-linux-musl --release
    NIBRUNNER_INTEGRATION=1 NIBRUNNER_TEST_TENANT="${CARGO_TARGET_DIR:-target}/x86_64-unknown-linux-musl/release/test-tenant" cargo test -p nibrunnerd --test integration-guest {{build-only}} {{args}} -- --test-threads 1 --nocapture

# Rust and the docs site both; `just fmt --check` refuses instead of rewriting. Biome runs from the
# package.json scripts, which name their config: docs/biome.json is `root: false` so that an editor
# opened on the repo applies it, and a Biome started inside docs/ then has to be told where it is.
fmt *args:
    cargo fmt --all {{args}}
    cd docs && bun run {{ if args =~ "--check" { "check:format" } else { "fix:format" } }}

# The last of these is the scrape page, held to the naming rules Prometheus reads an exposition
# page by. promtool rates a page by the samples on it, so what it is handed is a sample of every
# series rather than the catalogue `just metrics` writes — and through a file, because promtool
# finds nothing wrong with a page it was handed none of.
lint:
    cargo clippy --workspace --lib --bins -- -D warnings
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cd docs && bun run check:types
    cd docs && bun run check:lint
    cargo run -q -p nibrunnerd --bin metrics-page -- target/metrics-page.prom
    promtool check metrics < target/metrics-page.prom

# The docs site under docs/ is a Fumadocs app on Bun, which mise.toml pins; `bun install` in there first.
docs-dev:
    cd docs && bun run dev

# One Linux x86_64 binary with the site inside, docs/dist/app: what nibrun runs. `bun run build:local`
# in docs/ is the same for this machine.
docs-build:
    cd docs && bun run build

# Every file in the tree that is written from the code rather than by hand, each with the check
# CI runs on it: `just <recipe>` writes it afresh, `just check-<recipe>` fails when what is checked
# in is behind.
protocol_schemas := "crates/protocol/schema"
config_example := "deploy/config.example.toml"
config_schema := "deploy/config.schema.json"
metrics := "crates/nibrunnerd/metrics.json"
openapi := "crates/nibrunnerd/filesystem.openapi.json"

# The JSON Schemas in crates/protocol/schema, from the protocol crate's types.
schema into=protocol_schemas:
    cargo run -q -p nibrunner-protocol --features schema --bin protocol-schema -- "{{into}}"

check-schema: (check-generated "schema" protocol_schemas)

# deploy/config.example.toml, from `HostConfig::example` in the daemon crate.
config-example into=config_example:
    cargo run -q -p nibrunnerd --bin config-example -- "{{into}}"

check-config-example: (check-generated "config-example" config_example)

# deploy/config.schema.json — config.toml as a JSON Schema — from `HostConfig::schema`.
config-schema into=config_schema:
    cargo run -q -p nibrunnerd --bin config-schema -- "{{into}}"

check-config-schema: (check-generated "config-schema" config_schema)

# crates/nibrunnerd/metrics.json — every series the scrape page publishes — from the declarations
# the page renders. The docs site's reference page is rendered from this file.
metrics into=metrics:
    cargo run -q -p nibrunnerd --bin metrics -- "{{into}}"

check-metrics: (check-generated "metrics" metrics)

# crates/nibrunnerd/filesystem.openapi.json — the socket that lists what a guest holds, from the
# route it serves and the refusals it declares. The docs site's reference page is rendered from it.
openapi into=openapi:
    cargo run -q -p nibrunnerd --features schema --bin openapi -- "{{into}}"

check-openapi: (check-generated "openapi" openapi)

# Runs `recipe` into a scratch copy of `path` — a file, or a directory of them — and diffs the two.
[private]
check-generated recipe path:
    #!/usr/bin/env bash
    set -euo pipefail
    fresh=$(mktemp -d)
    trap 'rm -rf "$fresh"' EXIT
    {{just_executable()}} {{recipe}} "$fresh/$(basename '{{path}}')"
    diff -ru '{{path}}' "$fresh/$(basename '{{path}}')" || {
        echo "{{path}} is behind the code: run \`just {{recipe}}\` and commit the result"
        exit 1
    }
