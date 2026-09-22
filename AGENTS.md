## Project

nibrunner is one binary, `nibrunnerd`, that turns a Linux machine with `/dev/kvm` into a microVM
host. A JSON document names the apps; the daemon boots each one in its own Firecracker microVM,
routes its hostname, sleeps and wakes it, and keeps its volume.

Rust workspace (`crates/*`) with the docs site under `docs/`.

## Stack

- **Language:** Rust; the host binary is one static x86_64 musl binary
- **Tasks:** `just`; `mise` installs `just` and `bun`
- **Linter/Formatter:** rustfmt and clippy for the Rust, Biome (`docs/biome.json`) for the docs
  site; editors format on save
- **State:** SQLite through sqlx, with the offline query data committed under `.sqlx/`
- **Docs:** Fumadocs on Bun under `docs/`, served at [nibrunner.dev](https://nibrunner.dev)
- **Commits:** Conventional Commits, checked on pull request titles by `check-pr-title`

## Layout

- `crates/protocol` — the control-plane protocol: the desired and reported state documents.
  `schema/*.json` is generated from its types
- `crates/guest-contract` — what host and guest agree on: drive order, kernel args, vsock ports,
  the frame codecs
- `crates/nft-render` — slot arithmetic, the nftables ruleset rendered whole, the parsers for what
  `nft` answers
- `crates/nibrunnerd` — the daemon. `domain/` holds the logic and needs no kernel; `ports.rs`
  names what it needs from outside as traits; `adapters/` implement them against Firecracker,
  nft, taps, volumes and the proxy; `repositories/` are the SQLite tables; `services/` wire
  domain to adapters and `controllers/` are the passes that run on a clock; `install/` lays a
  host out; `config.rs` is `config.toml`
- `crates/init` — the guest's PID 1
- `crates/test-tenant` — the tenant the guest tests boot: one static binary that answers, writes,
  remembers and misbehaves on demand
- `guest/` — the kernel, the image build script and the manifest of pins and digests
- `deploy/` — the installer, the systemd unit, and the generated `config.example.toml` and
  `config.schema.json`
- `docs/` — the site; pages are MDX under `docs/content/docs/`

## Code style

- **Code must be self-explanatory — this is strict.** Express intent through naming, types and
  structure, not prose. Do not write a comment that restates what the code already says. A comment
  is warranted only for what the code cannot say: a tradeoff, an external constraint (a
  Firecracker, nftables, kernel or ext4 quirk; a contract with the control plane), a maintenance
  note. Why, never what. If you feel the need to explain _what_ a block does, rename or extract it
  instead. Delete comments that no longer earn their place. This holds in the justfile and the
  workflows too: a step's `name:` is its label, and a comment on it is only for a why the command
  hides.
- The workspace lints are in `Cargo.toml`, and `just lint` runs clippy with `-D warnings`, so every
  warning fails. `unwrap_used` and `panic` are among them: production code returns an error, or
  `expect`s with the reason it cannot fail. Tests and the `testing` feature are exempt, in `lib.rs`.
- `unsafe_code` warns too. An `unsafe` block sits in the smallest item that can hold it, under
  `#[allow(unsafe_code)]`, with a `reason` when it is not obvious.
- `unreachable_pub` warns: `pub` only what another crate reaches, `pub(crate)` otherwise.
- rustfmt: `max_width = 110`, imports and modules reordered. Do not fight it.
- A proper noun in a doc comment that clippy takes for an identifier goes in `clippy.toml`'s
  `doc-valid-idents`, not in backticks.
- Errors are `thiserror` enums whose messages read as the sentence an operator will find in a log.
- A metric is a `Metric` static beside the pass that emits it — name, help, kind and labels in one
  place, because `# HELP` and `# TYPE` are optional and a page whose two halves name a series
  differently scrapes without complaint. Its prefix says what is measured: `nibrunner_app_` for one
  app, `host_` for the machine, `volume_`/`checkpoint_`/`export_` for what it holds, `proxy_` for
  the listener, `vm_` for an operation on a microVM. `metrics::declared()` is the catalogue, and a
  test holds the page to emitting every series in it.
- Tests are named as the sentence they prove: `a_booted_vm_is_not_a_running_app`, not `test_boot`.
- Docs site: Biome is strict — one parameter per function (`useMaxParams: 1`; wrap several in an
  object), no magic numbers, braces on every block, components declared with `function`, no barrel
  files, sorted Tailwind classes. `bun run fix:lint` and `bun run fix:format` in `docs/` apply
  what can be applied.

## Validation

After finishing an implementation, always run:

1. `just fmt` — rustfmt and Biome, rewriting in place
2. `just lint` — clippy with `-D warnings`, then the docs site's types and lint
3. `just test` — everything that needs no kernel
4. `just integration --no-run` — the kernel tests at least compile
5. `just guest-tests --no-run` — the microVM tests at least compile

`just integration` itself needs root, Linux, `nft`, `mke2fs` and `/dev/net/tun`, and is the only
place a ruleset load, a real `mke2fs` or a tap is considered proven. `just guest-tests` needs all
of that plus `/dev/kvm` and a guest image from `just guest-image`, and is the only place a boot, a
sleep, a wake or a route into a guest is considered proven. CI runs both on every pull request;
run them yourself on a Linux box when the change touches an adapter or the guest.

Some files in the tree are written from the code rather than by hand. After changing what they
come from, regenerate and commit; CI's `just check-<recipe>` fails otherwise:

- `crates/protocol/schema/*.json` from the types in `crates/protocol` — `just schema`
- `deploy/config.example.toml` from `HostConfig::example` in `crates/nibrunnerd/src/config.rs` —
  `just config-example`
- `deploy/config.schema.json` from `HostConfig::schema` in the same file — `just config-schema`
- `.sqlx/` from every `sqlx::query!` — `cargo sqlx prepare` against a SQLite file with
  `crates/nibrunnerd/migrations` applied. The build reads the committed data and needs no
  database; a query it does not know fails to compile.

Tests live beside the code in `#[cfg(test)] mod tests` (the `protocol` crate keeps its own in
`src/tests.rs`); the daemon's kernel-needing ones are `crates/nibrunnerd/tests/integration.rs`,
and the ones that boot a microVM are `crates/nibrunnerd/tests/guest/`, one file per family of
invariant. Both are gated on `NIBRUNNER_INTEGRATION=1`. What more than one test needs — fixtures,
`mockall` mocks, a host laid out the way `install` lays one, a host running on this machine — is in
`crates/nibrunnerd/src/test_support/`, behind the `testing` feature. The unit lane is the planner, the health state machine, the backoff, the ruleset
asserted as text, the codecs against byte fixtures taken from the C headers, and the reconcile pass
driven against mocked collaborators.

`crates/nibrunnerd/migrations/0001_host_state.sql` is edited in place rather than followed by a
`0002` while nobody runs a host that has to survive an upgrade. Do not propose a new migration file
for a schema change, and do not flag the edit.

## Building

`just build` is the workspace for the machine you are on. `just build-release` is what a host runs:
one static x86_64 Linux binary, linked against musl — an x86_64 Linux box needs `musl-tools`,
anything else crosses through `zig` and `cargo-zigbuild`. The daemon's `build.rs` fetches the
Firecracker release it pins and embeds it; `NIBRUNNER_FIRECRACKER_BINARY` points it at one to build
without fetching. `just guest-image` builds `guest/rootfs.ext4` and needs Linux, root, docker and
e2fsprogs.

## Run scripts

When running a command, check the `justfile` first. The docs site's commands are the `scripts` in
`docs/package.json`, which is their source of truth: the justfile only calls them, inside `fmt`,
`lint`, `docs-dev` and `docs-build`. A new JS check is a `package.json` script wired into `fmt` or
`lint`, never a recipe of its own or a command spelled out in the justfile.

## The docs site

`docs/` is a Fumadocs app on TanStack Start. `bun install` in there once, then `just docs-dev`.
Pages are MDX under `docs/content/docs/`: `(docs)/index.mdx` is the introduction,
`(docs)/getting-started/` is read in order, `(docs)/guides/` is one feature per page, and the
`reference/` pages are rendered from the JSON Schemas by `<SchemaReference file="..." />`, which
`docs/src/lib/remark-schema-reference.ts` expands at build time — no generated MDX is checked in.
A page that moves leaves its old path in the `moved` map in `docs/src/routes/docs/$.tsx`. Link the
docs by their nibrunner.dev URL, from the daemon's messages and the README alike. Style: short,
steps and tables first, the why in a callout. `just docs-build` produces `docs/dist/app`, one Linux
binary with the site inside; it is deployed by hand with `nib run` from `main` once a pull request
merges.

## READMEs

The root `README.md` is the project homepage: what nibrunner is, the quick start, and one link to
the docs site. It is the only README. Crates, `deploy/`, `guest/` and `docs/` carry none — the
layout is this file's to tell, and user-facing documentation is the docs site. Do not add a
`README.md` elsewhere; add a nested `AGENTS.md` only when a directory needs telling an agent
something its files cannot show. `.github/CONTRIBUTING.md` is the human entry point: how to set
up locally, the tools, the commit format, and a pointer here for the rest.

## Keeping this file up to date

When a change affects code style, tooling, conventions, or project taste (a new lint, a formatter
setting, a naming pattern, a dependency choice, a new generated file, etc.), propose updating this
file to reflect it.

## Pull requests

Work goes on a branch and lands on `main` through a pull request, squash-merged with the pull
request's title as the commit subject — so the title is a Conventional Commit (`feat:`, `fix:`,
`docs:`, `chore:`, …), and so is every commit on the branch. Never push to `main` directly.

Keep PR descriptions minimal — the diff is self-explanatory, so don't enumerate every change.
State the intent in a line or two.
