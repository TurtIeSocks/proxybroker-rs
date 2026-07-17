# Contributing

proxybroker-rs is a single-maintainer project with a deliberately strict, fully
offline test suite. This page covers what you need to build it, run the checks CI
enforces, and understand the provider liveness audit.

## Building

The project builds on the **stable** Rust toolchain — `rust-toolchain.toml` pins
stable, so a fresh checkout uses it automatically. A crates.io library that needs
nightly is a library most people cannot use; keeping to stable is a hard constraint.

```sh
# Default build: cli + server + geo + geo-bundled.
cargo build

# Release binary.
cargo build --release
```

Optional functionality lives behind [feature flags](../architecture/feature-flags.md).
Enable the ones you need, for example:

```sh
cargo build --features metrics,progress,watch,store-sqlite
```

## Running the checks

CI enforces exactly the checks you should run locally: formatting, clippy with
warnings-as-errors, and the full test suite across the feature matrix. Run them
before opening a PR.

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
cargo test --no-default-features --locked
```

`clippy` runs with `-D warnings` — a warning fails the build. `--locked` mirrors CI:
it fails rather than silently updating `Cargo.lock`.

## The CI matrix

The [CI workflow](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/.github/workflows/ci.yml)
runs on every pull request and every push to `main`. It is split into parallel jobs.

| Job | What it does |
|---|---|
| **fmt + clippy + test** | `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, then three build/test configs (below) |
| **musl + docker + installer smoke** | Builds the static `x86_64-unknown-linux-musl` binary, builds and runs the `FROM scratch` Docker image, and shellchecks `install.sh` |
| **store-redis (redis service)** | Runs the Redis backend tests against a real `redis:7` service container |

### The three test configurations

The test job runs three configs because each exercises code the others miss:

1. **`--all-features`** — the geo path; since `default` is all four default features,
   this also compiles the default library and CLI binary.
2. **`--no-default-features`** — the geo-free path a pure-library consumer gets. The
   broker has genuinely divergent no-geo runtime code (a no-op geo attach, a
   geo-less builder arm) that the all-features run never hits.
3. **`--features cli` (no `server`)** — a supported combo that neither of the above
   compiles, so a misplaced `#[cfg]` gate on a server-only function can slip through
   both. Build-only.

The whole suite is designed to run **fully offline** — local mock servers only, no
network, no flakiness. That is constraint C5 from
[the systematic refactor](./systematic-refactor.md), and it is load-bearing: a test
suite that needs the internet is one that fails in CI for reasons unrelated to the
code.

### Distribution smoke

The `dist` job proves the shipping artifacts stay self-contained. The static musl
binary must build, run with zero runtime data files (everything is embedded), and
carry the CC BY 4.0 DB-IP attribution in `--version`. The `FROM scratch` Docker image
must do the same offline. The `install.sh` installer must pass `shellcheck`.

### store-redis service

The Redis backend's atomic upsert can't be faithfully mocked (it depends on Lua
atomicity), so its integration tests run against a real Redis service container. This
is the one deliberately non-offline test path; the fold arithmetic and key layout are
also pure-tested in the main offline run.

## Provider liveness audit

The bundled provider registry (50 curated sources, see the
[roadmap](./roadmap.md#cross-cutting-providers-p1)) is guarded two ways:

- **Offline** — a registry integrity test and format-archetype fixtures guard the
  registry's *shape*. These run in the normal test job.
- **Liveness** — a separate
  [Provider audit workflow](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/.github/workflows/provider-audit.yml)
  fetches every bundled source and flags any that yield zero proxies (dead or
  format-changed).

The audit is **scheduled** (Mondays 06:00 UTC) and manually triggerable, and
deliberately does **not** run on pull requests or pushes — a source rotting upstream
must never block a merge. A red audit run is a maintenance signal ("re-curate the
provider data"), not a broken PR. You can run it locally:

```sh
cargo test --test provider_audit --locked -- --ignored --nocapture
```

The audit test is `#[ignore]`d, so it runs only with `--ignored`. `--nocapture`
prints the per-source yield table; the test fails (listing the dead URLs) if any
source yields nothing.

## Commit and review conventions

- **One commit per item.** A request that bundles several distinct fixes should land
  as one conventional-style commit each, so history stays reviewable, revertable, and
  bisectable.
- **Fix all red checks before advancing.** Never move on with a known-failing test,
  type error, or lint — a pre-existing failure masks the new ones your change
  introduces.
- **Keep it offline-testable.** Any new network-dependent behaviour needs a local
  mock, not a live dependency.
