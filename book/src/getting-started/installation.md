# Installation

There are four ways to get `proxybroker`: from crates.io (library or CLI), from source, as a
prebuilt static binary, or as a Docker image.

## From crates.io

```sh
cargo add proxybroker          # add the library to your project
cargo install proxybroker      # install the CLI binary
```

`cargo install` builds the binary with the default features (`cli`, `server`, `geo`,
`geo-bundled`), so the resulting `proxybroker` includes the local server and the bundled
country database out of the box.

## Build from source

```sh
git clone https://github.com/TurtIeSocks/proxybroker-rs
cd proxybroker-rs
cargo build --release
```

The binary lands at `target/release/proxybroker`.

The project builds on the **stable** Rust toolchain — a pinned `rust-toolchain.toml` sets
`channel = "stable"`, so the build is reproducible regardless of your default toolchain. A
library that needs nightly is one most people cannot use; nothing here requires it. The crate
sets `rust-version = "1.85"` as its minimum supported Rust version.

## Prebuilt static binary (`install.sh`)

For Linux (musl) or macOS, an installer script downloads the release binary for your OS/arch,
**verifies its SHA-256 checksum**, and installs it — no toolchain, no `sudo`:

```sh
curl -fsSL https://raw.githubusercontent.com/TurtIeSocks/proxybroker-rs/main/install.sh | sh
```

By default it installs to `~/.local/bin`; the script warns if that directory is not on your
`PATH`. Two environment variables tune it:

| Variable | Default | Meaning |
|---|---|---|
| `PROXYBROKER_VERSION` | latest release tag | Which release to install. |
| `PROXYBROKER_BIN_DIR` | `$HOME/.local/bin` | Install directory. |

Supported targets: `x86_64`/`aarch64` Linux musl, and `x86_64`/`aarch64` Apple Darwin. On
Windows, download the `.zip` from the Releases page.

The Linux binary is a **fully static musl build** — TLS is ring-only rustls (no aws-lc-rs) —
so it has no runtime libc or data-file dependencies. The geo database and provider list are
embedded into the binary itself, so it stands alone.

## Docker (`FROM scratch`)

The bundled `Dockerfile` produces a `FROM scratch` image containing just the static binary
plus the licence files. Because the geo database and provider list are embedded, the image
needs no data volumes:

```sh
docker build -t proxybroker .
docker run --rm proxybroker find --types HTTP --limit 5
```

## Feature flags in one paragraph

The crate is split into Cargo features so library users can pull in only what they need. The
defaults — `cli`, `server`, `geo`, `geo-bundled` — give you the full binary with the local
server and the bundled country database. Optional features add a metrics endpoint, a progress
bar, SQLite/Redis persistence, a terminal dashboard (`tui`), an MCP server (`mcp`), a
filesystem watcher (`watch`), and a drop-in hyper connector (`connector`). See
[Feature Flags](../architecture/feature-flags.md) for the full table and what each one enables.

### A geo-free build

Building with `--no-default-features` gives you the **library only**, with no geo data, no
server, and no CLI dependencies — and therefore no data-attribution obligation:

```sh
cargo build --no-default-features
```

You can also keep geolocation code while dropping the bundled database (turn off
`geo-bundled` but keep `geo`) and supply your own database at runtime with `--geo-db`. See
[Geolocation & ASN](../architecture/geo-asn.md) and [Data & Licensing](../data-and-licensing.md)
for details.
