# Feature flags

proxybroker-rs is a library first and a CLI second, so almost everything beyond the core find/check
engine is behind a cargo feature. This keeps the default binary lean and lets pure-library users pull
in only what they need. Every feature below is declared in `Cargo.toml`.

## The default set

```toml
default = ["cli", "server", "geo", "geo-bundled"]
```

The default build gives you the `proxybroker` binary, the local rotating server, country geolocation,
and the embedded DB-IP database. `--no-default-features` strips all of that back to the bare
library — the find/check engine with no CLI, no server, and no geo.

## Every feature

| Feature | Default | Enables | Pulls in |
| --- | :---: | --- | --- |
| `cli` | yes | The `proxybroker` binary: arg parsing, output formatting, logging setup. | `clap`, `tracing-subscriber` |
| `server` | yes | The local rotating proxy server (`serve`, `Pool`, `Strategy`). | — |
| `geo` | yes | Country-lookup code ([`GeoDb`](./geo-asn.md)). | `maxminddb` |
| `geo-bundled` | yes | Embeds the DB-IP Country Lite database (~3.9 MB gzipped). Turn off to supply your own. | — (implies `geo`) |
| `metrics` | no | Prometheus metrics endpoint for `serve` (a hand-rolled text exporter). | — (implies `server`) |
| `progress` | no | Live progress bar during `find`. | `indicatif` (implies `cli`) |
| `persist` | no | The `Store` trait + observer machinery for `--state`. No backend of its own. | — |
| `store-sqlite` | no | SQLite backend for `--state`. Bundled SQLite: static link, no system `libsqlite3`. | `rusqlite` (implies `persist`) |
| `store-redis` | no | Redis backend for `--state` (atomic EWMA upsert via a Lua script). | `redis` (implies `persist`) |
| `tui` | no | The `proxybroker top` terminal dashboard. | `ratatui`, `crossterm` (implies `cli` + `server`) |
| `watch` | no | Live-reload of the `serve --load` file via a filesystem watcher. | `notify` (implies `server`) |
| `mcp` | no | Exposes the live pool over MCP stdio (`proxybroker mcp`). | `rmcp` (implies `server` + `cli`) |
| `connector` | no | A drop-in hyper-util connector routing each connection through the rotating pool. | `tower-service`, `hyper-util/client-legacy` (implies `server`) |

Features that imply others do so through cargo's dependency edges — enabling `tui`, for example,
automatically turns on `cli` and `server`.

## Common combinations

**CLI only — no server, no geo.** The smallest useful binary: find and check proxies, print them, but
do not open a listening socket or bundle a geo database.

```sh
cargo build --no-default-features --features cli
```

**No geo.** Keep the CLI and server, drop the geo code and the bundled database entirely (no
[attribution duty](../data-and-licensing.md), a smaller binary):

```sh
cargo build --no-default-features --features cli,server
```

**Persistence with a backend.** The `persist` feature is only the `Store` *contract*; pair it with a
backend:

```sh
cargo build --features store-sqlite      # or store-redis
```

**musl-clean by design.** The whole default graph is ring-only (rustls with `rustls-no-provider` +
the `ring` provider installed at startup), so aws-lc-rs — whose `-sys` C/asm crate is the musl
cross-compile blocker — never enters the dependency tree. This is not a feature you toggle; it holds
for the default build *and* the optional backends: `store-sqlite` compiles SQLite from source
(static, no system library), and `store-redis` uses a ring-only rustls for `rediss://`. So a
`--target x86_64-unknown-linux-musl` build works with the default features and the stores alike.
