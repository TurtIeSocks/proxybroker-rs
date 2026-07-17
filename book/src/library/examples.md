# Examples

Every runnable example lives in [`examples/`](https://github.com/TurtIeSocks/proxybroker-rs/tree/main/examples).
Run one with `cargo run --example <name>`.

Six examples declare `required-features = ["server"]` in `Cargo.toml`. Because
`server` is part of the default feature set, the plain `cargo run --example
<name>` command works out of the box — you only need an explicit `--features`
flag if you build with `--no-default-features`. The other examples depend only on
the core library and need no features at all.

## Find and grab

| Example | Demonstrates | Command |
| --- | --- | --- |
| `find` | `Broker::find` returns a `Stream` that yields proxies as they pass checking; prints each with its schemes and average response time. The Rust equivalent of proxybroker2's `basic.py`. | `cargo run --example find` |
| `find_and_save` | Find working proxies and write them to `proxies.txt`, one scheme-prefixed URL per line (`http://host:port` / `https://host:port`). Mirrors proxybroker2's `find_and_save.py`. | `cargo run --example find_and_save` |
| `grab` | `Broker::grab` — gather proxies from providers **without** checking them (fast but unverified), filtered to the US or GB via `GrabQuery::countries`. Mirrors `only_grab.py`. | `cargo run --example grab` |
| `stats` | Find, then print an aggregate summary via `proxies.stats()` — counts by protocol, anonymity level, and country, plus the error histogram over every proxy checked. | `cargo run --example stats` |
| `checking_depth` | Deep-checking knobs on `FindQuery`: `RetryPolicy::transient`, `relaxed_validity`, `trust_check`, and a `liveness_url` fallback; then reads `percentile`, `capabilities`, and `trust` off each proxy. | `cargo run --example checking_depth` |
| `custom_provider` | Supply your own source with `ProviderSpec` (URL + protocols + optional 2-group `(host, port)` regex) and `Broker::builder().providers(..)`. The Rust equivalent of proxybroker2's `custom_providers/`. | `cargo run --example custom_provider` |

## Serving (`server` feature — on by default)

| Example | Demonstrates | Command |
| --- | --- | --- |
| `serve` | Fill a `Pool` from `find`, then `serve` a local rotating proxy and fetch a page through it; retries a different proxy on failure. Mirrors `proxy_server.py` + `use_existing_proxy.py`. | `cargo run --example serve` |
| `serve_authenticated` | Auth both ways: gate clients with a `Proxy-Authorization` credential (407 on a miss) and relay through authenticated upstreams via `Proxy::with_auth` + `Credentials`. | `cargo run --example serve_authenticated` |
| `serve_tuned` | A production-tuned server through `PoolConfig`: `Strategy::Sticky` + `sticky_header`, `countries`, `fail_timeout`, `prefer_connect`, `http_allowed_codes`, plus the `serve` min-queue and backlog parameters. | `cargo run --example serve_tuned` |
| `byo_pool` | Bring your own proxies: fill a `Pool` from any `Stream<Item = Proxy>` with `Pool::spawn`, then `wait_ready`, `len`, and `remove`. Fully self-contained, no network. | `cargo run --example byo_pool` |
| `proxycontrol` | The `proxycontrol` control API: steer a live server by addressing requests to the magic `proxycontrol` host (`/api/remove/<ip:port>`, `/api/history/url:<url>`). Self-contained. | `cargo run --example proxycontrol` |
| `socks5_frontend` | The SOCKS5 front-end: a client speaks SOCKS5 to the local server (auto-detected from the first byte), which tunnels through a pooled upstream. | `cargo run --example socks5_frontend` |

If you build without default features, add the flag explicitly, e.g.:

```sh
cargo run --example serve --no-default-features --features server
```

## Related

- [Broker](./broker.md) — `find`, `grab`, and the builder used throughout.
- [Pool & server](./pool.md) — `Pool`, `PoolConfig`, and `serve`.
- [Persistence](./persistence.md) — warm-starting from stored history.
- [feature flags](../architecture/feature-flags.md) — the full feature matrix.
