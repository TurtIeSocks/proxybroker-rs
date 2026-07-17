# Observability

proxybroker-rs exposes four ways to see what it is doing at runtime: a Prometheus
metrics endpoint, structured JSON logs, a live progress bar, and live-reload of a
served pool file. Each is opt-in — some behind a build feature, all behind a flag —
so the default binary stays lean and silent.

| Facility | Flag | Feature | Applies to |
|---|---|---|---|
| Prometheus metrics | `--metrics <ADDR>` | `metrics` | [`serve`](../cli/serve.md) |
| JSON logs | `--log-format json` | `cli` (always) | all subcommands |
| Progress bar | `--progress` | `progress` | [`find`](../cli/find.md) |
| Live-reload | `--watch` | `watch` | [`serve --load`](../cli/serve.md) |

See [feature flags](../architecture/feature-flags.md) for how to build with the
optional features enabled.

## Prometheus metrics (`--metrics`)

When [`serve`](../cli/serve.md) is built with the `metrics` feature, `--metrics
<ADDR>` starts a second listener that serves the live pool state in
[Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/)
(version `0.0.4`). Any `GET` to that address returns the current snapshot.

```sh
# Build with metrics, then serve with a scrape endpoint on :9090.
cargo build --release --features metrics
proxybroker serve --types HTTP HTTPS --metrics 127.0.0.1:9090
```

The exporter is hand-rolled — no `prometheus` crate is pulled in, since the surface
is tiny and stable. It reads a single `Pool::snapshot()` per request. The exposed
series:

| Metric | Type | Meaning |
|---|---|---|
| `proxybroker_pool_size{scheme="http"}` | gauge | Proxies in the pool serving HTTP |
| `proxybroker_pool_size{scheme="https"}` | gauge | Proxies in the pool serving HTTPS |
| `proxybroker_pool_error_rate_avg` | gauge | Mean proxy error rate over the pool |
| `proxybroker_pool_resp_time_avg_seconds` | gauge | Mean proxy response time (seconds) |
| `proxybroker_pool_probe_latency_avg_seconds` | gauge | Mean judge-probe latency (check-time) over the pool |
| `proxybroker_evictions_total` | counter | Proxies hard-evicted from the pool |
| `proxybroker_rotations_total` | counter | Mid-request rotations to a different proxy |

Sample output:

```text
# HELP proxybroker_pool_size Proxies currently available in the pool.
# TYPE proxybroker_pool_size gauge
proxybroker_pool_size{scheme="http"} 42
proxybroker_pool_size{scheme="https"} 17
# HELP proxybroker_pool_error_rate_avg Mean proxy error rate over the pool.
# TYPE proxybroker_pool_error_rate_avg gauge
proxybroker_pool_error_rate_avg 0.08
# HELP proxybroker_pool_resp_time_avg_seconds Mean proxy response time over the pool.
# TYPE proxybroker_pool_resp_time_avg_seconds gauge
proxybroker_pool_resp_time_avg_seconds 1.34
# HELP proxybroker_pool_probe_latency_avg_seconds Mean judge-probe latency (check-time) over the pool.
# TYPE proxybroker_pool_probe_latency_avg_seconds gauge
proxybroker_pool_probe_latency_avg_seconds 0.42
# HELP proxybroker_evictions_total Proxies hard-evicted from the pool.
# TYPE proxybroker_evictions_total counter
proxybroker_evictions_total 5
# HELP proxybroker_rotations_total Mid-request rotations to a different proxy.
# TYPE proxybroker_rotations_total counter
proxybroker_rotations_total 12
```

Error rate is an **aggregate** gauge, not a per-address one: per-proxy labels would
be unbounded cardinality for a constantly-rotating pool. Per-proxy detail lives
behind the [`proxycontrol`](../cli/serve.md) control API instead.

Library users can render the same text directly:

```rust
use proxybroker::serve_metrics; // async: serve_metrics(addr, pool) -> ServerHandle
// or render the body yourself:
let body = proxybroker::server::render_metrics(&pool);
```

Both `render_metrics` and `serve_metrics` are gated on the `metrics` feature.

## Structured JSON logs (`--log-format json`)

The global `--log-format` option (default `text`) controls how the `tracing`
event stream is rendered. `--log-format json` emits line-delimited JSON — one
object per event — suitable for piping into a log aggregator.

```sh
proxybroker find --types HTTP --log info --log-format json 2>events.ndjson
```

Logs always go to **stderr**, so they never mix with proxy output on stdout. The
verbosity is set by the global `--log` option (`error`, `warn`, `info`, `debug`,
`trace`; default `warn`), or overridden by the standard `RUST_LOG`-style
environment filter if present. In JSON mode the whole stream renders as JSON,
including the per-check structured events emitted at each check outcome.

Both `--log` and `--log-format` are global options — they apply to every
subcommand ([`grab`](../cli/grab.md), [`find`](../cli/find.md),
[`check`](../cli/check.md), [`serve`](../cli/serve.md), and the others).

## Progress bar (`--progress`)

[`find`](../cli/find.md) accepts `--progress` to draw a live status line on stderr
while checking runs. It renders only when the binary is built with the `progress`
feature; otherwise the flag is a no-op (so scripts stay portable across builds).

```sh
cargo build --release --features progress
proxybroker find --types HTTP HTTPS --limit 100 --progress
```

The bar is a spinner (not a percentage bar — a streaming `find` has no known
total) and shows the running counts polled from the shared stats collector:

```text
⠹ checked 318 · working 74 · avg 1.12s
```

Like `--show-stats`, the bar is drawn to stderr so it never contaminates the
proxy list on stdout. It clears itself when `find` completes.

## Live-reload a served pool (`--watch`)

When [`serve --load <FILE>`](../cli/serve.md) is built with the `watch` feature,
adding `--watch` starts a filesystem watcher on the NDJSON pool file. On each
change it re-parses the file and reconciles the running pool — additions join,
removals drop — without restarting the server.

```sh
cargo build --release --features watch
proxybroker serve --load pool.ndjson --watch
```

Details:

- `--watch` requires `--load` (there is nothing to watch when the pool is filled
  from a live `find`). Without it, the flag warns and is ignored.
- Write bursts are coalesced with a short debounce, so an editor's replace-on-save
  triggers exactly one reconcile.
- A parse error (for example, a half-written file) is logged and the pool left
  untouched — a bad write never empties a running pool.

The watcher shares the pool's add/remove seam with the adaptive re-check loop
(`serve --recheck`), so both can safely mutate one live pool concurrently.

## Related

- [`serve` reference](../cli/serve.md) — every serving flag, including the pool
  selection strategies whose rotations and evictions the metrics count.
- [`proxybroker top`](../cli/top.md) — a live terminal dashboard over the pool
  (the `tui` feature), an interactive alternative to scraping `--metrics`.
- [Feature flags](../architecture/feature-flags.md) — which build features gate
  `metrics`, `progress`, and `watch`.
