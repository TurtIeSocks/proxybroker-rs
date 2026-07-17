# mcp

Serve a live proxy pool over the [Model Context Protocol](https://modelcontextprotocol.io) on
**stdio**, so agent tooling can pull healthy proxies and feed failures back into the same eviction
machinery. It is a thin veneer over the same pool used by [serve](./serve.md).

`mcp` is behind the `mcp` feature, which is **off by default**. Build with it enabled:

```sh
cargo build --features mcp
proxybroker mcp --types HTTP HTTPS
```

`stdout` is the MCP JSON-RPC channel; all human-facing logging goes to `stderr`. The pool fills in
the background, so `get_proxy` may return `null` until the first proxies land.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--types <TYPE>...` | — | Protocols to find for the pool (required). E.g. `HTTP HTTPS`. |
| `--limit <N>` | `100` | Stop filling the pool after this many working proxies. |
| `--countries <CC>...` | — | Keep only proxies in these ISO country codes. Alias: `--only-cc` (comma-separated). |
| `--timeout <SECS>` | `8` | Per-request timeout, in seconds. |

Protocol values are exactly as in [find](./find.md). There is deliberately no `--max-error-rate` /
`--max-resp-time`: those pool thresholds only gate re-admission via the server relay, which the MCP
handlers never call.

## Exposed tools

The server exposes exactly three tools:

### `get_proxy`

Check out the best healthy proxy for a scheme, optionally filtered to a country. The proxy stays in
the pool (it is returned immediately, so it keeps rotating by priority).

Arguments:

| Field | Type | Meaning |
| --- | --- | --- |
| `scheme` | string | `"http"` or `"https"`. |
| `country` | string, optional | ISO country code filter (case-insensitive). |

Result (or `null` if none is available):

```json
{
  "proxy": "1.2.3.4:8080",
  "types": ["HTTP", "HTTPS"],
  "avg_resp_time": 0.42,
  "error_rate": 0.0
}
```

### `pool_status`

A snapshot of the live pool. No arguments. Result:

```json
{
  "total": 100,
  "working": 100,
  "by_protocol": { "HTTP": 80, "HTTPS": 40 },
  "by_country": { "US": 30, "DE": 12 },
  "avg_resp_time": 0.71,
  "errors": {}
}
```

### `report_dead`

Report a proxy as dead so it is removed from the pool and no longer handed out. The failure happened
out-of-process, so the honest action is removal — not synthesizing an error into the histogram.

Arguments:

| Field | Type | Meaning |
| --- | --- | --- |
| `proxy` | string | The dead proxy's `host:port`. |

Result:

```json
{ "removed": true }
```

## See also

- [serve](./serve.md) — the rotating proxy server over the same pool.
- [top](./top.md) — a live TUI dashboard over the same pool.
- [feature flags](../architecture/feature-flags.md) — enabling `mcp` and store backends.
