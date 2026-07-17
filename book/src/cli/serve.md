# serve

Run a local **rotating proxy server**. It accepts client connections and relays each one
through a pool of checked proxies, transparently retrying on a different proxy when one fails.
Point any HTTP client (or a SOCKS5 client) at `serve`'s address and every request rides an
upstream proxy picked from the live pool.

`serve` is behind the `server` feature, which is on by default. See
[feature flags](../architecture/feature-flags.md) for the optional add-ons (`metrics`, `watch`,
`store-sqlite`, `store-redis`) that unlock some flags below.

```sh
# Fill the pool with working HTTP proxies, listen on 127.0.0.1:8888.
proxybroker serve --types HTTP
```

The server binds immediately and fills its pool in the background from a live
[find](./find.md). It prints its listen address to stderr and runs until `Ctrl-C`.

## How it fills the pool

There are two ways to source proxies:

| Mode | Flag | Behaviour |
| --- | --- | --- |
| Live find | `--types` | Runs [find](./find.md) continuously to keep the pool topped up to `--limit`. |
| Load a file | `--load <PATH>` | Fills from an NDJSON file of already-checked proxies (from a prior `--save`), then drains as they are used — no top-up. |

`--types` and `--load` are mutually exclusive; one of them is required. The `--load` file is the
NDJSON artifact written by [`find --save`](./find.md) / [`check --save`](./check.md).

```sh
# Serve a previously-saved pool without re-finding.
proxybroker find --types HTTP HTTPS --limit 50 --save pool.ndjson
proxybroker serve --load pool.ndjson
```

## Options

All find-style filters (`--types`, `--lvl`, `--strict`, `--post`, `--dnsbl`, `--countries`) are
threaded into the pool-fill query. Protocol and anonymity values are exactly as in
[find](./find.md) (e.g. `HTTP HTTPS SOCKS5 CONNECT:80`).

### Listener

| Flag | Default | Meaning |
| --- | --- | --- |
| `--host <ADDR>` | `127.0.0.1:8888` | Address to listen on. |
| `--backlog <N>` | `1024` | TCP listen backlog (queued pending connections). |
| `--min-queue <N>` | `0` | Wait until the pool holds at least this many proxies before accepting clients. |
| `--auth <USER:PASS>` | — | Require client authentication (see below). |
| `--timeout <SECS>` | `8` | Per-request timeout, in seconds. |
| `--max-tries <N>` | `3` | Attempts (each through a different proxy) per client request. |

### Pool fill

| Flag | Default | Meaning |
| --- | --- | --- |
| `--types <TYPE>...` | — | Protocols to find for the pool. Required unless `--load`. |
| `--load <PATH>` | — | Fill from a saved NDJSON file instead of finding. Conflicts with `--types`. |
| `--lvl <LVL>...` | any | Anonymity levels to accept for HTTP. |
| `--strict` | off | Require the anonymity level to match exactly. |
| `--post` | off | Use POST instead of GET for the pool-fill test request. |
| `--dnsbl <ZONE>...` | — | DNS blocklist zones; reject proxies listed in any. |
| `--limit <N>` | `100` | Keep the pool topped up to this many working proxies. |
| `--countries <CC>...` | — | Keep only proxies in these ISO country codes. Alias: `--only-cc` (comma-separated). |

### Selection and eviction

| Flag | Default | Meaning |
| --- | --- | --- |
| `--strategy <S>` | `best` | How to pick an upstream per request (see below). |
| `--sticky-header <HEADER>` | — | With `--strategy sticky`, key the session on this request header instead of the client IP (HTTP only). |
| `--prefer-connect` | off | Prefer proxies that support `CONNECT:80` when otherwise equally ranked. |
| `--max-error-rate <R>` | `0.5` | Drop a proxy once its error rate exceeds this (0.0–1.0). |
| `--max-resp-time <S>` | `8.0` | Drop a proxy once its average response time (seconds) exceeds this. |
| `--fail-timeout <SECS>` | `30` | Seconds a proxy is benched after a failure before it is re-probed. |
| `--http-allowed-codes <CODE>...` | — | For HTTP requests, retry through another proxy when the upstream status is outside this set (e.g. `200 204 301 302`), to dodge block pages. Empty = accept any status. |

### Selection strategies

`--strategy` accepts:

| Value | Behaviour |
| --- | --- |
| `best` | Lowest error rate, then fastest response (the default). |
| `round-robin` | Rotate through eligible proxies in pool order. |
| `random` | Uniform random pick. |
| `sticky` | Pin each client to one upstream, keyed by client IP (or `--sticky-header`). Falls back to `best` for a new client or when the pinned proxy is gone. |

With `--prefer-connect`, `CONNECT:80` support is a tie-break for `best`/`sticky` (health still
dominates) and a primary filter for `round-robin`/`random`.

### Country filter

`--countries` (alias `--only-cc`) constrains the pool to specific ISO country codes. The filter is
applied on **admission** — even on the `--load` path, which never ran find's country filter — so a
warm or bring-your-own pool is screened too. A proxy with no geolocation is rejected whenever a
country filter is set.

```sh
# US or German exit proxies only. Both spellings are equivalent.
proxybroker serve --types HTTP --countries US DE
proxybroker serve --types HTTP --only-cc US,DE
```

### Persistence and re-checking

These need a persistence build (the `persist` feature; see
[feature flags](../architecture/feature-flags.md)). `--state` additionally needs a store
backend (`store-sqlite` or `store-redis`) to durably remember proxies; `--recheck` alone works
on any `persist` build, keeping its scores in memory when no `--state` is given.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--state <PATH_OR_URL>` | — | Remember proxies across runs. A file path uses SQLite; a `redis://`/`rediss://` URL uses Redis. Warm-starts the pool from stored history and folds each fresh check back in. |
| `--recheck` | off | Adaptively re-check pooled proxies on a cadence proportional to their stability. Needs the `persist` feature (any of `store-sqlite`/`store-redis`/`persist`). With `--state` the decay scores persist across runs; without it they are kept in memory and reset on restart. |
| `--recheck-rate <N>` | `5.0` | Global re-check ceiling, checks/sec. |
| `--recheck-min <SECS>` | `60` | Shortest re-check cadence (a flaky proxy). |
| `--recheck-max <SECS>` | `3600` | Longest re-check cadence (a rock-solid proxy). |
| `--decay-halflife <SECS>` | `21600` | Score half-life for an unseen proxy. |

### Metrics

With the `metrics` feature built in:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--metrics <ADDR>` | — | Serve a Prometheus text metrics endpoint on this address. |

Any request to the metrics endpoint returns the current pool metrics in Prometheus text
exposition format (`version=0.0.4`):

```
proxybroker_pool_size{scheme="http"}            <gauge>
proxybroker_pool_size{scheme="https"}           <gauge>
proxybroker_pool_error_rate_avg                 <gauge>
proxybroker_pool_resp_time_avg_seconds          <gauge>
proxybroker_pool_probe_latency_avg_seconds      <gauge>
proxybroker_evictions_total                     <counter>
proxybroker_rotations_total                     <counter>
```

Error rate is an **aggregate** gauge over the pool, not per-address — per-proxy labels would be
unbounded cardinality for a rotating pool. Per-proxy detail lives behind the control API below.

```sh
proxybroker serve --types HTTP --metrics 127.0.0.1:9090
curl -s http://127.0.0.1:9090/
```

### Live reload

With the `watch` feature built in:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--watch` | off | Live-reload the `--load` file: apply additions/removals to the running pool without a restart. Requires `--load`. |

```sh
# Edit pool.ndjson while the server runs; changes are reconciled into the live pool.
proxybroker serve --load pool.ndjson --watch
```

## Client authentication (`--auth`)

`--auth USER:PASS` gates clients. An HTTP client without a matching
`Proxy-Authorization: Basic base64(user:pass)` gets `407 Proxy Authentication Required` before any
pool proxy is touched. The same credential also gates the SOCKS5 front-end via RFC 1929
(username/password). Absent, the server is open.

```sh
proxybroker serve --types HTTP --auth alice:s3cret
curl -x http://alice:s3cret@127.0.0.1:8888 http://example.com/
```

The client's gate credential is a hop-by-hop secret: it is stripped from the request before it is
forwarded upstream, so it never leaks to the (untrusted) upstream proxy.

## Upstream proxy authentication

If a pooled proxy carries its own credentials (a paid/authenticated upstream), the server applies
them automatically — as SOCKS5 RFC 1929 during negotiation, or as `Proxy-Authorization` on a
CONNECT/forward request. These upstream secrets are never serialized, so they stay out of
`--format json`. Supplying authenticated upstreams is a library-level operation; see the
`serve_authenticated` example.

## The `X-Proxy-Info` header

The server tells the client which upstream served the request via an `X-Proxy-Info: <host>:<port>`
header:

- On an HTTP forward request, it is injected after the response status line.
- On a CONNECT tunnel, it rides the `200 Connection established` ack (the one place a CONNECT
  client can see it — the tunnel body is opaque).
- A SOCKS5 tunnel is opaque and carries **no** `X-Proxy-Info`; its success reply uses a stub bound
  address so the upstream identity is not leaked.

## SOCKS5 front-end

The same listener accepts plain HTTP, HTTP `CONNECT`, **and** SOCKS5 — the protocol is
auto-detected from the client's first byte (`0x05` ⇒ SOCKS5). Only the SOCKS5 `CONNECT` command is
supported (BIND/UDP are rejected); all three address types (IPv4, IPv6, domain) work. With
`--auth`, the SOCKS5 client must authenticate via RFC 1929, symmetric with the HTTP `407` gate.

```sh
proxybroker serve --types SOCKS5
curl --socks5 127.0.0.1:8888 http://example.com/
```

## The `proxycontrol` control API

Introspect and steer a running server — without a restart — by sending it requests, as its own
client, addressed to the magic `proxycontrol` host. These requests are intercepted before proxy
selection, so they never consume a pool proxy. On an authenticated server the client-auth gate is
checked first, so introspection cannot reveal pool membership.

| Request | Result |
| --- | --- |
| `GET http://proxycontrol/api/remove/<ip:port>` | Evict that proxy from the live pool. Always `204 No Content`, whether or not it matched. |
| `GET http://proxycontrol/api/history/url:<url>` | Report the upstream that last served `<url>` for this client: `200` with `{"proxy": "<ip:port>"}`, or `204` on a miss. |

```sh
# With the server as your HTTP proxy:
curl -x http://127.0.0.1:8888 http://proxycontrol/api/remove/203.0.113.5:3128
curl -x http://127.0.0.1:8888 "http://proxycontrol/api/history/url:http://example.com/"
```

## See also

- [find](./find.md) — the finding/checking pass whose filters `serve` reuses.
- [top](./top.md) — a live TUI dashboard over the same pool.
- [mcp](./mcp.md) — expose the live pool to agents over MCP.
- [feature flags](../architecture/feature-flags.md) — which features gate which flags.
