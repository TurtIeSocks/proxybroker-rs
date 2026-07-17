# Architecture overview

proxybroker-rs is a from-scratch Rust port of Python's proxybroker2. The crate is organized
as a set of small modules with exactly one home for each concept, wired together by the
[`Broker`](../library/broker.md). This page is the map: what each module does, how data flows
through a `find` / `grab` / `serve` run, and the one ownership rule that shapes the whole design.

## Module map

Every `pub mod` from `src/lib.rs`. Modules marked *(feature)* only compile when their
[cargo feature](./feature-flags.md) is enabled.

| Module | Responsibility |
| --- | --- |
| `broker` | The orchestrator. `Broker::grab` scrapes providers; `Broker::find` scrapes, checks, and yields working proxies as a `ProxyStream`. Holds the builder. |
| `provider` | Where candidate proxies come from. `ProviderSpec` (data, not code) plus the bundled registry and directory loader. See [providers](./providers.md). |
| `parse` | The one home for IP:port scanning. `find_addrs_global` / `find_addrs_line` / `parse_proxy_lines`. |
| `resolver` | DNS resolution (hickory) and this host's external-IP discovery — the anonymity baseline. |
| `judge` | Judge endpoints that echo request headers and client IP; the `JudgePool`, probed eagerly. |
| `negotiator` | Per-protocol connection setup: HTTP, HTTPS, SOCKS4/5, CONNECT:80/25. Owns the `Stream` enum. |
| `checker` | The `Checker`: validate one proxy across protocols, classify anonymity, run the trust verdict. See [checking](./checking.md). |
| `proxy` | The `Proxy` value type and its geo/ASN/capability/credential companions. NDJSON read/write. |
| `types` | The canonical shared vocabulary: `Proto`, `AnonLevel`, `TypeSpec`, `Scheme`, `JudgeScheme`, `Caps`. |
| `stats` | Aggregate run statistics (`Stats`). |
| `utils` | Shared primitives: IP canonicalization, status-code parsing, request headers, markers. |
| `error` | The crate error types: `Error` (setup/run) and `ProxyError` (per-proxy failure buckets). |
| `geo` *(geo)* | `GeoDb`: country/ASN lookup over a MaxMind-format database. See [geo & ASN](./geo-asn.md). |
| `server` *(server)* | The local rotating proxy server: `serve`, `Pool`, `Strategy`, `ServerHandle`. |
| `connector` *(connector)* | A hyper-util connector routing each connection through the rotating pool. |
| `persist` *(persist)* | The `Store` trait and observer machinery for `--state`; no backend of its own. |
| `scheduler` *(server + persist)* | Background re-checker that decays scores and re-probes stored proxies. |
| `watch` *(server + watch)* | Live-reload of a `serve --load` file via a filesystem watcher. |
| `mcp` *(mcp + server)* | Exposes the live pool over MCP stdio (`proxybroker mcp`). |
| `tui` *(tui + server)* | The `proxybroker top` terminal dashboard. |

## Data flow

### `grab` — scrape only

```
providers (ProviderSpec)  ──fetch──►  page body  ──extract──►  Candidate { host, port, protocols }
```

`Broker::grab` fetches each provider concurrently and runs [`ProviderSpec::extract`](./providers.md),
which scans the whole page for `IP:port` pairs, canonicalizes the IP, and deduplicates. No proxy
is contacted — a grabbed `Candidate` is unverified.

### `find` — scrape, then check

```
grab ──► Candidate ──► Proxy (unchecked)
                          │
                          ▼
             Checker::check(&mut proxy)
                          │
   resolver → negotiator → judge/liveness → anonymity + trust
                          │
                          ▼
             ProxyStream yields working Proxy
```

`Broker::find` feeds each candidate to the [`Checker`](./checking.md). The checker connects,
negotiates the protocol, sends a test request to a judge, and classifies the result. Only proxies
that pass are yielded on the `ProxyStream`, up to the query `limit`.

### `serve` — check, then route

`serve` runs a `find` internally to fill a live `Pool`, then listens locally and forwards each
client connection through a selected upstream proxy (by [`Strategy`](../library/pool.md)). The
server feature is required.

## Ownership: Proxy is plain data, the socket lives in the checker

A [`Proxy`](../library/proxy.md) is a **plain value**: an IP, a port, its confirmed protocols and
anonymity levels, geolocation, and timing stats. It owns **no socket, no connection, no task**. It
is `Clone`, serializable to NDJSON, and cheap to pass around.

Every live resource — the `TcpStream`, the negotiated `Stream`, the judge round-trip — is created,
used, and dropped **inside** `Checker::attempt`. The checker opens `TcpStream::connect((proxy.host,
proxy.port))`, negotiates, sends the request, reads the response, and records the outcome back onto
the `&mut Proxy` (`record_attempt`, `add_type`, `record_trust`). When `attempt` returns, the socket
is gone; the `Proxy` carries only the *facts* observed.

This separation is why the design has no process-global judge state and no `asyncio.Event` to
deadlock on (see [checking](./checking.md)): the checker owns its judges and its sockets for exactly
the lifetime of a check, and the `Proxy` that survives is inert data. It is also why persistence
(the [`Store`](../library/persistence.md) trait) can save and reload a `Proxy` losslessly on identity —
there is nothing live to serialize.
