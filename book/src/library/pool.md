# Pool

`Pool` is a rotating pool of checked proxies, refilled from a `Stream<Item = Proxy>` (typically a
[`ProxyStream`](./broker.md#proxystream) from `find`) and drained one proxy at a time to serve client
requests. It lives behind the `server` feature and is re-exported from `proxybroker::server`.

The pool avoids proxybroker2's `heapq` selection (which raises `TypeError` on tied `f64`s, since
Python compares the `Proxy` objects and they define no `__lt__`). Selection here orders ties with
`f64::total_cmp`, so equal response times are deterministic, never fatal.

## Building a pool

| Constructor | Use |
| --- | --- |
| `Pool::spawn(stream, config)` | Spawn a background importer that drains `stream` into the pool. Generic over any `Stream<Item = Proxy> + Send + 'static`. |
| `Pool::from_proxies(proxies, config)` | A pool over an already-known `Vec<Proxy>` (bring-your-own / tests). No importer; considered exhausted immediately. |

Both return an `Arc<Pool>`. On import, each proxy is screened against `config.countries` — a warm or
BYO pool that never went through `find`'s country filter is still admission-checked.

```rust
use futures_util::stream;
use proxybroker::server::{Pool, PoolConfig};
use proxybroker::{Proto, Proxy};
use std::collections::BTreeSet;

# async fn f() {
let mk = |ip: &str| {
    let mut p = Proxy::new(ip.parse().unwrap(), 8080, BTreeSet::from([Proto::Http]));
    p.add_type(Proto::Http, None); // confirmed-working for HTTP
    p
};
let source = stream::iter(vec![mk("203.0.113.1"), mk("203.0.113.2")]);

let pool = Pool::spawn(source, PoolConfig::default());
pool.wait_ready(1).await;                 // block until warm (or source exhausted)
println!("pool warmed: {} proxies", pool.len());
# }
```

`wait_ready(n)` blocks until at least `n` proxies are pooled or the source is exhausted — so a
too-small source can never hang startup forever. `wait_ready(0)` returns immediately.

## PoolConfig

`PoolConfig` tunes eviction and selection. It implements `Default`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_tries` | `usize` | `3` | Attempts (with different proxies) per client request. |
| `max_error_rate` | `f64` | `0.5` | Evict a proxy once its error rate exceeds this (after `min_req`). |
| `max_resp_time` | `f64` | `8.0` | Evict once average response time (seconds) exceeds this. |
| `min_req` | `u32` | `5` | Grace: no eviction until this many requests handled. |
| `countries` | `Option<BTreeSet<String>>` | `None` | Admission allow-list of **uppercased** ISO codes. `None` = any. |
| `strategy` | `Strategy` | `Best` | How to pick an upstream per request. |
| `sticky_header` | `Option<String>` | `None` | For `Sticky`, key sessions on this header instead of client IP (HTTP only). |
| `max_sessions` | `usize` | `10_000` | Upper bound on the sticky-session map. |
| `fail_timeout` | `Duration` | `30s` | How long a failed proxy is benched before re-probe. |
| `prefer_connect` | `bool` | `false` | Bias selection toward `CONNECT:80`-capable proxies. |
| `http_allowed_codes` | `Option<Vec<u16>>` | `None` | For HTTP, retry through another proxy when the upstream status is outside this set. |

## Strategy

`Strategy` chooses which eligible upstream serves each request:

| Variant | Selection |
| --- | --- |
| `Best` (default) | Lowest `(error_rate, avg_resp_time)`. |
| `RoundRobin` | Rotate through scheme-eligible proxies in pool order. |
| `Random` | Uniform pick among scheme-eligible proxies. |
| `Sticky` | Pin a client to one upstream while it stays in the pool; fall back to `Best` for a new client or when the pin is gone. |

Selection is two-tiered: **ready** proxies (never benched, or the bench window elapsed) are ranked
first; only if none are ready does the pool fall back to benched ones (better than a 502).

## ClientKey

`Strategy::Sticky` keys each session on a `ClientKey`:

```rust
pub enum ClientKey {
    Ip(IpAddr),      // the client's peer IP (the default)
    Header(String),  // the value of --sticky-header, HTTP requests only
}
```

## Checking proxies in and out

| Method | Purpose |
| --- | --- |
| `get(scheme, key) -> Option<Proxy>` | Async: check out a proxy for `scheme` via the strategy, waiting for the importer if momentarily empty. `None` once exhausted with nothing suitable. |
| `try_get(scheme, country) -> Option<Proxy>` | Non-blocking best-by-priority checkout, optional country filter. |
| `put_ok(proxy)` | Return a proxy that served successfully — ready for immediate reselection. |
| `put_failed(proxy)` | Return a failed proxy — benched for `fail_timeout`, then dropped outright if persistently unhealthy. |

## Mutating a live pool

| Method | Purpose |
| --- | --- |
| `add(proxy)` | Add a checked proxy, deduped on `(host, port)` (no-op if present). |
| `remove(host, port) -> bool` | Drop every proxy at that address; returns whether any were removed. |
| `remove_addr(host, port) -> bool` | Alias of `remove` under the re-check/watch vocabulary. |
| `addrs() -> BTreeSet<(IpAddr, u16)>` | Snapshot the current `(host, port)` set. |
| `proxies() -> Vec<Proxy>` | Non-consuming clone of every pooled proxy. |
| `len()` / `is_empty()` | Current pool size. |

`remove` is exactly what `GET http://proxycontrol/api/remove/<ip:port>` does to a running server:

```rust
# use proxybroker::server::{Pool, PoolConfig};
# use proxybroker::{Proto, Proxy};
# use std::collections::BTreeSet;
# fn f(pool: std::sync::Arc<Pool>) -> Result<(), Box<dyn std::error::Error>> {
let removed = pool.remove("203.0.113.2".parse()?, 8080);
println!("removed → {removed}; pool now has {}", pool.len());
# Ok(()) }
```

## PoolSnapshot

`pool.snapshot()` returns a cheap `PoolSnapshot` — a live view taken under a single lock:

```rust
pub struct PoolSnapshot {
    pub http: usize,          // proxies serving Scheme::Http
    pub https: usize,         // proxies serving Scheme::Https
    pub total: usize,
    pub avg_error_rate: f64,  // mean over the pool
    pub avg_resp_time: f64,   // mean over the pool, seconds
}
```

For a richer aggregate (counts by protocol/anonymity/country, latency percentiles), feed
`pool.proxies()` into [`Stats::from_proxies`](../operations/observability.md):

```rust
# use proxybroker::{server::Pool, Stats};
# fn f(pool: &Pool) {
let stats = Stats::from_proxies(&pool.proxies());
let _ = stats.total;
# }
```

The pool also exposes cumulative counters `evictions()` and `rotations()` (both `u64`).

## serve() and ServerHandle

`serve` starts the local rotating proxy server on `addr`, relaying every client connection through
the pool and retrying on a different proxy when one fails.

```rust
pub async fn serve(
    addr: SocketAddr,
    pool: Arc<Pool>,
    resolver: Arc<Resolver>,
    timeout: Duration,
    min_queue: usize,        // wait for this many pooled proxies before serving (B13)
    backlog: u32,            // TCP listen backlog
    auth: Option<String>,    // "user:pass" gate; Some → 407 without matching credentials
) -> std::io::Result<ServerHandle>
```

It binds immediately (so `local_addr()` works at once) and runs the accept loop in a background task.
`ServerHandle` controls its lifetime:

```rust
# use proxybroker::server::ServerHandle;
# fn f(handle: ServerHandle) {
let addr = handle.local_addr(); // useful when bound to port 0
handle.shutdown();              // stop accepting and shut down
# let _ = addr; }
```

Dropping the handle also shuts the server down.

## See also

- [Broker](./broker.md) — `find` produces the `ProxyStream` that fills a pool.
- [Proxy](./proxy.md) — the value type the pool holds.
- [feature flags](../architecture/feature-flags.md) — `server`, `metrics`, and friends.
