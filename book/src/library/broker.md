# Broker

The [`Broker`](https://docs.rs/proxybroker) is the entry point to the library: it turns a set of
[providers](../architecture/providers.md) into a stream of proxies. Two operations sit on top of it:

| Method | What it does | Returns |
| --- | --- | --- |
| `grab(GrabQuery)` | Scrape providers, **without** checking. Dedup on `(host, port)`, optional country filter, cap at `limit`. | `ProxyStream` |
| `find(FindQuery)` | Scrape **and check** — probe judges, classify anonymity, keep only working proxies. | `Result<ProxyStream, Error>` |
| `check(stream, FindQuery)` | Check proxies you already have (e.g. from a file) instead of scraping. | `Result<ProxyStream, Error>` |

`grab` returns immediately (the work runs in a spawned task). `find` and `check` do their fail-fast
setup up front — discovering the host's external IPs and verifying at least one judge — so
[`Error::NoTypes`], [`Error::ExtIpUnknown`], and [`Error::NoJudges`] surface from the `await`, not
as a silently-empty stream.

## The crypto provider note

This crate builds `reqwest` with `rustls-no-provider` (to keep `aws-lc-rs` out of the dependency
graph, which is the musl cross-compile blocker). The trade-off: reqwest bakes in **no** crypto
provider, so a bare `reqwest::Client::new()` **panics** until one is installed.

[`BrokerBuilder::build`](#brokerbuilder) and `Resolver::new` call
`install_default_crypto_provider()` for you, so the normal paths need nothing. You only call it
yourself when you build a **custom** `reqwest::Client` to pass to [`BrokerBuilder::client`](#brokerbuilder):

```rust
use proxybroker::install_default_crypto_provider;

install_default_crypto_provider(); // idempotent; call before building your own reqwest::Client
let client = reqwest::Client::builder()
    .timeout(std::time::Duration::from_secs(10))
    .build()?;
let broker = proxybroker::Broker::builder().client(client).build();
# Ok::<(), Box<dyn std::error::Error>>(())
```

`install_default_crypto_provider()` is idempotent (a `std::sync::Once`), so calling it more than
once is harmless.

## BrokerBuilder

`Broker::builder()` returns a `BrokerBuilder`. Every setter is consuming (returns `Self`); unset
fields fall back to defaults. `build()` is infallible.

| Setter | Purpose |
| --- | --- |
| `providers(Vec<ProviderSpec>)` | Use a specific provider list instead of the bundled registry. |
| `client(reqwest::Client)` | Supply your own HTTP client (timeouts, proxy, TLS). |
| `resolver(Resolver)` | Supply a resolver — mainly for tests that stub external-IP discovery and DNS to run offline. |
| `geo(GeoDb)` | Attach a geo database for country lookup/filtering, overriding the bundled default. Requires the `geo` feature. |
| `without_geo()` | Attach **no** geo database. Country filtering then rejects every proxy, and proxies carry no `geo`. Skips loading the bundled DB. Requires `geo`. |
| `asn_db(GeoDb)` | Attach a **separate** ASN database (`--asn-db`) so checked proxies carry `proxy.asn`. No bundled default. Requires `geo`. |

With default features (`geo-bundled`), `build()` auto-attaches the bundled DB-IP Country-Lite
database unless you supplied one or called `without_geo()`. See [feature flags](../architecture/feature-flags.md)
for what each feature pulls in.

```rust
use proxybroker::Broker;

let broker = Broker::builder().build(); // bundled providers + bundled geo
```

## FindQuery

`FindQuery` describes what to find and check. `types` is required — an empty `types` makes `find`
return [`Error::NoTypes`]. Every other field has a default. You can construct it as a struct literal
(with `..Default::default()`) or through [`FindQueryBuilder`](#findquerybuilder).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `types` | `Vec<TypeSpec>` | `[]` (required) | Protocols (and optional anonymity levels) a proxy must support. |
| `countries` | `Option<Vec<String>>` | `None` | Keep only proxies in these ISO country codes. |
| `limit` | `Option<usize>` | `None` (unlimited) | Stop after this many working proxies. |
| `judges` | `Vec<String>` | `[]` (bundled defaults) | Judge URLs to probe. |
| `dnsbl` | `Vec<String>` | `[]` | DNS blocklist zones; a listed IP is rejected. |
| `timeout` | `Duration` | `8s` | Per-request timeout. |
| `max_conn` | `usize` | `200` | Max concurrent checks in flight. |
| `retry` | `RetryPolicy` | default | Attempts per protocol + backoff schedule. |
| `post` | `bool` | `false` | Use `POST` for the test request. |
| `strict` | `bool` | `false` | Require the anonymity level to match exactly. |
| `liveness_url` | `Option<String>` | `None` | Fallback liveness URL when no judge verifies. |
| `relaxed_validity` | `bool` | `false` | Relax validity to marker+IP, recording Referer/Cookie as capabilities. |
| `require_cookie` | `bool` | `false` | Keep only proxies that forwarded our Cookie header. |
| `require_referer` | `bool` | `false` | Keep only proxies that forwarded our Referer header. |
| `require_connect25` | `bool` | `false` | Keep only proxies with a confirmed CONNECT:25 (SMTP) tunnel. |
| `trust_check` | `bool` | `false` | Run honeypot detection and record the verdict. |
| `require_trusted` | `bool` | `false` | Keep only proxies with a clean trust verdict (implies `trust_check`). |

### FindQueryBuilder

`FindQuery::builder()` returns a `FindQueryBuilder`. `build()` is infallible — the `NoTypes` guard
lives in `find`/`check`, not the builder, so the builder stays composable. The builder covers the
common fields; the A4/A6 capability flags (`require_cookie`, `require_connect25`, `trust_check`, …)
have **no** builder setter — set those public fields directly on the struct.

| Setter | Notes |
| --- | --- |
| `types(Vec<TypeSpec>)` | Required by `find`/`check`. |
| `countries(Vec<String>)` | |
| `limit(usize)` | `0` maps to unlimited (the CLI's `--limit 0` convention lives here). |
| `judges(Vec<String>)` | Empty defers to bundled defaults. |
| `dnsbl(Vec<String>)` | |
| `timeout(Duration)` | |
| `max_conn(usize)` | |
| `max_tries(usize)` | Overrides just the attempt count on the retry policy. |
| `retry(RetryPolicy)` | The full policy; `max_tries`, if also set, overrides its count. |
| `post(bool)` | |
| `strict(bool)` | |
| `liveness_url(Option<String>)` | |

## GrabQuery

`GrabQuery` is much smaller — grabbing does not check, so there are no judge/timeout/retry knobs.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `countries` | `Option<Vec<String>>` | `None` | Keep only proxies in these ISO country codes. |
| `limit` | `Option<usize>` | `None` (unlimited) | Stop after this many proxies. |

`GrabQuery` derives `Default`, so `GrabQuery::default()` grabs everything the providers return.

## ProxyStream

Both `grab` and `find` return a `ProxyStream`, which implements
[`futures_util::Stream`](https://docs.rs/futures-util)`<Item = Proxy>`. Consume it like any stream:

```rust
use futures_util::StreamExt;
# async fn f(stream: &mut proxybroker::ProxyStream) {
while let Some(proxy) = stream.next().await {
    println!("{}", proxy.addr());
}
# }
```

The stream ends when the source is exhausted, the limit is reached, or the stream is **dropped** —
dropping fires a cancellation token that aborts in-flight checks, so there is no detached-task leak.

For `find` (not `grab`), the stream also carries running statistics over **every** checked proxy,
working or not. Read them after the stream is fully drained for a complete picture:

```rust
# fn f(stream: &proxybroker::ProxyStream) {
if let Some(stats) = stream.stats() {
    // aggregate over all checked proxies — see the Stats type
    let _ = stats;
}
# }
```

`stats()` returns `Some` only for `find`; it is `None` for `grab` (nothing is checked).

## A real `find` example

This mirrors `examples/find.rs`. It finds up to ten working HTTP or HTTPS proxies and prints each as
it is confirmed.

```rust
use futures_util::StreamExt;
use proxybroker::{Broker, FindQuery, Proto, TypeSpec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::builder().build();

    let mut proxies = broker
        .find(FindQuery {
            types: vec![TypeSpec::any(Proto::Http), TypeSpec::any(Proto::Https)],
            limit: Some(10),
            ..Default::default()
        })
        .await?;

    while let Some(proxy) = proxies.next().await {
        // schemes() is HTTP/HTTPS support; types() has the per-protocol anonymity level.
        println!(
            "Found proxy: {:<21} {:?}  {:.2}s",
            proxy.addr(),
            proxy.schemes(),
            proxy.avg_resp_time(),
        );
    }
    Ok(())
}
```

The same query via the builder, which normalizes `limit(0)` to unlimited:

```rust
use proxybroker::{FindQuery, Proto, TypeSpec};

let query = FindQuery::builder()
    .types(vec![TypeSpec::any(Proto::Http), TypeSpec::any(Proto::Https)])
    .limit(10)
    .build();
# let _ = query;
```

## See also

- [Proxy](./proxy.md) — the value type each `ProxyStream` yields.
- [Pool](./pool.md) — feed a `ProxyStream` into a rotating proxy pool.
- [feature flags](../architecture/feature-flags.md) — `geo`, `server`, and friends.
