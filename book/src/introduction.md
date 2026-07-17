# Introduction

`proxybroker` is a Rust library and command-line tool that **finds, checks, and serves**
public HTTP(S) and SOCKS4/5 proxies. It scrapes proxy lists from many providers, verifies
that each proxy actually works, classifies its anonymity level, and can run a local rotating
proxy server in front of the working set.

It is a from-scratch Rust/[tokio](https://tokio.rs) rewrite of
[proxybroker2](https://github.com/bluet/proxybroker2) (Python/asyncio), which is itself the
maintained successor to the original [ProxyBroker](https://github.com/constverum/ProxyBroker).
All three are Apache-2.0; this crate is a derivative work carrying the same licence. See the
[Data & Licensing](./data-and-licensing.md) chapter for attribution and a statement of changes.

## Why it exists

There was no Rust equivalent that shipped a real **library** API. The project is
library-first: everything the CLI does is available as public types, and the `proxybroker`
binary is a thin shell over the library. You can embed the broker, the checker, the pool, and
a drop-in rotating connector directly in your own async Rust program.

## Highlights

| Capability | What it does |
|---|---|
| Library-first API | [`Broker`](./library/broker.md), [`Proxy`](./library/proxy.md), pool, and connector are all public. |
| Anonymity classification | `find` labels each HTTP proxy Transparent / Anonymous / High Anonymous. |
| Many protocols | HTTP, HTTPS, SOCKS4, SOCKS5, plus `CONNECT:<port>` tunnel checks. |
| Rotating proxy server | `serve` runs a local pool with pluggable selection strategies. |
| Static single binary | Fully static musl build; ships in a `FROM scratch` Docker image. |
| Bundled geo data | Country lookup via the CC BY 4.0 DB-IP database (optional feature). |
| Machine-readable output | JSON / NDJSON / JSON-array / CSV / URL / template formats. |

## The three core verbs

Everything centres on three commands (mirrored by [`Broker`](./library/broker.md) methods):

| Verb | Command | What it does |
|---|---|---|
| **grab** | [`proxybroker grab`](./cli/grab.md) | Scrape providers and emit proxies **without** checking them — fast, but unverified. |
| **find** | [`proxybroker find`](./cli/find.md) | Scrape, check that each proxy works, and classify its anonymity. |
| **serve** | [`proxybroker serve`](./cli/serve.md) | Run a local proxy server that rotates through working proxies. |

A minimal `find`:

```sh
proxybroker find --types HTTP HTTPS --limit 10
```

The same thing as a library:

```rust
use proxybroker::{Broker, FindQuery, Proto, TypeSpec};
use futures_util::StreamExt;

let broker = Broker::builder().build();
let mut stream = broker.find(
    FindQuery::builder()
        .types(vec![TypeSpec::any(Proto::Http)])
        .limit(10)
        .build(),
).await?;
while let Some(proxy) = stream.next().await {
    println!("{}", proxy.addr());
}
```

Alongside the three verbs, the CLI also offers [`check`](./cli/check.md) (verify a list of
proxies you already have) and, behind build features, [`top`](./cli/top.md) (a live terminal
dashboard) and [`mcp`](./cli/mcp.md) (serve the live pool over MCP stdio).

## How this book is organized

- **[Getting Started](./getting-started/installation.md)** — [install](./getting-started/installation.md)
  the CLI or library, then run the [quick-start](./getting-started/quick-start.md) commands.
- **[CLI Reference](./cli/overview.md)** — every subcommand and flag: [overview & global
  options](./cli/overview.md), [grab](./cli/grab.md), [find](./cli/find.md),
  [check](./cli/check.md), [serve](./cli/serve.md), [top](./cli/top.md), [mcp](./cli/mcp.md),
  and [output formats](./cli/output-formats.md).
- **[Library Guide](./library/broker.md)** — using the crate from Rust: the
  [broker & queries](./library/broker.md), the [`Proxy` type](./library/proxy.md), the
  [pool & selection](./library/pool.md), [persistence](./library/persistence.md), the
  [rotating connector](./library/connector.md), and worked [examples](./library/examples.md).
- **[Architecture](./architecture/overview.md)** — how it works inside: the
  [module map](./architecture/overview.md), [providers & scraping](./architecture/providers.md),
  the [checking pipeline](./architecture/checking.md), [geolocation & ASN](./architecture/geo-asn.md),
  and the [feature flags](./architecture/feature-flags.md).
- **[Reference](./data-and-licensing.md)** — [data & licensing](./data-and-licensing.md) and
  [observability](./operations/observability.md).
- **[Project](./project/roadmap.md)** — [roadmap](./project/roadmap.md),
  [deferred backlog](./project/deferred-backlog.md), the
  [systematic refactor](./project/systematic-refactor.md) design record, and
  [contributing](./project/contributing.md).
