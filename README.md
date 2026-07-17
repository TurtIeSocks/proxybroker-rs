# proxybroker

[![crates.io](https://img.shields.io/crates/v/proxybroker.svg)](https://crates.io/crates/proxybroker)
[![docs.rs](https://img.shields.io/docsrs/proxybroker)](https://docs.rs/proxybroker)
[![downloads](https://img.shields.io/crates/d/proxybroker.svg)](https://crates.io/crates/proxybroker)
[![license](https://img.shields.io/crates/l/proxybroker.svg)](LICENSE)

Find, check, and serve public HTTP(S) and SOCKS4/5 proxies. A Rust library and CLI.

A rewrite of [proxybroker2](https://github.com/bluet/proxybroker2) (Python/asyncio) in
Rust/tokio, which is itself the maintained successor to
[ProxyBroker](https://github.com/constverum/ProxyBroker). Both are Apache-2.0; this is a
derivative work and carries the same licence. See [NOTICE](NOTICE) for attribution and a
statement of changes.

## Install

```sh
cargo add proxybroker          # library
cargo install proxybroker      # CLI

# prebuilt binary (Linux musl / macOS), no toolchain — verifies a checksum, installs to ~/.local/bin:
curl -fsSL https://raw.githubusercontent.com/TurtIeSocks/proxybroker-rs/main/install.sh | sh
```

Docker — a `FROM scratch` image (just the static binary; all geo/provider data is embedded).
Pull the published image from the GitHub Container Registry (tagged per release, plus `latest`):

```sh
docker run --rm ghcr.io/turtiesocks/proxybroker-rs:latest find --types HTTP --limit 5
```

Or build it yourself from the repo:

```sh
docker build -t proxybroker .
docker run --rm proxybroker find --types HTTP --limit 5
```

The binary is a fully static `x86_64`/`aarch64` musl build (TLS is ring-only rustls — no
aws-lc-rs), so it has no runtime libc or data-file dependencies.

All three commands — `grab`, `find`, `serve` — work end-to-end. See
`docs/systematic-refactor/` for the port's design record.

## Usage

```sh
proxybroker grab --limit 10                      # scrape providers, no checking
proxybroker find --types HTTP HTTPS --limit 10   # scrape + check + classify anonymity
proxybroker find --types SOCKS5 --format json    # machine-readable output
proxybroker find --types HTTP --show-stats       # + an aggregate summary on stderr
proxybroker find --types HTTP --dnsbl zen.spamhaus.org   # reject blocklisted IPs
proxybroker serve --types HTTP --host 127.0.0.1:8888     # local rotating proxy server

# bring your own providers (YAML/JSON configs, one provider per file):
proxybroker --provider-dir ./my-providers find --types HTTP
proxybroker --provider-dir ./my-providers --providers-only grab   # ignore the bundled set
```

As a library:

```rust
use proxybroker::{Broker, FindQuery, Proto, TypeSpec};
use futures_util::StreamExt;

let broker = Broker::builder().build();
let mut stream = broker.find(FindQuery {
    types: vec![TypeSpec::any(Proto::Http)],
    limit: Some(10),
    ..Default::default()
}).await?;
while let Some(proxy) = stream.next().await {
    println!("{}", proxy.addr());
}
```

## Why this exists

There was no Rust equivalent with a library API. Checked before starting (2026-07-15):

| crate | latest | published | ships a lib? | scope |
|---|---|---|---|---|
| [`proxy-rs`](https://crates.io/crates/proxy-rs) | 0.3.7 | 2023-10-24 | **no** | closest analogue — scraper + checker + serve, but binary-only and unmaintained |
| [`proxy-scraper-checker`](https://crates.io/crates/proxy-scraper-checker) | 0.1.3 | 2024-06-14 | no | one source only |
| [`open_proxies`](https://crates.io/crates/open_proxies) | 0.1.1 | 2022-11-15 | yes | checker only |
| [`proxy-scraper`](https://crates.io/crates/proxy-scraper) | 0.2.0 | 2024-05-03 | yes | different domain (MTProxy/Shadowsocks link parsing) |

`proxy-rs` is the real precedent, and it publishes no library target on any version. This
crate is library-first, with the CLI as a thin shell over it.

## Features

| feature | default | what it does |
|---|---|---|
| `cli` | yes | the `proxybroker` binary (clap, logging, output formats) |
| `server` | yes | the local rotating proxy server |
| `geo` | yes | country lookup code |
| `geo-bundled` | yes | bundles the DB-IP database (~3.9 MB). Turn off to supply your own. |

`--no-default-features` gives you the library with no geo data, no server, and no CLI
dependencies.

## Geolocation data

When built with `geo-bundled` (on by default), this crate includes the DB-IP Country Lite
database:

> IP Geolocation by DB-IP (https://db-ip.com)

licensed [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). See
[LICENSE-DATA](LICENSE-DATA).

**Why not MaxMind GeoLite2**, which the Python version bundles? GeoLite2's EULA requires
licensees to destroy superseded copies within 30 days of a new release. A published
crates.io version is immutable — it cannot be destroyed — so bundling GeoLite2 in a
published crate cannot be made compliant by attribution or feature flags. (The Python
project's bundled copy was built 2017-09-06 and is 8.9 years stale, and its `update-geo`
command has been broken since MaxMind retired the anonymous download endpoint in 2019.)
DB-IP Lite is CC BY 4.0, has no update-or-destroy clause, and no ShareAlike obligation.

You can always bring your own database — including your own lawfully-licensed GeoLite2:

```sh
proxybroker --geo-db /path/to/GeoLite2-Country.mmdb find --types HTTP
```

### ASN attribution

To tag each proxy with the Autonomous System that owns its IP (its network operator), pass a
separate ASN database with `--asn-db`. This is opt-in and unbundled — no ASN data ships with the
crate — for the same licensing reason as the City data above:

```sh
proxybroker --asn-db /path/to/GeoLite2-ASN.mmdb find --types HTTP --format json
```

Each proxy then carries an `asn` object (`{ "number": 15169, "org": "Google LLC" }`, or `null`
when no `--asn-db` resolved it) in `--format json`, and the `{{asn}}` / `{{asn_org}}` tokens work
in `--output-format` templates. `--geo-db` and `--asn-db` are independent and can be combined.

Full analysis with primary sources: [`docs/systematic-refactor/research.md`](docs/systematic-refactor/research.md).

## Licence

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
Bundled geo data is CC BY 4.0 — see [LICENSE-DATA](LICENSE-DATA). The code licence does
not cover the data, and the data licence does not cover the code.
