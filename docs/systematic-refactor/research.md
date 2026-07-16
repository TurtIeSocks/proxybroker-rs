# proxybroker2 → Rust: consolidated research

Four research passes plus three adversarial verification passes. Where a verifier
refuted a claim, the verifier's finding is authoritative and the original claim has
been corrected below. See **Corrections applied** for the full list of what the first
pass got wrong — that section tells you which claims were checked adversarially and
which were not.

Status of adversarial coverage:
- GeoLite2 licensing — verified (conclusion held; case understated, now strengthened)
- HTTP stack — verified (2 defects, 1 compile-breaking; thesis held)
- DNS + GeoIP — verified (3 defects, 1 compile-breaking; thesis held)
- **Architecture — NOT adversarially verified.** Treat §6 as reasoned judgment, not
  fact-checked ground truth.

---

## Verdict summary

1. **HTTP stack** — split it: `reqwest` 0.13.4 for provider scraping; raw `tokio` + `hyper` 1.10.1 (`hyper-util` 0.1.20 `TokioIo`) for the checker, because reqwest has no per-request proxy, an opaque error type, and no access to the negotiated stream.
2. **SOCKS** — `tokio-socks` 0.5.3 for the checker (`connect_with_socket` + `into_inner()` compose straight into hyper; 23 reply-code-mapped error variants); consider `fast-socks5` 1.0.0 only if the local server must *speak* SOCKS5.
3. **DNS** — `hickory-resolver` 0.26.1, builder-only API (`Resolver::builder_tokio()` / `builder_with_config` + `TokioRuntimeProvider`); use its built-in `ResponseCache` and delete the hand-rolled Python DNS cache; add `moka` only for a layer hickory doesn't provide.
4. **GeoIP** — `maxminddb` 0.29.0 reader (two-stage `lookup().decode::<T>()`), **do not vendor GeoLite2**; vendor DB-IP Country Lite (CC BY 4.0, 3.86 MB gzipped) behind a default-on feature, always with a user-supplied-path override.
5. **Cargo layout** — single crate, lib+bin, `default = ["cli"]` + `[[bin]] required-features = ["cli"]` (tokei/bat pattern); one crate name serves `cargo add` and `cargo install`.
6. **Streaming API** — `Broker::find(q) -> Result<ProxyStream, Error>` where `ProxyStream: Stream<Item = Proxy>` — named concrete type, bounded mpsc inside for backpressure, abort-on-drop; per-proxy failures are counted, never yielded as `Err`.

---

## GeoLite2 licensing

**Not legal advice.** Documented research with primary sources; get counsel before
betting the crate on it.

### The finding

The controlling document is the **GeoLite End User License Agreement**, updated
**February 12, 2026** — https://www.maxmind.com/en/geolite/eula (date confirmed
independently by the verifier at both `/en/geolite/eula` and `/en/geolite2/eula`).

This is *not* the pre-2019 standalone CC BY-SA 4.0 license. MaxMind replaced that with
the EULA effective 2019-12-30
(https://blog.maxmind.com/2019/12/significant-changes-to-accessing-and-using-geolite2-databases/).
CC BY-SA 4.0 now survives only as a document *incorporated by reference inside* the
EULA (§1/§3) — a materially different and weaker thing.

**Binding trigger** (preamble):
> "By downloading or using our GeoLite Database, you are accepting and agreeing to the terms and conditions set forth in this GeoLite End User License Agreement"

**§1 — precedence clause (the decisive text):**
> "This Agreement controls in the event of any conflict with the above-referenced documents"

…with the Creative Commons License ranked **last** in the document hierarchy. This is
the single strongest fact in the analysis. The "CC carve-out" theory reads §6's
"except as explicitly permitted by the Creative Commons License" as CC trumping the
EULA; §1 says the opposite in terms.

**§3 — limited grant of rights:**
> "Subject to the terms and conditions of this Agreement, to the extent the Services contain any copyrightable elements those copyrightable elements are governed by the Creative Commons License. You must provide attribution of your use to MaxMind (an example of attribution: 'This product includes GeoLite Data created by MaxMind, available from https://www.maxmind.com.'"
>
> "In addition and if you are using the Services for internal use ... MaxMind also hereby grants you a non-exclusive, non-transferable limited license to access and use the Services for your own internal business purposes."

The hedge "to the extent the Services contain any copyrightable elements" is
load-bearing: under *Feist*, facts and thin compilations aren't copyrightable, so the
CC grant may cover very little. MaxMind's real leverage is **contract**, not copyright,
and a CC license cannot release you from contract terms you accepted.

**§6.1 — disclosure:**
> "Except as explicitly permitted by the Creative Commons License, you will not disclose the Services to any third party without notifying MaxMind of the anticipated disclosure and obtaining MaxMind's prior written consent to the disclosure. To the extent you disclose the Services to a third party as permitted by this Agreement, you will impose upon the third party the same or substantially similar contractual duties imposed on you ... You are responsible for the acts or omissions of any third parties with which you share the Services."

**§6.3 — destruction (the clause that decides the question):**
> "From time to time, MaxMind will release an updated version of the GeoLite Databases, and you agree to promptly use the updated version. You shall cease use of and destroy (i) any old versions of the Services within thirty (30) days following the release of the updated GeoLite Databases..."

MaxMind restates this in plain English on its dev portal — *"you must delete GeoLite
databases within 30 days of a new release"*
(https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/). Cite the portal, not
the §6.3 inference.

Separately (dev portal, **not** an EULA clause): GeoLite users are limited to
**30 database downloads per day**, and an account + license key is required to
download. The EULA itself only says MaxMind "may limit the number of queries" (§6(4)).

**MaxMind's stated position** (https://support.maxmind.com/hc/en-us/articles/4408928143643-Commercial-Redistribution-License-for-GeoLite2):
> "If you would like to include data from MaxMind's GeoLite databases in a product or service you provide to your users or customers, you will need a Commercial Redistribution License for GeoLite."

### The structural argument (independent of the CC debate)

**A published crates.io/PyPI artifact is immutable and permanent by design. You cannot
destroy the mmdb embedded in version 0.3.1 thirty days later — crates.io does not allow
deleting published versions.** The §6.3 destruction duty is *not* carved out by the CC
exception (that carve-out is scoped to the §6.1 disclosure restriction only). This
conflict is unfixable by attribution, feature flags, or CC lawyering.

Corroborating datapoint: `node-geolite2-redist`
(https://github.com/GitSquared/node-geolite2-redist), the most aggressive CC-carve-out
project in the wild, **does not vendor the mmdb in the npm package** — it downloads at
runtime from a GitHub mirror and auto-updates, specifically to try to satisfy §6.3.

**Answer to the precise question: attribution alone does NOT suffice.** Attribution
satisfies §3. It does nothing for §6.1's duty-passthrough or §6.3's destruction
obligation.

**Unverified / open:** No court test of the CC-carve-out theory exists, and no evidence
MaxMind has ever enforced against node-geolite2-redist. Absence of enforcement is not
permission.

### proxybroker2's own compliance: not compliant, on multiple independent grounds

Measured from the bundled file (`proxybroker/data/GeoLite2-Country.mmdb`):
`build date 2017-09-06`, ~8.9 years stale, 3,108,778 bytes.

| Ground | Status |
|---|---|
| §3 attribution | Compliant — `README.md:617` has the exact MaxMind attribution string |
| §6.3 destruction | **Breach.** DB is 8.9 years stale; MaxMind has shipped hundreds of updates since (UNVERIFIED: the specific "~450 updates" figure is a computed estimate — 8.9y × 52 — not a MaxMind statement. 8.9 years vs 30 days needs no precision.) |
| §6.1 duty-passthrough | **Breach.** pip users receive the mmdb with no EULA passthrough |
| License declaration | **Breach.** `pyproject.toml:10` declares `license = "Apache-2.0"` over a package containing MaxMind data — Apache-2.0 permits sublicensing and attribution-free redistribution, rights proxybroker2 does not have and cannot grant |
| MaxMind KB position | **Breach.** Ships data in "a product you provide to your users" with no Commercial Redistribution License |

Aggravating: `proxybroker/utils.py:195` and `cli.py:130` show `update-geo` is
permanently broken — MaxMind retired the unauthenticated endpoint on 2019-12-30. The
bundled DB can never be refreshed by design. **Do not use proxybroker2 as the
compliance model; it is the cautionary tale.**

### What the Rust ecosystem does (crates.io API)

| Crate | Downloads | Crate size | Vendors a DB? |
|---|---|---|---|
| `maxminddb` | 26,230,736 | 0.05 MB | No — reader only |
| `geoip2` | 61,692 | 0.01 MB | No — reader only |
| `mmdb-grpc` | 16,340 | 0.02 MB | No |
| `tor-geoip-db` | 41,171 | 1.43 MB | **Yes — but not MaxMind** |

No significant Rust crate vendors a GeoLite2 mmdb. The exception proves the rule:
`tor-geoip-db` (Tor Project's arti) vendors geo data, and its `doc/export_info_v4.md`
(read from the downloaded `.crate`) says:
> "This file has been converted from the IPFire Location database ... Vendor: IPFire Project — License: CC BY-SA 4.0"

The most sophisticated Rust project vendoring geo data deliberately chose a
non-MaxMind, cleanly-CC-licensed source. That is the precedent.

**crates.io size limit:** the Cargo Book states *"crates.io currently has a 10MB size
limit on the `.crate` file"*
(https://doc.rust-lang.org/cargo/reference/publishing.html). Applies to the gzipped
`.crate`; raisable on request (https://github.com/rust-lang/crates.io/issues/195).

### Alternatives (primary sources fetched, files measured)

| Source | License | Redistributable in a crate? | Notes |
|---|---|---|---|
| **DB-IP Lite** | **CC BY 4.0** | **Yes** | Attribution + link back. **No ShareAlike.** No signup. MMDB, monthly. |
| IPinfo Lite | CC BY-SA 4.0 | Likely | UNVERIFIED: search-verified only, primary license doc not fetched. |
| IPLocate | CC BY-SA 4.0 | With difficulty | `ip-to-country.mmdb` is 16.1 MB — over the default crates.io limit |
| IPFire Location | CC BY-SA 4.0 | Yes (Tor's choice) | Proprietary format, needs conversion |
| IP2Location LITE | Custom | **No** | FAQ: redistribution with your app allowed, but *"Third party database repository is not allowed."* A crates.io publish is exactly that. Disqualified. |
| MaxMind GeoLite2 | EULA + CC BY-SA | **No** | Per above |

DB-IP Country Lite, downloaded and tested:
```
raw:            7.80 MB
gzip -9:        3.86 MB   ← fits crates.io's 10MB limit with headroom
database_type:  DBIP-Country-Lite
build date:     2026-07-01
8.8.8.8 -> country.iso_code = 'US'
1.1.1.1 -> country.iso_code = 'AU'
```
Schema-compatible with GeoLite2-Country (same `country.iso_code` / `continent.code`),
reads with the standard `maxminddb` crate.

### Recommendation

**Vendor DB-IP Country Lite under CC BY 4.0, behind a default-on cargo feature, with a
user-supplied-path override. Do not vendor GeoLite2.**

1. Default feature `geo-bundled` embeds `dbip-country-lite.mmdb` (3.86 MB gzipped — the
   `.crate` stays under 10 MB, no exception request needed).
2. Always support a user-supplied path (`--geo-db /path/to/x.mmdb`,
   `Resolver::with_geo_db()`). This is the ecosystem norm and lets users bring their
   **own** GeoLite2 — legal for them (they accepted the EULA and used their own key),
   zero redistribution risk for you.
3. `--no-default-features` → zero geo data, zero attribution obligation.
4. Attribution (CC BY 4.0 requires it): `IP Geolocation by DB-IP (https://db-ip.com)`
   in README, `--help`/`--version`, and a `NOTICE`/`LICENSE-DATA` file.
5. Declare licenses honestly — `license = "Apache-2.0"` for code plus explicit
   `LICENSE-DATA` (CC BY 4.0) covering `data/`. Never let a blanket Apache-2.0 imply
   you can sublicense the data. This is precisely proxybroker2's mistake.
6. Refresh the mmdb each release — hygiene, not a legal duty.

**Why DB-IP over the CC BY-SA options:**
- **No ShareAlike.** If you ever trim, re-index, or convert a BY-SA database, you've
  arguably made Adapted Material you must relicense CC BY-SA. CC BY 4.0 removes the
  question.
- **It fits.** IPLocate is 16.1 MB; IPFire needs format conversion. DB-IP is 3.86 MB
  gzipped, already MMDB.
- **No update clause.** CC BY 4.0 imposes no duty to destroy stale copies, so an
  immutable published artifact is inherently compliant. This is the single most
  important reason to switch — it's exactly the trap §6.3 sets.

**Residual risks, plainly:** DB-IP Lite is less accurate than GeoLite2 (the trade for a
clean license) — mitigated by the user-supplied-path override. DB-IP's terms are stated
on its download page rather than in a formal contract; archive the page at the version
you ship.

**If someone insists on bundling GeoLite2:** the only defensible path is a Commercial
Redistribution License (sales@maxmind.com), not the CC-carve-out theory.

**Sources:** [GeoLite EULA 2026-02-12](https://www.maxmind.com/en/geolite/eula) ·
[EULA mirror](https://www.maxmind.com/en/geolite2/eula) ·
[MaxMind dev portal](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/) ·
[2019 license-change blog](https://blog.maxmind.com/2019/12/significant-changes-to-accessing-and-using-geolite2-databases/) ·
[Commercial Redistribution KB](https://support.maxmind.com/hc/en-us/articles/4408928143643-Commercial-Redistribution-License-for-GeoLite2) ·
[Who is covered by the EULA](https://support.maxmind.com/hc/en-us/articles/4408936666523-Who-is-Covered-by-the-GeoLite2-End-User-License-Agreement) ·
[GeoLite2 Commercial Redistribution](https://www.maxmind.com/en/geolite2-commercial-redistribution) ·
[Cargo Book publishing](https://doc.rust-lang.org/cargo/reference/publishing.html) ·
[crates.io#195](https://github.com/rust-lang/crates.io/issues/195) ·
[crates.io API](https://crates.io/api/v1/crates) ·
[tor-geoip-db .crate](https://static.crates.io/crates/tor-geoip-db/tor-geoip-db-0.1.0-pre2.crate) ·
[DB-IP Lite](https://db-ip.com/db/lite.php) ·
[DB-IP IP-to-Country Lite](https://db-ip.com/db/download/ip-to-country-lite) ·
[IP2Location LITE FAQs](https://lite.ip2location.com/faqs) ·
[IPLocate free DBs](https://www.iplocate.io/free-databases) ·
[IPinfo Lite](https://ipinfo.io/lite) ·
[IPFire Location](https://www.ipfire.org/location/) ·
[node-geolite2-redist](https://github.com/GitSquared/node-geolite2-redist)

---

## HTTP stack

**Verdict: split the stack. reqwest for scraping, raw tokio + hyper 1.x for checking.**
The Python's decision to hand-roll was correct and stays correct.

### reqwest 0.13.4 — good for scraping, wrong for checking

Three structural facts disqualify it for the checker:

**1. No per-request proxy.**
[`RequestBuilder`](https://docs.rs/reqwest/0.13.4/reqwest/struct.RequestBuilder.html)
has `timeout`, `version`, `headers` — no `proxy`. Only
`ClientBuilder::proxy(self, proxy: Proxy)`. `Proxy::custom(|url| ...)` keys off the
*target* URL, not the attempt, so it cannot rotate proxies per request. Checking N
proxies means building N Clients, each with its own pool and TLS config.

**2. Error classification too coarse.**
[`reqwest::Error`](https://docs.rs/reqwest/0.13.4/reqwest/struct.Error.html) exposes
`is_builder`, `is_redirect`, `is_status`, `is_timeout`, `is_request`, `is_connect`,
`is_body`, `is_decode`, `is_upgrade`, plus `url`/`url_mut`/`with_url`/`without_url`/
`status`. Fields are private; `source()` walks unspecified internals with no stability
guarantee. "SOCKS5 auth rejected", "SOCKS5 host unreachable", "TCP refused" — **and DNS
failure** — all collapse to `is_connect() == true`. Disqualifying for proxybroker's
error taxonomy.

**3. No access to the negotiated stream.**

**Verified `Proxy` API** ([docs](https://docs.rs/reqwest/0.13.4/reqwest/struct.Proxy.html)):
```rust
pub fn http<U: IntoProxy>(proxy_scheme: U) -> Result<Proxy>
pub fn https<U: IntoProxy>(proxy_scheme: U) -> Result<Proxy>
pub fn all<U: IntoProxy>(proxy_scheme: U) -> Result<Proxy>
pub fn custom<F, U: IntoProxy>(fun: F) -> Proxy  // F: Fn(&Url) -> Option<U> + Send + Sync + 'static
pub fn basic_auth(self, username: &str, password: &str) -> Proxy
pub fn custom_http_auth(self, header_value: HeaderValue) -> Proxy
```
SOCKS support is broader than the prose docs admit — the source
([proxy.rs ~L129](https://docs.rs/reqwest/0.13.4/src/reqwest/proxy.rs.html)) parses all
four, defaulting to port 1080:
```rust
"socks4" | "socks4a" | "socks5" | "socks5h" => Some(1080),
```

**Breaking changes 0.12 → 0.13** ([CHANGELOG](https://github.com/seanmonstar/reqwest/blob/master/CHANGELOG.md)):
rustls is now the **default** backend (aws-lc, not ring); `rustls-tls` feature renamed
to `rustls`; rustls roots features removed in favor of `rustls-platform-verifier`;
`query`/`form` now opt-in; TLS methods renamed (`tls_backend_rustls()` over
`use_rustls_tls()`, soft-deprecated aliases remain). 0.13.4 itself added
`tls_sslkeylogfile`, blocking `http2_keep_alive_*`, native-tls TLS 1.3, a redirect
header-stripping fix, an HTTP/3 happy-eyeballs fix, and hickory-resolver 0.26.

`ClientBuilder::connector_layer<L>(self, layer: L)` (tower `Layer`) exists and could
wrap the connector, but you'd reimplement the handshake inside it — at which point
reqwest adds nothing.

**Use reqwest for the ~50-provider scrape:** `user_agent`, `timeout`, `connect_timeout`,
`local_address`, gzip/brotli — one Client, `FuturesUnordered` for concurrency.

### hyper 1.10.1 client over a stream you negotiated yourself

The load-bearing capability.
[`handshake`](https://docs.rs/hyper/1.10.1/hyper/client/conn/http1/fn.handshake.html) —
signature and all four bounds verified exact:
```rust
pub async fn handshake<T, B>(io: T) -> Result<(SendRequest<B>, Connection<T, B>)>
where
    T: Read + Write + Unpin,
    B: Body + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
```
**The `Read`/`Write` here are `hyper::rt::Read`/`Write`, NOT tokio's**
([hyper::rt](https://docs.rs/hyper/1.10.1/hyper/rt/index.html) exports Executor, Read,
Sleep, Timer, Write) — the most common porting mistake. Bridge with
[`TokioIo`](https://docs.rs/hyper-util/0.1.20/hyper_util/rt/tokio/struct.TokioIo.html)
(hyper-util 0.1.20), which adapts both directions and has `new`/`inner`/`inner_mut`/
`into_inner`.

Shape, per hyper's [`examples/client.rs`](https://github.com/hyperium/hyper/blob/master/examples/client.rs):
```rust
let stream: TcpStream = /* YOUR negotiated stream: post-SOCKS5, post-CONNECT, post-TLS */;
let io = TokioIo::new(stream);
let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
tokio::task::spawn(async move { if let Err(err) = conn.await { /* connection-level error */ } });
let mut res = sender.send_request(req).await?;
```
[`SendRequest`](https://docs.rs/hyper/1.10.1/hyper/client/conn/http1/struct.SendRequest.html):
`send_request(&mut self, req) -> impl Future<Output = Result<Response<Incoming>>>`, plus
`ready`, `is_ready`, `is_closed`.

**This solves the absolute-form requirement.** Quoting the docs (verbatim-verified): the
request `Uri` "is serialized as-is" — origin-form (`/path`) vs absolute-form
(`https://hyper.rs/guides`, "required when sending to an HTTP proxy") — and this "is
however not enforced or validated." hyper hands you the raw URI control reqwest hides.

Splitting `handshake` from `conn.await` also gives the timing decomposition proxybroker
measures: TCP connect, SOCKS handshake, HTTP round-trip as three separate spans.

**CONNECT tunneling** ([`hyper::upgrade`](https://docs.rs/hyper/1.10.1/hyper/upgrade/index.html)):
send a `CONNECT`, then `hyper::upgrade::on(&mut res)` → `OnUpgrade` → `Upgraded`.
Recover the raw stream ([verified exact](https://docs.rs/hyper/1.10.1/hyper/upgrade/struct.Upgraded.html)):
```rust
pub fn downcast<T: Read + Write + Unpin + 'static>(self) -> Result<Parts<T>, Self>
```
Gotcha: those bounds are again `hyper::rt`, so `T` is `TokioIo<TcpStream>`, not
`TcpStream`. [`Parts`](https://docs.rs/hyper/1.10.1/hyper/upgrade/struct.Parts.html)
carries `io: T` **and `read_buf: Bytes`** — bytes already read past the response.
Ignoring `read_buf` corrupts the tunnel.

### hyper 1.10.1 server for the local proxy

Per [`examples/hello.rs`](https://github.com/hyperium/hyper/blob/master/examples/hello.rs):
```rust
let listener = TcpListener::bind(addr).await?;
loop {
    let (tcp, _) = listener.accept().await?;
    let io = TokioIo::new(tcp);
    tokio::task::spawn(async move {
        http1::Builder::new()
            .timer(TokioTimer::new())
            .serve_connection(io, service_fn(handler))
            .with_upgrades()          // REQUIRED for CONNECT
            .await
    });
}
```
[`Builder::serve_connection`](https://docs.rs/hyper/1.10.1/hyper/server/conn/http1/struct.Builder.html):
`pub fn serve_connection<I, S>(&self, io: I, service: S) -> Connection<I, S>` where
`I: Read + Write + Unpin`, `S: HttpService<Incoming>`.

`with_upgrades` is on **`Connection`, not `Builder`**
([Connection](https://docs.rs/hyper/1.10.1/hyper/server/conn/http1/struct.Connection.html)) —
`pub fn with_upgrades(self) -> UpgradeableConnection<I, S> where I: Send` — so it must
chain *after* `serve_connection`. This is exactly the hyper-0.14-vs-1.x trap. Also
`graceful_shutdown(self: Pin<&mut Self>)`. Since the local proxy relays CONNECT,
`with_upgrades()` is mandatory.

### TLS: rustls 0.23 + tokio-rustls 0.26.4

[tokio-rustls 0.26.4](https://docs.rs/tokio-rustls/0.26.4/tokio_rustls/) (depends rustls
`^0.23.27`; current rustls is **0.23.42**, published 2026-07-13) exposes at crate root:
`TlsConnector`, `TlsConnectorWithAlpn`, `TlsAcceptor`, `TlsStream`,
`LazyConfigAcceptor`. Pattern: `TlsConnector::from(Arc<ClientConfig>)` then
`.connect(ServerName, stream)` → `Connect` → `TlsStream`. Docs warn `poll_flush()` is
required after writing — data written by `poll_write` is not guaranteed to reach the
`TcpStream` (BufWriter-like).

**On invalid certs.** You're deliberately hitting hosts through MITM-capable
intermediaries. Two options:
- reqwest: `danger_accept_invalid_certs(bool)` / `danger_accept_invalid_hostnames(bool)`
  ([ClientBuilder](https://docs.rs/reqwest/0.13.4/reqwest/struct.ClientBuilder.html)) — blunt on/off.
- rustls direct: implement
  [`ServerCertVerifier`](https://docs.rs/rustls/latest/rustls/client/danger/trait.ServerCertVerifier.html)
  (required methods: `verify_server_cert`, `verify_tls12_signature`,
  `verify_tls13_signature`, `supported_verify_schemes`; `requires_raw_public_keys` and
  `root_hint_subjects` are defaulted) and install it:
  ```rust
  pub fn dangerous(&mut self) -> DangerousClientConfig<'_>   // rustls::client::ClientConfig
  pub fn set_certificate_verifier(&mut self, verifier: Arc<dyn ServerCertVerifier>)
  ```
  Note the doc paths are `rustls::client::ClientConfig` /
  `rustls::client::danger::DangerousClientConfig` — the crate-root URLs 404 (confirmed).

**Recommend rustls direct with a custom verifier that records rather than ignores.** A
blanket `danger_accept_invalid_certs(true)` throws away signal; a custom
`ServerCertVerifier` can always return Ok while **capturing the presented chain** —
which is how you detect a proxy MITM-ing TLS, a proxy-quality signal the Python version
can't easily get. Choose rustls over native-tls regardless: it's reqwest 0.13's default
and cross-platform-deterministic (no per-OS cert-store divergence across the check
fleet).

`ClientConfig::builder() -> ConfigBuilder<Self, WantsVerifier>`;
`alpn_protocols: Vec<Vec<u8>>` is public and empty by default — **set it to
`[b"http/1.1"]` explicitly**, or ALPN is simply not offered.

### Composition

```
TcpStream::connect  ──t0──▶  tokio-socks connect_with_socket  ──t1──▶  into_inner()
   ──▶  [optional tokio-rustls .connect()]  ──▶  TokioIo::new()
   ──▶  hyper handshake  ──t2──▶  send_request
```
Every arrow is a timing boundary you control and a typed error you can classify. reqwest
collapses the whole chain into one opaque `is_connect() == true`. You don't hand-roll
protocol bytes; you hand-roll only the *boundaries*, which is where the measurement
lives.

**Budget an afternoon for two things:** hyper's `Read`/`Write`-vs-tokio trait split, and
`Upgraded::downcast`'s `read_buf`.

---

## SOCKS negotiation

**Recommendation: `tokio-socks` 0.5.3 for the checker.** The deciding factor is the
error enum.

**tokio-socks 0.5.3**
([Socks5Stream](https://docs.rs/tokio-socks/0.5.3/tokio_socks/tcp/socks5/struct.Socks5Stream.html),
[Socks4Stream](https://docs.rs/tokio-socks/0.5.3/tokio_socks/tcp/socks4/struct.Socks4Stream.html)):
```rust
// impl block carries S: AsyncSocket + Unpin; the target bound is on the fn:
//   connect_with_socket<'t, T>(socket: S, target: T) -> Result<Socks5Stream<S>>
//   where T: IntoTargetAddr<'t>
Socks5Stream::connect_with_socket(socket, target)
Socks5Stream::connect_with_password_and_socket(socket, target, user, pass)
Socks4Stream::connect_with_socket(socket, target)
Socks4Stream::connect_with_userid_and_socket(socket, target, user_id)
fn into_inner(self) -> S
```
- **SOCKS4 and SOCKS5** — both.
- **`connect_with_socket` takes an already-connected socket** → you own
  `TcpStream::connect`, so TCP-connect times separately from handshake.
- **`into_inner()` returns the negotiated stream** → straight into `TokioIo::new(...)` →
  hyper handshake. This is the key composition; yes, the negotiated stream is reusable.
- **[`Error`](https://docs.rs/tokio-socks/0.5.3/tokio_socks/enum.Error.html) has 23
  variants mapping SOCKS reply codes 1:1** (count verified exact):
  `GeneralSocksServerFailure`, `ConnectionNotAllowedByRuleset`, `NetworkUnreachable`,
  `HostUnreachable`, `ConnectionRefused`, `TtlExpired`, `CommandNotSupported`,
  `AddressTypeNotSupported`, `NoAcceptableAuthMethods`, `PasswordAuthFailure`,
  `AuthorizationRequired`, `ProxyServerUnreachable`, `InvalidResponseVersion`, …
  **This is exactly the classification reqwest destroys.**

**fast-socks5 1.0.0**
([client::Socks5Stream](https://docs.rs/fast-socks5/1.0.0/fast_socks5/client/struct.Socks5Stream.html))
also supports pre-connected streams —
`use_stream<S>(socket: S, auth: Option<AuthenticationMethod>, config: Config) -> Result<Self>`
where `S: AsyncRead + AsyncWrite + Unpin` — plus `get_socket` / `get_socket_ref`, and it
has SOCKS4/4a client support. UNVERIFIED: its server module (`run_tcp_proxy`) and the
claim "tokio-socks is client-only" were not checked against docs.rs.

Note: `AsyncSocket` is a blanket impl over `AsyncRead + AsyncWrite`, so the bound
difference between the two crates is cosmetic, not substantive.

**Verdict:** tokio-socks for the checker (richer, reply-code-mapped error enum).
Consider fast-socks5 only if the local server (task 4) must *speak* SOCKS5 to clients
rather than just HTTP/CONNECT. Both beat hand-rolling — the handshake is the boring
part; the timing and error boundaries are what matter, and both crates preserve those.

**Hand-roll only for anonymity detection** — and note that isn't SOCKS work. Inspecting
which headers arrive at a judge happens at the HTTP layer via hyper, comparing sent vs.
received (`Via`, `X-Forwarded-For`, `X-Real-IP`, and your real IP in the echo).

---

## DNS + GeoIP

### hickory-resolver 0.26.1

Docs: https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/

**API differs sharply from the 0.24/trust-dns era:**

| Assumption (0.24-era / trust-dns) | Reality in 0.26.1 |
|---|---|
| `TokioAsyncResolver::tokio(cfg, opts)` | Gone. Builder-only: `Resolver::builder_tokio()` |
| `NameServerConfigGroup` | Gone from the config module entirely. Use `ServerGroup` (for built-in consts) + `Vec<NameServerConfig>` |
| `NameServerConfig { socket_addr, protocol, .. }` | Now `{ ip: IpAddr, trust_negative_responses: bool, connections: Vec<ConnectionConfig> }` — `ip`, not `socket_addr`; protocol moved to `ConnectionConfig`/`ProtocolConfig` |
| `ResolverConfig::from_parts(.., NameServerConfigGroup)` | `from_parts(Option<Name>, Vec<Name>, Vec<NameServerConfig>)` |
| `.build()` is infallible | `build() -> Result<Resolver<P>, NetError>` |
| Error type `ResolveError` | **`NetError`** |

`TokioResolver` **does exist** — `pub type TokioResolver = Resolver<TokioRuntimeProvider>;`
(`tokio` feature), documented at the crate root:
https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/type.TokioResolver.html

**Verified signatures** —
[Resolver](https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/struct.Resolver.html):
```rust
fn builder_tokio() -> Result<ResolverBuilder<TokioRuntimeProvider>, NetError>  // from system resolv.conf
fn builder(provider: R) -> Result<ResolverBuilder<R>, NetError>
fn builder_with_config(config: ResolverConfig, provider: R) -> ResolverBuilder<R>  // NOT a Result

async fn lookup_ip(&self, host) -> Result<LookupIp, NetError>
async fn lookup(&self, name, record_type) -> Result<Lookup, NetError>
async fn ipv4_lookup(&self, query) -> ...   // A-only — closest to aiodns query(host, 'A')
async fn reverse_lookup(&self, query) -> ...
fn clear_cache(&self)
fn options(&self) -> &ResolverOpts
```
[ResolverBuilder](https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/struct.ResolverBuilder.html):
```rust
fn with_options(self, options: ResolverOpts) -> Self
fn options_mut(&mut self) -> &mut ResolverOpts
fn build(self) -> Result<Resolver<P>, NetError>
```
[ResolverOpts](https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/config/struct.ResolverOpts.html)
is `#[non_exhaustive]` — **cannot struct-literal it; `Default::default()` then mutate**.
Fields: `timeout: Duration` (default 5s), `attempts: usize` (default 2), `cache_size: u64`,
`ip_strategy: LookupIpStrategy`, `num_concurrent_reqs: usize` (default 2),
`positive_min_ttl`/`negative_min_ttl`/`positive_max_ttl`/`negative_max_ttl: Option<Duration>`,
`ndots`, `edns0`.

[config module](https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/config/index.html)
(complete listing):
- Structs: `ConnectionConfig`, `NameServerConfig`, `ResolverConfig`, `ResolverOpts`,
  `ServerGroup`, `OpportunisticEncryptionConfig`, `OpportunisticEncryptionPersistence`
- Enums: `LookupIpStrategy`, `ProtocolConfig`, `ResolveHosts`, `ServerOrderingStrategy`,
  `OpportunisticEncryption`
- Consts: `GOOGLE`, `CLOUDFLARE`, `QUAD9` (type `ServerGroup<'_>`)

[ResolverConfig](https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/config/struct.ResolverConfig.html):
`udp_and_tcp(config: &ServerGroup<'_>) -> Self`, `from_parts(...)`, `add_name_server`;
struct is `non_exhaustive`.
[NameServerConfig](https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/config/struct.NameServerConfig.html):
`udp_and_tcp(ip: IpAddr) -> Self` (also `udp`, `tcp`, `tls`, `https`, `quic`, `h3`).

**Verified code shape** (custom nameservers + timeout — what `resolver.py` needs):
```rust
use hickory_resolver::Resolver;
use hickory_resolver::config::{NameServerConfig, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;   // NOT hickory_resolver::name_server

let mut cfg = ResolverConfig::from_parts(None, vec![], vec![]);
for ip in ["8.8.8.8".parse().unwrap(), "1.1.1.1".parse().unwrap()] {
    cfg.add_name_server(NameServerConfig::udp_and_tcp(ip));
}

let mut opts = ResolverOpts::default();   // non_exhaustive → must go through Default
opts.timeout  = Duration::from_secs(5);
opts.attempts = 2;
opts.cache_size = 1024;                   // built-in ResponseCache

let mut builder = Resolver::builder_with_config(cfg, TokioRuntimeProvider::default());
*builder.options_mut() = opts;
let resolver = builder.build()?;          // Result!

let ip = resolver.lookup_ip("example.com").await?.iter().next();
```
System-config path (mirrors aiodns default): `Resolver::builder_tokio()?.build()?`.

Naming: `ConnectionProvider` is the **trait** (crate root); `TokioRuntimeProvider` is the
concrete impl, re-exported via `hickory_net as net`. There is no `name_server` module and
no `TokioConnectionProvider` — see Corrections.

### maxminddb 0.29.0

Docs: https://docs.rs/maxminddb/0.29.0/maxminddb/

| Assumption (0.23/0.24-era) | Reality in 0.29.0 |
|---|---|
| `reader.lookup::<geoip2::Country>(ip) -> Result<Country, Error>` | **Two-stage:** `lookup(ip) -> Result<LookupResult, _>`, then `.decode::<T>() -> Result<Option<T>, _>` |
| Lookup miss = `Err(AddressNotFoundError)` | Miss = **`Ok(None)`** from `decode()`; also `has_data() -> bool` |
| `Reader::open_mmap` on the default build | Behind the `mmap` feature — and **0 features are default** |
| Error type `MaxMindDBError` | **`MaxMindDbError`** (lowercase `b`) |

[Reader](https://docs.rs/maxminddb/0.29.0/maxminddb/struct.Reader.html)`<S: AsRef<[u8]>>`:
```rust
fn open_readfile<P: AsRef<Path>>(database: P) -> Result<Reader<Vec<u8>>, MaxMindDbError>
fn from_source(buf: S) -> Result<Reader<S>, MaxMindDbError>
fn lookup(&'de self, address: IpAddr) -> Result<LookupResult<'de, S>, MaxMindDbError>
fn metadata(&self) -> &Metadata
fn verify(&self) -> Result<(), MaxMindDbError>
```
[LookupResult](https://docs.rs/maxminddb/0.29.0/maxminddb/struct.LookupResult.html)`<'a, S>`:
```rust
fn has_data(&self) -> bool
fn decode<T>(&self) -> Result<Option<T>, MaxMindDbError> where T: Deserialize<'a>
fn decode_path<T>(&self, path: &[PathElement<'_>]) -> Result<Option<T>, MaxMindDbError>
fn network(&self) -> Result<IpNetwork, MaxMindDbError>
```
[`geoip2::Country<'a>`](https://docs.rs/maxminddb/0.29.0/maxminddb/geoip2/struct.Country.html) —
UNVERIFIED field list (the verifier did not confirm this one): `continent`,
`country: country::Country<'a>`, `registered_country`, `represented_country`, `traits`.

[`geoip2::country::Country<'a>`](https://docs.rs/maxminddb/0.29.0/maxminddb/geoip2/country/struct.Country.html)
— verified exact:
```rust
geoname_id: Option<u32>
is_in_european_union: Option<bool>
iso_code: Option<&'a str>     // BORROWED — not Option<String>
names: Names<'a>              // bare, not Option
```

**Verified code shape:**
```rust
use maxminddb::{Reader, geoip2};

let reader = Reader::open_readfile("Country.mmdb")?;   // Reader<Vec<u8>>

let res = reader.lookup(ip)?;
let code: Option<String> = res
    .decode::<geoip2::Country>()?                 // Result<Option<Country>, _>
    .and_then(|c| c.country.iso_code)
    .map(str::to_owned);                          // copy out — see lifetime note
```
Fast path avoiding full-struct deserialize (good for a hot IP→country loop; the
[`path!`](https://docs.rs/maxminddb/0.29.0/maxminddb/macro.path.html) macro exists and
this is verbatim its doc example shape):
```rust
use maxminddb::path;
let code: Option<String> = reader.lookup(ip)?.decode_path(&path!["country", "iso_code"])?;
```

**Lifetime gotcha (load-bearing):** `iso_code: Option<&'a str>` borrows from the
`Reader`'s backing buffer, and `lookup(&'de self)` ties `'de` to the reader. The decoded
`Country` cannot outlive the `Reader`, and you **cannot** hold a `Reader` and a decoded
`Country` in the same struct (self-referential). Port pattern: keep the reader in an
`Arc<Reader<Vec<u8>>>` / `OnceLock` for process lifetime and `.to_owned()` the ISO code
at the lookup site — matching `resolver.py`, which stores a plain country string.

**Features** (https://docs.rs/crate/maxminddb/0.29.0/features): `default`, `memmap2`,
`mmap`, `simdutf8`, `unsafe-str-decode` — **none enabled by default**. `open_mmap` isn't
in the default-features docs build, so its signature is UNVERIFIED. Version-proof
alternative needing no feature flag: `Reader::from_source(mmap)` where
`mmap: memmap2::Mmap` (`Mmap: AsRef<[u8]>` satisfies `S`). Honestly, a country mmdb is a
few MB; `open_readfile` into a `Vec<u8>` is simpler and fine.

### Caching

**Recommendation: hickory's built-in `ResponseCache` first; `moka` only above it;
nothing for GeoIP.**

- **Check whether you need a DNS cache at all.** hickory-resolver has a built-in
  `ResponseCache` (crate root, confirmed) driven by `ResolverOpts::cache_size`,
  `positive_min_ttl`/`positive_max_ttl`, and `Resolver::clear_cache()`. It caches by DNS
  record TTL, which is *more* correct than cachetools' fixed wall-clock TTL. The Python
  hand-rolls a cache only because aiodns has none. **The laziest correct port deletes
  the DNS cache layer entirely** and sets `opts.cache_size` + `opts.positive_min_ttl`.
- **If you need a layer above it** (a post-processed `host → chosen IP` decision, or
  negative results with your own policy), use
  [`moka::future::Cache`](https://docs.rs/moka/0.12.15/moka/future/struct.Cache.html).
  The decisive feature is **`try_get_with`**, which coalesces concurrent lookups of the
  same key into one in-flight resolution — proxybroker2 resolves the same handful of
  judge hostnames from many concurrent tasks, and a `HashMap` behind a `Mutex` gives a
  thundering herd (or, if you hold the lock across the `.await`, a serialized resolver).
  ```rust
  let cache: Cache<String, Arc<Vec<IpAddr>>> = Cache::builder()
      .max_capacity(10_000)
      .time_to_live(Duration::from_secs(300))
      .build();

  let ips = cache.try_get_with(host.clone(), async {
      resolver.lookup_ip(&host).await.map(|r| Arc::new(r.iter().collect()))
  }).await?;
  ```
  Bounds: `K: Hash + Eq + Send + Sync + 'static`, `V: Clone + Send + Sync + 'static` —
  hence `Arc<Vec<IpAddr>>` so clones are cheap.
  **UNVERIFIED:** `try_get_with`'s exact signature and its "does not cache errors"
  semantics (returns `Err(Arc<E>)`, inserts nothing) were not checked against docs.rs.
  Confirm before relying on the error behavior.
- **GeoIP IP→country: don't cache.** An mmdb lookup is a pointer-chase over an
  in-memory buffer — sub-microsecond. A cache lookup costs about the same as the thing
  it caches, and moka's per-entry housekeeping makes it a net loss. Python caches it
  only because pure-Python mmdb decode is slow.
- **`HashMap` + `Mutex`:** fine only if you skip TTL and coalescing — but TTL is the
  requirement.
- **`quick_cache`:** faster on the raw hit path, weaker TTL story, no async coalescing.
  Wrong trade for a network-bound cache.

### External IP of the machine

**Recommendation: plain HTTP GET with the `reqwest` you already have. Do not add a crate.**

`public-ip` is the obvious pick and it's a trap: latest is **0.2.2, published
2022-01-07** (verified via https://crates.io/api/v1/crates/public-ip) — ~4.5 years
stale, depending on the **pre-rename `trust-dns-*` stack** and old `hyper` 0.14. Adding
it to a project already on hickory-resolver 0.26 pulls in a second, ancient,
unmaintained DNS implementation plus a duplicate HTTP stack.

```rust
async fn external_ip(client: &reqwest::Client) -> Option<IpAddr> {
    for url in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"] {
        if let Ok(resp) = client.get(url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(body) = resp.text().await {
                if let Ok(ip) = body.trim().parse() { return Some(ip); }
            }
        }
    }
    None
}
```
Call once at startup, store in a `OnceCell`/`OnceLock`. If you want the DNS-based path
(Google's `o-o.myaddr.l.google.com TXT` / OpenDNS `myip.opendns.com`), implement it on
the hickory resolver you already built — again no new dependency.

### Dependency picks

| Need | Crate | Note |
|---|---|---|
| DNS | `hickory-resolver` 0.26.1 | Builder-only; `NameServerConfigGroup` gone; `build()` returns `Result`; `TokioRuntimeProvider` |
| GeoIP | `maxminddb` 0.29.0 | `lookup().decode::<T>() -> Result<Option<T>,_>`; `iso_code: Option<&'a str>` |
| DNS cache | hickory's `ResponseCache` first; `moka` 0.12.15 only above it | `try_get_with` for coalescing |
| GeoIP cache | none | mmdb lookup already sub-µs |
| External IP | none — `reqwest` GET | `public-ip` unmaintained since 2022, pulls old trust-dns |

`resolver.py`'s 557 LOC should shrink considerably — both caches are work Python had to
do by hand and that hickory/maxminddb either do for you or make unnecessary.

---

## Architecture recommendations

> **Caveat: this section was not adversarially verified.** The Python-side measurements
> (registry counts, file sizes, line references) and the fetched precedent manifests are
> first-pass findings. The crates.io 10 MB limit and the GeoLite2 licensing conclusion
> *were* independently verified (see above).

### Cargo layout — single crate, lib+bin, `cli` feature gating the binary

Precedent manifests fetched:

| Crate | Layout | Mechanism |
|---|---|---|
| **tokei** | single crate lib+bin | `default = ["cli"]`, `[[bin]] required-features=["cli"]`, clap/colored/env_logger all `optional = true` |
| **bat** | single crate lib+bin | `default = ["application", "git"]`; in-file comment: *"Feature required for bat the application. Should be disabled when depending on bat as a library."* |
| **ripgrep** | workspace, 9 crates | because `grep`/`ignore`/`globset` are independently valuable |
| **typos** | workspace, `default-members = ["crates/typos-cli"]` | lib `typos` + bin crate `typos-cli` |

```toml
[package]
name = "proxybroker"

[features]
default = ["cli", "rustls-tls"]
cli = ["dep:clap", "dep:tracing-subscriber", "dep:serde_json", "server", "geo"]

[[bin]]
name = "proxybroker"
required-features = ["cli"]
```

Rationale:
1. **Name discoverability decides it.** One crate name means `cargo add proxybroker`
   and `cargo install proxybroker` both do the obvious thing. typos is the
   counterexample: the lib owns `typos`, so `cargo install typos` installs the library
   and produces no binary. You have one good name; don't split it.
2. **ripgrep's workspace doesn't apply.** It split because `ignore`/`globset` have
   standalone audiences. A "proxy provider scraper" with no checker has none.
3. **Compile time is a wash, mildly favoring single-crate.** A workspace only
   parallelizes independent crates; yours is a linear chain (`providers` → `checker` →
   `cli`) with zero pipelining plus per-crate metadata overhead. The real lever is the
   feature flag, which works identically in one crate.
4. Recheck at ~15k LOC. Splitting later is a non-breaking `pub use` re-export.

The wart: `default = ["cli"]` means library consumers must write
`default-features = false` or silently pull clap. tokei and bat both live with this, and
it's the right trade (the CLI is the mass-market entry point). Mitigate with bat's
comment style in `Cargo.toml` and a **Using as a library** README section leading with
the `default-features = false` line.

### Providers — enum-of-data (`ProviderSpec`) + one `dyn` trait escape hatch

Counted from the actual `PROVIDERS` registry:
- **38 entries total.**
- **18 are bare `Provider(url=..., proto=...)`** — zero code, pure data.
- **20 are subclasses**, but most override only `_pipe`, and nearly every `_pipe` is the
  same shape (fetch seed page → regex out links → template onto a base URL → fetch all),
  differing **only in the link regex and the base URL**. That's data.
- Only ~4 need real code: `Spys_ru`, `Xseo_in`, `Nntime_com` (JS port-deobfuscation via a
  `charEqNum` XOR table), plus base64/percent-decode variants.

True split: **~34/38 pure data, ~4 need real code.**

**The forcing constraint:** the Python already has `ConfigurableProvider` and
`load_provider_configs_from_directory()` — providers loadable from YAML/JSON, documented
as *"Safe for Docker bind-mounts: only data files are read, no Python is executed."*
It's in the public `__all__`. **This kills the closure-builder option outright** —
closures aren't `Deserialize`, `Debug`, or inspectable. If provider config must
round-trip through YAML for feature parity, the provider description *has to be a data
type*. The design is chosen for you.

```rust
/// The 34/38 case. Deserialize-able => YAML/JSON provider dirs port for free.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderSpec {
    pub name: String,
    pub proto: Vec<Proto>,
    pub plan: Plan,
    #[serde(default)]
    pub parse: Parse,
    #[serde(default = "default_max_conn")]
    pub max_conn: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Plan {
    Single { url: Url },                                  // the 18 bare providers
    Crawl  { seed: Url, #[serde(with = "serde_regex")] link: Regex, base: Url },  // ~12 _pipe overrides
    Pages  { template: String, range: std::ops::Range<u32> },
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Parse {
    #[default]
    Default,                                              // global IP:PORT regex
    Regex  { #[serde(with = "serde_regex")] re: Regex },  // (host)(port) captures
    Base64 { #[serde(with = "serde_regex")] re: Regex },  // proxy_list_org
    UrlDecoded,                                           // proxz_com
}

/// The 4/38 case. One method, one escape hatch.
#[async_trait::async_trait]
pub trait CustomProvider: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn fetch(&self, http: &Http) -> Result<Vec<Seed>, ProviderError>;
}

#[derive(Clone)]
pub enum Provider {
    Spec(ProviderSpec),
    Custom(Arc<dyn CustomProvider>),
}
```

`#[async_trait]` is still required for the escape hatch: async fn in traits stabilized in
1.75 but is [still not `dyn`-compatible](https://rust-lang.github.io/async-fundamentals-initiative/explainer/async_fn_in_dyn_trait.html),
and [`async-trait`'s docs](https://docs.rs/async-trait) say so. Precedent:
`object_store` puts `#[async_trait]` on its flagship public `ObjectStore` trait
(`src/lib.rs:907`). One `Box`-per-provider-fetch is unmeasurable next to an HTTP
round-trip. Don't hand-roll `BoxFuture` to save a dep.

Offer closures as **sugar on top of** the trait, not instead of it
(`impl<F, Fut> CustomProvider for FnProvider<F>`).

**Practical note:** don't port all 38 blind. `gatherproxy.com`, `proxz.com`,
`foxtools.ru`, and the blogspot-based ones are near-certainly dead by 2026 — the registry
comments (`# 49`, `# 5500`) are yield estimates from ~2018. Probe liveness first; a dead
provider is a test-flake generator you maintain for nothing.

### Streaming — `find() -> Result<ProxyStream, Error>`, `ProxyStream: Stream<Item = Proxy>`

A **named concrete struct implementing `Stream`** — not `Box<dyn Stream>`, not a raw
channel, not a callback.

```rust
impl Broker {
    pub async fn find(&self, q: Query) -> Result<ProxyStream, Error>;
}

pub struct ProxyStream { rx: mpsc::Receiver<Proxy>, tasks: JoinSet<()>, /* … */ }
impl Stream for ProxyStream { type Item = Proxy; }
```

- **Callbacks are out.** Async callbacks mean `Box<dyn Fn -> BoxFuture>`, inverted
  control, awkward `?`, and cancellation-by-bool.
- **Raw `mpsc::Receiver` is out as a public type.** It welds your API to tokio's exact
  channel type and leaks the impl. Use mpsc *internally* — the right engine — and wrap it.
- **`Stream` is the idiom**, and the combinator ecosystem (`.take(10)`, `.filter()`,
  `.chunks()`, `StreamExt::next`) comes free. It's also the closest analogue to the
  Python's `asyncio.Queue` loop.
- **Concrete struct beats `BoxStream`.** `object_store` returns
  `BoxStream<'static, Result<ObjectMeta>>` (`src/lib.rs:1241`) *only because `list()` is
  a trait method* and trait methods can't return RPIT-with-a-name. `Broker::find` is an
  **inherent** method, so name the type: nameable in user structs, can carry inherent
  methods (`.stats()`), no vtable.

Two details that matter more than the choice:
1. **Backpressure.** Python gets it free from `asyncio.Queue(maxsize=max_conn)`. Use a
   **bounded** `mpsc::channel(n)` internally so a slow consumer throttles the checker
   instead of buffering thousands of proxies into RAM.
2. **Drop = cancel.** Own the workers in a `JoinSet` (or a `CancellationToken`) *inside*
   `ProxyStream` and abort on `Drop`. `find().take(10)` then does the right thing.
   That single property is the biggest ergonomic win over the Python, where
   `Broker.stop()` is manual and wired to a SIGINT handler (`api.py`'s
   `stop_broker_on_sigint` plumbing, which you can then delete).

CLI and library needs don't diverge — the CLI is one more consumer:
```rust
let mut s = broker.find(q).await?;
while let Some(p) = s.next().await { writeln!(out, "{p}")?; }
```
`--limit N` is `.take(n)`. Ctrl-C is `tokio::select!` + drop.

### Errors — two enums, split by whether the caller can act

`errors.py` has 11 types, but they're two taxonomies with different lifecycles:
- `ProxyConnError`, `ProxyRecvError`, `ProxySendError`, `ProxyTimeoutError`,
  `ProxyEmptyRecvError`, `BadStatusError`, `BadResponseError`, `BadStatusLine` — all
  carry an `errmsg` string, and `proxy.py:333` does
  `self.stat["errors"][err.errmsg] += 1`. **These never reach the user.** `checker.py`
  catches them in bulk (lines 208–246) and moves on. The `errmsg` field is the tell: it
  exists to be a **histogram bucket key**.
- `ResolveError`, `NoProxyError` — these actually reach the caller.

```rust
/// Fatal. The run cannot continue or start. Returned in Err.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("no providers configured")]                     NoProviders,
    #[error("no judges reachable; cannot verify anonymity")] NoJudges,
    #[error("invalid provider config at {path}")]
    Config { path: PathBuf, #[source] source: ConfigError },
    #[error("dns resolver init failed")]
    Resolver(#[source] hickory_resolver::NetError),   // NOTE: NetError, not ResolveError
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Per-proxy. Expected, high-volume, NOT an error to the caller. Never in Err.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    #[error("connection_failed")]   ConnFailed,
    #[error("connection_timeout")]  Timeout,
    #[error("connection_is_reset")] Reset,
    #[error("empty_response")]      EmptyRecv,
    #[error("bad_status")]          BadStatus,
    #[error("bad_response")]        BadResponse,
    #[error("resolve_failed")]      Resolve,
}
```
`ProxyError` is `Copy + Hash` deliberately — it's the key type for
`HashMap<ProxyError, u32>`, directly porting the `Counter()` of `errmsg`. The
`#[error("...")]` strings *are* the old `errmsg` values, so stats output stays
wire-compatible.

**The consequential call: `Item = Proxy`, not `Item = Result<Proxy, Error>`.** A typical
run produces thousands of per-proxy failures and tens of successes. As `Err` items the
95% use case becomes `match r { Ok(p) => ..., Err(_) => continue }` — discarding ~99% of
items, a `continue` every user writes identically. **A proxy that fails its check is not
an error — it's a non-result**, the scraping equivalent of a filter predicate returning
false. `find()` means "find me working proxies"; a broken proxy isn't a failure of
`find`, it's `find` doing its job.

So: fatal errors surface once, up front, in `Result<ProxyStream, Error>`. Per-proxy
failures land in `tracing` (at `debug`) and counters:
```rust
impl ProxyStream { pub fn stats(&self) -> &Stats; }   // errors: HashMap<ProxyError, u32>
```
— exactly Python's `proxy.stat["errors"]` / `Broker.show_stats()`, and what the CLI needs
for its summary and exit code.

`thiserror` in the lib, `anyhow` in `main.rs` only — the standard split, and what ripgrep
does (`anyhow = "1.0.75"` in the root bin package).

### Feature flags

`object_store`'s feature table is the model: it separates base features from
batteries-included ones, makes HTTP transport and crypto/TLS selectable, and comments
that the default *"intentionally does NOT include reqwest or a bundled crypto provider."*

```toml
[features]
default = ["cli", "rustls-tls", "geo-bundled"]

# The binary. Arg parsing + log formatting + everything the CLI exposes.
cli = ["dep:clap", "dep:tracing-subscriber", "dep:serde_json", "server", "geo"]

# Local proxy server (server.py / ProxyPool). Pure library users mostly don't want this.
server = ["dep:hyper", "dep:hyper-util", "dep:http-body-util"]

# Country lookup CODE.
geo = ["dep:maxminddb"]

# Country lookup DATA — DB-IP Country Lite, CC BY 4.0. See GeoLite2 licensing above.
geo-bundled = ["geo"]

# TLS backend. Mutually exclusive in practice; rustls is the default.
rustls-tls = ["reqwest/rustls"]     # NOTE: feature renamed rustls-tls -> rustls in reqwest 0.13
native-tls = ["reqwest/native-tls"]

[[bin]]
name = "proxybroker"
required-features = ["cli"]
```
Optional: `cli`, `server`, `geo`, `geo-bundled`, TLS backend.
Not optional: providers, checker, negotiators, DNS resolver, `tracing` (facade only —
it's ~free with no subscriber; gate `tracing-subscriber`, the heavy half).

Don't feature-gate individual providers — 38 features is a combinatorial CI nightmare to
save a few hundred bytes of regex.

**`geo` gates the code; `geo-bundled` gates the data — and the data is DB-IP, not
GeoLite2.** (The first-pass architecture report recommended gating code only and never
bundling data at all; the licensing research supersedes it — bundling is fine with a
CC BY 4.0 source. The 3.1 MB GeoLite2 mmdb in the Python package stays unbundled
forever.)
```rust
#[cfg(feature = "geo")]
impl GeoDb { pub fn open(path: impl AsRef<Path>) -> Result<Self, Error>; }
```
CLI gets `--geo-db <PATH>` + `PROXYBROKER_GEO_DB` env var resolving to a standard data
dir (use `etcetera` — both bat and tokei depend on it). Set `include = [...]` in
`[package]` to positively list what ships, so no stray data file sneaks in (tokei does
exactly this).

Set `reqwest = { default-features = false }` and let features add the backend back —
otherwise `default-features = false` on your crate still drags in a backend transitively
and the flag does nothing.

**Sources:** [tokei](https://raw.githubusercontent.com/XAMPPRocky/tokei/master/Cargo.toml) ·
[bat](https://raw.githubusercontent.com/sharkdp/bat/master/Cargo.toml) ·
[ripgrep](https://raw.githubusercontent.com/BurntSushi/ripgrep/master/Cargo.toml) ·
[typos](https://raw.githubusercontent.com/crate-ci/typos/master/Cargo.toml) ·
[object_store](https://raw.githubusercontent.com/apache/arrow-rs-object-store/main/src/lib.rs) ·
[async-trait](https://docs.rs/async-trait) ·
[async fn in dyn trait](https://rust-lang.github.io/async-fundamentals-initiative/explainer/async_fn_in_dyn_trait.html) ·
[crates.io#195](https://github.com/rust-lang/crates.io/issues/195)

---

## Corrections applied

Every place the adversarial pass caught the first pass. **The verifier wins in all cases
below.** No first-pass claim survived on a primary source the verifier ignored.

### Compile-breaking (would have failed `cargo build`)

1. **`reqwest::Error::is_dns()` does not exist in 0.13.4.** The HTTP report asserted it
   twice ("0.13.4 added `Error::is_dns()`") and listed it among available methods.
   [docs.rs/reqwest/0.13.4/reqwest/struct.Error.html](https://docs.rs/reqwest/0.13.4/reqwest/struct.Error.html)
   has no `is_dns`. The
   [CHANGELOG](https://github.com/seanmonstar/reqwest/blob/master/CHANGELOG.md) places it
   under **Unreleased**, not 0.13.4 — the classic "read the Unreleased section as
   shipped" failure. `err.is_dns()` is a `no method named is_dns` error.
   *Irony: removing it makes the report's "error classification is too coarse" argument
   stronger, since DNS failures also collapse into `is_connect()`.* Corrected in
   **HTTP stack**.

2. **`hickory_resolver::name_server::TokioConnectionProvider` does not exist.** The
   DNS report's §1 code shape imported it. Both halves are wrong: the `name_server`
   *module* 404s in 0.26.1 (crate root lists `caching_client`, `config`, `lookup`,
   `lookup_ip`, `metrics`, `recursor`, `system_conf`), and `TokioConnectionProvider` is a
   `hyper-hickory` type, not a hickory-resolver one. Correct import:
   `use hickory_resolver::net::runtime::TokioRuntimeProvider;` (via the root re-export
   `hickory_net as net`). Corrected in **DNS + GeoIP**.

3. **The DNS report's own hedge laundered that hallucination.** It wrote *"the doc
   examples and the trait bound name disagree cosmetically… confirm with `cargo check`"*
   — telling the reader the code was probably fine. There is no disagreement:
   `ConnectionProvider` is the trait, `TokioRuntimeProvider` the concrete impl,
   `TokioConnectionProvider` a fabricated conflation. The hedge was the most misleading
   sentence in the document. **Deleted.**

### False claims (not compile errors, but wrong)

4. **The reqwest Socks4 `panic!` "landmine" is dead code.** The HTTP report warned that
   `basic_auth`/`custom_http_auth`/`headers` `panic!("Socks4 is not supported for this
   method")`. That `panic!` sits inside a `/* */` **block comment** (~L620–757 of
   [proxy.rs](https://docs.rs/reqwest/0.13.4/src/reqwest/proxy.rs.html)) in an
   `impl ProxyScheme` block. The **active** `impl Proxy` methods (~L280–346) contain no
   panic or `unreachable`. **The stated landmine does not exist in 0.13.4. Paragraph
   deleted.**

5. **`TokioResolver` does exist.** The DNS report's flags table claimed it wasn't
   documented. It's a crate-root type alias
   (`pub type TokioResolver = Resolver<TokioRuntimeProvider>;`, `tokio` feature):
   https://docs.rs/hickory-resolver/0.26.1/hickory_resolver/type.TokioResolver.html
   "Not documented on the `Resolver` *page*" is technically true and substantively
   misleading — type aliases never render on the aliased struct's page. **Row removed
   from the table; the alias is now documented as existing.**

6. **"GeoLite users are limited to 30 database downloads per day" is not EULA text.**
   The GeoLite report placed it inside its §1 EULA block, implying it was a clause. The
   EULA has no daily-download clause (§6(4) only says MaxMind "may limit the number of
   queries"). The figure is real and comes from the dev portal. **Moved out of the EULA
   quotation and re-attributed.**

7. **"~450 weekly updates since 2017" is a computed estimate, not a fact.** (8.9y × 52 ≈
   463.) Presented as fact. **Now marked UNVERIFIED and hedged.** Immaterial — 8.9 years
   vs 30 days needs no precision.

8. **Uncited speculation about MaxMind's drafting intent removed.** The original wrote
   *"MaxMind appears to have drafted this deliberately to neutralize the CC
   permissions."* No source. The argument doesn't need it. **Deleted.**

### The first pass *understated* its own case (verifier strengthened it)

9. **EULA §1's precedence clause was missed entirely** — *"This Agreement controls in
   the event of any conflict with the above-referenced documents"*, with the Creative
   Commons License ranked last. This is more decisive against the CC-carve-out theory
   than all three of the report's own arguments combined. The report called the question
   "genuinely ambiguous"; with §1 on the table it is considerably less ambiguous, and
   against the carve-out. **Added, and it now leads the licensing section.**

10. **The 30-day duty was inferred from §6.3 legalese when MaxMind states it in plain
    English** on the dev portal: *"you must delete GeoLite databases within 30 days of a
    new release."* Far more citable. **Added.**

### Incompleteness / imprecision (fixed)

11. **hickory `config` module listing was not exhaustive** — omitted
    `OpportunisticEncryptionConfig`, `OpportunisticEncryptionPersistence` (structs) and
    `OpportunisticEncryption` (enum), while presented as complete. **Completed.**

12. **maxminddb features listing understated** — 4 flags plus `default`
    (`default`, `memmap2`, `mmap`, `simdutf8`, `unsafe-str-decode`), not 3. The
    load-bearing "0 enabled by default" claim was correct. **Completed.**

13. **tokio-socks signatures omitted the target bound.** Actual:
    `connect_with_socket<'t, T>(socket: S, target: T) -> Result<Socks5Stream<S>>`
    where `T: IntoTargetAddr<'t>` (`S: AsyncSocket + Unpin` is on the impl block).
    **Added.**

14. **The tokio-socks-vs-fast-socks5 bound difference is cosmetic, not substantive.**
    `AsyncSocket` is a blanket impl over `AsyncRead + AsyncWrite`. The HTTP report framed
    it as a differentiator. **Reframed** — the error enum, not the bounds, is the real
    deciding factor.

15. **EULA section numbering:** disclosure is §6.1, destruction §6.3; the first pass
    lumped both as "§6". **Fixed.**

### What the adversarial pass could NOT refute

- The GeoLite2 core conclusion: **the 30-day destruction clause is fundamentally
  incompatible with an immutable package registry, independent of the CC debate.**
  Verified against MaxMind's own plain-language statement.
- "Attribution alone does not suffice." Correct.
- The DB-IP CC BY 4.0 recommendation, and the BY-vs-BY-SA distinction. Correct.
- The report did **not** confuse the EULA with the pre-Dec-2019 standalone CC license —
  the trap the verification was designed to catch. It correctly identified CC BY-SA as
  *incorporated by reference inside* the current EULA.
- The HTTP thesis (split reqwest/hyper; tokio-socks for the checker; custom
  `ServerCertVerifier`). Every hyper signature — including the two predicted to be
  hallucinated (`handshake` bounds, `Upgraded::downcast`/`read_buf`) — is exact, and the
  `with_upgrades`-on-`Connection`-not-`Builder` detail is precisely the 0.14-vs-1.x trap,
  correctly avoided. `tokio-rustls`'s `TlsConnectorWithAlpn` is real.
- The DNS/GeoIP substantive flags: `NameServerConfigGroup` really is gone, `build()`
  really returns `Result`, the error type really is `NetError`, maxminddb's two-stage
  `lookup().decode()` and lowercase-b `MaxMindDbError` are real, `iso_code` really is
  `Option<&'a str>`, `public-ip` really is stuck at 0.2.2 (2022-01-07).

---

## Open questions

1. **The architecture report was never adversarially verified.** Its Python-side
   measurements (38 registry entries, 18 bare, ~4 needing real code; `proxy.py:333`;
   `checker.py:208–246`; `api.py`'s `stop_broker_on_sigint`) and fetched precedent
   manifests are single-source. Nothing in it is high-risk, but nothing in it was
   double-checked either.

2. **`moka::future::Cache::try_get_with`** — signature and the "does not cache errors"
   semantics were not verified against docs.rs. Confirm before relying on the error
   behavior. Moot if hickory's `ResponseCache` proves sufficient (the recommended first
   move).

3. **`maxminddb::geoip2::Country<'a>`'s top-level field list** — unverified. The nested
   `country::Country` fields *are* verified, which is what the lookup path actually
   touches.

4. **`maxminddb::Reader::open_mmap` signature** — not in the default-features docs build,
   so unverified. Sidestepped by `from_source(mmap)`, which needs no feature flag.

5. **fast-socks5's server module** (`run_tcp_proxy`) and the claim **"tokio-socks is
   client-only"** — unverified. Only matters if the local proxy must speak SOCKS5 to
   clients.

6. **IPinfo Lite's license** — search-verified only; the primary license document was
   never fetched. Moot under the DB-IP recommendation.

7. **hickory's provider type path** — `hickory_resolver::net::runtime::TokioRuntimeProvider`
   is the verifier's correction, derived from the crate's own doc examples; the docs.rs
   URL 404s because re-exports render under the origin crate (`hickory-net`). This should
   `cargo check` clean, but it's the one import worth confirming on the first build.

8. **Provider liveness.** Which of the 38 providers still work in 2026 is unmeasured. The
   yield comments (`# 49`, `# 5500`) are ~2018 estimates. Probe before porting.

9. **DB-IP Lite accuracy delta vs GeoLite2** — unquantified. Mitigated by the
   user-supplied-path override, but if country accuracy turns out to be a hard
   requirement for some consumer, that's a product decision, not a research finding.

10. **No court test of the CC-carve-out theory, and no evidence of MaxMind enforcement
    against node-geolite2-redist.** Absence of enforcement is not permission. This is the
    residual legal uncertainty, and it's why the recommendation routes around MaxMind
    entirely rather than betting on the theory.
