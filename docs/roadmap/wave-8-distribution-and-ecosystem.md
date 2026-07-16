# Wave 8 — Distribution & ecosystem

*Packaging and integrations. The theme: make the finished engine reachable — as a single
static binary, as a `FROM scratch` container, as a drop-in Rust connector, and as an MCP tool
surface — without adding speculative abstraction. The two "wait for a real consumer" features
(E1, E4) live here on purpose and carry explicit **gates**.*

This wave touches almost no core logic. Everything below is a thin veneer over surfaces that
already exist: `server::Pool` (`src/server.rs:55`), `negotiator::{negotiate, Stream, Target}`
(`src/negotiator.rs:100,46,36`), `geo::GeoDb::bundled` (`src/geo.rs:24`, `include_bytes!`), and
the release matrix (`.github/workflows/release.yml`).

## Build order (respects dependencies)

1. **Shared refactor — lift `choose_proto` to `pub(crate)`** (`src/server.rs:269`). It is the
   proxy-protocol selection E1's connector needs verbatim; today it is a private `fn` in
   `server.rs`. Move it to `pub(crate) fn choose_proto(proxy: &Proxy, scheme: Scheme) -> Proto`
   (same file, or a new `pub(crate) mod pool_select` if E1/E4 both want it without the `server`
   feature — see E1). One commit, no behaviour change; the existing `server` tests stay green.
2. **D1 — Static musl binary + installer + Docker.** No library code; pure packaging/CI. Land
   first — it validates the whole crate builds static, and is decoupled from everything else.
3. **P1 — More bundled providers + dead-source curation.** Pure data + fixtures. Independent of
   D1; slot it here so the fatter provider set ships in the same release D1 cuts.
4. **E1 — `RotatingProxyConnector`.** Gated on a concrete consumer; the offline integration
   test *is* that consumer (see gate). Needs the shared refactor.
5. **E4 — MCP server.** Independent of E1. Needs three small `Pool` methods (specified below).

Rationale for D1/P1 before E1/E4: the packaging + data features are zero-risk and unblock a
release; the two ecosystem features are version-coupled and each pin a churny dependency, so
they come last and each stay feature-gated and off the default build.

---

## D1 — Static musl binary + shell installer + `FROM scratch` Docker

**Goal.** Ship a fully static `x86_64-unknown-linux-musl` (and `aarch64-...-musl`) binary, a
`curl … | sh` installer that fetches the right release asset, and a `FROM scratch` Docker image
— exploiting that all runtime data is embedded (`geo.rs` `include_bytes!`, `providers.yaml`
`include_str!` at `provider.rs:110`), so the container needs no filesystem at all.

**Public surface.** No library API change. New artifacts:
- Release assets `proxybroker-<tag>-x86_64-unknown-linux-musl.tar.gz` (+ aarch64), added to the
  matrix in `.github/workflows/release.yml`.
- `install.sh` at the repo root: `curl -fsSL https://raw.githubusercontent.com/<repo>/main/install.sh | sh`.
  Flags via env: `PROXYBROKER_VERSION` (default: latest release), `PROXYBROKER_BIN_DIR`
  (default `$HOME/.local/bin`). Detects `uname -s`/`uname -m`, maps to a target triple, downloads
  the matching `.tar.gz` + `.sha256`, verifies the checksum, extracts the binary + the licence
  files that already travel with each archive (`LICENSE,LICENSE-DATA,NOTICE`, per the existing
  `include:` at `release.yml:183`).
- `Dockerfile` at the repo root: multi-stage, `FROM scratch` final. Published image documented
  in the README; not auto-pushed unless a registry secret exists (out of scope for this spec —
  the Dockerfile + a `docker build` CI smoke test is the deliverable).

**Design.**
- **Matrix.** Extend the `binaries` job's `matrix.include` (`release.yml:139`) with two musl
  targets. The existing jobs use native runners with `taiki-e/upload-rust-binary-action@v1`,
  which supports musl via the `cross`/musl-tools toolchain. The friction the roadmap flags is
  real: **reqwest's `rustls` backend pulls `aws-lc-rs`** (stated verbatim in the comment at
  `release.yml:132`), whose `-sys` crate compiles C/asm and is painful under musl. The crate's
  *own* TLS is already `ring` (`Cargo.toml:47-48`: `rustls`/`tokio-rustls` both pinned with the
  `ring` feature, no aws-lc), so the only aws-lc pull is transitive through reqwest.
  - **Mitigation (recommended, resolves an Open Question):** switch reqwest to its ring-backed
    rustls feature so the whole dependency graph is ring-only, which cross-compiles to musl with
    just `musl-tools` + `CC_x86_64_unknown_linux_musl=musl-gcc`. Verify the exact reqwest 0.13
    feature name **against Cargo, not memory** (the crate already warns about this at
    `Cargo.toml:40`: "0.13 renamed the 0.12-era `rustls-tls` feature to `rustls`"). If reqwest
    0.13 exposes no ring option, fall back to installing NASM/cmake + the musl cross toolchain in
    the CI step (heavier, but the native-runner pattern already installs NASM for Windows at
    `release.yml:161`).
  - **`cargo-dist` note (roadmap constraint):** `cargo dist` would generate this matrix + the
    installer for us and owns the musl toolchain wiring. It is the lower-maintenance path if the
    hand-rolled matrix proves brittle; recorded as the escalation, not adopted up front (lazy-
    that-holds — we already have a working `release.yml`, so we extend it before replacing it).
- **Static linking.** musl targets link static libc by default; confirm no `openssl-sys` sneaks
  in (there is none today — TLS is rustls). The geo DB and providers are compiled in, so the
  binary has zero runtime file dependencies — the property the scratch image relies on.
- **Dockerfile.** Stage 1: `rust:alpine` or the musl builder, `cargo build --release --locked
  --target x86_64-unknown-linux-musl`. Stage 2: `FROM scratch`, `COPY --from=build
  /…/proxybroker /proxybroker`, plus the three licence files (CC BY 4.0 attribution must travel
  with the embedded DB-IP data — same duty enforced for the tarballs at `release.yml:183` and in
  `--version` at `bin/proxybroker.rs:28`). `ENTRYPOINT ["/proxybroker"]`.
- **Installer safety.** `set -eu`, checksum verification before `install`, no `sudo`, writes to a
  user dir by default. Print the attribution line on success.

**Offline test plan.** Packaging has little unit surface; the tests are build-shaped and run in
CI without network to a registry:
- **First failing test:** a CI job `musl-smoke` (new workflow or a job in `ci.yml`) that runs
  `cargo build --locked --target x86_64-unknown-linux-musl --no-default-features --features
  cli,server,geo,geo-bundled` and then executes the binary with `--version`, asserting exit 0
  and that the output contains `IP Geolocation by DB-IP`. This fails today (target not installed
  / aws-lc musl break) and passes once the toolchain + backend are sorted.
- **Dockerfile smoke:** a CI step `docker build -t pb-scratch . && docker run --rm pb-scratch
  --version` asserting the same string — proves the scratch image is self-contained (no libc, no
  data files) offline.
- **Installer shellcheck:** `shellcheck install.sh` in CI (static, offline). A functional test of
  the download path needs the network and is therefore **not** a unit test — it is exercised
  implicitly by the first real release, matching the project's "liveness is a CI audit, not a
  unit test" stance.

**Acceptance criteria.**
- [ ] `release.yml` matrix builds `x86_64-` and `aarch64-unknown-linux-musl` tarballs with the
      licence files included, checksummed, attached to the Release.
- [ ] The whole dependency graph is ring-only under musl (or the musl cross toolchain is wired),
      verified by a green `cargo build --target …-musl --locked`.
- [ ] `install.sh` detects OS/arch, verifies the checksum, installs to `$HOME/.local/bin`, and
      passes `shellcheck`.
- [ ] `Dockerfile` produces a `FROM scratch` image that runs `proxybroker --version` with no
      runtime data files, verified by a `docker build`+`docker run` CI step.
- [ ] `cargo test --all-features --locked` (the existing release gate at `release.yml:78`) still
      green — no code changed.

**Risks / deviations / principle-flags.**
- ⚠ **musl + aws-lc-rs cross-compile** (roadmap C-flag). Mitigation: switch reqwest to ring so
  the graph is ring-only; escalate to `cargo dist` if hand-rolling the toolchain is brittle.
- ⚠ **CC BY 4.0 data hygiene.** The static binary embeds the DB-IP database; the installer,
  tarballs, and Docker image **must** carry `LICENSE-DATA` + `NOTICE`. This is already enforced
  for tarballs; D1 must not drop it for the new artifacts.
- No speculative abstraction: no config, no plugin system — two targets, one script, one
  Dockerfile.

**Effort.** S–M (roadmap: S). The musl/aws-lc yak-shave is the only thing that can push it to M.

---

## P1 — More bundled providers (12 → ~50) + dead-source curation

**Goal.** Grow `data/providers.yaml` from 12 live entries to ~50, each carrying a recorded HTML/
text fixture and an offline parse test, and drop entries proven dead in a curation pass — so the
default `grab`/`find` yield rises without adding a network dependency to the test suite.

**Public surface.** No API change. `data/providers.yaml` gains entries in the existing
`ProviderSpec` YAML shape (`provider.rs:22`): `url`, optional `protocols`, optional `pattern`,
optional `timeout`. New test fixtures under `tests/fixtures/providers/`.

**Design.**
- Extraction already generalizes across formats: the default whole-text `IP:port` scanner
  (`parse::find_addrs_global`, wired at `provider.rs:78`) subsumes plain-text lists and HTML
  tables, and a bespoke source supplies a 2-group `pattern` (`provider.rs:228`
  `extract_with_pattern`). So **most new providers are one YAML entry with no code**; only a
  source whose layout defeats the global scanner needs a `pattern`.
- Curation: the current file (`data/providers.yaml`) documents that proxybroker2's other ~25
  entries were dead as of 2026-07-15. The pass re-checks the current 12 for liveness (CI audit,
  below), removes any newly-dead, and adds candidates from the free-proxy-list ecosystem
  (geonode, proxy-list.download, openproxylist, proxyspace, etc. — each verified to *parse* a
  recorded fixture; liveness verified separately).
- Each addition ships:
  1. a `data/providers.yaml` entry;
  2. a `tests/fixtures/providers/<slug>.txt|.html` — a **trimmed** recorded response (a handful
     of real rows, no PII beyond public proxy IPs, no copyrighted page chrome — strip to the
     address table);
  3. coverage by the generic fixture test below.

**Offline test plan.**
- **First failing test:** `tests/providers_fixtures.rs::every_bundled_provider_parses_its_fixture`
  — iterate `bundled_registry()` (`provider.rs:109`); for each spec that has a fixture file named
  by a deterministic slug of its `url`, load the fixture, call `spec.extract(&body)`, and assert
  it yields ≥1 `Candidate` with a canonical host and non-zero port. Fails first because the
  fixtures + the harness do not yet exist. This is a pure-function test over recorded bytes — no
  sockets — extending the in-module pattern at `provider.rs:255` (`extracts_from_plain_text_list`)
  to file-backed fixtures.
- **Registry integrity test:** `bundled_registry_is_valid_and_deduplicated` — asserts
  `bundled_registry()` parses (already implicitly covered by the `.expect` at `provider.rs:111`),
  that every entry has a non-empty `url`, and that URLs are unique. Guards the curation pass
  against accidental dupes.
- **Liveness is NOT a unit test.** A `providers-audit` CI job (scheduled, `workflow_dispatch`)
  fetches each `url` and reports zero-yield sources for manual curation. It is allowed to fail /
  be flaky and must never gate a PR — exactly the roadmap's "liveness as a periodic CI audit"
  rule. It lives in its own workflow, not `ci.yml`.

**Acceptance criteria.**
- [ ] `data/providers.yaml` has ~50 entries, each either scanner-parsable or with a working
      `pattern`.
- [ ] Every bundled entry with a recorded fixture passes `every_bundled_provider_parses_its_fixture`
      offline.
- [ ] Dead entries from the prior set removed; a comment records the audit date (as the current
      header does).
- [ ] A `providers-audit` workflow exists, is scheduled, and does not gate PRs.
- [ ] `cargo publish --dry-run` still packages `data/providers.yaml` (`Cargo.toml:19` `include`).

**Risks / deviations / principle-flags.**
- ⚠ **Offline-testable** (roadmap C-flag). Enforced by recorded fixtures; no test may hit a
  provider URL.
- ⚠ **Fixture hygiene.** Trim fixtures to the address rows; do not commit full copyrighted pages.
  Public proxy IPs only.
- Lazy-that-holds: no per-provider parser classes — the scanner + optional `pattern` already
  cover the space (`provider.rs` module doc, critique #36).

**Effort.** M, ongoing — dominated by finding and trimming ~40 fixtures, not by code.

---

## E1 — `RotatingProxyConnector` (drop-in reqwest/hyper connector)

**Goal.** A `tower_service::Service<Uri>` that, per outbound connection, checks out a healthy
proxy from a `Pool`, negotiates the tunnel via the existing `negotiate`, retries on a different
proxy when one fails, and hands hyper the negotiated `Stream` — so a Rust program gets pooled,
self-healing proxy rotation with **no local server and no port**.

> **GATE (roadmap + project principle).** hyper 1.x `Connect` + TLS layering is version-coupled,
> and the roadmap says *do not build the trait surface until there is a concrete consumer.* The
> concrete consumer that unlocks this feature is the **offline integration test** below: a real
> `hyper_util::client::legacy::Client` making a request through the connector to a mock upstream.
> If that test can be written and passes, the seam is proven and E1 ships. If it cannot (hyper/
> hyper-util trait mismatch we can't satisfy without speculative glue), E1 **stays gated** — we
> record the blocker and stop, rather than inventing a trait with one hypothetical impl.

**Public surface (feature-gated: new `connector` feature → `dep:tower-service`).**
```rust
// src/connector.rs — behind feature = "connector"
pub struct RotatingProxyConnector { /* Arc<Pool>, Arc<Resolver>, RotateConfig */ }

#[derive(Debug, Clone)]
pub struct RotateConfig {
    /// Proxies to try (each a different checkout) before returning an error. Default: 3.
    pub max_tries: usize,
    /// Per-connection negotiation timeout. Default: 8s.
    pub timeout: std::time::Duration,
}
impl Default for RotateConfig { /* max_tries: 3, timeout: 8s */ }

impl RotatingProxyConnector {
    /// Core constructor: wrap an existing pool + resolver. This is the honest seam — the pool
    /// must already be fed (via `Pool::spawn`/`from_proxies`).
    pub fn from_pool(pool: std::sync::Arc<Pool>, resolver: std::sync::Arc<Resolver>, cfg: RotateConfig) -> Self;
}

// tower_service::Service<http::Uri> for RotatingProxyConnector
//   type Response = ProxyConn;          // wraps negotiator::Stream for hyper
//   type Error    = std::io::Error;
//   type Future   = Pin<Box<dyn Future<Output = Result<ProxyConn, io::Error>> + Send>>;
```
- **Re-exports** in `lib.rs` behind `#[cfg(feature = "connector")]`:
  `pub use connector::{RotatingProxyConnector, RotateConfig};`
- **`broker.rotating(...)` sugar — Open Question, not v1.** The roadmap's
  `Client::builder().connect_via(broker.rotating())` is aspirational: **reqwest 0.13 does not
  expose a custom-connector hook** (verify against reqwest, not memory — its `Client` wraps a
  hyper-util connector it does not let you replace). So the real drop-in target is
  `hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector)`, not
  reqwest. A `Broker::rotating(query) -> impl Future<Output = Result<RotatingProxyConnector>>`
  convenience (spawn a pool via `self.find(query)`, wrap it) is a thin add *if* a consumer wants
  it — deferred until then. Document the reqwest limitation in the module doc so no one expects
  `connect_via`.

**Design (grounded).**
- The per-connection loop is **exactly** `server::handle_client`'s retry loop
  (`server.rs:242-264`): `for _ in 0..max_tries { let Some(mut proxy) = pool.get(scheme).await …;
  let proto = choose_proto(&proxy, scheme); … match relay { Ok => put; Err => record_attempt +
  put (self-ejecting via PoolConfig) } }`. E1 reuses:
  - `Pool::get(scheme)` (`server.rs:105`) for healthy checkout (best-priority, `total_cmp`
    ordered);
  - the shared `choose_proto` (lifted to `pub(crate)` in the shared refactor);
  - `negotiate(proto, tcp, &target, timeout)` (`negotiator.rs:100`) to build the tunnel;
  - `Pool::put(proxy)` (`server.rs:127`) which drops the proxy when it crosses the health
    thresholds — **this is the "ejects dead ones" behaviour, already implemented**;
  - `Proxy::record_attempt` (`proxy.rs:153`) for the error/timing histogram that feeds eviction.
- **Uri → routing.** `uri.scheme_str()` → `Scheme::Https` for `https`, `Scheme::Http` for `http`;
  `uri.host()` + `uri.port_u16().unwrap_or(default)` → `Target { host, ip: resolver.resolve(host)
  .await.ok(), port }` (same construction as `server.rs:236`).
- **Response type.** hyper-util's `Client` requires the connector's response to implement
  `hyper::rt::Read + Write` and `hyper_util::client::legacy::connect::Connection`. Wrap the
  negotiated `negotiator::Stream` (which is `AsyncRead + AsyncWrite + Unpin`, `negotiator.rs:60`)
  in `hyper_util::rt::TokioIo` and impl `Connection::connected()` → `Connected::new()`. This is
  the version-coupled seam — **pin `hyper = 1.10` and `hyper-util = 0.1`** (already the
  `Cargo.toml:43-44` pins) and test against exactly those.
- **TLS-to-target is out of scope for v1.** The connector returns the tunnel to `target`
  host:port. For an `https://` URL the caller layers their own TLS (or uses hyper's HTTPS
  connector on top). Do **not** re-use the checker's `AcceptAllVerifier` (`negotiator.rs:284`) —
  that verifier is correct *only* for proxy-liveness testing and would be a security hole for
  real client traffic. The `Https` scheme here means "the proxy must support tunnelling"
  (`choose_proto` picks a CONNECT/SOCKS proto), not "we terminate TLS." Documented in the module
  doc; expanding to terminate-and-verify TLS is a later feature with its own consumer.
- **Feature gating.** `connector` feature adds only `tower-service` (tiny; already in hyper-util's
  tree). hyper/hyper-util are already non-optional deps used by the checker, so no new heavy dep.

**Offline test plan (this test IS the gate's concrete consumer).**
- **First failing test:** `tests/rotating_connector.rs::client_routes_through_pooled_proxy` —
  1. Start a mock upstream proxy on `127.0.0.1:0` that speaks plain HTTP and returns a fixed body
     (reuse the `mock_upstream` shape from `tests/serve.rs:18`).
  2. Build `Pool::from_proxies(vec![http_proxy_at(upstream)], PoolConfig::default())`
     (`tests/serve.rs:48` pattern) and a `Resolver::new(3s)`.
  3. `let connector = RotatingProxyConnector::from_pool(pool, resolver, RotateConfig::default());`
  4. `let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector);`
  5. `client.get("http://1.2.3.4/".parse().unwrap()).await` → assert 200 and the fixed body.
  Fails first because `src/connector.rs` does not exist. Fully offline (all sockets on
  127.0.0.1), matching constraint C5.
- **Retry/eject test:** `ejects_failing_proxy_and_retries` — pool of two proxies where the first
  is a dead `127.0.0.1:0`-bound-then-closed socket; assert the request still succeeds via the
  second and that the connector attempted ≤ `max_tries` times (observe via a request counter on
  the mock, or via `Pool` emptiness afterward).
- **Exhaustion test:** empty pool → the `Service::call` future resolves to an `io::Error`
  (mirrors the 502 path at `server.rs:245`, but as an error to the hyper client, not an HTTP
  response — the connector layer has no HTTP response to write).

**Acceptance criteria.**
- [ ] `RotatingProxyConnector` impls `tower_service::Service<http::Uri>` and drives a real
      `hyper_util::client::legacy::Client` to a 200 through a mock proxy, offline.
- [ ] Retries on a different proxy on failure; dead proxies self-eject via `Pool::put`'s existing
      threshold logic (no new eviction code).
- [ ] `choose_proto` is shared with `server.rs` (one definition), not duplicated.
- [ ] Feature-gated (`connector`), off by default; `cargo build` with default features unchanged.
- [ ] Module doc states the reqwest-`connect_via` limitation and the TLS-to-target scope
      boundary, and does not reuse `AcceptAllVerifier`.

**Risks / deviations / principle-flags.**
- ⚠ **Version-coupled hyper Connect + TLS** (roadmap C-flag). Mitigation: pin hyper 1.10 /
  hyper-util 0.1 (already pinned), test against them, document the `TokioIo`/`Connection` seam,
  and **gate on the offline integration test** — if it can't be written, stop.
- ⚠ **No speculative abstraction.** The `tower_service::Service` surface is the *minimum* hyper
  demands from a connector — it has a real consumer (the test's hyper client), so it is not a
  one-impl trait invented on spec. `broker.rotating()` sugar and TLS-to-target are explicitly
  deferred until a consumer pulls them.
- ⚠ **Security.** Must not leak the proxy-testing `AcceptAllVerifier` into real client traffic.
  Flagged in the design; enforced by keeping TLS-to-target out of v1.
- **Open Question:** reqwest 0.13 custom-connector support — verify against the crate; if it
  gained one, add a reqwest example. Until verified, the documented target is hyper-util.

**Effort.** M (roadmap: M). Most of it is nailing the `Connection`/`TokioIo` seam against the
pinned hyper.

---

## E4 — MCP server (`proxybroker mcp`)

**Goal.** A stdio MCP server exposing three tools — `get_proxy(scheme, country)`,
`pool_status()`, `report_dead(proxy)` — as a thin veneer over a live `Pool`, so agent tooling can
pull healthy proxies and feed failures back into the same eviction machinery.

**Public surface.**
- **CLI:** a new subcommand `proxybroker mcp` (behind `feature = "mcp"`, and — like `serve` —
  `cfg(feature = "server")` since it needs `Pool`). Args mirror `ServeArgs`
  (`bin/proxybroker.rs:75`): `--types` (required), `--limit` (default 100), `--countries`,
  `--timeout`, `--max-error-rate`, `--max-resp-time`. It builds a `Pool` via `broker.find(...)` +
  `Pool::spawn` (identical to `serve_cmd` at `bin/proxybroker.rs:276-296`), then serves MCP over
  stdio instead of binding a TCP listener.
- **Tools (tiny, fixed surface):**
  - `get_proxy(scheme: "http"|"https", country?: string) -> { proxy: "host:port", types: [...],
    avg_resp_time, error_rate }` — checks out the best proxy for the scheme (and optional country),
    returns its address + metadata.
  - `pool_status() -> { total, working, by_protocol, by_country, avg_resp_time, errors }` — a
    `Stats` snapshot of the live pool.
  - `report_dead(proxy: "host:port") -> { removed: bool }` — removes that proxy from the pool
    (the "closes back into the pool" path).
- **Three small `Pool` methods** (new, in `src/server.rs`, `pub` behind `server`) — the veneer
  needs read/remove access the current `get`/`put` don't provide:
  ```rust
  impl Pool {
      /// A non-consuming snapshot for status/selection preview. Clones under the lock.
      pub fn snapshot(&self) -> Vec<Proxy>;
      /// Best proxy for a scheme, optionally filtered to a country code; checks it out
      /// (like `get`) but returns immediately (None) instead of waiting on the importer.
      pub fn try_get(&self, scheme: Scheme, country: Option<&str>) -> Option<Proxy>;
      /// Remove a proxy by address; true if one was present. Feeds `report_dead`.
      pub fn remove(&self, addr: std::net::SocketAddr) -> bool;
  }
  ```
  `snapshot` + `Stats::from_proxies` (`stats.rs:34`) give `pool_status` for free. `try_get`
  generalizes `best_for` (`server.rs:141`) with a country predicate over `Proxy::geo`
  (`proxy.rs:32`). Keep the tool handler thin: `get_proxy` clones the checked-out proxy's addr for
  the response and immediately `put`s it back (so it stays available and rotates by priority),
  matching "thin veneer over Pool"; `report_dead` calls `remove`.

**Design.**
- **rmcp SDK, pinned** (roadmap C-flag: rmcp churns). Add `rmcp = "=<pinned>"` (exact-version pin)
  behind `feature = "mcp"`. Verify the current tool-registration + stdio-transport API **against
  the crate/docs, not memory** (use context7 / the rmcp examples) — the derive-macro and
  `ServiceExt::serve` surface have moved across 0.x releases. Keep the tool surface to exactly the
  three above; no resources, no prompts, no dynamic tool list — the smallest thing that holds.
- **New module `src/mcp.rs`** behind `#[cfg(feature = "mcp")]`, holding the tool struct that owns
  `Arc<Pool>` (+ `Arc<Resolver>` if a future tool needs it) and the three handlers. `main`/`run`
  in `bin/proxybroker.rs` gains a `Command::Mcp(McpArgs)` arm (guarded `cfg(all(feature = "mcp",
  feature = "server"))`) that constructs the pool and calls `proxybroker::mcp::serve_stdio(pool)`.
- **`report_dead` semantics.** "Closes back into the pool" = the reported proxy is removed so the
  next `get_proxy` won't hand it out again. Implement as `Pool::remove(addr)`. (We do *not* try to
  `record_attempt` an external failure and re-`put` — the agent used the proxy out-of-process, so
  the honest action is removal, not synthesizing a fake error into the histogram.)
- **Serialization.** Reuse `Proxy`'s existing `Serialize` (`proxy.rs:180`) for the metadata shape,
  or a small purpose-built response struct — pick the smaller once the rmcp result type is known
  (Open Question below). `Scheme` parses from the `"http"/"https"` string.

**Offline test plan.**
- **First failing test:** `tests/mcp_pool.rs::get_proxy_returns_best_and_report_dead_removes_it` —
  build `Pool::from_proxies(vec![fast, slow], PoolConfig::default())` (the `tests/serve.rs:48`
  pattern), then test the **handlers directly** (not through a spawned stdio transport):
  `mcp::handle_get_proxy(&pool, Scheme::Http, None)` returns the lower-priority (`fast`) proxy's
  addr; `mcp::handle_report_dead(&pool, fast.addr())` returns `removed: true`; a second
  `handle_get_proxy` returns `slow`; `handle_pool_status(&pool)` reports `total` decremented.
  Fails first because `src/mcp.rs` + the `Pool` methods don't exist. No network — pure pool
  manipulation, offline (C5).
- **Pool method units** (in `server.rs` `#[cfg(test)]`, extending the existing `best_for` tests at
  `server.rs:406`): `try_get_filters_by_country`, `remove_by_addr_returns_false_when_absent`,
  `snapshot_does_not_consume`.
- **stdio round-trip (optional, gated):** if rmcp exposes an in-memory duplex transport, one test
  that drives `initialize` + a `tools/call` for `pool_status` over it. If it only offers real
  stdio, skip it (don't fork a subprocess in a unit test) — the handler-level tests are the
  contract, keeping the suite offline and fast. The thin veneer means the rmcp glue is nearly
  logic-free, so testing the handlers is sufficient.

**Acceptance criteria.**
- [ ] `proxybroker mcp` builds behind `--features mcp,server` and exposes exactly three tools.
- [ ] `Pool::{snapshot, try_get, remove}` added, with unit tests; `get`/`put`/`best_for` behaviour
      unchanged.
- [ ] Handler tests pass offline: `get_proxy` returns best-priority, `report_dead` removes,
      `pool_status` reflects the change.
- [ ] `rmcp` pinned to an exact version; tool surface is the three tools only.
- [ ] Feature-gated and off by default; default `cargo build` and `cargo test` unchanged.

**Risks / deviations / principle-flags.**
- ⚠ **rmcp churn** (roadmap C-flag). Mitigation: exact-version pin, verify the API against the
  crate (context7), tiny fixed tool surface, logic in testable free functions so an rmcp bump only
  touches the transport glue.
- ⚠ **No speculative abstraction.** Three tools, one `Pool`, no config beyond the pool-fill args
  that `serve` already has. The three new `Pool` methods are the minimum the tools need — each has
  a caller.
- **Deviation from a naive port:** `report_dead` *removes* rather than logging a synthetic error,
  because the failure happened out-of-process; recorded here.
- **Open Question:** whether to reuse `Proxy`'s `Serialize` or a bespoke MCP result struct —
  decide once the pinned rmcp result/`Content` type is known (it may want a flat JSON object).

**Effort.** M (roadmap: M), gated mostly by getting the pinned rmcp registration API right.

---

## What must stay green

No Wave 8 feature changes core behaviour; each is additive and (E1/E4) feature-gated. The
following existing guarantees must not regress:

- **Server relay + eviction** — `tests/serve.rs` (`server_relays_http_request_through_a_pool_proxy`,
  `server_returns_502_when_pool_is_empty`) and the `best_for` unit tests
  (`server.rs:406-434`). The shared-refactor lift of `choose_proto` and E4's new `Pool` methods
  must leave `get`/`put`/`best_for` byte-identical in behaviour.
- **Negotiation** — `tests/negotiate_connect.rs` and the `connect_request` byte tests
  (`negotiator.rs:324-365`). E1 consumes `negotiate`/`Stream` unchanged.
- **Provider extraction** — `provider.rs` in-module tests (`extracts_from_*`,
  `matches_python_pipeline_on_messy_input`, `load_provider_dir_*`) and
  `tests/provider_fetch.rs`. P1 only *adds* entries + fixtures; the extraction pipeline is
  untouched, and `bundled_registry()` must still parse (`provider.rs:111` `.expect`).
- **Geo** — `geo.rs` `bundled_db_resolves_known_ips`. D1's static/musl build must keep the
  `include_bytes!` DB embedded and the `--version` attribution line intact
  (`bin/proxybroker.rs:28`).
- **Error/stats contract** — `error.rs` `errmsg_strings_match_python_byte_for_byte`,
  `proxy.rs` serialize/round2 tests, `stats.rs`. E4's `pool_status` reads `Stats` but must not
  alter the histogram strings or the `Serialize` shape.
- **Release gate** — `cargo test --all-features --locked` (`release.yml:78`) and
  `cargo package --locked` (`release.yml:98`) stay green; the `include` set (`Cargo.toml:19`)
  still carries `data/*`, `LICENSE-DATA`, `NOTICE`.
- **Default build unchanged** — `default = ["cli","server","geo","geo-bundled"]`
  (`Cargo.toml:22`). `connector` and `mcp` are new opt-in features; nothing in the default set
  gains a dependency except (D1) the possible reqwest ring-backend feature swap, which is
  behaviour-neutral and must keep every existing test green.
