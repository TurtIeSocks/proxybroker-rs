# Wave 6 — Observability

*Instrument what Waves 1–5 built. No new engine behaviour — every number this wave exposes
already lives in `Pool` (`src/server.rs`), `Proxy` (`src/proxy.rs`), or `StatsCollector`
(`src/stats.rs`). The wave's job is to **surface** those numbers on the wires operators use:
Prometheus, a live terminal bar, structured JSON logs, a reproducible benchmark, and (deferred)
a TUI.*

Ground truth this wave leans on, verified against the source:

- `StatsCollector` already records **every** checked proxy (working or not) into the shared
  `Arc<Mutex<StatsCollector>>` in `Broker::find_task` (`src/broker.rs:300`), and
  `ProxyStream::stats()` (`src/broker.rs:446`) hands back a `Stats` snapshot on demand. F2 needs
  nothing from the engine.
- `Pool::put` (`src/server.rs:127`) is the single eviction point — the `unhealthy` branch at
  `src/server.rs:131` `return`s without re-pushing, dropping the proxy. That is where F1's
  eviction counter increments.
- `handle_client` (`src/server.rs:225`) retries on a *different* proxy in its `for _ in 0..max_tries`
  loop; the `Err(e)` arm at `src/server.rs:256` is exactly "rotate to the next proxy". That is
  F1's rotation counter.
- `Checker::check_one` (`src/checker.rs:158`) calls `proxy.record_attempt(...)` at four terminal
  outcomes (`src/checker.rs:173,180,184,188`). Those are F3's structured-event sites.
- The offline check harness (mock judge + mock HTTP proxy on `127.0.0.1`, `tests/check_http.rs`
  `echo_server`) is the mock-socket fixture F5 reuses — no `Connector` trait needed.

## Build order (within the wave)

1. **F3 — structured tracing events.** Smallest, no new dependency (tracing is already a dep),
   touches only `src/checker.rs`. Land first; every later feature benefits from the events.
2. **Shared refactor: `Pool::snapshot()`.** Before F1, add one read-only method on `Pool`
   (`src/server.rs`) that locks `state` once and returns a plain `PoolSnapshot` value (counts by
   scheme, mean error rate, mean resp time, live count). F1 renders it to Prometheus text; F4
   (later) renders it to a table. One method, two consumers — justified, not speculative.
3. **F1 — Prometheus metrics endpoint.** Adds the eviction/rotation atomics to `Pool` and a
   hand-rolled text exporter served on `serve --metrics`. Depends on the snapshot method.
4. **F2 — `--progress` live bar.** Pure CLI + one `tokio::select!` tweak in `src/bin/proxybroker.rs`;
   polls the already-existing `ProxyStream::stats()` on a timer.
5. **F5 — criterion benchmark harness.** Reuses the F3-instrumented check path and the existing
   loopback mock fixture; independent of F1/F2.
6. **F4 — ratatui TUI dashboard.** ⚠ **Blocked on Wave 7 (persistence).** Spec'd here for
   completeness; do not build until `persist.rs` exists to give the dashboard history. Ships last,
   in a later wave.

Feature-gate policy (honours "feature-gate any new optional dependency"): each optional exporter
is its own Cargo feature — `metrics` (F1, no new dep), `progress` (F2, `indicatif`),
`tui` (F4, `ratatui` + `crossterm`). F3 needs no gate (tracing is always present). F5 is
dev-only (`criterion` under `[dev-dependencies]`, a `[[bench]]` target). The default binary stays
lean: none of `progress`/`tui` are in `default`.

---

## F3 — Structured `tracing` events per check outcome

**Goal.** Emit one structured `tracing` event at every terminal check outcome carrying
`addr`, `proto`, `outcome`, and `rtt`, so a JSON-log pipeline can observe per-check results
without scraping human text. `--log-format json` renders the whole log stream as JSON.

**Public surface.**
- No new *library* API. Events use `target: "proxybroker::check"` and structured fields.
- CLI (in `src/bin/proxybroker.rs`, `cli` feature): new global flag
  `--log-format <text|json>` (default `text`). Wired into `init_tracing`
  (`src/bin/proxybroker.rs:389`).
- New Cargo: enable the `json` feature on the already-present `tracing-subscriber` dependency
  (`tracing-subscriber = { version = "0.3", features = ["env-filter", "json"], optional = true }`).
  No new crate.

**Design.**
- In `Checker::check_one` (`src/checker.rs:158`), the `match self.attempt(...)` already branches
  the four outcomes. Emit an event in each arm, right next to the existing
  `proxy.record_attempt(...)` call, with a shared field set:
  ```rust
  tracing::info!(
      target: "proxybroker::check",
      addr = %proxy.addr(),
      proto = proxy_proto.as_str(),      // Proto::as_str, src/types.rs:46
      outcome = "working",                // "working" | "invalid" | "timeout" | "error"
      rtt = rtt_secs,                     // f64 seconds; None-render for non-Working via `?`
      "check outcome",
  );
  ```
  `Working` carries `rtt = start.elapsed().as_secs_f64()` (the same value passed to
  `record_attempt`); `Invalid`/`Timeout`/`Error` carry `rtt = tracing::field::Empty` or the error
  string in an `error` field. Keep the existing `tracing::debug!` DNSBL line
  (`src/checker.rs:110`) as-is.
- `outcome` is a fixed lowercase string set (not the `ProxyError` name) so dashboards can group
  on it; the specific error keeps its byte-for-byte `ProxyError::as_str` (`src/error.rs:56`) in a
  separate `error` field, preserving the stats contract.
- `init_tracing` gains a branch: when `--log-format json`, build `fmt().json()` instead of the
  default text formatter; otherwise unchanged. `EnvFilter` handling stays exactly as it is
  (`src/bin/proxybroker.rs:391`).

**Offline test plan** (no network — pure in-process tracing capture).
- **First failing test:** `tests/tracing_events.rs::check_emits_structured_outcome_event`.
  Reuse the `tests/check_http.rs` `echo_server` mock judge + mock proxy. Install a capturing
  subscriber for the scope of one `checker.check(&mut proxy).await`:
  a `tracing_subscriber` `fmt().json()` layer whose `MakeWriter` appends bytes to a
  `Arc<Mutex<Vec<u8>>>`, set active via `tracing::subscriber::with_default(...)`. After the
  check, parse the captured lines as JSON and assert one event has
  `fields.outcome == "working"`, `fields.proto == "HTTP"`, `fields.addr == proxy.addr()`, and a
  numeric `fields.rtt`.
- Second test `timeout_outcome_is_labelled`: point the checker at a dead `127.0.0.1:0`-style
  unroutable port (bind then drop, or a listener that never responds within a 1s timeout) and
  assert an event with `outcome == "timeout"` is emitted (and retried `max_tries` times).
- No subscriber leakage: `with_default` is scoped, so tests stay independent.

**Acceptance criteria.**
- [ ] Every `record_attempt` site in `check_one` has a matching `proxybroker::check` event.
- [ ] Event fields: `addr`, `proto`, `outcome` ∈ {working,invalid,timeout,error}, `rtt` (Working) / `error` (failures).
- [ ] `--log-format json` produces valid line-delimited JSON on stderr; `text` is unchanged from today.
- [ ] Byte-for-byte `ProxyError::as_str` strings are unchanged (stats contract intact).
- [ ] The capture test passes with no network.

**Risks / deviations / principle-flags.**
- ⚠ *Log volume.* At `info` a busy `find` emits one event per check per protocol. Mitigation:
  events are on `target: "proxybroker::check"`, so `RUST_LOG=proxybroker::check=warn` mutes them
  independently; default CLI filter (`proxybroker={level}`, default `warn`) already suppresses
  `info` unless the user opts in.
- No `#[non_exhaustive]` concerns: this is additive.

**Effort.** S.

---

## F1 — Prometheus metrics endpoint

**Goal.** `serve --metrics <addr>` exposes a Prometheus text-format endpoint reporting live pool
size by scheme, aggregate proxy error-rate / response-time, and cumulative eviction and rotation
counts — so a running server is scrapeable. Hand-rolled text format, no exporter crate, gated on
a `metrics` feature so the default binary gains nothing it does not use.

**Public surface.**
- New feature in `Cargo.toml`: `metrics = ["server"]` (no new dependency; implies `server`).
- Library (`src/server.rs`, behind `feature = "server"`; the render fn behind `feature = "metrics"`):
  ```rust
  // Cheap, allocation-light live view of the pool. Shared with F4 later.
  pub struct PoolSnapshot {
      pub http: usize,          // proxies serving Scheme::Http
      pub https: usize,         // proxies serving Scheme::Https
      pub total: usize,
      pub avg_error_rate: f64,  // mean of Proxy::error_rate() over the pool
      pub avg_resp_time: f64,   // mean of Proxy::avg_resp_time() over the pool
  }
  impl Pool {
      pub fn snapshot(&self) -> PoolSnapshot;      // locks state once
      pub fn evictions(&self) -> u64;              // cumulative
      pub fn rotations(&self) -> u64;              // cumulative
  }

  #[cfg(feature = "metrics")]
  pub fn render_metrics(pool: &Pool) -> String;   // Prometheus text exposition

  #[cfg(feature = "metrics")]
  pub async fn serve_metrics(addr: SocketAddr, pool: Arc<Pool>)
      -> std::io::Result<ServerHandle>;           // tiny GET responder
  ```
- CLI (`src/bin/proxybroker.rs`, `ServeArgs`): `--metrics <ADDR>` as `Option<SocketAddr>`,
  default `None` (off). When set, spawn `serve_metrics` alongside `serve`.

**Design.**
- **New pool state.** Add two `std::sync::atomic::AtomicU64` fields to `Pool` (`src/server.rs:55`):
  `evictions`, `rotations`, both `AtomicU64::new(0)` in `Pool::spawn` and `Pool::from_proxies`.
  - `Pool::put` (`src/server.rs:127`): in the `unhealthy` branch (`src/server.rs:131`), before the
    `return`, `self.evictions.fetch_add(1, Relaxed)` — keep the existing `tracing::debug!`.
  - `handle_client` (`src/server.rs:256`): in the `Err(e)` arm, after `pool.put(proxy)`,
    `pool.rotations.fetch_add(1, Relaxed)` — every retry to a different proxy is one rotation.
    (`handle_client` currently receives `max_tries: usize`, not the `Arc<Pool>` counters directly —
    it already holds `pool: Arc<Pool>`, so the field is reachable.)
- **Snapshot.** `Pool::snapshot` locks `state` once and folds the `Vec<Proxy>`: count via
  `Proxy::schemes()` (`src/proxy.rs:130`) for `http`/`https`, and mean of `error_rate()` /
  `avg_resp_time()` (`src/proxy.rs:103,112`). Single lock acquisition — no per-metric locking.
- **Text exporter** (`render_metrics`). Hand-write the exposition format — it is a handful of
  lines and adding the `prometheus` crate (+ its `protobuf`/`lazy_static` tree) would bloat the
  default-feature-adjacent build for no gain. Shape:
  ```
  # HELP proxybroker_pool_size Proxies currently available in the pool.
  # TYPE proxybroker_pool_size gauge
  proxybroker_pool_size{scheme="http"} 42
  proxybroker_pool_size{scheme="https"} 30
  # TYPE proxybroker_pool_error_rate_avg gauge
  proxybroker_pool_error_rate_avg 0.04
  # TYPE proxybroker_pool_resp_time_avg_seconds gauge
  proxybroker_pool_resp_time_avg_seconds 0.83
  # TYPE proxybroker_evictions_total counter
  proxybroker_evictions_total 7
  # TYPE proxybroker_rotations_total counter
  proxybroker_rotations_total 19
  ```
- **HTTP responder.** `serve_metrics` mirrors the existing raw-tokio accept loop in `serve`
  (`src/server.rs:179`): bind a `TcpListener`, and for each connection read until `\r\n\r\n`
  (ignore the request line — any GET returns metrics), then write
  `HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: N\r\n\r\n{body}`.
  Reuse the `ServerHandle` + `CancellationToken` shutdown pattern already in `src/server.rs:153`.
  No hyper — the server module deliberately avoids it (`src/server.rs:6`).
- **CLI wiring.** In `serve_cmd` (`src/bin/proxybroker.rs:266`), after building `pool`, if
  `args.metrics` is `Some(addr)` call `serve_metrics(addr, pool.clone()).await?` and hold the
  handle until shutdown.

**Offline test plan** (`tests/metrics.rs`, `#![cfg(feature = "metrics")]`).
- **First failing test:** `metrics_endpoint_reports_pool_and_counters`.
  Build a pool with `Pool::from_proxies` over two mock-addr proxies (one HTTP-only, one SOCKS5 →
  both schemes), as `tests/serve.rs` does. Start `serve_metrics` on `127.0.0.1:0`. Open a
  `TcpStream`, send `GET /metrics HTTP/1.1\r\n\r\n`, read the response, assert the body contains
  `proxybroker_pool_size{scheme="http"} 2` and `...{scheme="https"} 1`.
- `evictions_counter_increments`: construct a proxy that is `unhealthy` per `PoolConfig`
  (`requests() >= min_req` and `error_rate() > max_error_rate` — drive with `record_attempt`),
  call `pool.put(proxy)`, assert `pool.evictions() == 1` and the rendered text shows
  `proxybroker_evictions_total 1`.
- `rotations_counter_increments`: reuse the `tests/serve.rs` relay harness with a mock upstream
  that errors on the first proxy and succeeds on a second; assert `pool.rotations() >= 1`.
- `render_metrics_is_valid_exposition`: unit-assert the `# TYPE` lines and value formatting
  (2-dp floats via `{:.2}` to match `Proxy` rounding, `src/proxy.rs:53`).

**Acceptance criteria.**
- [ ] `--metrics 127.0.0.1:9090` serves a scrapeable endpoint; absent flag = no listener, zero cost.
- [ ] `pool_size{scheme=...}` matches `Pool::snapshot`; counters are monotonic.
- [ ] Eviction increments exactly at the `Pool::put` unhealthy branch; rotation at each retry.
- [ ] No new third-party dependency; `metrics` feature off by default.
- [ ] All four tests pass offline.

**Risks / deviations / principle-flags.**
- ⚠ *Per-proxy error-rate cardinality.* The roadmap line says "per-proxy error rate", but
  labelling by proxy address is unbounded-cardinality (a Prometheus anti-pattern for a rotating
  pool that churns addresses). **Deviation:** expose an **aggregate** `pool_error_rate_avg`
  gauge, not per-address series. Record the rationale in `decisions.md`. If a real consumer needs
  per-proxy detail, the control API (Wave 3 B6 `/api/history`) is the right surface, not metrics.
- ⚠ *Judge latency.* The roadmap lists "judge latency", but judge-probe timing is a `find`/`Checker`
  concept (`src/checker.rs`), not present in the `serve`-time `Pool`. The nearest live number is
  the pool's `avg_resp_time` (relayed-request RTT), which F1 exposes. **Open Question:** plumb
  actual judge-probe latency into a shared metric later, or accept `resp_time_avg` as the serve-time
  proxy for it? Recommend deferring — no consumer yet (lazy-that-holds).
- ⚠ *Hand-rolled format drift.* Mitigation: `render_metrics_is_valid_exposition` pins the format;
  the surface is tiny and stable (Prometheus text 0.0.4).

**Effort.** S/M.

---

## F2 — `--progress` live bar during `find`

**Goal.** A live `indicatif` progress bar during `find`, showing checked / working / rate,
updated on a timer by polling the already-shared stats — no engine change.

**Public surface.**
- New feature: `progress = ["cli", "dep:indicatif"]`; `indicatif = { version = "0.18", optional = true }`.
- CLI (`FindArgs`, `src/bin/proxybroker.rs:129`): `--progress` bool flag, default off.
- New pure helper (testable, in the binary crate): `fn render_progress(s: &Stats) -> String`
  returning e.g. `"checked 128 · working 34 · avg 0.72s"`.
- No new library API — `ProxyStream::stats()` (`src/broker.rs:446`) already exists and is the
  sole data source.

**Design.**
- `ProxyStream::stats()` returns `Some(Stats)` for `find` (the shared
  `Arc<Mutex<StatsCollector>>`, `src/broker.rs:440`). The bar polls it on a timer while the same
  task drains the stream — no second task, no new public accessor, so the internal `Arc` stays
  encapsulated.
- Extend `write_stream` (`src/bin/proxybroker.rs:362`) to take
  `progress: Option<&indicatif::ProgressBar>`. When `Some`, replace the `while let Some(p) =
  stream.next().await` loop with a `tokio::select!`:
  ```rust
  let mut tick = tokio::time::interval(Duration::from_millis(120));
  loop {
      tokio::select! {
          maybe = stream.next() => match maybe {
              Some(proxy) => { /* write proxy to sink */ bar.inc(1); }
              None => break,
          }
          _ = tick.tick() => {
              if let Some(s) = stream.stats() {         // &stream — the next() future is
                  bar.set_message(render_progress(&s)); // dropped before this arm runs
              }
          }
      }
  }
  bar.finish_and_clear();
  ```
  `tokio::select!` drops the non-selected `stream.next()` future before running the tick arm, so
  the `&mut stream` borrow is released and `stream.stats()` (`&self`) is legal. Grab still passes
  `None` and keeps the simple loop.
- The bar draws to stderr (`ProgressBar::new_spinner().with_draw_target(ProgressDrawTarget::stderr())`)
  so it never mixes with proxy output on stdout — same discipline as `--show-stats`
  (`src/bin/proxybroker.rs:354`).
- `find` (`src/bin/proxybroker.rs:319`) constructs the bar when `args.progress`, threads it into
  `write_stream`, and (if `--show-stats`) still prints the final `Stats` after.

**Offline test plan.**
- **First failing test:** `tests/progress.rs::render_progress_formats_counts` (or a `#[cfg(test)]`
  unit test on the helper). Build a `Stats` via `Stats::from_proxies(&[...])` (as
  `src/stats.rs` tests do) and assert `render_progress` contains `"checked 3"`, `"working 2"`,
  and the avg. Pure, no I/O.
- Integration `tests/progress.rs::stats_grow_during_find`: reuse the `tests/find.rs` mock harness;
  drive `find`, and while draining, poll `stream.stats()` between `next()` calls asserting `total`
  is monotonically non-decreasing and ends equal to the number of proxies checked. This exercises
  the exact polling contract the bar relies on, offline.
- The bar's rendering itself is not asserted (terminal side effect); the pure helper + the polling
  contract are what carry the risk.

**Acceptance criteria.**
- [ ] `--progress` shows a live bar on stderr during `find`; stdout output byte-identical to without it.
- [ ] Bar updates on a ~120 ms timer via `ProxyStream::stats()`, no new library API, no second task.
- [ ] `grab` is unaffected (passes `None`).
- [ ] `progress` feature off by default; `indicatif` not pulled into the default build.
- [ ] Helper + polling-contract tests pass offline.

**Risks / deviations / principle-flags.**
- ⚠ *Total is unknown* (streaming, `--limit 0` = unlimited). Mitigation: use a **spinner** with a
  message, not a percentage bar; report absolute counts + rate, not a fraction.
- Minor: `write_stream` grows one optional param. Acceptable — it is the one sink, and the
  alternative (a parallel progress-aware copy) duplicates the file/stdout branching.

**Effort.** S/M.

---

## F5 — Reproducible benchmark harness (criterion, mock sockets)

**Goal.** A criterion benchmark that measures the **CPU** cost of the check pipeline on
deterministic input, isolated from real network I/O by loopback mock judge/proxy fixtures, and
reports proxies/sec (+ a peak-RSS number on a supported platform).

**Public surface.**
- Dev-only. `Cargo.toml`:
  ```toml
  [dev-dependencies]
  criterion = { version = "0.7", features = ["async_tokio"] }

  [[bench]]
  name = "check_pipeline"
  harness = false
  ```
  No change to the shipped library/binary surface.

**Design.**
- **Mock-socket realisation.** The constraint is "mock sockets so the benchmark is CPU-bound, not
  I/O-bound". `Checker` calls `TcpStream::connect((proxy.host, proxy.port))` *internally*
  (`src/checker.rs:205`); there is no injectable stream. Introducing a `Connect` trait to swap in
  an in-memory duplex would be a one-impl abstraction that fights "no speculative abstraction".
  **Decision:** realise "mock sockets" as **loopback fixtures on `127.0.0.1`** — the exact
  `echo_server` mock judge + mock HTTP proxy from `tests/check_http.rs`. Loopback stays in-kernel,
  never touches a NIC or the internet, and is deterministic; the measured delta between runs is
  dominated by the check pipeline's parsing/validation/anonymity-classification CPU, not wall-clock
  network latency. This is the "mock judge fixture" the roadmap names.
- **Bench body** (`benches/check_pipeline.rs`): build a Tokio `Runtime`; in setup, start one mock
  judge (`JUDGE_PAGE`) and one mock HTTP proxy (`HIGH_PAGE`) once; build a `Checker` via
  `Checker::new` (judges verified once, outside the measured loop). Benchmark, with
  `b.to_async(&rt).iter(...)`, a single `checker.check(&mut proxy)` over a freshly-cloned `Proxy`
  each iteration (so per-proxy stats don't accumulate). Group throughput:
  `group.throughput(Throughput::Elements(1))` → criterion reports elements/sec = proxies/sec.
- **Determinism.** Fixed judge page, fixed proxy page, fixed timeout, `max_tries = 1`. Marker is
  per-request random (`fresh_marker`), but the echo server reflects it, so validation is stable.
  No provider scraping, no DNS (stub the resolver as `tests/find.rs::stubbed_resolver` does).
- **Peak RSS.** criterion does not measure memory. Report it from a tiny separate harness rather
  than polluting the criterion group: a `#[cfg(test)]`/example that runs N checks then reads peak
  RSS via `getrusage(RUSAGE_SELF).ru_maxrss` (macOS/Linux; dev platform is darwin). **Open
  Question:** take the small `libc` dev-dependency for `getrusage`, or parse `/proc/self/status`
  `VmHWM` (Linux-only)? Recommend `libc` under `[dev-dependencies]` for cross-Unix, printed by the
  bench's setup, not asserted.

**Offline test plan** (the bench must also ship a real offline test, per C5).
- **First failing test:** `tests/bench_pipeline.rs::bench_fixture_runs_one_check_offline`.
  Stand up the same mock judge + mock proxy fixture the bench uses (factored into a shared
  `benches`/test helper or duplicated minimally), build the `Checker`, run exactly one
  `checker.check(&mut proxy).await`, and assert it returns `true` and confirms `Proto::Http`.
  This proves the benchmark's fixture is deterministic and network-free before criterion ever runs it.
- CI hook: `cargo bench --bench check_pipeline -- --test` (criterion's test mode runs each bench
  once without measuring) as a fast smoke check that the bench compiles and executes offline. Not a
  perf gate — perf numbers are advisory, run manually.

**Acceptance criteria.**
- [ ] `cargo bench` reports proxies/sec for the check pipeline on deterministic loopback input.
- [ ] Zero internet: judge, proxy, and resolver are all mocked/stubbed on `127.0.0.1`.
- [ ] `criterion` is a dev-dependency only; no change to library/binary features or size.
- [ ] `bench_fixture_runs_one_check_offline` passes; `cargo bench -- --test` runs green.
- [ ] Peak-RSS number printed (platform-gated), not asserted.

**Risks / deviations / principle-flags.**
- ⚠ *"Mock sockets" ≠ in-memory duplex.* Realised as loopback fixtures to avoid a one-impl
  `Connect` trait. The CPU win is still isolated: loopback removes real network latency/jitter, so
  run-to-run deltas track pipeline CPU. Deviation recorded here and in `decisions.md`. If loopback
  syscall overhead ever dominates a specific micro-measurement, revisit with a duplex stream *then*,
  driven by a real need.
- ⚠ *Peak RSS is platform-specific.* Gated to Unix (`getrusage`); Windows prints "unavailable".
- ⚠ *offline-testable* (register entry A6/F5/P1). Fully satisfied — all fixtures are `127.0.0.1`.

**Effort.** M.

---

## F4 — ratatui TUI dashboard (`proxybroker top`) — DEFERRED to Wave 7

**Goal.** A live terminal dashboard: a sortable pool table (addr, schemes, error-rate, resp-time,
country) with per-proxy sparklines of recent latency.

**⚠ Blocking dependency.** Sparklines and any "recent" view need **history**, which only exists
once Wave 7 persistence (`persist.rs`, SQLite `--state`) lands — a dashboard over the current
ephemeral `Pool` has one instantaneous row set and nothing to spark. **Do not build F4 in Wave 6.**
The roadmap places it last for exactly this reason (roadmap.md Wave 6, F4 note). Spec captured now
so the surface is designed; implementation waits.

**Public surface (provisional).**
- Feature: `tui = ["dep:ratatui", "dep:crossterm"]`, off by default.
- CLI: new subcommand `Command::Top(TopArgs)` (behind `feature = "tui"`), reading from the Wave 7
  persistence layer (a `--state <db>` path) and/or a running server's metrics/control endpoint.
- Data source: `Pool::snapshot` (from F1's shared refactor) for the live row, plus the Wave 7
  history store for sparkline series.

**Design (sketch, to finalise in Wave 7).**
- `ratatui` `Table` widget over the snapshot rows; column sort via key handlers (crossterm events).
- `Sparkline` widget fed by a bounded ring of recent `avg_resp_time` samples per proxy — the ring
  is populated from the persistence layer's timestamped check history, **not** kept in the pool
  (which stays ephemeral by design, roadmap principle).
- Reuses `PoolSnapshot` (F1) so the live column set has exactly one producer.

**Offline test plan (provisional).**
- Render-to-buffer test: `ratatui`'s `TestBackend` renders a frame from a fixed snapshot +
  synthetic history vector; assert the buffer contains expected addresses and sorted order. No
  terminal, no network — fully offline. First failing test (when built):
  `top_renders_sorted_pool_table`.

**Acceptance criteria (provisional).**
- [ ] `proxybroker top` renders a live sortable table + sparklines over persisted history.
- [ ] `tui` feature off by default; `ratatui`/`crossterm` not in the default build.
- [ ] `TestBackend` render test passes offline.
- [ ] Built only after Wave 7 persistence exists.

**Risks / deviations / principle-flags.**
- ⚠ *Ephemeral-by-design vs. needing history.* Resolved by sourcing history from the Wave 7
  persistence layer, keeping `Pool` itself stateless-of-history.
- ⚠ *Premature build.* Explicitly deferred; listed here only so F1's `PoolSnapshot` is designed
  with F4's second consumer in mind (justifying the shared refactor).

**Effort.** M (in Wave 7, not Wave 6).

---

## What must stay green

Existing behaviour and tests this wave must not regress:

- **`tests/serve.rs`** — the relay path and 502-on-empty behaviour. F1 adds counters to `Pool::put`
  and `handle_client` but must not change relay semantics; both tests must still pass unchanged.
- **`tests/find.rs`** — `find` streaming, limit, and `NoTypes`. F2 changes only the CLI drain loop,
  not `Broker::find`/`find_task`; the `ProxyStream` public contract (`stats()`, `Stream` impl) is
  untouched.
- **`tests/check_http.rs`** — anonymity classification and the four check outcomes. F3 adds tracing
  events beside the existing `record_attempt` calls; the return values and `proxy.types()` results
  must be identical.
- **`src/stats.rs` tests** — `StatsCollector` counting and `Stats` display. F2 only *reads*
  `Stats`; no field or `Display` change.
- **`src/error.rs::errmsg_strings_match_python_byte_for_byte`** — F3's `outcome` field is a new,
  separate label; the `ProxyError::as_str` histogram-key strings stay byte-for-byte.
- **Default-feature build stays lean** — `progress`/`tui` are not in `default`; `metrics` adds no
  new crate; a `cargo build` (default features) and `cargo build --no-default-features` must both
  still succeed, and `cargo publish --dry-run` (release CI) must still pass with the new optional
  deps correctly gated.
- **`cargo test --all-features`** and **`cargo clippy --all-features`** green before the wave's PRs
  merge; one commit per feature (`feat: …`) per the one-commit-per-item rule.
