# Wave 1 — Inputs & foundation

*Implementation-ready spec. Read alongside `docs/roadmap/roadmap.md` (Wave 1 table + principle
register) and `docs/systematic-refactor/decisions.md`.*

## Theme

Close the biggest capability gap (**check a user-supplied list**) and land the foundational
refactors everything downstream leans on (a `Deserialize`/`Serialize` round-trip, file-based
save/load for warm restart, a `FindQuery` builder). Everything here stays offline-testable via
local mock TCP/HTTP servers on `127.0.0.1`, exactly as `tests/find.rs`, `tests/check_http.rs`,
and `tests/serve.rs` already do.

## Build order (respect the dependencies)

0. **Shared refactor first** (behaviour-preserving, no new public API): extract the
   checker+resolver+ext-IP setup out of `Broker::find` into `Broker::build_checker`, and extract
   the per-proxy check pipeline out of `find_task` into `check_stream(impl Stream<Item = Proxy>)`.
   `find` must stay green (`tests/find.rs`) after this — it is a pure internal reshape.
1. **A1 — `check`**: `Broker::check`, the `check` subcommand, and `parse::parse_proxy_lines`.
   Builds directly on the step-0 seams.
2. **C1 — `Deserialize for Proxy`**: hand-written, mirrors the manual `Serialize`. Prereq for C2.
3. **C2 — save/load**: NDJSON reader/writer + `find --save` / `check --load` / `serve --load`.
   Depends on C1.
4. **E2 — `FindQuery` builder**: isolated, done while `broker.rs` is open.

One commit per feature (the step-0 refactor is its own commit):
`refactor: extract build_checker + check_stream from find`, then `feat: check a user-supplied
proxy list (A1)`, `feat: Deserialize for Proxy (C1)`, `feat: save/load a checked pool (C2)`,
`feat: FindQuery builder (E2)`.

---

## Step 0 — Shared refactor (lands with A1's commit or just before it)

Two private helpers on `Broker` (`src/broker.rs`), lifted verbatim from the current `find` /
`find_task` bodies so behaviour is identical:

```rust
// Replaces the resolver + external_ips + Checker::new block currently inline in `find`
// (broker.rs:195-215). Both find() and check() call it.
async fn build_checker(&self, query: &FindQuery) -> Result<Arc<Checker>, Error> {
    let resolver = match &self.resolver {
        Some(r) => r.clone(),
        None => Arc::new(Resolver::new(query.timeout)?),
    };
    let real_ext_ips = resolver.external_ips().await?;
    let checker = Checker::new(
        CheckerConfig {
            judges: query.judges.clone(),
            types: query.types.clone(),
            timeout: query.timeout,
            max_tries: query.max_tries,
            post: query.post,
            strict: query.strict,
            dnsbl: query.dnsbl.clone(),
        },
        resolver,
        &self.client,
        real_ext_ips,
    )
    .await?;
    Ok(Arc::new(checker))
}

// The per-proxy concurrency pipeline lifted out of `find_task` (broker.rs:251-322): the
// Semaphore cap, the TaskTracker wait-group, the atomic `sent`/limit accounting, the
// per-check `stats.record`, and the cancel-on-drop select. Source-agnostic.
async fn check_stream<S>(
    self,
    source: S,
    checker: Arc<Checker>,
    max_conn: usize,
    limit: Option<usize>,
    tx: mpsc::Sender<Proxy>,
    cancel: CancellationToken,
    stats: Arc<std::sync::Mutex<StatsCollector>>,
) where
    S: Stream<Item = Proxy> + Send,
{
    let sem = Arc::new(Semaphore::new(max_conn));
    let tracker = TaskTracker::new();
    let sent = Arc::new(AtomicUsize::new(0));
    let mut source = std::pin::pin!(source);
    while let Some(mut proxy) = source.next().await {
        if cancel.is_cancelled() || is_limit_reached(&sent, limit) {
            break;
        }
        let Ok(permit) = sem.clone().acquire_owned().await else { break };
        // ...clone checker/tx/sent/cancel/stats, then the EXACT tracker.spawn { select! { ... } }
        // body from find_task:293-316, unchanged.
    }
    tracker.close();
    tracker.wait().await;
}
```

`find_task` is then rewritten to build its source as a `Stream<Item = Proxy>` from the existing
provider machinery and hand it to `check_stream`. The provider fetch (`fetches`, the
`buffer_unordered(MAX_CONCURRENT_PROVIDERS)` stream at broker.rs:258-263) is already a stream;
the dedup + `attach_geo` + `country_ok` logic (broker.rs:266-280) becomes a functional pipeline
with **no new dependency**:

```rust
let source = fetches
    .flat_map(|cands| futures_util::stream::iter(cands))
    .filter_map({
        let broker = self.clone();
        let mut seen: BTreeSet<(IpAddr, u16)> = BTreeSet::new();
        move |cand| {
            // dedup on (host, port); parse host as IpAddr; Proxy::new; broker.attach_geo;
            // country_ok(&proxy, countries) -> keep or drop. Returns ready(Option<Proxy>).
            std::future::ready(/* Option<Proxy> */)
        }
    });
self.check_stream(source, checker, query.max_conn, query.limit, tx, cancel, stats).await;
```

`find` itself keeps its exact public shape (still returns `Result<ProxyStream, Error>`, still
errors `NoTypes` / `ExtIpUnknown` / `NoJudges` up front); it just delegates setup to
`build_checker` and spawns `check_stream` via the source above.

**Refactor test plan:** no new test — `tests/find.rs` (`find_streams_working_checked_proxies`,
`find_respects_the_limit`, `find_requires_types`) is the regression gate and must stay green.
Run it after the reshape and before writing any A1 code.

---

## A1 — `check` a user-supplied proxy list

**Goal.** Let a user check proxies they already have (from stdin, a file, or an in-memory `Vec`)
instead of only ones scraped from providers — the roadmap's #1 gap. `find` feeds provider
candidates into the shared pipeline; `check` feeds parsed user input into the same pipeline.

**Public surface (lib).**

```rust
// src/broker.rs — new, symmetric with `find`.
impl Broker {
    pub async fn check<S>(&self, proxies: S, query: FindQuery) -> Result<ProxyStream, Error>
    where
        S: Stream<Item = Proxy> + Send + 'static;
}
```

```rust
// src/parse.rs — the one home for extracting proxy addresses from text (module already owns
// find_addrs_line). Reuses find_addrs_line + resolver::parse_ip_lenient.
/// Parse `host:port` lines into unchecked proxies (expected_types empty = "check everything
/// requested", per checker.rs:119-125). Non-IP lines and out-of-range ports are skipped.
pub fn parse_proxy_lines(text: &str) -> Vec<Proxy>;
```

Supporting visibility change: `resolver::parse_ip_lenient` (currently a private free fn,
resolver.rs:111) becomes `pub(crate)` so `parse.rs` can reuse it (handles the leading-zero IPv4
quirk the same way `find`'s provider path does).

**CLI surface.** New `check` subcommand in `src/bin/proxybroker.rs`, mirroring `FindArgs` for the
checker knobs, plus an input source:

| Flag | Type / default | Meaning |
|---|---|---|
| `--types <TYPE>...` | required (unless `--load`) | protocols to check |
| `--infile <PATH>` | optional; default = **stdin** | read addresses from a file instead of stdin |
| `--lvl`, `--judges`, `--dnsbl`, `--timeout`(8), `--max-conn`(200), `--max-tries`(3), `--post`, `--strict`, `--limit`(0=unlimited), `--countries`, `--format`(default), `--outfile`, `--show-stats` | same as `FindArgs` | identical semantics |

(`--load` and `--save` are added under C2, below.)

`check`'s CLI flow: read the input text (file or stdin) → `parse::parse_proxy_lines(&text)` →
`broker.check(futures_util::stream::iter(proxies), query)` → reuse the existing `write_stream`
(bin:362). `--countries` maps to `FindQuery.countries` as `find` does; geo is attached per-proxy
inside `check` so serialized output/NDJSON carries country.

**Design.** `Broker::check` reuses `build_checker` (step 0) for the identical resolver +
`external_ips` + `Checker::new` setup, then constructs the same `ProxyStream` triple as `find`
(broker.rs:217-237): bounded `mpsc` channel (`CHANNEL_CAPACITY`), a `CancellationToken` whose
`drop_guard` aborts in-flight checks, and a shared `StatsCollector`. It spawns `check_stream`
over the caller's `proxies` stream (optionally `.map`-ing `attach_geo` in first). `check`'s
`query.types` empty path is already covered — `build_checker` → `Checker::new` returns
`Error::NoTypes` (checker.rs:77), same as `find`.

`parse_proxy_lines` walks `find_addrs_line(text)` (parse.rs:33) → `(ip_str, port_str)` pairs,
runs each `ip_str` through `parse_ip_lenient`, parses `port_str` as `u16` (dropping >65535), and
builds `Proxy::new(ip, port, BTreeSet::new())`. Empty `expected_types` is deliberate: the checker
treats it as "unknown → check all requested protocols" (checker.rs:123).

**Offline test plan.**
- **First failing test:** `src/parse.rs` unit test `parses_addr_lines_into_proxies` — feed
  `"1.2.3.4:8080\n010.0.0.1:3128\ngarbage\n5.6.7.8:99999\n"`, assert two proxies with addrs
  `1.2.3.4:8080` and `10.0.0.1:3128` (leading-zero normalized, junk + overflow-port dropped). No
  I/O.
- `tests/check.rs::check_streams_working_proxies` — clone the `tests/find.rs` scaffold (the
  `echo_server`, `JUDGE_PAGE`/`HIGH_PAGE`, `stubbed_resolver`). Stand up a mock judge + two mock
  HTTP proxies, build a broker with the stubbed resolver, then
  `broker.check(stream::iter(vec![Proxy::new(p1_addr…), Proxy::new(p2_addr…)]), query).await?`
  and `collect()`. Assert both stream out and `is_working()`. Fully offline.
- `tests/check.rs::check_respects_the_limit` — three input proxies, `limit: Some(1)`, assert
  exactly one emitted (proves `check_stream`'s shared limit accounting).

**Acceptance criteria.**
- [ ] `Broker::check` exists, reuses `build_checker` + `check_stream`, returns `ProxyStream` with
      working `stats()` and cancel-on-drop.
- [ ] `parse::parse_proxy_lines` parses `host:port` lines, normalizes leading-zero IPv4, skips
      non-IP lines and out-of-range ports.
- [ ] `check` subcommand reads stdin by default / `--infile` when given, honors every checker knob.
- [ ] `tests/find.rs` still green (shared pipeline unbroken).
- [ ] Two new offline `tests/check.rs` tests + the `parse` unit test pass with no network.

**Risks / deviations / principle-flags.**
- ⚠ *`check_stream` is a generic used by two callers* — not speculative: it has exactly two real
  consumers (`find`, `check`) the day it lands, which is the bar for extraction, not a
  one-impl trait.
- Behavioural parity: proxybroker2 exposes list-checking through the same `find` pipeline with a
  file provider; splitting it into an explicit `check` verb is a deliberate, clearer deviation —
  record in `decisions.md`.

**Effort.** M.

---

## C1 — `Deserialize for Proxy`

**Goal.** Read a `Proxy` back from the JSON its manual `Serialize` (proxy.rs:180-216) emits, so a
saved pool reloads without re-checking. Round-trips the serialized fields.

**Public surface.** No new named API — an `impl<'de> Deserialize<'de> for Proxy` in `src/proxy.rs`
(hand-written, next to the `Serialize` impl). `Proxy` gains `#[derive(PartialEq)]` so tests can
assert `from_str(to_string(p)) == p` (it already derives `Clone`; `Eq` is **not** derivable — the
`runtimes: Vec<f64>` field blocks it).

**Design.** Mirror the serialized shape exactly (proxy.rs:182-214):

```
{ "host": "1.2.3.4", "port": 8080,
  "geo": { "country": {"code","name"}, "region": {...}, "city": null },
  "types": [ {"type": "HTTP", "level": "High"}, ... ],
  "avg_resp_time": f64, "error_rate": f64 }
```

Deserialization rebuilds a `Proxy` directly (the impl is in `proxy.rs`, so it has private-field
access — no new constructor needed):
- `host` via `String` → `IpAddr::from_str`; `port` as `u16`.
- `geo.country.{code,name}`: both empty strings → `None`, else `Some(Country { code, name })`.
  `region`/`city` are read and ignored (Country-Lite has no data there; serialize always emits
  empty — proxy.rs:186-199).
- `types[]`: each `{type,level}` → `Proto::from_str` (reuses `types.rs` `FromStr`) + level via
  `AnonLevel::from_str`, with `""` → `None`. Populate the private `types` `BTreeMap`.
- `avg_resp_time` / `error_rate` are **outputs**, not inputs: they are read (and discarded).
  `runtimes`, `requests`, `errors` start empty/zero, per the roadmap. `expected_types` starts
  empty (never serialized).

So the round-trip is exact for `{host, port, geo, confirmed types}` and lossy-by-design for the
derived stats. A freshly loaded proxy therefore reports `avg_resp_time() == 0.0` and
`error_rate() == 0.0` until it is used again — which is correct for a warm-start pool.

**Offline test plan.**
- **First failing test:** `src/proxy.rs` unit test `deserialize_round_trips_serialized_fields` —
  build a proxy with host/port/geo(`US`)/two confirmed types (`Http:High`, `Connect80:None`) and
  **no recorded attempts**, then `let back: Proxy = serde_json::from_str(&serde_json::to_string(&x)
  .unwrap()).unwrap(); assert_eq!(back, x);`. With zero stats the `==` holds field-for-field.
- `deserialize_empty_geo_is_none` — feed JSON with `geo.country.code == ""`, assert `back.geo`
  is `None`.
- `deserialize_ignores_derived_stats` — feed JSON carrying `avg_resp_time: 3.14`, assert
  `back.avg_resp_time() == 0.0` (runtimes start empty).

**Acceptance criteria.**
- [ ] `impl Deserialize for Proxy` mirrors the `Serialize` nested shape (geo/region/city/types).
- [ ] `serde_json::from_str(&serde_json::to_string(&p))` reproduces host/port/geo/types.
- [ ] Empty `geo.country.code` → `geo == None`; empty `level` → `None`.
- [ ] `Proxy` derives `PartialEq`; existing `proxy.rs` tests + `serializes_to_python_as_json_shape`
      still pass.

**Risks / deviations / principle-flags.**
- ⚠ *Lossy round-trip on stats* — a genuine, documented limitation, not a bug: `Serialize` only
  emits derived aggregates, so `runtimes`/`errors` cannot be reconstructed. State this in the
  impl doc-comment and the roadmap (C2 warm-start pools begin with clean stats).
- No `#[derive(Deserialize)]` — the wire shape is nested and asymmetric with the struct layout,
  so a hand impl mirroring the hand `Serialize` is the faithful, lazy choice.

**Effort.** S/M.

---

## C2 — Save/load a checked pool (`--save` / `--load`)

**Goal.** Warm restart: persist checked proxies to a file and reload them into a stream or pool
without re-checking. `Pool::from_proxies` already consumes `Vec<Proxy>` (server.rs:65) — only a
disk loader is missing.

**Public surface (lib).** Reader/writer-generic NDJSON helpers in `src/proxy.rs` (I/O-agnostic so
tests use an in-memory cursor — no temp files, fully offline). Re-exported from `lib.rs`.

```rust
// src/proxy.rs
/// One JSON object per line (NDJSON). Skips blank lines; propagates the first parse error.
pub fn read_ndjson<R: std::io::BufRead>(reader: R) -> std::io::Result<Vec<Proxy>>;
pub fn write_ndjson<W: std::io::Write>(writer: W, proxies: &[Proxy]) -> std::io::Result<()>;
```

**CLI surface.**

| Subcommand | New flag | Behaviour |
|---|---|---|
| `find` | `--save <PATH>` | additionally write each streamed (working) proxy as NDJSON to `PATH` (independent of `--format`, which still drives stdout/`--outfile`) |
| `check` | `--save <PATH>` | same |
| `check` | `--load <PATH>` | load pre-checked proxies from NDJSON and emit them **without checking**; makes `--types` optional (`required_unless_present = "load"`) |
| `serve` | `--load <PATH>` | fill the pool from NDJSON via `Pool::from_proxies` instead of running `find`; makes `--types` optional (`required_unless_present = "load"`) |

**Design.**
- **Save.** NDJSON of a working proxy is exactly `serde_json::to_string(&proxy)` + `"\n"` — the
  same bytes `Format::Json` already emits (bin:202). In `write_stream` (bin:362), when `--save` is
  set, open the save file once and append each streamed proxy's JSON line alongside the normal
  format output. (A streamed proxy in `find`/`check` is by construction working, so the saved set
  is the checked-good set.)
- **Load (`check --load`).** `proxy::read_ndjson(BufReader::new(File::open(path)?))` → `Vec<Proxy>`
  → wrap with `futures_util::stream::iter` and hand straight to `write_stream`. No broker, no
  checker, no network — the proxies are already checked. `--types` is unused on this path.
- **Load (`serve --load`).** Same `read_ndjson`, then `Pool::from_proxies(loaded, PoolConfig{..})`
  (server.rs:65, already exists and marks the pool exhausted-immediately) instead of
  `broker.find(...) → Pool::spawn`. The rest of `serve_cmd` (bin:266-306) is unchanged.

**Offline test plan.**
- **First failing test:** `src/proxy.rs` unit test `ndjson_round_trips_via_cursor` — build 2–3
  proxies (with geo + confirmed types, zero stats), `write_ndjson(&mut buf, &proxies)`, then
  `read_ndjson(Cursor::new(buf))`, assert `== proxies`. In-memory, no files. (Leans on C1.)
- `read_ndjson_skips_blank_lines` — input with a trailing/interior blank line still parses cleanly.
- `tests/serve.rs::serve_loads_a_saved_pool` — write NDJSON for one mock-upstream proxy to a
  `tempfile`-style path (or feed `read_ndjson` a cursor and call `Pool::from_proxies` directly),
  stand up the existing `mock_upstream`, `serve(...)`, and assert the relayed body comes back —
  proving a loaded pool serves without re-checking. Reuses the `tests/serve.rs` scaffold; no
  network.

**Acceptance criteria.**
- [ ] `proxy::read_ndjson` / `write_ndjson` round-trip a `Vec<Proxy>` through a cursor.
- [ ] `find --save` / `check --save` write valid NDJSON that `read_ndjson` reloads.
- [ ] `check --load` emits loaded proxies with **no** checker/network activity; `--types` optional.
- [ ] `serve --load` fills the pool via `Pool::from_proxies` and serves; `--types` optional.
- [ ] New `proxy.rs` NDJSON tests + the `serve` load test pass offline.

**Risks / deviations / principle-flags.**
- ⚠ *Ephemeral-by-design* — file save/load is the roadmap's deliberate, minimal persistence step
  that must ship **before** SQLite (D2, Wave 7). Keep it to flat NDJSON of the existing wire shape;
  no schema, no index, no migration. This is the "prove file-based is insufficient before adding a
  database" gate.
- ⚠ *Loaded pools start with clean stats* (C1's lossy round-trip). Acceptable for warm start:
  eviction thresholds (`PoolConfig`) re-accumulate from live use; note it in `--load` help text.
- No JSON-schema versioning yet — that is C4 (Wave 4), before external consumers depend on it.

**Effort.** S.

---

## E2 — `FindQuery` builder

**Goal.** Replace `FindQuery { ..Default::default() }` at every call site (bin:277, bin:332,
lib.rs doctest) with a typed builder matching the existing `BrokerBuilder` style (broker.rs:355).

**Public surface (lib).**

```rust
// src/broker.rs
impl FindQuery {
    pub fn builder() -> FindQueryBuilder;
}

#[derive(Default)]
pub struct FindQueryBuilder { /* Option-wrapped mirror of the 10 fields */ }

impl FindQueryBuilder {
    pub fn types(self, types: Vec<TypeSpec>) -> Self;
    pub fn countries(self, countries: Vec<String>) -> Self;
    pub fn limit(self, limit: usize) -> Self;          // 0 → None, matching the CLI mapping
    pub fn judges(self, judges: Vec<String>) -> Self;
    pub fn dnsbl(self, dnsbl: Vec<String>) -> Self;
    pub fn timeout(self, timeout: Duration) -> Self;
    pub fn max_conn(self, max_conn: usize) -> Self;
    pub fn max_tries(self, max_tries: usize) -> Self;
    pub fn post(self, post: bool) -> Self;
    pub fn strict(self, strict: bool) -> Self;
    pub fn build(self) -> FindQuery;                    // infallible, like BrokerBuilder::build
}
```

Re-export `FindQueryBuilder` from `lib.rs` next to `FindQuery`.

**Design.** Consuming-`self` setters returning `Self`, unset fields falling back to
`FindQuery::default()` (broker.rs:87-102) in `build()` — the same pattern as `BrokerBuilder`
(broker.rs:365-427). `types` and `strict`/`post` are the interesting defaults (empty / false);
everything else defers to `Default`. `build()` is infallible — `find()`/`check()` already own the
`NoTypes` guard, so the builder must not duplicate it (a builder that can't fail is the lazier,
more composable choice, consistent with `BrokerBuilder`). `limit(0)` maps to `None` here so the
CLI's "0 = unlimited" convention (bin:313, bin:335) has one home.

Update `serve_cmd` (bin:277) and `find` (bin:332) to use the builder; update the `lib.rs` doctest
to showcase it.

**Offline test plan.**
- **First failing test:** `src/broker.rs` unit test `find_query_builder_matches_default` —
  `FindQuery::builder().types(vec![TypeSpec::any(Proto::Http)]).limit(10).build()` equals the
  hand-written `FindQuery { types: …, limit: Some(10), ..Default::default() }`. Requires
  `FindQuery: PartialEq` (derive it — all fields are `PartialEq`; `Duration` included).
- `find_query_builder_limit_zero_is_unlimited` — `.limit(0).build().limit == None`.

**Acceptance criteria.**
- [ ] `FindQuery::builder()` covers all 10 fields; `build()` is infallible.
- [ ] `limit(0)` → `None`.
- [ ] `serve_cmd`, `find`, and the `lib.rs` doctest use the builder (no stray
      `..Default::default()` at those call sites).
- [ ] Builder unit tests pass.

**Risks / deviations / principle-flags.**
- Deriving `PartialEq` on `FindQuery` is a small addition purely to make the builder testable —
  harmless (already `Clone + Debug`).
- No `#[non_exhaustive]` gymnastics — `FindQuery` is a plain owned struct; the builder is
  ergonomics, not encapsulation.

**Effort.** S.

---

## What must stay green

Existing behaviour these changes must not regress — run after each feature (per-feature specs
above name the narrow gate; this is the full checklist for the wave boundary / pre-commit):

- `tests/find.rs` — the step-0 refactor reshapes `find_task` internally; all three tests
  (`find_streams_working_checked_proxies`, `find_respects_the_limit`, `find_requires_types`) must
  still pass unchanged. This is the primary regression gate for the shared-pipeline extraction.
- `tests/check_http.rs`, `tests/judge_probe.rs`, `tests/resolver_extip.rs` — `Checker`,
  `Resolver`, and `build_checker` semantics are untouched; the eager judge probe and `NoJudges`
  path must behave identically.
- `tests/serve.rs` — `Pool::from_proxies` / `Pool::spawn` / relay path unchanged by C2's loader;
  the 502-on-empty path and the relay-through-pool path must still hold.
- `src/proxy.rs` unit tests — `serializes_to_python_as_json_shape` and the stats tests pin the
  wire shape C1 must round-trip; adding `Deserialize` + `#[derive(PartialEq)]` must not alter
  `Serialize` output byte-for-byte.
- `src/types.rs`, `src/error.rs` unit tests — `Proto`/`AnonLevel` `FromStr` (reused by C1's
  deserialize) and the byte-for-byte `errmsg` contract are unchanged.
- CLI parity: `grab`, `find`, `serve` keep their current flags and defaults; `check` is additive.
  The `--limit 0 = unlimited` mapping stays single-homed (now in `FindQueryBuilder::limit` +
  the CLI grab path).
