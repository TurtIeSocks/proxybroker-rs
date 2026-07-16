# Wave 7 — Persistence & adaptive

*The deliberate category change: durable state.* Waves 1–6 keep a proxy alive only for the
length of one process. Wave 7 lets a proxy be *remembered* — its uptime, its rolling success,
when we last saw it, how fast it was — and then acts on that memory: re-probing on a cadence
proportional to how stable a proxy has proven, and reloading the source file without a restart.

This wave openly fights two stated principles — **ephemeral by design** and **no speculative
abstraction** (roadmap principle-conflict register, rows D2/D3). The mitigation is the whole
point of the sequencing: **Wave 1 already shipped file-based `--save`/`--load` (C2)**, a flat
JSON/txt snapshot of a checked pool. That is sufficient for warm-restart of *which* proxies to
try. It is **not** sufficient for cross-run *history*: a flat snapshot cannot accumulate an EWMA
across ten runs, cannot record "last seen 3 days ago", cannot answer "has this proxy been up all
week". D2 escalates to SQLite *only* for that accumulation, and only behind a feature gate so a
pure-library user still compiles with zero new dependencies. We state this escalation
explicitly in D2 below rather than reaching for a database by reflex.

## Build order (respects dependencies)

1. **Shared seam first** — a persistence/observer hook at the two sites that already fold a
   finished proxy's outcome into shared state (`broker.rs:300` `stats.lock().record(&proxy)`;
   `server.rs:251-258` `record_attempt` + `pool.put`). This is a ~15-line refactor that lands
   with D2 and is reused by D3.
2. **D2 — SQLite persistent state** (`persist` feature). Everything downstream reads/writes the
   store.
3. **D3 — Adaptive re-check + decay scheduler.** Needs D2 (the durable score is the decay
   source) and the running server's `Pool`.
4. **E3 — Watch / live-reload the `--load` file.** Independent of D2/D3 mechanically, but pairs
   with the D3 re-check loop (both mutate a running `Pool`), so it lands last and reuses the
   `Pool` add/remove API D3 introduces.

---

## D2 — SQLite persistent state

**Goal.** Remember every proxy across runs — uptime history, rolling success (EWMA), last-seen,
latency — in one denormalized SQLite table, so a warm start inherits real reputation instead of
re-deriving it from scratch. Feature-gated (`persist`) so pure-library users stay zero-dep.

**Public surface.**

New module `src/persist.rs`, gated `#[cfg(feature = "persist")]`, re-exported from `lib.rs` under
the same gate.

```rust
// src/persist.rs
pub struct Store { /* owns a rusqlite::Connection behind a Mutex */ }

impl Store {
    /// Open (creating if absent) the state DB at `path`, running any pending migration.
    /// Sets `PRAGMA user_version` to `SCHEMA_VERSION` after migrating.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Store, crate::Error>;

    /// Fold one finished proxy's current-session outcome into its durable row: bump
    /// requests/errors, update the success EWMA and rolling latency, set last_seen = now,
    /// increment uptime_checks when working. Upsert keyed on (host, port).
    pub fn upsert(&self, proxy: &Proxy) -> Result<(), crate::Error>;

    /// Reconstruct every remembered proxy for a warm start. Priority-relevant aggregates
    /// (requests, error count, avg latency) and confirmed types are seeded onto each Proxy.
    pub fn load(&self) -> Result<Vec<Proxy>, crate::Error>;
}

/// Current on-disk schema version, written to PRAGMA user_version. Bump + add a migration arm
/// when the single table changes.
pub const SCHEMA_VERSION: i64 = 1;
```

New seeding constructor on `proxy.rs` (needed because `requests`, `errors`, `runtimes` are
private and warm-start must reproduce `priority()`):

```rust
// src/proxy.rs — add near Proxy::new
/// Rebuild a proxy from persisted aggregates so `priority()`/`error_rate()`/`avg_resp_time()`
/// reflect its stored history. Seeds the private stat fields directly; used only by the
/// `persist` store on warm start.
pub fn restored(
    host: IpAddr, port: u16,
    types: BTreeMap<Proto, Option<AnonLevel>>,
    requests: u32, errors_total: u32, avg_resp_time: f64,
) -> Proxy;
```

CLI (`bin/proxybroker.rs`), global-ish flag added to `FindArgs` and `ServeArgs`:

```
--state <PATH>     Remember proxies across runs in a SQLite DB at PATH. Requires the
                   `persist` build feature. Default: unset (no persistence).
```

`Cargo.toml`:

```toml
[features]
persist = ["dep:rusqlite"]
# verify the exact current version with cargo, not memory (project convention).
rusqlite = { version = "0.32", features = ["bundled"], optional = true }
```

`bundled` compiles SQLite from source → no system `libsqlite3` dependency, static link, matches
the musl-binary goal (W8 D1). No ORM crate.

**Design.**

*One denormalized table.* Every column a proxy needs lives in one row; no joins, no normalized
`attempts` history table (that is the speculative abstraction the principle warns against — we
never query per-attempt rows, only aggregates):

```sql
CREATE TABLE IF NOT EXISTS proxies (
    host          TEXT NOT NULL,
    port          INTEGER NOT NULL,
    types         TEXT NOT NULL,   -- JSON: [{"type":"HTTP","level":"High"}, ...] (serde)
    requests      INTEGER NOT NULL DEFAULT 0,
    errors        INTEGER NOT NULL DEFAULT 0,
    ewma_success  REAL NOT NULL DEFAULT 0.0,   -- rolling P(working), 0..1
    avg_latency   REAL NOT NULL DEFAULT 0.0,   -- seconds
    first_seen    INTEGER NOT NULL,            -- unix secs
    last_seen     INTEGER NOT NULL,            -- unix secs
    uptime_checks INTEGER NOT NULL DEFAULT 0,  -- times seen working (numerator of uptime)
    PRIMARY KEY (host, port)
);
```

*Migrations via `PRAGMA user_version`.* `open()` reads `PRAGMA user_version`; if `0`, create the
table and set it to `SCHEMA_VERSION`; future schema changes add an arm (`1 -> 2`) — a `match` on
the read version, not a migration framework.

*EWMA fold.* On `upsert`, read the prior row (if any). The session sample is
`s = if proxy.is_working() { 1.0 } else { 0.0 }`; new `ewma = ALPHA * s + (1 - ALPHA) * prior`
with `ALPHA = 0.3` (a `const` — no config for a constant, per the lazy principle; promote to a
flag only if a real user needs it). `avg_latency` folds `proxy.avg_resp_time()` the same way when
non-zero; `requests`/`errors` accumulate (`prior + proxy.requests()` etc.);
`uptime_checks += is_working`; `last_seen = now`; `first_seen` set only on insert. Written with an
`INSERT ... ON CONFLICT(host,port) DO UPDATE`.

*Upsert hook (the shared seam).* `upsert` is called at the two sites that already terminate a
proxy's lifecycle:
- `broker.rs` `find_task`, immediately after the existing `stats.lock().unwrap().record(&proxy)`
  (line 300) — thread an `Option<Arc<Store>>` into `find_task` exactly as `stats` is threaded.
  This is the literal meaning of "record_attempt also upserts": the checker's `check` calls
  `record_attempt` internally (`checker.rs:173-190`); the broker upserts the finished proxy once
  `check` returns.
- `server.rs` `handle_client`, after `proxy.record_attempt(...)` / `pool.put(proxy)`
  (lines 251-258) — `Pool` gains `#[cfg(feature = "persist")] store: Option<Arc<Store>>` and
  `put` fires `store.upsert` on the returned proxy.

*Warm start.* `serve`/`find` with `--state`: `Store::open`, then `Store::load()` seeds the
initial pool (union with C2's `--load` snapshot if both given; DB wins on `(host,port)` conflict
because it carries history). `load` maps each row → `Proxy::restored(...)`, parsing `types` JSON
back into the confirmed-types map.

*Why not extend C2's file format?* Stated explicitly per the roadmap escalation rule: a
JSON-lines snapshot is write-once-per-run and has no read-modify-write across runs — you cannot
fold an EWMA or accumulate `uptime_checks` without re-reading and rewriting the whole file under a
lock every check, which is exactly the concurrent-upsert workload SQLite exists for. We escalate
only after C2 shipped and proved it cannot carry history.

**Offline test plan.** New `tests/persist.rs`, `#![cfg(feature = "persist")]`, all against a
temp-dir DB path (`std::env::temp_dir()` + unique name, or the scratchpad) — zero network.

- **First failing test — `upsert_then_load_roundtrips_a_proxy`**: build a working `Proxy`
  (`add_type(Proto::Http, Some(High))`, a couple of `record_attempt` calls), `Store::open` a
  temp DB, `upsert`, drop, reopen, `load()` → assert one proxy back with the same `addr()`,
  `is_working()`, confirmed types, and a `priority()` reflecting the stored latency/error rate.
- `user_version_is_set_on_open`: after `open`, query `PRAGMA user_version` == `SCHEMA_VERSION`.
- `ewma_folds_across_two_runs`: upsert a working sample, reopen, upsert a failing sample; read
  the row and assert `ewma_success` moved toward the failing sample by `ALPHA` (exact arithmetic).
- `requests_and_errors_accumulate`: two runs' `requests`/`errors` sum in the row.
- `migration_from_v0_creates_table`: open a fresh path, assert table exists and version bumped
  from 0 → 1 (guards the migration arm).

**Acceptance criteria.**
- [ ] `persist` feature off by default; `cargo build` (no features) and `cargo build
      --no-default-features` pull in **no** `rusqlite`.
- [ ] Exactly one table; migrations gated on `PRAGMA user_version`; no ORM/migration crate.
- [ ] `--state` on both `find` and `serve`; absent → identical behaviour to today.
- [ ] `upsert` wired at the two existing record sites, not a third bespoke path.
- [ ] Warm start via `Store::load()` reproduces `priority()` ordering from stored history.
- [ ] All `tests/persist.rs` pass offline; existing suites unchanged.

**Risks / deviations / principle-flags.**
- ⚠ **ephemeral-by-design + no-speculative-abstraction.** Mitigation: feature-gated, one table,
  no per-attempt history, C2 shipped first and its insufficiency stated above.
- ⚠ **`Proxy::restored` seeds private fields** — a small hole in `Proxy`'s "plain data, computed
  getters" invariant. Mitigation: it is the *only* mutating constructor beyond `new`, documented
  as persist-only; it seeds `runtimes` with the stored average (so `avg_resp_time()` returns it)
  and `errors` under a single bucket to preserve `error_rate()` — record this as a deviation in
  `decisions.md`: the reconstructed error **histogram** is lossy (one bucket), the error **rate**
  is faithful. That is acceptable because warm-start only needs `priority()`, never the per-bucket
  breakdown (which is a fresh-session stat).
- ⚠ **`bundled` SQLite** lengthens first compile. Mitigation: off by default; only `--state`
  users pay it.

**Effort.** M.

---

## D3 — Adaptive re-checking + decay scheduler

**Goal.** Re-probe pooled proxies on a cadence proportional to their stability (stable → rarely,
flaky → often), and decay a proxy's score the longer it goes unseen — so a served pool stays
fresh without a human re-running `find`. Needs D2 (the durable score is the decay source and the
re-check outcome is upserted).

**Public surface.**

New module `src/scheduler.rs`, gated `#[cfg(all(feature = "server", feature = "persist"))]`
(it drives the server `Pool` and reads/writes the D2 store).

```rust
// src/scheduler.rs
pub struct RecheckConfig {
    /// Shortest re-check interval (a brand-new/flaky proxy).
    pub min_interval: Duration,   // default 60s
    /// Longest re-check interval (a rock-solid proxy).
    pub max_interval: Duration,   // default 1h
    /// Global ceiling on re-check starts per second, across all proxies. Must be << the
    /// judges' tolerance to avoid IP-blocking. Default 5.0.
    pub rate_per_sec: f64,
    /// Score half-life: a proxy unseen for this long has its effective score halved.
    pub decay_halflife: Duration, // default 6h
}

impl Default for RecheckConfig { /* the defaults above */ }

/// Spawn the re-check loop: pops due proxies off a next-check heap, re-probes each through the
/// shared `Checker`, upserts the outcome (D2), and returns survivors to `pool`. Honors the
/// global rate ceiling + jitter and never exceeds the checker's own `max_conn`. Stops when
/// `cancel` fires. Returns a handle for tests to await/step.
pub fn spawn_rechecker(
    pool: Arc<Pool>,
    checker: Arc<Checker>,
    store: Arc<Store>,
    cfg: RecheckConfig,
    cancel: CancellationToken,
) -> RecheckHandle;
```

CLI additions to `ServeArgs`:

```
--recheck                    Enable adaptive re-checking (requires --state and the persist +
                             server features). Default: off.
--recheck-rate <PER_SEC>     Global re-check ceiling, checks/sec. Default: 5.
--recheck-min <SECS>         Shortest cadence. Default: 60.
--recheck-max <SECS>         Longest cadence. Default: 3600.
--decay-halflife <SECS>      Score half-life for unseen proxies. Default: 21600.
```

**Design.**

*The heap feeding the existing checker.* A `BinaryHeap<Reverse<Scheduled>>` where
`Scheduled { due: tokio::time::Instant, host, port }`, ordered so the soonest-due pops first.
The loop `tokio::time::sleep_until(top.due)`, then re-checks that proxy. **Key on
`tokio::time::Instant`, not `std::time::Instant`** — so `tokio::time::pause()`/`advance()` drive
it deterministically in tests (std `Instant` ignores tokio's clock).

*Cadence proportional to stability.* On each completed re-check, compute the next interval from
the proxy's durable `ewma_success` (read back from the D2 store, or carried on the just-checked
proxy): `interval = lerp(min_interval, max_interval, ewma_success)` — `ewma≈1` (stable) → near
`max_interval`; `ewma≈0` (flaky) → near `min_interval`. Push `Scheduled { due: now + interval +
jitter, .. }`.

*Decay if unseen.* Decay is applied at *selection/scoring* time, not by mutating rows on a timer
(no background write storm): when the server's `Pool::get`/`best_for` or the scheduler ranks a
proxy, multiply its stored score by `0.5f64.powf(age / decay_halflife)` where
`age = now - last_seen`. This generalizes the existing `proxy.rs:priority()` `(error_rate,
avg_resp_time)` key: D3 introduces a `decayed_score(proxy, now, cfg)` free function that folds
`ewma_success`, latency, and the decay factor into one comparable, and `best_for`
(`server.rs:141`) sorts on it when persistence is active (falls back to today's `priority()`
tuple otherwise — the `server`-only, no-`persist` build is unchanged).

*Global rate ceiling + jitter (the IP-block guard).* A token-bucket limiter
(`min gap = 1.0 / rate_per_sec` between re-check *starts*, refilled over time) throttles pops
regardless of how many proxies are due. Each `due` gets `± up to 50%` uniform jitter (`rand`,
already a dep) so a burst of proxies inserted together does not thundering-herd the judges.
**Must not outrun `max_conn`:** re-checks acquire from the *same* `Semaphore`/permit budget the
`find` pipeline uses (`broker.rs:251` `Semaphore::new(query.max_conn)`) — pass that semaphore into
the scheduler so live serving traffic and re-check traffic share one concurrency cap, never
double it.

*Outcome handling.* Each re-check calls `Checker::check(&mut proxy)` (`checker.rs:106`, already
`&self`, already records attempts), then `store.upsert(&proxy)` (D2), then `pool.put(proxy)` —
which already evicts an over-error/over-latency proxy (`server.rs:127`). A proxy that fails hard
is dropped from both the pool and the heap; its row persists (with the worsened EWMA) so a later
run knows to distrust it.

**Offline test plan.** New `tests/recheck.rs`,
`#![cfg(all(feature = "server", feature = "persist"))]`, `tokio::test(start_paused = true)`.
Reuse the `find.rs`/`serve.rs` mock kit: a mock judge echoing `REAL_IP`, mock proxies as
`echo_server`s, a stubbed `Resolver::with_ip_endpoints`, and a temp `Store`.

- **First failing test — `scheduler_reprobes_on_cadence`**: seed a `Pool` with one working mock
  proxy + a `Store`, `spawn_rechecker` with `min_interval = 10s`. Under paused time,
  `tokio::time::advance(11s)`; assert the mock proxy's `echo_server` received a second request
  (an `AtomicUsize` hit counter) and the store row's `requests` incremented.
- `stable_proxy_gets_longer_cadence`: a proxy with `ewma≈1` is scheduled near `max_interval`, one
  with `ewma≈0` near `min_interval` — assert the two computed `due` deltas differ in the expected
  direction (test the `next_interval(ewma, cfg)` pure fn directly, no I/O).
- `rate_ceiling_caps_starts`: enqueue 100 due proxies with `rate_per_sec = 5`; advance 1s; assert
  ≤ ~5 re-check starts fired (hit counters), proving the token bucket throttles independent of
  backlog.
- `decay_lowers_unseen_score`: `decayed_score` pure-fn test — a proxy `last_seen` one half-life
  ago scores half of the same proxy seen now. No I/O.
- `recheck_shares_max_conn`: with `max_conn = 1`, assert no two re-checks overlap (a mock proxy
  that records concurrent in-flight count never exceeds 1).

**Acceptance criteria.**
- [ ] Re-check gated behind `--recheck` **and** requires `--state`; erroring clearly if `--state`
      is absent.
- [ ] Cadence monotonically increases with stability between `min`/`max`.
- [ ] Decay halves an unseen proxy's score at exactly one half-life.
- [ ] Global start-rate never exceeds `--recheck-rate`; every `due` carries jitter.
- [ ] Re-check concurrency shares the `max_conn` semaphore — never a second cap.
- [ ] Every re-check outcome is upserted (D2); evicted proxies leave the pool but keep their row.
- [ ] `tests/recheck.rs` passes offline under paused tokio time.

**Risks / deviations / principle-flags.**
- ⚠ **Re-check traffic can get the host IP-blocked by judges.** Mitigation (the whole feature's
  raison d'être): global token-bucket ceiling defaulting to a conservative 5/sec, uniform jitter,
  and a hard reuse of the `max_conn` semaphore so re-checks can never add load beyond the existing
  cap.
- ⚠ **`tokio::time::Instant` vs `std::time::Instant`.** Deviation from the rest of the codebase
  (which uses `std::Instant` in `checker.rs`): the scheduler must use tokio's clock to stay
  testable. Documented at the type.
- ⚠ **`best_for` gains a persist-aware branch.** Mitigation: the decayed comparator is only
  compiled/used under `persist`; the plain `server` build keeps today's `priority()` tuple sort
  byte-for-byte, guarded by existing `server.rs` tests.

**Effort.** M.

---

## E3 — Watch / live-reload the source file

**Goal.** Watch the `--load` file (Wave 1 C2) with the `notify` crate; when it changes, diff the
parsed proxies against the live pool and add/remove without restarting the server. Pairs with the
D3 re-check loop — both mutate a running `Pool`.

**Public surface.**

`Cargo.toml`:

```toml
[features]
watch = ["dep:notify"]
# verify the current major with cargo, not memory (project convention).
notify = { version = "7", optional = true }
```

New `Pool` methods (`server.rs`, needed by both E3 and D3 — land with whichever ships first):

```rust
impl Pool {
    /// Add a checked proxy to a running pool (dedup on (host,port); no-op if already present).
    pub fn add(&self, proxy: Proxy);
    /// Remove any pooled proxy at this address; returns whether one was removed.
    pub fn remove_addr(&self, host: IpAddr, port: u16) -> bool;
    /// Snapshot the current (host,port) set, for reconciliation.
    pub fn addrs(&self) -> std::collections::BTreeSet<(IpAddr, u16)>;
}
```

New module `src/watch.rs`, gated `#[cfg(all(feature = "server", feature = "watch"))]`:

```rust
// src/watch.rs
/// Reconcile the pool to exactly the `desired` set: add proxies present in `desired` but not in
/// the pool, remove pooled proxies absent from `desired`. Pure over the pool's public API —
/// deterministic and offline-testable.
pub fn reconcile(pool: &Pool, desired: Vec<Proxy>);

/// Spawn a filesystem watcher on `path`; on each (debounced) change, re-parse the file via the
/// C2 loader and `reconcile` the pool. Stops when `cancel` fires.
pub fn spawn_watch(
    pool: Arc<Pool>,
    path: PathBuf,
    cancel: CancellationToken,
) -> std::io::Result<WatchHandle>;
```

CLI addition to `ServeArgs` (builds on C2's `--load`):

```
--watch    Live-reload the --load file: apply additions/removals to the running pool without
           restart. Requires --load and the `watch` build feature. Default: off.
```

**Design.**

*Reconciliation is a pure function.* `reconcile` reads `pool.addrs()`, computes the two set
differences against `desired`, then calls `pool.add`/`pool.remove_addr`. It touches no I/O and no
clock — this is the deterministic core, tested directly.

*The watcher wraps it.* `spawn_watch` uses `notify`'s `recommended_watcher` on the file's parent
directory (editors replace-on-save, which fires as create/remove of the path, not modify — watch
the dir and filter to `path`). Events land on a `std::sync::mpsc`; a small tokio task drains it
with **debounce/coalesce** (a `tokio::time::sleep` window, e.g. 250ms, resetting on each event) so
a burst of write events triggers exactly one re-parse. On fire: re-run the C2 file loader; on a
parse error, log and keep the current pool (a half-written file must not empty the pool).

*Feeds the same `Pool` as D3.* `add`/`remove_addr` are the seam both features share; a live
server can simultaneously run the D3 re-checker and the E3 watcher against one `Pool` — the pool's
`Mutex<Vec<Proxy>>` (`server.rs:56`) already serializes them.

*Removal semantics.* A proxy removed from the file is dropped from the pool but a request already
mid-relay through it completes (the relay holds an owned `Proxy` checked out via `get`,
`server.rs:243` — `remove_addr` only touches the idle set). No forced disconnect.

**Offline test plan.** New `tests/watch.rs`,
`#![cfg(all(feature = "server", feature = "watch"))]`. Pure-function tests are the deterministic
core; one integration test exercises the real watcher against a temp file.

- **First failing test — `reconcile_adds_and_removes` (pure, no I/O, no network)**: build a
  `Pool::from_proxies` with proxies A, B; `reconcile(&pool, vec![B, C])`; assert `pool.addrs()`
  == {B, C} (A removed, C added, B untouched).
- `reconcile_is_idempotent`: reconciling to the current set is a no-op (no spurious adds/removes;
  assert `addrs()` unchanged).
- `watch_reparses_on_file_change` (integration): write addrs `[A]` to a temp file, `spawn_watch`,
  append `B` and rewrite, then poll `pool.addrs()` (bounded loop with a short real sleep — the one
  place real time is unavoidable, since `notify` is OS-driven) until it contains B or a 2s
  deadline. Uses `Pool` + local file only; **no network**.
- `watch_ignores_a_malformed_write`: write a garbage line; assert the pool is unchanged (parse
  failure is swallowed, pool preserved).

**Acceptance criteria.**
- [ ] `watch` feature off by default; no `notify` in a default build.
- [ ] `--watch` requires `--load`; errors clearly otherwise.
- [ ] `reconcile` is a pure function over `Pool`'s public API and is unit-tested without I/O.
- [ ] File changes add/remove pooled proxies within the debounce window; a malformed file is a
      no-op, never an empty pool.
- [ ] In-flight relays through a just-removed proxy complete normally.
- [ ] `tests/watch.rs` passes offline.

**Risks / deviations / principle-flags.**
- ⚠ **`notify` timing is OS-driven** — the one integration test cannot use paused tokio time.
  Mitigation: the behavioural contract (`reconcile`) is a pure function tested deterministically;
  the integration test only proves the wiring, with a bounded real-time poll, no network.
- ⚠ **Editor replace-on-save** fires remove+create, not modify. Mitigation: watch the parent
  directory and filter to the target path; debounce coalesces the pair into one re-parse.
- ⚠ **A half-written file** could momentarily parse to fewer proxies. Mitigation: parse-error →
  keep current pool; debounce waits for writes to settle before re-parsing.

**Effort.** S.

---

## What must stay green

- **Zero-dep library build.** `cargo build --no-default-features` and a plain `cargo build` must
  pull in **no** `rusqlite` and **no** `notify`. `persist`, `watch`, and the D3 scheduler are all
  additive feature gates; the default and pure-library builds are byte-for-byte unchanged.
- **`tests/serve.rs`** — `server_relays_http_request_through_a_pool_proxy` and
  `server_returns_502_when_pool_is_empty` must pass untouched. The `Pool` gains fields/methods
  (`add`, `remove_addr`, optional `store`) but `from_proxies`, `spawn`, `get`, `put`, and
  `best_for`'s default (non-persist) ordering keep their exact current behaviour.
- **`tests/find.rs`** — the full pipeline (Semaphore, TaskTracker, limit, dedup) is unchanged;
  the D2 upsert hook sits *beside* the existing `stats.record` call and only fires when a `Store`
  is threaded in.
- **`proxy.rs` unit tests** — `record_attempt_*`, `round2_matches_python_round`,
  `serializes_to_python_as_json_shape`, `schemes_follow_protocol_families` must all still pass;
  `Proxy::restored` is additive and must not alter `new`'s stat-field initialization.
- **`best_for` tie-safety** — `tied_response_times_do_not_panic` (the `server.py` heapq bug this
  port fixed) must remain green; the D3 decayed comparator must likewise use `f64::total_cmp`,
  never a partial compare that can panic.
- **Error taxonomy** — `error.rs`'s byte-for-byte `errmsg` contract is untouched; new failure
  modes (DB open failure, watch setup failure) surface through `Error::Io`/a new
  `#[non_exhaustive]` variant, never by mutating an existing `ProxyError` string.

## Open questions

1. **`--state` + `--load` precedence.** Recommended: DB rows win on `(host,port)` conflict
   (they carry history); the C2 snapshot only contributes proxies the DB has never seen. Confirm
   this is the desired merge, or whether `--load` should be authoritative for *membership* and the
   DB only for *scores*.
2. **EWMA sample granularity.** Recommended: one sample per completed check (`is_working` → 1/0).
   Alternative: per-`record_attempt` success ratio, which reacts faster but couples the store to
   the checker's retry internals. Defaulting to per-check keeps the seam at the existing
   `stats.record` site.
3. **D3 feature gate.** Recommended: `all(feature="server", feature="persist")` — decay needs a
   durable `last_seen`. If a memory-only adaptive re-check (no DB) is wanted, D3 would need its own
   in-memory score map; deferred unless a user asks (no-speculative-abstraction).
