# Good/Bad Assessment — proxybroker2 modules

Verdict per module for the Rust port. Evidence is measured, not estimated:

- **Churn** — `git log --format=format: --name-only -- proxybroker/<m>.py | wc -l`
- **Tests** — LOC of the module's dedicated `tests/test_<m>.py`, plus how many other test
  files reference it
- **Liveness** — real HTTP probes run 2026-07-15 (see `liveness.txt`, `liveness_custom.txt`)
- **Port risks** — from the 21-agent function-level trace (`trace.md`)

Total: 5,438 LOC source against 5,000 LOC of tests — a near 1:1 ratio. This suite is the
port's single most valuable asset: it is an executable specification of intended
behaviour, and `test_public_contracts.py` (435 LOC) is a literal parity checklist.

| Module | LOC | Churn | Tests | Verdict | Confidence |
|---|---:|---:|---:|---|---|
| `errors.py` | 49 | 4 | 0 (ref×7) | **Rewrite** | High |
| `utils.py` | 201 | 27 | 240 (ref×4) | **Port + redesign** | High |
| `negotiators.py` | 237 | 16 | 272 (ref×2) | **Port + redesign** | High |
| `judge.py` | 171 | 28 | **0** (ref×1) | **Port + redesign** | Medium |
| `checker.py` | 390 | 21 | 363 (ref×2) | **Port + redesign** | High |
| `proxy.py` | 492 | 37 | 352 | **Port + redesign** | High |
| `resolver.py` | 557 | 42 | 829 | **Replace-with-lib** | High |
| `providers.py` | 806 | **47** | 131 | **Rewrite as data** | High |
| `provider_utils.py` | 728 | 14 | 521 | **Merge** | Medium |
| `server.py` | 540 | 33 | 366 | **Port + redesign** | Medium |
| `api.py` | 625 | 42 | 188 | **Port + redesign** | High |
| `cli.py` | 535 | 34 | 623 | **Rewrite** | High |
| `__init__.py` | 103 | 31 | — | **Rewrite** | High |
| `__main__.py` | 4 | 3 | — | **Drop** | High |

Tally: 1 replace-with-lib · 7 port+redesign · 4 rewrite · 1 merge · 1 drop. **Nothing is
a straight 1:1 port.** That is the honest finding — an asyncio→tokio port that claims
"mostly mechanical" is lying about the concurrency semantics.

---

### `errors.py` — Rewrite
- **Evidence:** `[STALE]` 4 commits, no dedicated test, referenced by 7 test files.
- **Why not port:** the design is Python-specific and partly broken. `errmsg` is a *class
  attribute read reflectively off the class object* (`proxy.log(err=BadResponseError)` →
  `stat['errors'][err.errmsg] += 1`). Three of the ten exceptions (`ProxyError`,
  `NoProxyError`, `ResolveError`) have **no `errmsg` at all** — passing them to `log()` is
  a latent `AttributeError`. `ProxyRecvError` and `ProxySendError` both use
  `errmsg="connection_is_reset"`, silently merging two distinct failures into one counter.
- **Tension with goals:** the `errmsg` strings are a **user-visible stats contract**
  (they surface in `show_stats`), so the strings must survive even though the class
  hierarchy must not.
- **Rust:** two `thiserror` enums split by whether the caller can act (goals C6 / research
  §Architecture). Keep the exact `errmsg` strings via `fn errmsg(&self) -> &'static str`.

### `utils.py` — Port + redesign
- **Evidence:** 240 LOC of dedicated tests; referenced by 4 more. Well specified.
- **Why redesign:** `get_headers()` returns a dict whose **iteration order determines the
  emitted request bytes**. Python 3.7+ guarantees insertion order; Rust's `HashMap` is
  randomized per process (SipHash, random seed). A naive port produces nondeterministic
  request bytes — passes locally, flakes in CI, and changes what proxies actually see.
  Must be `IndexMap` or `Vec<(K,V)>`. Same hazard recurs in `negotiators.py`.
- **Also:** `get_status_code` returns `400` on unparseable input as a **sentinel callers
  depend on** — preserve as `.unwrap_or(400)`, not an `Err`.

### `negotiators.py` — Port + redesign
- **Evidence:** 272 LOC of byte-level tests — port against those assertions, not by
  re-deriving the `struct.pack` format strings.
- **Why redesign:** three structural mismatches. (1) `NGTRS` is a runtime dict of *class
  objects*; Rust gets a closed 6-variant enum. (2) `Proxy` owns the negotiator while the
  negotiator holds a back-ref to `Proxy` — a **reference cycle** Python's GC absorbs and
  Rust rejects; fixed by inverting ownership. (3) `HttpsNgtr` upgrades the live stream to
  TLS **in place** via asyncio `start_tls`; Rust TLS connectors consume the stream by
  value, so the transport must be an enum swapped with `mem::replace`.
- **Deviation to sign off:** `**kwargs` lets `ip=None` reach `ipaddress.ip_address(None)`,
  which raises an **uncaught** `ValueError` — a crash, not a `BadResponseError`. A typed
  target struct makes that unrepresentable, which is a deliberate behaviour change.

### `judge.py` — Port + redesign · **Confidence: Medium**
- **Evidence:** `[UNTESTED]` — **the only module with no dedicated test file**, yet 28
  commits. Highest churn-to-coverage ratio in the repo.
- **Why redesign:** `Judge.available` and `Judge.ev` are **class attributes** — process-global
  mutable state. That is why `Checker.__init__` must call `Judge.clear()`: two `Broker`s in
  one process stomp each other. Making judges instance state owned by `Checker` fixes a
  real bug (goals: deviation recorded in `map.md`).
- **Why only medium confidence:** no tests means no executable spec. The anonymity baseline
  (`marks{via,proxy}`) is subtle and untested; getting it wrong silently misclassifies
  every proxy's anonymity level. **Write Rust tests here first** — this is the module where
  the port is most likely to be quietly wrong.

### `checker.py` — Port + redesign
- **Evidence:** 363 LOC of tests. `[COMPLEX]` — `check()` is a long sequential state machine.
- **Why redesign:** protocol iteration order comes from dict order; must not become
  `HashMap` iteration. `check_judges()` must run **before** any proxy check to establish
  the anonymity baseline — an ordering constraint enforced only by convention in Python.

### `resolver.py` — **Replace-with-lib**
- **Evidence:** 557 LOC, churn 42 (2nd highest), **829 LOC of tests** (most-tested module).
- **Why replace rather than port:** most of it is a hand-rolled DNS cache
  (`cachetools`) + resolver plumbing. `hickory-resolver` 0.26 ships a built-in
  `ResponseCache`. Porting 557 lines to reimplement what the library already does is
  exactly the code not worth writing. **Serves the dependency-reduction goal.**
- **Keep:** external-IP discovery and the `real_ext_ips` **set** semantics (dual-stack
  hosts legitimately return both v4 and v6 — a set, not a scalar).
- **Split:** GeoIP moves to its own `geo.rs` behind a feature flag (licensing, C2).

### `providers.py` — **Rewrite as data** · hottest file in the repo
- **Evidence:** churn **47 — the highest**. Only 131 LOC of tests against 806 LOC of source:
  **the worst coverage-to-churn ratio in the project.**
- **Measured liveness (2026-07-15):** **~10 of 38 registry entries are dead.** 9 domains do
  not resolve at all (`hugeproxies.com`, `proxy.rufey.ru`, `geekelectronics.org`,
  `go4free.xyz`, `get-proxy.net`, `freeproxylists.com`, `proxyb.net`, `proxylist.me`,
  `proxz.com`); 4 more return 403/404/502. 13 confirmed still yielding proxies.
- **Why rewrite as data:** the churn *is the point*. Provider sites rot continuously, so
  the highest-churn file in the project is churning for reasons that have nothing to do
  with the code being wrong. Encoding them as Rust source means every dead site costs a
  recompile-and-republish. As config, it costs a YAML edit. Python half-learned this
  (`ConfigurableProvider` + YAML loaders exist); the port starts there.
- **Do not port the dead ones.** ~26% of the file is waste.
- **Caveat, stated plainly:** a root domain returning 200 is **weak evidence** — it proves
  DNS and a web server, not that the scraper's regex still matches. Beyond the 9 confirmed-
  dead, per-provider parse validation is the only real test. Treat the surviving count as
  an upper bound.

### `provider_utils.py` — Merge · **Confidence: Medium**
- **Evidence:** 521 LOC of tests, low churn (14) — recent, well-tested, deliberate.
- **Why merge:** it is the *newer* provider abstraction (`SimpleProvider`,
  `PaginatedProvider`, `APIProvider`, `ConfigurableProvider` + directory config loading),
  living beside the older `providers.py` hierarchy. In Rust these collapse into one
  `ProviderSpec` enum-of-data plus one trait escape hatch. Two parallel abstractions is a
  Python-side accretion the port should not inherit.
- **Medium confidence:** all four classes and all four `load_*` functions are in the public
  `__all__` (goals C6), so the merge must not silently drop a documented capability. The
  map must show each one's Rust home.

### `server.py` — Port + redesign · **Confidence: Medium**
- **Evidence:** 366 LOC of tests, churn 33.
- **Why medium:** the trickiest concurrency in the project. Backpressure is expressed as
  `await self._proxies.join()` — the grabber pauses while the pool is full. The Rust design
  must reproduce that or it will either spin forever or starve. `ErrorOnStream` handling
  branches on `'Timeout' in repr(e)` — **a substring search over an exception's repr** — which
  has no Rust equivalent and must become a typed variant.
- Behind the `server` feature: pure library users mostly don't want a listening socket.

### `api.py` — Port + redesign
- **Evidence:** churn 42, only 188 LOC of tests for 625 LOC — the orchestrator is
  under-tested relative to its centrality.
- **Why redesign:** the whole delivery model changes. `find()` is a fire-and-forget spawner
  delivering out-of-band on a user-supplied queue terminated by a `None` poison pill; Rust
  returns a `Stream` terminated by dropping the sender. `_on_check` is a bounded queue
  impersonating a semaphore → `tokio::sync::Semaphore`. `_update_limit` relies on
  **integer underflow** (`limit -= 1; if limit == 0`) so `limit=0` means "unlimited" by
  never reaching zero → `Option<usize>`.

### `cli.py` — Rewrite
- **Evidence:** **623 LOC of tests** — the second-most-tested module. click→clap is a total
  rewrite regardless; the tests define the CLI contract to preserve.
- **`update-geo` is dead code.** `utils.py:195` / `cli.py:130` hit a MaxMind endpoint retired
  on 2019-12-30. It has been non-functional for 6+ years. We ship DB-IP instead → the
  subcommand's fate is a map decision, not a port.

### `__init__.py` — Rewrite → `lib.rs`
- Version is read by **regex-scraping `pyproject.toml` at import time**. Rust has
  `env!("CARGO_PKG_VERSION")`. The `logging`/`warnings` filter setup becomes `tracing`, and a
  library must not configure global logging — that is the binary's job.

### `__main__.py` — Drop
- 4 lines of `python -m` plumbing. `[[bin]]` replaces it entirely.
