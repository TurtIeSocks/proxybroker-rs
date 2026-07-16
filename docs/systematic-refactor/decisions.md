# Decisions — the authoritative resolution

Inputs to this document:

- `trace.md` — 55 risks ranked by likelihood of sinking the port; 3 confirmed by executing
  the Python.
- `map.md` §Critique — 40 findings; verdict: **"not implementable as written."**
- `research.md` — verified crate APIs + the GeoLite2 licensing analysis.
- The compiler — the final arbiter. Several entries below exist because `cargo` disagreed
  with a document.

Where a document and the compiler disagree, **the compiler wins**. Where two documents
disagree, the resolution is stated here and this file wins. Nothing below is a suggestion.

---

## The convergence that shapes the design

Three findings arrived independently, from three different processes, and resolve with a
single decision:

| # | Finding | Found by |
|---|---|---|
| A | `Judge.available` / `Judge.ev` are process-global mutable state; `Checker.__init__` must call `Judge.clear()` to stop two Brokers stomping each other | main-thread reading of `judge.py` |
| B | `asyncio.Event` is **level**-triggered; `tokio::sync::Notify` is **edge**-triggered. `checker.py:111` sets the gate once, `checker.py:173` waits on it per-check. A naive port hangs **every** check spawned after the judges verify — which is all of them. | trace agent, **verified by execution** |
| C | Judge-probe timing was specified three incompatible ways (lazy `OnceCell` + `warm()`, eager-in-constructor, eager-awaited-in-`find`) | design critique #4 |

**Decision: judges are probed eagerly in `Checker`'s constructor, and `Checker` owns them
by value.**

This is not a compromise, it kills all three:
- No global state → no `Judge.clear()` → (A) gone.
- No event to wait on → level-vs-edge is unrepresentable → (B) gone.
- `Checker::check` becomes unconstructible before the baseline exists → the ordering
  constraint is a **type fact**, not a convention → (C) gone.
- `checker.py:137`'s `raise RuntimeError("Not found judges")` becomes
  `Checker::new() -> Result<_, Error::NoJudges>`.

Verified: `checker.py:111/173` and `judge.py:69-71` are exactly as described;
`asyncio.Event.wait()` after `.set()` returns immediately (demonstrated).

---

## CRITICAL — silent failure. Resolved before any code.

### 1. `_on_check` is a queue impersonating **two** primitives, not one
`api.py:102/470/472/485`. I originally read this as "a queue impersonating a semaphore →
use `tokio::sync::Semaphore`". **That read was incomplete and would have lost proxies.**

It drives **two independent signals** that a Rust permit-drop would fuse into one:
- `put(None)` — acquires a concurrency slot (blocks at `maxsize=max_conn`).
- `task_done()` — releases `join()` waiters (a **WaitGroup** barrier).
- `get_nowait()` — frees the slot.

Verified by the trace agent: `join()` returns while `qsize()` is non-zero. The two
counters are genuinely independent.

**Decision: implement BOTH, separately.**
- `Arc<Semaphore>` + `acquire_owned()` **before** spawn, permit **moved into** the task so
  `Drop` frees it (race-free by construction).
- A separate outstanding-checks barrier (`JoinSet`, or `AtomicUsize` + `Notify`) for the
  `join()` semantics.

Getting this wrong is not a crash: implement only the semaphore and `_grab` returns before
checks finish, `_done()` cancels in-flight checks, and **proxies are silently dropped**.
Port it as `mpsc(max_conn)` and the receiver drains instantly, `send()` never blocks,
backpressure vanishes, and 200 becomes tens of thousands of tasks until FDs run out.

### 2. Rust's `regex` crate **cannot compile the default provider pattern**
`utils.py:34` `IPPortPatternGlobal` uses `(?=.*?...)`. Confirmed by the compiler:

```
error: look-around, including look-ahead and look-behind, is not supported
```

This is the **default** pattern for every provider, plus three provider-specific patterns
(`providers.py:355/410/476`).

**Decision: hand-write a two-pass scanner in `src/parse.rs`. Do NOT add `fancy-regex`.**
The lookahead means "an IP followed later by another IP or a port" — that is a two-pass
scan wearing a regex costume. Backtracking would reintroduce exactly the ReDoS risk
`utils.py`'s own comments boast of avoiding. One design agent independently reached the
same answer (`provider::parse::find_ip_port`, "hand-written two-pass, IPv6 masking").

`src/parse.rs` is the **one** home for IP-scanning. The critique (#40) found it specified
in three places (`provider::parse::find_ip_port`, `utils::get_all_ip`,
`broker::load_data`). One algorithm, one home; `utils` re-exports or dies.

### 3. Termination: the `None` poison pill is one-shot and non-broadcast
`api.py:492/522`, `server.py:98`. Exactly one consumer takes the `None` and does not
re-inject it. It only works because `loop.stop()` nukes every other waiter.

**Decision: termination is dropping the sender.** No sentinel value. `ProxyStream` ends
when the channel closes — broadcast and multi-consumer safe by construction.

### 4. Cancellation actually has to exist
Critique #14/#15. The headline API promise is "drop the stream, the fleet stops", and
nothing implemented it: per-proxy tasks spawned via `tokio::spawn` are **detached** and
outlive `ProxyStream`, still holding permits and sockets. `take_limit_slot` stopped
exactly one task while every other in-flight task kept emitting.

**Decision:** `Run` owns a `CancellationToken` + a tracked `JoinSet`/`TaskTracker`.
`ProxyStream::drop` fires the token. Limit exhaustion fires the same token. The CLI's
redundant `.take(n)` goes.

### 5. `--limit 0` must mean unlimited
Critique #16. Python relies on **integer underflow** (`limit -= 1; if limit == 0`), so
`0` never reaches zero. `Option<usize>` + `StreamExt::take(0)` would make the **default**
`find` return nothing.

**Decision:** `0 → None` mapped explicitly in `build_query`, with a test asserting the
default returns proxies.

### 6. No `finally` — `?` silently skips cleanup
`proxy.py` connect/recv/send, `checker.py:262`, `server.py:443-446`. Python's `finally`
returns proxies to the pool and increments `error_rate`'s **denominator**. `?` skips it,
so the stats corrupt silently and proxies leak.

**Decision:** RAII guards for anything a `finally` protects. Not `?`-and-hope.

---

## Type identity — resolved in code, not prose

Critique #1 (its own top-3): *"thirteen names for five concepts… write one page naming
every shared type and its home. This is the highest-leverage half-day in the project."*

**Done: `src/types.rs`, and it is code, not a doc.** A doc drifts; a type that does not
compile stops you. 8 tests green.

| Canonical | Killed |
|---|---|
| `Proto` (in `types.rs`) | `ProxyType`, `NegotiatorKind` |
| `AnonLevel` | `Anonymity` |
| `ProxyError` (in `error.rs`, singular) | `ProxyFailure`, `errors.rs` |
| `Stream` (in `negotiator.rs`, re-exported) | the duplicate in `proxy.rs` |
| `JudgePool` (in `judge.rs`) | the duplicate in `checker.rs` |
| `Scheme` (`types.rs`), `JudgeScheme` (`types.rs`) | both duplicates |
| `TypeSpec` (`types.rs`) | `types + http_levels`, `IntoIterator<(NegotiatorKind, BTreeSet)>` |

Tests in `types.rs` encode behaviour the parallel designs disagreed about, each verified
against the Python interpreter rather than asserted:
`display_order_matches_python` (CONNECT:80 **before** CONNECT:25 — `'0'`(48) < `'5'`(53);
one design claimed otherwise), `judge_scheme_routing_matches_python`,
`only_http_carries_anonymity`.

---

## Socket ownership — one answer

Critique #1. Three modules gave three answers. `proxy.rs` designed a `ProxyConn` transport
layer with eight methods that `checker.rs` and `negotiator.rs` had both explicitly deleted
— a week of work with zero callers.

**Decision: `checker.rs` + `negotiator.rs` win.** `Proxy` = data + `record_attempt()`. It
owns no socket. Delete `ProxyConn`, `ConnectOpts`, `replace_stream_with`, `into_io`
(which could not compile anyway — you cannot move a field out of a `Drop` type).

---

## Errors

`error.rs` was **factually wrong** to drop `Error::NoJudges` as "speculative, no Python
grounding". Verified: `checker.py:137` `raise RuntimeError("Not found judges")`.

**Decision:** `Error` gains `NoJudges`, `NoTypes`, `NoProviders`, `ExtIpUnknown`.
`error.rs` is not the authority on what other modules need from the shared enum.

**Keep `ProxyError::Reset` merging Recv+Send.** Not laziness — `ProxyRecvError` and
`ProxySendError` share `errmsg="connection_is_reset"` in Python, so they are already **one
histogram bucket**. Deriving the key from Rust variant names would silently **split** the
bucket and change `error_rate`. No test would catch it. Direction lives in the tracing
message, exactly where Python put it.

**Delete `ProxyError::BadStatusLine`** — nothing constructs it once hyper owns status
parsing.

---

## Deferred, with reasons

| Item | Decision |
|---|---|
| `Proxy.log: Vec<LogEntry>` (critique #23) | **Drop the vec.** `tracing` span per proxy; keep `Stats` + `runtimes`. `broker.rs` deleted `unique_proxies` for unbounded growth, then recreated it inside every pooled `Proxy`. |
| Pool importer serialization (critique #22) | One dedicated importer task owning the `Receiver`, pushing to `PoolState` + firing a `Notify`. Kills the `tokio::Mutex<Receiver>` that serialized N waiters × 5s. |
| `Plan::Builtin` string registry (critique #30) | **Delete.** A plugin system for a closed set shipping in the same binary. |
| `StreamFailure`, `Strategy` (1 variant), `NoProxy` (3 identical variants), `Parse::Auto`, `Plan::{Single,Urls}`, `Registry::{merge,retain,is_empty}` | **Delete.** Named over-engineering; no callers. |
| `resolver.rs` family pinning (critique #27) | **Verify or drop.** If `local_address(UNSPECIFIED)` pinning silently no-ops, every v6 probe returns v4, gets rejected, and costs 7×timeout — a 35s startup stall, not a graceful degrade. |
| `utils.rs` (critique #39) | **The module every other module assumed someone else wrote.** Owed: `get_headers` (ordered — `IndexMap`, never `HashMap`; iteration order is wire-visible), `get_all_ip`, `canonicalize_ip`, `get_status_code` (returns 400 as a **sentinel** callers depend on). |

---

## Bug-compatibility: explicitly not a goal

Where the Python is wrong, the Rust is right and the deviation is recorded. Confirmed
upstream bugs found during the port:

1. **`providers.py:706/710` — missing trailing comma.** `proto=("SOCKS4")` is a `str`, not
   a tuple. `api.py:409`'s `bool(pr.proto & types.keys())` intersects the string's
   **characters** (`{'S','O','C','K','4'}`) against protocol names → always empty → both
   proxyscrape SOCKS providers are **silently dropped**. Measured liveness says those are
   among the highest-yield sources still alive (534 and 2,084 ip:port). Worth an upstream
   issue. `Vec<Proto>` has no comma to forget.
2. **`heapq.heappush((avg_resp_time, proxy))`** (`server.py:127`) — on tied `f64`, Python
   compares the `Proxy` objects, which define no `__lt__` → `TypeError`. Confirmed by
   execution. (`f64` is not `Ord` in Rust either — this needs a deliberate total order.)
3. **The SMTP disable path is a no-op** → `secrets.choice([])` `IndexError`. Confirmed by
   execution. Python drops one proxy; a Rust `.unwrap()` would panic the whole run.
4. **`update-geo` has been dead since 2019** (MaxMind retired the anonymous endpoint).
5. **Bundled GeoLite2 is 8.9 years stale** (build 2017-09-06, decoded from the file).

## Quiet killers — compile fine, ship wrong

- `utf-8 'ignore'` **drops** bytes; `String::from_utf8_lossy` **inserts** U+FFFD. The
  judge echo-greps decide whether a proxy passes. Use a lossy-drop equivalent deliberately.
- `HashMap` iteration randomizes **wire-visible** header order → `IndexMap` everywhere.
- Leading-zero IPv4 (`010.1.1.1`) parses differently → silently drops real proxies.
- Strict base64 empties three providers (Python's decoder is lenient).

## Port order

Dependency-dictated, not risk-ranked: `types` → `error` → `utils`/`parse` → `resolver` →
`proxy` → `negotiator` → `judge` → `checker` → `provider` → `broker` → `server` → `cli`.

**Write characterization tests first** for the transport/check/serve path — it has zero
Python coverage, therefore no oracle. Expect a wave of newly-visible errors the Python was
swallowing in done-callbacks.
