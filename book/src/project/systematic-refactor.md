# The Systematic Refactor

proxybroker-rs is a from-scratch Rust port of Python's
[proxybroker2](https://github.com/bluet/proxybroker2). It was not a line-by-line
transliteration — it was a deliberate, evidence-driven refactor that treated the
original as a specification to be understood, not a text to be copied. The full
working documents live in the repository under
[`docs/systematic-refactor/`](https://github.com/TurtIeSocks/proxybroker-rs/tree/main/docs/systematic-refactor).

## The method: trace → goals → map → port

The port ran through four artifacts before most of the Rust was written, each
feeding the next:

1. **Trace** ([`trace.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/systematic-refactor/trace.md))
   — an execution-level read of the Python: 55 risks ranked by how likely each was
   to sink the port. Three of them were confirmed by *actually running* the Python,
   not by reading it.
2. **Goals** ([`goals.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/systematic-refactor/goals.md))
   — what the port is *for*: ship a Rust library **and** a CLI, on stable Rust, with
   every network path testable offline. Explicit non-goals too: no Python interop, no
   bug-compatibility, no invented performance target.
3. **Map** ([`map.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/systematic-refactor/map.md))
   — a module-by-module correspondence between Python and Rust, plus a 40-finding
   critique whose verdict on a naive port was blunt: *"not implementable as written."*
4. **Port** — resolved every conflict in
   [`decisions.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/systematic-refactor/decisions.md),
   in a strict dependency order: `types` → `error` → `utils`/`parse` → `resolver` →
   `proxy` → `negotiator` → `judge` → `checker` → `provider` → `broker` → `server` →
   `cli`.

When a design document and the compiler disagreed, the compiler won. When two
documents disagreed, `decisions.md` settled it and became authoritative.

## Byte-for-byte parity where it matters

The port is idiomatic Rust, not transliterated Python — a faithful port of
`Proxy.as_json()` is `impl Serialize`, not a method returning a map. But in the
places where behaviour is wire-visible or statistically load-bearing, the port
matches the original exactly, on purpose:

- **Error histogram buckets.** `ProxyError::Reset` deliberately merges the receive
  and send cases, because Python files both under one `errmsg="connection_is_reset"`
  bucket. Splitting them by Rust variant name would silently change the reported
  error rate, and no test would catch it. Direction lives in the tracing message,
  exactly where Python put it.
- **Header ordering.** Request headers use an ordered `IndexMap`, never a `HashMap`
  — iteration order is wire-visible, and a judge's echo-grep decides whether a proxy
  passes.
- **Display order.** `CONNECT:80` sorts before `CONNECT:25` (`'0'` < `'5'`), verified
  against the Python interpreter rather than assumed.
- **Lenient parsing.** UTF-8 is decoded lossy-drop (Python's `errors='ignore'` drops
  bytes rather than inserting U+FFFD); base64 is decoded leniently (strict decoding
  empties three providers); leading-zero IPv4 is handled so real proxies are not
  silently dropped.

Where the Python is *wrong*, the Rust is right and the deviation is recorded. The
port found and corrected several confirmed upstream bugs — a missing trailing comma
that silently dropped both proxyscrape SOCKS sources, a `heapq` tie-break that raised
`TypeError` on equal response times, and a SMTP-disable path that could `IndexError`.

## Key design decisions

### Socket ownership — one answer

Three modules had proposed three different transport designs. One of them, a
`ProxyConn` layer with eight methods, had already been deleted by the two modules
that would have called it — a week of work with zero callers, one variant of which
could not even compile (you cannot move a field out of a `Drop` type). The
resolution: `checker.rs` and `negotiator.rs` win. **`Proxy` is data plus
`record_attempt()`; it owns no socket.** The transport lives in the checker and the
negotiator, and the shared `Stream` type has exactly one home.

### Judges are probed eagerly, and owned by value

Three independent findings — process-global mutable judge state, a level-vs-edge
mismatch between `asyncio.Event` and `tokio::sync::Notify`, and a probe-timing
question specified three incompatible ways — collapsed into one decision: judges are
probed eagerly in the `Checker` constructor, which owns them by value. That makes
`Checker::check` *unconstructible* before the baseline exists, so an ordering
constraint that was a fragile convention in Python becomes a type fact in Rust.

### The licensing pivot

The Python distribution vendors a MaxMind `GeoLite2-Country.mmdb` inside its package.
Whether a crates.io crate may redistribute that data is a real licensing question
with a real answer — and the answer is no under MaxMind's terms. The port pivoted the
bundled geo data to **DB-IP Country Lite, licensed CC BY 4.0**, which *is*
redistributable provided attribution is carried. That attribution is baked into
`--version` output and the `NOTICE`, and the `geo-bundled` build feature can be turned
off to ship zero geo data and zero attribution duty. The whole crate is Apache-2.0,
matching proxybroker2 (a port is a derivative work) and crediting the original
authors. See [Data & Licensing](../data-and-licensing.md) for the full story.

## Why this way

The port was justified by wanting a Rust library, not by a measured Python
bottleneck — so no "10× faster" claim was invented, because a claim with no baseline
is unfalsifiable fiction. What *was* committed: no accidental pessimisation (the
concurrency model keeps its bounded-queue, capped-in-flight shape), and every
network-dependent path testable offline against a local mock server. Those
constraints, plus the deferral discipline in the [roadmap](./roadmap.md) and
[deferred backlog](./deferred-backlog.md), are what kept the rewrite honest.
