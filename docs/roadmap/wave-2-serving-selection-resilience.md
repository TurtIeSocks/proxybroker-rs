# Wave 2 — Serving: selection & resilience

*Make the rotating server actually good.* Every feature in this wave lands in `src/server.rs`
(`Pool` / `best_for` / `handle_client` / `relay` / `serve`) and its CLI shell
`src/bin/proxybroker.rs` (`ServeArgs` / `serve_cmd`). Two features (B3, B4) also touch the
filter/output path (`FindQuery`, `country_ok` in `src/broker.rs`).

## Theme

The server today is a competent byte-splicer with one selection rule (lowest
`(error_rate, avg_resp_time)`, `best_for` at `server.rs:141`) and a blind per-request retry
(`handle_client` loops `for _ in 0..max_tries`, `server.rs:242`). It cannot filter what fills
the pool by anonymity or country, cannot pin a client to one upstream, cannot re-probe a proxy
that failed, and copies bytes without ever looking at the upstream status. This wave closes all
of that.

## Build order (respects dependencies)

1. **B3 — Serve filter passthrough.** Pure plumbing in `serve_cmd`; no `server.rs` change. Ships
   first because it is isolated and unblocks nothing but is the cheapest real hole to close.
2. **B4 — Country filter on serve + check.** Also plumbing; the `check` half reuses the existing
   `country_ok` (`broker.rs:343`). Independent of B3.
3. **★ Shared refactor — the selection seam.** Before B1 touches `best_for`, land the one
   structural change the rest of the wave rides on: (a) capture the client peer `SocketAddr` in
   the accept loop (`server.rs:196` currently discards it as `Ok((s, _))`), thread it into
   `handle_client`; (b) turn the free fn `best_for(pool, scheme)` into
   `best_for(pool, scheme, &SelectCtx)` where `SelectCtx` carries the strategy, the
   prefer-connect flag, and the optional sticky key. This is the single isolated selection point
   the roadmap promises — every later feature is a field on `SelectCtx` or a branch inside
   `best_for`.
4. **B1 — Selection strategy + sticky sessions.** Introduces `Strategy` and `ClientKey`, fills
   in `SelectCtx`.
5. **B5 — Health-scored selection + re-probe timer.** Wraps pooled proxies with a
   `blocked_until` timestamp; extends `best_for`'s ranking + a backup tier.
6. **B2 — Inline rotate-on-error / retry failover.** Formalizes the `handle_client` loop around a
   *commit boundary* (retry only while no byte has reached the client).
7. **B11 — `--http-allowed-codes`.** Peeks the upstream HTTP status inside `relay` before the
   splice; a bad code becomes a retryable error feeding B2's loop. HTTP scheme only.
8. **B10 — `--prefer-connect`.** A tie-break field already present on `SelectCtx`; a comparator
   branch in `best_for`.
9. **B13 — `--min-queue` / `--backlog`.** Startup controls in `serve` / `serve_cmd`; independent,
   lands last.

---

## B3 — Serve filter passthrough (`--lvl` / `--strict` / `--post` / `--dnsbl`)

**Goal.** Let `serve` fill its pool with anonymity-filtered, DNSBL-screened proxies. Today
`serve_cmd` builds a `FindQuery` with `..Default::default()` (`bin/proxybroker.rs:277`), so
`lvl`/`strict`/`post`/`dnsbl` are unreachable — anonymity-filtered serving is impossible.

**Public surface.** New flags on `ServeArgs` (`bin/proxybroker.rs:76`), mirroring `FindArgs`:

```rust
/// Anonymity levels to accept for HTTP (e.g. High Anonymous). Default: any.
#[arg(long, num_args = 1.., value_name = "LVL", value_parser = parse_lvl)]
lvl: Vec<AnonLevel>,
/// DNS blocklist zones; reject proxies listed in any (e.g. zen.spamhaus.org).
#[arg(long, num_args = 1.., value_name = "ZONE")]
dnsbl: Vec<String>,
/// Use POST instead of GET for the pool-fill test request.
#[arg(long)]
post: bool,
/// Require the anonymity level to match exactly.
#[arg(long)]
strict: bool,
```

No library API change — `FindQuery` already carries every field.

**Design.** In `serve_cmd` (`bin/proxybroker.rs:266`), stop using `TypeSpec::any` and instead
build `TypeSpec { proto, levels }` exactly as `find` does (`bin/proxybroker.rs:323`), and set the
four passthrough fields on the `FindQuery`:

```rust
let levels = (!args.lvl.is_empty()).then_some(args.lvl);
let types = args.types.into_iter()
    .map(|proto| TypeSpec { proto, levels: levels.clone() })
    .collect();
let stream = broker.find(FindQuery {
    types,
    countries: (!args.countries.is_empty()).then_some(args.countries),
    limit: Some(args.limit.max(1)),
    dnsbl: args.dnsbl,
    timeout: Duration::from_secs(args.timeout),
    post: args.post,
    strict: args.strict,
    ..Default::default()
}).await?;
```

The pool then only ever receives proxies matching the filter — the filtering happens upstream in
`find_task` (`broker.rs`), nothing in `server.rs` changes.

**Offline test plan.** `serve_cmd` is a thin CLI adapter; the filter logic it delegates to is
already covered by `tests/find.rs`. The parity worth locking is *the mapping* — that `--lvl`
becomes `TypeSpec.levels` and `--strict`/`--post` reach `FindQuery`. Cheapest offline assertion:
a unit test in a new `#[cfg(test)]` block that constructs `ServeArgs` and asserts the built
`FindQuery`. Since `serve_cmd` currently inlines the query, first extract a pure
`fn serve_query(args: &ServeArgs) -> FindQuery` (no I/O), then:

- **First failing test:** `serve_query_threads_lvl_and_strict` — build a `ServeArgs` with
  `lvl = [High]`, `strict = true`, `post = true`, `dnsbl = ["zen.spamhaus.org"]`; assert the
  returned `FindQuery` has `types[0].levels == Some(vec![High])`, `strict == true`,
  `post == true`, `dnsbl == ["zen.spamhaus.org"]`. Fails today because the fields are dropped.

**Acceptance criteria.**
- [ ] `serve --lvl High` fills the pool with only High-anon HTTP proxies.
- [ ] `--strict`, `--post`, `--dnsbl` each reach the `FindQuery`.
- [ ] `serve_query` is pure (no async, no broker) and unit-tested offline.
- [ ] `tests/serve.rs` still green.

**Risks / deviations / principle-flags.** Extracting `serve_query` is the *smallest* change that
makes the mapping testable without a network — honors offline-testable without a mock server.
No new dependency.

**Effort.** S.

---

## B4 — Country filter on serve + check (`--only-cc US,DE`)

**Goal.** Restrict served (and checked) proxies to a country allow-list. `serve` already has
`--countries` that flows into `FindQuery.countries`; this feature adds the same allow-list as an
**admission predicate on the pool** so that a warm/BYO pool (`Pool::from_proxies`) is also
filtered, and confirms the `check` subcommand honors it too.

**Public surface.**
- CLI: keep the existing `--countries` on `ServeArgs`/`FindArgs`. Add `--only-cc` as an alias
  accepting a comma-joined list (`--only-cc US,DE`) in addition to the space-separated
  `--countries US DE`, matching proxybroker2's spelling. Implement via clap
  `value_delimiter = ','` on a shared arg, or a second `#[arg(long = "only-cc", value_delimiter = ',')]`
  that merges into the same `Vec<String>`.
- Library: an admission filter on `Pool`:
  ```rust
  // PoolConfig gains:
  pub countries: Option<BTreeSet<String>>,   // uppercased ISO codes; None = no filter
  ```
  Both `Pool::spawn` and `Pool::from_proxies` reject a pushed proxy whose `geo.code` is not in
  the set (a no-op when `None`).

**Design.** Reuse the exact predicate `country_ok` (`broker.rs:343`) — lift it to a shared
`pub(crate)` helper (or duplicate the three-line body in `server.rs`; lazy-that-holds says
duplicate rather than create a new module for one predicate, but since it is *identical* logic,
re-export `crate::broker::country_ok` behind `pub(crate)`). Apply it:
- In the `Pool::spawn` importer loop (`server.rs:88`), before `pool.state.lock().push(proxy)`.
- In `Pool::from_proxies` (`server.rs:65`), filter the incoming `Vec`.

For `check`: the `check` subcommand (Wave 1, A1) already country-filters its output stream via
the same `country_ok`, so B4's "on check" half is a **verification** that `--only-cc` reaches
`FindQuery.countries` there — no new code beyond the CLI alias.

**Offline test plan.** `tests/serve.rs`-style, no network.
- **First failing test:** `pool_admits_only_allowed_countries` — build two `Proxy` values with
  `geo = Some(Country{code:"US",..})` and `Some(Country{code:"FR",..})`, `Pool::from_proxies`
  with `PoolConfig { countries: Some({"US"}), ..default }`; assert `pool.get(Scheme::Http)`
  yields only the US proxy and then `None`.
- `pool_no_filter_admits_all` — `countries: None` admits both (guards the no-op path).
- CLI unit: `serve_query_threads_only_cc` — `--only-cc us,de` (lowercase) yields
  `countries == Some(["us","de"])`; the uppercasing happens in the predicate, matching
  `grab_task` (`broker.rs:141`).

**Acceptance criteria.**
- [ ] A pool configured with `countries` admits only matching proxies, in both `spawn` and
      `from_proxies`.
- [ ] `--only-cc US,DE` and `--countries US DE` are equivalent.
- [ ] `check --only-cc` filters output (verified against the A1 path).
- [ ] Case-insensitive match (predicate uppercases both sides, as `country_ok` does).

**Risks / deviations / principle-flags.** ⚠ Duplicated three-line predicate vs a new shared
module. Mitigation: re-export the existing `country_ok` as `pub(crate)` from `broker.rs` — no new
abstraction, one definition. ⚠ The `--only-cc` alias plus `--countries` is two spellings for one
field; document that they merge, and dedup on read.

**Effort.** S.

---

## B1 — Selection strategy + sticky sessions

**Goal.** Replace the single hard-coded "best" pick with a chooseable `Strategy`
(`Best`, `RoundRobin`, `Random`, `Sticky`), and let `Sticky` pin a client (by IP, or a
configurable header) to one upstream across requests — the #1 rotating-proxy ask.

**Public surface.**
```rust
// server.rs — new public enum
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Strategy {
    #[default] Best,     // current behaviour: lowest (error_rate, avg_resp_time)
    RoundRobin,          // rotate through eligible proxies in pool order
    Random,              // uniform pick among eligible
    Sticky,              // same client → same proxy while it stays healthy
}

// The sticky client identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientKey {
    Ip(std::net::IpAddr),   // default: client peer IP
    Header(String),         // value of --sticky-header (e.g. an X-Session-Id)
}

// PoolConfig gains:
pub strategy: Strategy,           // default Best
pub sticky_header: Option<String>, // header name to key Sticky on; None = client IP
```

CLI (`ServeArgs`):
```rust
/// Selection strategy for picking an upstream per request.
#[arg(long, value_enum, default_value_t = SelectStrategy::Best)]
strategy: SelectStrategy,   // ValueEnum mirror of server::Strategy
/// With --strategy sticky, key the session on this request header instead of client IP.
#[arg(long, value_name = "HEADER")]
sticky_header: Option<String>,
```

**Design.**

*The selection seam (shared refactor, landed with B1).* `best_for` (`server.rs:141`) becomes:

```rust
struct SelectCtx {
    scheme: Scheme,
    strategy: Strategy,
    prefer_connect: bool,          // filled by B10; false for now
    sticky: Option<(u16, u16)>,    // index-agnostic pin, see below
    round_robin_cursor: usize,     // Pool-held, advanced under the state lock
}
fn best_for(pool: &[Pooled], ctx: &SelectCtx) -> Option<usize> { ... }
```

Dispatch inside `best_for` over `ctx.strategy`:
- `Best` — today's `min_by(priority, total_cmp)` over the scheme-eligible subset.
- `RoundRobin` — among the eligible indices, pick `cursor % eligible.len()`, advance the cursor
  (stored on `Pool`, mutated under the same `state` lock `get` already holds, `server.rs:111`).
- `Random` — `fastrand::usize(..eligible.len())`. `fastrand` is already a transitive dep; if not,
  feature-gate it or use a tiny xorshift seeded from `Instant` — avoid pulling `rand` for one
  call.
- `Sticky` — see below.

*Sticky.* `Pool` gains `sessions: Mutex<HashMap<ClientKey, (IpAddr, u16)>>` mapping a client to
the **address** (not index — indices churn under `swap_remove`) of the proxy it is pinned to. The
flow, driven from `handle_client`:
1. The accept loop (`server.rs:191`) stops discarding the peer: `Ok((s, peer)) => (s, peer)`, and
   passes `peer.ip()` into `handle_client`.
2. `handle_client` computes the `ClientKey`: `ClientKey::Ip(peer_ip)` by default, or
   `ClientKey::Header(value)` if `sticky_header` is set and the header is present in
   `req.raw` (parse it out of the already-buffered request — `parse_client_request` returns
   `req.raw` for HTTP; add a helper to read a named header). Absent header → fall back to IP.
3. `pool.get(scheme, key)` (new signature, was `get(scheme)` at `server.rs:105`): under the state
   lock, look up `sessions[key]`; if a pooled proxy with that `(host,port)` is present and
   scheme-eligible, return it; else run the strategy pick, and on `Sticky` record
   `sessions.insert(key, chosen.addr)`.
4. On `put` after a healthy return, the proxy re-enters the pool `Vec`; the session map still
   points at its address, so the next request from that client re-finds it. On eviction
   (`put` drops it, `server.rs:131`), the address is simply absent next time → a fresh pick +
   map update (self-healing, no explicit map cleanup needed; entries for departed clients are
   bounded — see risks).

`Best`/`RoundRobin`/`Random` ignore the key. Only `Sticky` reads/writes `sessions`.

**Offline test plan.** All against `Pool` directly (no server socket), `tests/serve.rs` style.
- **First failing test:** `sticky_returns_same_proxy_for_same_client` — `Pool::from_proxies` with
  two distinct proxies, `strategy: Sticky`; call `get(Http, ClientKey::Ip(A))`, `put` it back,
  `get(Http, ClientKey::Ip(A))` again → asserts the same `addr()` both times; a `get` with
  `ClientKey::Ip(B)` may differ. Fails today (no `Strategy`, no keyed `get`).
- `round_robin_cycles_through_pool` — three eligible proxies, `RoundRobin`; four sequential
  `get`+`put` calls visit addr1,2,3,1.
- `random_stays_in_eligible_set` — 100 draws all land on scheme-eligible proxies.
- `best_is_default_and_unchanged` — `Strategy::Best` reproduces
  `best_for_picks_lowest_response_time` (the existing test, `server.rs:407`).
- `sticky_falls_back_when_pinned_proxy_evicted` — pin client A to proxy P, drop P via an
  unhealthy `put`, next `get` for A returns a different proxy and rebinds.
- End-to-end: extend `tests/serve.rs` with `sticky_pins_client_over_two_requests` using two mock
  upstreams that echo distinct bodies, two sequential client requests on one TCP connection reuse
  → same upstream body.

**Acceptance criteria.**
- [ ] `Strategy::Best` is the default and byte-identical to today.
- [ ] `RoundRobin` / `Random` / `Sticky` each dispatch inside `best_for`.
- [ ] Sticky keys on client IP by default, on `--sticky-header` when set + present.
- [ ] Sticky self-heals when the pinned proxy is evicted.
- [ ] Peer IP is captured in the accept loop and reaches `handle_client`.

**Risks / deviations / principle-flags.**
- ⚠ **Unbounded `sessions` map.** A long-lived server sees many client IPs. Mitigation: cap the
  map (e.g. evict oldest when > `max_sessions`, default 10_000) or only insert on `Sticky`
  (already the case) — a bounded `HashMap` with a simple size guard, no LRU crate. Open question:
  is a size cap enough, or is a TTL wanted? Default to the size cap (lazy-that-holds).
- ⚠ **Concurrent requests from one client.** `get` `swap_remove`s the pinned proxy, so a second
  in-flight request from the same client cannot find it and picks another. This matches Python's
  best-effort stickiness. Documented as accepted, not a bug.
- ⚠ **`rand` dependency.** For `Random`, prefer `fastrand` (tiny, likely already present) or a
  seeded xorshift over pulling `rand`. Feature-gate if it is a genuinely new dep.
- **Open question:** header extraction for `ClientKey::Header` on a CONNECT request — CONNECT has
  no forwardable headers before the tunnel. Decide: `--sticky-header` applies to HTTP scheme
  only; CONNECT clients always key on IP. (Recommended.)

**Effort.** M.

---

## B5 — Health-scored selection + re-probe timer

**Goal.** Generalize the lowest-`priority()` pick: a proxy that fails is benched for a
`fail_timeout` window (a **backup tier**), and re-enters normal ranking once the window elapses —
so a transient failure does not permanently demote or (worse) instantly re-select a bad proxy.

**Public surface.**
```rust
// PoolConfig gains:
pub fail_timeout: Duration,   // how long a failed proxy is benched. Default 30s (server.py parity).
```
CLI (`ServeArgs`):
```rust
/// Seconds a proxy is benched after a failure before it is re-probed.
#[arg(long, default_value_t = 30)]
fail_timeout: u64,
```
No new public type; the bench state is internal.

**Design.** The pool stops storing bare `Proxy` and stores a small internal wrapper:

```rust
struct Pooled {
    proxy: Proxy,
    blocked_until: Option<std::time::Instant>,  // Some(t) = benched until t
}
```

`Pool.state: Mutex<Vec<Pooled>>`. This keeps the fail-timeout **out of `Proxy`** — `Proxy` is the
serialized value type shared with `find`/`check` (`proxy.rs`), and a server-only bench timestamp
does not belong on it (lazy-that-holds: the state lives where it is used).

`best_for` gains a two-tier rank:
1. **Primary tier** — `Pooled` where `blocked_until` is `None` or `<= Instant::now()` (window
   elapsed). Rank these by the active strategy (B1). A benched proxy whose window elapsed is
   silently promoted here — that *is* the re-probe: it becomes eligible again and the next
   request tries it.
2. **Backup tier** — still-benched proxies, used only if the primary tier is empty (better to try
   a benched proxy than 502).

`put` sets the bench on failure. Reshape `handle_client`'s failure arm (`server.rs:256`): after
`proxy.record_attempt(None, Some(e))`, when returning to the pool, mark
`blocked_until = Some(now + fail_timeout)`. On success, `blocked_until = None`. The existing
hard-eviction check in `put` (`server.rs:128`) stays — a *persistently* unhealthy proxy
(`error_rate`/`avg_resp_time` over threshold after `min_req`) is still dropped entirely; benching
is the softer, faster reaction to a single failure.

Because `put` currently takes `Proxy`, change it to `put(&self, proxy: Proxy, outcome: Outcome)`
or add `put_failed`/`put_ok` — the lazy split is two methods (`put_ok`, `put_failed`) rather than
a new `Outcome` enum for two cases.

**Offline test plan.** `tokio::time` can be paused (`tokio::time::pause()`/`advance`) to test the
timer without real sleeps — fully offline.
- **First failing test:** `failed_proxy_is_benched_then_re_probed` — pool of one proxy, `put_failed`
  it with `fail_timeout = 30s`; `get` returns `None` immediately (benched, no backup needed check:
  with only one proxy, backup tier returns it — so use two proxies: benched P1, healthy P2 →
  `get` returns P2). Then `tokio::time::advance(31s)`; `get` returns P1 again (re-probe). Fails
  today (no bench concept).
- `benched_proxy_is_backup_when_pool_otherwise_empty` — single benched proxy, `get` still returns
  it (backup tier) rather than `None`.
- `healthy_put_clears_bench` — `put_failed` then `put_ok` the same proxy leaves it primary-tier.
- `persistent_unhealthy_still_evicted` — the existing `put` eviction threshold
  (`server.rs:128`) still drops a proxy over `max_error_rate` after `min_req` (guards no
  regression of the current behaviour).

**Acceptance criteria.**
- [ ] A failed proxy is benched for `fail_timeout` and skipped while a healthy one exists.
- [ ] After `fail_timeout` it is re-probed (re-enters primary ranking).
- [ ] A benched proxy is still used as backup when nothing else is eligible.
- [ ] Hard eviction (`max_error_rate`/`max_resp_time` after `min_req`) is unchanged.
- [ ] `Proxy` gains no server-only field.

**Risks / deviations / principle-flags.** ⚠ The `Pool.state` element type changes
(`Vec<Proxy>` → `Vec<Pooled>`), touching every `state.lock()` site (`get`, `put`, `spawn`
importer, `from_proxies`). Mitigation: it is a mechanical wrap; keep `Pooled` `pub(crate)` and
un-exported. ⚠ Using `Instant` makes tests need `tokio::time::pause` — acceptable, and it keeps
the timer monotonic (no wall-clock).

**Effort.** M.

---

## B2 — Inline rotate-on-error / retry failover

**Goal.** When a served request fails on one proxy, transparently retry down the pool instead of
returning an error — but only while the failure is *invisible to the client* (no byte forwarded
yet). Formalizes the existing `for _ in 0..max_tries` loop (`server.rs:242`) around an explicit
commit boundary.

**Public surface.** No new library type. Behaviour change plus reuse of `PoolConfig.max_tries`
(already `--max-tries`, `ServeArgs:107`). The eviction hook `put`/`put_failed` (B5) is reused as
the "return the bad proxy" path.

**Design.** The loop already rotates (each `pool.get` `swap_remove`s a fresh proxy, `put`s the
failed one back). The real work is defining **which failures are retryable**, because `relay`
(`server.rs:281`) writes to the *client* mid-relay:
- For `Scheme::Https`, `relay` writes `HTTP/1.1 200 Connection established` to the client
  (`server.rs:300`) once the tunnel is up. After that write, the client is committed to this
  upstream — a later splice error is **not** retryable (the client already saw success).
- For `Scheme::Http`, `relay` writes the buffered request *upstream* first (`server.rs:307`) and
  only then splices; a failure in `TcpStream::connect`/`negotiate`/upstream-write happens before
  any byte reaches the *client* and **is** retryable.

Formalize by having `relay` report *where* it failed. Return a richer error or a two-state result:

```rust
enum RelayOutcome {
    Ok,
    RetryableFailure(ProxyError),   // nothing sent to client yet — safe to try next proxy
    ClientCommitted(ProxyError),    // client already got bytes/ack — must abort, not retry
}
```

`handle_client` continues the loop only on `RetryableFailure`; on `ClientCommitted` it records
the attempt, `put_failed`s the proxy, and returns (the connection is already half-spoken). The
final 502 (`server.rs:263`) is emitted only when the loop exhausts `max_tries` **without ever
committing** — matching the existing empty-pool 502 (`server.rs:245`).

This is the smallest change: the loop, the counter, and the 502 all exist; B2 adds the
commit-boundary discriminant so a retry never corrupts a client that already received bytes.

**Offline test plan.** `tests/serve.rs` with mock upstreams that fail.
- **First failing test:** `retries_next_proxy_when_first_connect_fails` — pool of two HTTP
  proxies: proxy1 points at a **closed** port (connect fails), proxy2 at a live `mock_upstream`.
  Client sends an HTTP GET; assert the client receives proxy2's body (the retry succeeded
  transparently). Today the loop already retries connect failures, so assert it also records the
  failure and does not 502 — write it to pin the behaviour B2 formalizes; make it fail first by
  asserting the *new* `RelayOutcome` classification (unit-test `classify`/`relay` boundary).
- `does_not_retry_after_client_committed` — a CONNECT (Https) client; upstream tunnel established
  then dropped mid-splice → assert exactly one proxy attempted (no silent second CONNECT), client
  connection closes rather than a second `200 Connection established`.
- `502_after_max_tries_all_fail` — pool of two dead proxies, `max_tries: 2` → client gets a
  single 502 (extends the existing empty-pool 502 test, `serve.rs:77`).

**Acceptance criteria.**
- [ ] A pre-commit failure (connect/negotiate/upstream-write for HTTP) retries the next proxy.
- [ ] A post-commit failure (after the client got the CONNECT 200 or any spliced byte) does not
      retry.
- [ ] Exhausting `max_tries` with no commit yields exactly one 502.
- [ ] Each failed proxy is fed back through `put_failed` (B5 bench).

**Risks / deviations / principle-flags.** ⚠ Behaviour parity: proxybroker2 retries blindly and can
double-send; this deliberately *deviates* by never retrying past the commit boundary (a
correctness win — avoids duplicate requests / corrupt tunnels). Record in `decisions.md`.
⚠ The retryable/committed split must be exhaustive so a new `ProxyError` variant defaults to a
safe (non-retry-past-commit) classification.

**Effort.** M.

---

## B11 — `--http-allowed-codes` (retry on bad upstream status)

**Goal.** When the upstream returns an HTTP status outside an allowed set (e.g. a `403`/`503`
block page), treat it as a failure and retry through a different proxy — dodging captcha/block
pages. Pairs with B2's retry loop. HTTP scheme only (CONNECT is an opaque tunnel with no
observable status).

**Public surface.**
```rust
// PoolConfig gains (or a relay-time arg threaded from serve()):
pub http_allowed_codes: Option<Vec<u16>>,  // None = accept any status (today's behaviour)
```
CLI (`ServeArgs`):
```rust
/// Retry through another proxy when the upstream HTTP status is outside this set
/// (e.g. 200 204 301 302). Empty = accept any status. HTTP requests only.
#[arg(long, num_args = 1.., value_name = "CODE")]
http_allowed_codes: Vec<u16>,
```

**Design.** Today `relay` does a blind `copy_bidirectional(client, upstream)` (`server.rs:315`).
For `Scheme::Http` with `http_allowed_codes` set, the splice must be preceded by a **status
peek**:
1. After writing `req.raw` upstream (`server.rs:307`), read from `upstream` until the end of the
   status line (`\r\n`) into a small buffer (bounded, e.g. 64 bytes is enough for
   `HTTP/1.1 200 OK\r\n`; keep reading response header bytes into a `Vec` up to a cap if the
   status line spans a partial read).
2. Parse the status code (`HTTP/1.1 <code> ...`). If `code ∈ allowed`, proceed: **write the
   already-read bytes to the client first**, then `copy_bidirectional` the remainder (the peeked
   bytes must not be lost).
3. If `code ∉ allowed`, return `RelayOutcome::RetryableFailure(ProxyError::BadStatus(code))` —
   crucially *before* any byte reached the client, so B2's loop retries a different proxy. Record
   it as a distinct error bucket.

Because the peek happens before any client write, a bad status is always a **pre-commit** failure
(integrates cleanly with B2). For `Scheme::Https` or when `http_allowed_codes` is `None`, `relay`
takes the existing blind-splice path unchanged (zero overhead, no peek).

A new `ProxyError` variant (in `src/error.rs`, the `ProxyError` per-proxy enum) e.g.
`#[error("upstream returned disallowed status {0}")] BadStatus(u16)` — and B2's classifier maps
it to `RetryableFailure`.

**Offline test plan.** Mock upstream that returns a chosen status.
- **First failing test:** `retries_when_upstream_status_not_allowed` — `mock_upstream` variant
  returning `HTTP/1.1 403 Forbidden`; pool of proxy1→403-mock, proxy2→200-mock("GOOD");
  `http_allowed_codes: [200]`. Client HTTP GET → receives "GOOD" (retried past the 403). Fails
  today (blind copy forwards the 403 to the client).
- `allowed_status_is_forwarded_verbatim` — single upstream returns `301` with a body;
  `http_allowed_codes: [301]` → client receives the full 301 response including the peeked status
  line (guards no byte loss from the peek).
- `none_allowed_codes_accepts_any` — `http_allowed_codes: None` forwards a `500` unchanged (the
  existing behaviour is preserved when the feature is off).
- `partial_status_line_read` — mock writes the status line in two TCP writes with a delay; the
  peek still parses the code (guards the bounded-read loop).

**Acceptance criteria.**
- [ ] A disallowed status triggers a transparent retry (client never sees it, if another proxy
      succeeds).
- [ ] An allowed status is forwarded byte-for-byte, including the peeked status line.
- [ ] `None`/empty codes = today's blind splice, no peek, no overhead.
- [ ] Only HTTP requests are status-gated; CONNECT is untouched.
- [ ] The peek is bounded (cannot buffer an unbounded response).

**Risks / deviations / principle-flags.** ⚠ Peeking risks losing or reordering bytes.
Mitigation: buffer the peeked bytes and replay them to the client before the splice; test
`allowed_status_is_forwarded_verbatim` + `partial_status_line_read` lock this. ⚠ Only the status
line is inspected — no header/body parsing (keep it a status peek, not an HTTP parser; lazy-that-
holds). ⚠ Chunked/keep-alive upstreams: the peek reads only up to the status line, then splices
the rest raw, so keep-alive is unaffected.

**Effort.** S/M.

---

## B10 — `--prefer-connect` selection bias

**Goal.** Bias selection toward proxies that expose `CONNECT:80`, as a tie-break — Python parity
(`--prefer-connect`). A `CONNECT`-capable proxy tunnels cleanly and is preferred when otherwise
equal.

**Public surface.**
```rust
// SelectCtx already carries `prefer_connect: bool` (added in the B1 refactor).
// PoolConfig gains:
pub prefer_connect: bool,   // default false
```
CLI (`ServeArgs`):
```rust
/// Prefer proxies that support CONNECT:80 when otherwise equally ranked.
#[arg(long)]
prefer_connect: bool,
```

**Design.** A one-line comparator branch in `best_for`. In the `Best` strategy's `min_by`
(`server.rs:145`), when `ctx.prefer_connect`, prepend a sort key: proxies whose
`types()` contains `Proto::Connect80` sort before those that don't, *then* the existing
`(error_rate, avg_resp_time)` tuple breaks the remaining tie:

```rust
.min_by(|(_, a), (_, b)| {
    let key = |p: &Proxy| {
        let connect = if ctx.prefer_connect && !p.proxy.types().contains_key(&Proto::Connect80) { 1u8 } else { 0 };
        (connect, p.proxy.priority())   // 0 (has CONNECT) sorts first
    };
    // compare connect flag, then priority via total_cmp
})
```

For `RoundRobin`/`Random`, `prefer_connect` filters the eligible set to CONNECT-capable proxies
*if any exist*, else falls back to all (bias, not hard requirement). Sticky is unaffected (the
pin wins).

**Offline test plan.** Pure `best_for` unit tests, no I/O.
- **First failing test:** `prefer_connect_biases_toward_connect80` — pool of two HTTP proxies with
  identical `priority()`, one also `add_type(Connect80, None)`; `SelectCtx { prefer_connect: true,
  strategy: Best, .. }` → `best_for` picks the CONNECT-capable one; with `prefer_connect: false`
  it picks by the existing tie-order. Fails today (no `prefer_connect`).
- `prefer_connect_does_not_override_health` — a much faster non-CONNECT proxy vs a slow CONNECT
  one: decide and document — Python treats it as a *tie-break* (health first) or a *primary* key
  (CONNECT first)? **Open question.** Recommend tie-break-only for `Best` (health dominates),
  primary bias for RR/Random. Pin whichever with this test.

**Acceptance criteria.**
- [ ] With `--prefer-connect`, an otherwise-tied CONNECT:80 proxy wins.
- [ ] Without it, selection is unchanged.
- [ ] Documented interaction with health ranking (tie-break vs primary) matches the test.

**Risks / deviations / principle-flags.** ⚠ **Open question** above (tie-break vs primary key) —
list, do not guess; recommend tie-break for `Best`. No new dependency; one comparator branch.

**Effort.** S.

---

## B13 — `--min-queue` / `--backlog` startup controls

**Goal.** Python parity for startup-under-load: don't begin accepting clients until the pool has
at least `min_queue` proxies, and set the TCP listen backlog.

**Public surface.** CLI (`ServeArgs`):
```rust
/// Wait until the pool holds at least this many proxies before accepting clients.
#[arg(long, default_value_t = 0)]
min_queue: usize,
/// TCP listen backlog (queued pending connections).
#[arg(long, default_value_t = 1024)]
backlog: u32,
```
Library — `serve` grows two params (or takes a small `ServeConfig`):
```rust
pub async fn serve(
    addr: SocketAddr, pool: Arc<Pool>, resolver: Arc<Resolver>,
    timeout: Duration, min_queue: usize, backlog: u32,
) -> std::io::Result<ServerHandle>
```
Plus a pool readiness probe:
```rust
impl Pool {
    /// Wait until at least `n` proxies are available or the source is exhausted.
    pub async fn wait_ready(&self, n: usize) { ... }   // uses the existing Notify
    pub fn len(&self) -> usize { self.state.lock().unwrap().len() }
}
```

**Design.**
- **min_queue.** Before the accept loop starts serving, `pool.wait_ready(min_queue).await`.
  Reuse the existing `Notify` (`server.rs:57`) exactly as `get` does (`server.rs:109`): loop
  creating `notified()` before checking `len() >= n`, returning early if `exhausted` is set (so a
  too-small source can't hang startup forever). `serve` returns the bound `ServerHandle`
  immediately (so `local_addr()` works) but the accept loop task first awaits `wait_ready`.
  **Open question:** should `serve` block *before returning* until ready, or return immediately
  and gate only the accept loop? Recommend the latter (bind is instant, tests can read the addr),
  gate acceptance inside the spawned loop.
- **backlog.** `TcpListener::bind` (`server.rs:185`) does not expose the backlog. Switch to
  `tokio::net::TcpSocket`: `TcpSocket::new_v4()/new_v6()`, `set_reuseaddr`, `bind(addr)`,
  `listen(backlog)` → `TcpListener`. No new dependency (`TcpSocket` is in tokio's `net`).

**Offline test plan.** `tests/serve.rs` style; `tokio::time::pause` for the wait path.
- **First failing test:** `serve_waits_for_min_queue` — a `Pool::spawn` fed by a controllable
  stream (an `mpsc` the test drives, wrapped as a `ProxyStream`); `serve(min_queue: 2, ..)`. A
  client connects and its request must not be relayed until the test pushes 2 proxies. Assert
  ordering: push 1 → client still pending; push 2nd → client gets a relayed body. Fails today
  (accepts immediately).
- `min_queue_zero_accepts_immediately` — default `min_queue: 0`, one proxy, relays at once
  (guards the no-op path; keeps existing `tests/serve.rs` green).
- `backlog_sets_listen_queue` — `serve(backlog: 128, ..)` binds and accepts a connection; a pure
  smoke test that the `TcpSocket` path still yields a working listener (backlog size itself is not
  portably observable, so assert connectivity, not the queue depth).
- `wait_ready_returns_on_exhaustion` — a source that yields 1 then ends with `min_queue: 5`;
  `wait_ready(5)` returns (does not hang) once `exhausted` is set.

**Acceptance criteria.**
- [ ] `serve` does not relay client requests until `pool.len() >= min_queue`.
- [ ] `wait_ready` returns promptly on source exhaustion even if `min_queue` is never met.
- [ ] `--backlog` is applied via `TcpSocket::listen(backlog)`; the listener still works.
- [ ] `min_queue: 0` and default backlog reproduce today's behaviour.

**Risks / deviations / principle-flags.** ⚠ `serve`'s signature grows — if it becomes noisy,
introduce a `ServeConfig` struct (builder-friendly, matches project preference), but not before
two params force it (lazy-that-holds; likely `ServeConfig` given B1/B5/B11 also add serve-time
knobs — consider consolidating `strategy`/`sticky_header`/`fail_timeout`/`http_allowed_codes`/
`min_queue`/`backlog` into one `ServeConfig` when the second of these lands, and thread the rest
through `PoolConfig`). ⚠ `TcpSocket` requires choosing v4/v6 by the addr family — branch on
`addr.is_ipv4()`. No network in tests (`127.0.0.1:0`).

**Effort.** S.

---

## What must stay green

Existing behaviour and tests this wave must not regress:

- **`tests/serve.rs`** — `server_relays_http_request_through_a_pool_proxy` and
  `server_returns_502_when_pool_is_empty` must pass unchanged. The default strategy stays `Best`,
  default `min_queue` `0`, `http_allowed_codes` `None`, so an unconfigured server behaves exactly
  as today.
- **`server.rs` unit tests** — `best_for_picks_lowest_response_time`,
  `best_for_respects_scheme`, `tied_response_times_do_not_panic`, `split_host_port_variants`. The
  `best_for` refactor (new `SelectCtx`, `Vec<Pooled>`) must keep `Strategy::Best` byte-identical;
  update these tests only to pass a default `SelectCtx`/wrap in `Pooled`, never to change the
  asserted pick.
- **The `total_cmp` tie-ordering invariant** (`server.rs:12`, `proxy.rs:120`) — every new
  comparator branch (B5 backup tier, B10 prefer-connect) must still order tied `f64` with
  `total_cmp`, never a naive `<` that could panic or be nondeterministic.
- **`Proxy` stays serialization-stable** — B5 must not add a server-only field to `Proxy`
  (`proxy.rs`); the `serializes_to_python_as_json_shape` test and the whole `find`/`check` JSON
  contract depend on it. Bench state lives on the pool's `Pooled` wrapper.
- **`put` hard-eviction** (`server.rs:128`) — the `max_error_rate`/`max_resp_time`-after-`min_req`
  drop is unchanged; B5 benching is additive, not a replacement.
- **`find`/`grab`/`check` paths** — B3/B4 only add fields to an existing `FindQuery`; `tests/find.rs`
  and `tests/grab.rs` must stay green.
- **`ProxyError` exhaustiveness** — B2's `RelayOutcome` classifier and B11's `BadStatus` variant
  must keep every arm covered so a future variant defaults safely (no retry past a client commit).
