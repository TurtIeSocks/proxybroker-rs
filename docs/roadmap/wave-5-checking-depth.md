# Wave 5 — Checking depth

*Deepen the check engine. Every feature lands in `checker.rs` (`attempt` / `check_one` /
`response_is_valid` / `CheckerConfig`) and `proxy.rs` (`record_attempt` / `runtimes` /
`avg_resp_time`), with thin fan-out into `stats.rs`, `broker.rs` (`FindQuery`), and the CLI
(`FindArgs`).*

The theme: the checker today answers one question — *does this proxy work for protocol P, and
how anonymous is it?* — and hard-fails when the judge fabric is down. Wave 5 makes it answer
richer questions (how fast at the tails, what capabilities does it pass through, is it hostile)
and degrade gracefully instead of returning nothing.

## Build order (respects dependencies)

1. **A3 — Timing percentiles.** Fully isolated in `proxy.rs` + `stats.rs`; no checker changes.
   Cheapest, zero risk to the check path. Land first to warm up the wave.
2. **A2 — Judge-less liveness mode.** Reshapes `Checker::new`'s `NoJudges` gate and adds a
   probe-target branch to `attempt`. Independent of A3/A4/A5.
3. **A5 — Retry/backoff policy.** Replaces `CheckerConfig.max_tries` with a `RetryPolicy` and
   rewrites the `check_one` loop. Independent; touches `CheckerConfig` + `FindQuery` + CLI.
4. **A4 — Capability profile.** Introduces the shared `Observation` payload on
   `Attempt::Working` (the seam A6 extends), splits `response_is_valid`'s referer/cookie checks
   into per-flag capabilities, stores them on `Proxy`, makes them filterable.
5. **A6 — Honeypot / trust verdict.** Extends A4's `Observation` with a `TrustReport`, driven
   by a canary round-trip + injected-header scan. Built offline against recorded fixtures.

### Shared reshaping (no big-bang refactor)

There is **no** up-front refactor commit. Two localized seams get reshaped in place, each inside
the feature that first needs it:

- **Probe target enum** (A2): `attempt`'s `judge: &Judge` argument becomes
  `probe: &Probe<'_>`, where `Probe` is `Judged(&Judge)` or `Liveness(&LivenessTarget)`. The
  classification tail branches on it.
- **Observation payload** (A4, extended by A6): `Attempt::Working(Option<AnonLevel>)` becomes
  `Attempt::Working(Observation)` where `Observation { level, caps }` (A4) grows a `trust` field
  (A6). This keeps the "request vs. classification" split the prompt notes `attempt` already
  has — the request half is untouched; only the classification half enriches.

---

## A3 — Timing percentiles (p50/p90/p95)

**Goal.** `runtimes: Vec<f64>` is already retained on every `Proxy` (`proxy.rs:40`) but only the
mean is exposed (`avg_resp_time`, `proxy.rs:112`). Surface tail latency (p50/p90/p95) per proxy
and in the aggregate `Stats`.

**Public surface.**

```rust
// proxy.rs — a free function beside round2, so Stats can reuse it without a Proxy.
/// The `q`-quantile (q in 0.0..=1.0) of `data` by linear interpolation between closest ranks
/// (numpy "linear" / type-7), rounded to 2 dp. Empty → 0.0. Does not require sorted input.
pub fn percentile(data: &[f64], q: f64) -> f64;

// proxy.rs — Proxy method, beside avg_resp_time (proxy.rs:112).
impl Proxy {
    /// The `q`-quantile of this proxy's successful round-trip times, seconds, 2 dp. 0.0 if none.
    pub fn percentile(&self, q: f64) -> f64;   // self.runtimes → percentile(&self.runtimes, q)
}

// stats.rs — three new fields on Stats (stats.rs:14).
pub struct Stats {
    // ...existing...
    pub p50_resp_time: f64,
    pub p90_resp_time: f64,
    pub p95_resp_time: f64,
}
```

No new CLI flag by default — the percentiles print in the `--show-stats` summary
(`Stats: Display`, `stats.rs:114`). (Optional follow-up `--stats-format json`, owned by Wave 4
C5, will serialize them; out of scope here.)

**Design.**
- `percentile(data, q)`: clone-and-sort a `Vec<f64>` with `f64::total_cmp` (the codebase's
  established tie-safe ordering, cited in `proxy.rs:125`), compute `rank = q * (n - 1)`, and
  interpolate `sorted[floor] + frac * (sorted[ceil] - sorted[floor])`. Round via the same
  format-then-parse trick as `round2` (`proxy.rs:53`) so results match the crate's 2-dp
  convention. Linear interpolation (not nearest-rank) is chosen so `p50` equals the true median
  (`percentile(&[1.0, 2.0], 0.5) == 1.5`), which is what a dashboard reader expects.
- `Stats`: the aggregate percentile is taken over the **same population** `avg_resp_time`
  already uses — one `avg_resp_time()` value per proxy that recorded any timing
  (`StatsCollector::record`, `stats.rs:87-91`). Replace the collector's `rt_sum: f64` /
  `rt_n: usize` (`stats.rs:56-57`) with a single `resp_times: Vec<f64>`; push each proxy's
  `art` when `> 0.0`. In `finish` (`stats.rs:95`), the mean stays `sum/len` (byte-identical
  output — see "What must stay green") and the three percentiles come from
  `percentile(&resp_times, q)`.
- **Deliberately NOT** added to `Proxy`'s `Serialize` impl (`proxy.rs:180`): that shape is a
  byte-for-byte port of `proxy.py:as_json` (documented at `proxy.rs:178`) and has no percentile
  fields. Percentiles are a new *aggregate* concept, surfaced only through `Stats`.

**Offline test plan.** Pure unit tests, no I/O.
- **First failing test** — `proxy::tests::percentile_linear_interpolation`:
  `percentile(&[1.0,2.0,3.0,4.0], 0.5) == 2.5`, `percentile(&[], 0.5) == 0.0`,
  `percentile(&[5.0], 0.9) == 5.0`, and a p90/p95 case cross-checked against numpy in the commit
  message.
- `proxy::tests::proxy_percentile_uses_runtimes`: record a spread of successful attempts (and
  one timeout, which `record_attempt` excludes, `proxy.rs:159`), assert `p.percentile(0.5)`.
- `stats::tests::percentiles_over_per_proxy_means`: three proxies with distinct
  `avg_resp_time`s; assert `Stats.p50/p90/p95` and that `avg_resp_time` is unchanged from before.

**Acceptance criteria.**
- [ ] `percentile` free fn + `Proxy::percentile` return 2-dp linear-interpolated quantiles; empty → 0.0.
- [ ] `Stats` carries p50/p90/p95 over the per-proxy mean population.
- [ ] `Stats: Display` prints a percentile line; `avg_resp_time` output is unchanged.
- [ ] `Proxy` JSON shape is untouched (parity test still green).

**Risks / deviations / principle-flags.** None material. Linear vs. nearest-rank is a judgement
call (documented in code); no project principle in tension.

**Effort.** S (an afternoon).

---

## A2 — Judge-less liveness mode (graceful degradation)

**Goal.** Today, if no judge verifies, `Checker::new` returns `Error::NoJudges`
(`checker.rs:88-90`) and `Broker::find` yields nothing. When the caller supplies a liveness URL,
degrade instead: validate a proxy by fetching that URL through it and checking for a 200,
recording the working type with anonymity level `None`.

**Public surface.**

```rust
// checker.rs — one new field on CheckerConfig (checker.rs:52). Default None = parity (NoJudges).
pub struct CheckerConfig {
    // ...existing...
    /// When the judge pool is empty, fall back to a plain liveness check against this URL
    /// instead of failing with NoJudges. `None` keeps the strict judge-required behavior.
    /// Proxies confirmed this way carry anonymity level `None` (unclassifiable without a judge).
    pub liveness_url: Option<String>,
}

// broker.rs — mirror field on FindQuery (broker.rs:64), Default None.
pub struct FindQuery { /* ... */ pub liveness_url: Option<String> }
```

CLI (`FindArgs`, `bin/proxybroker.rs:130`):

```
--liveness-url <URL>   Fallback endpoint to probe when no judge verifies. Enables graceful
                       degradation; proxies confirmed via liveness report anonymity "None".
                       [default: none]
```

**Design.**
- `Checker` gains a private mode field. `enum CheckMode { Judged, Liveness(LivenessTarget) }`
  where `LivenessTarget { host: String, path: String, ip: IpAddr, scheme: JudgeScheme }` is
  built by reusing `Judge::parse` (`judge.rs:45`) on `liveness_url` and resolving the host with
  the `Resolver` already threaded into `Checker::new` (`checker.rs:73`).
- Rewrite the `judges.is_empty()` gate (`checker.rs:88-90`):
  ```rust
  let mode = if judges.is_empty() {
      match &cfg.liveness_url {
          Some(url) => CheckMode::Liveness(LivenessTarget::resolve(url, &resolver, cfg.timeout).await
                                            .ok_or(Error::NoJudges)?),
          None => return Err(Error::NoJudges),   // parity
      }
  } else {
      CheckMode::Judged
  };
  ```
  A malformed/unresolvable liveness URL still yields `Error::NoJudges` (there is genuinely
  nothing to check against).
- `check_one` (`checker.rs:158`) currently does `self.judges.random(scheme)` and returns `false`
  when the pool has none (`checker.rs:160-162`). In `Liveness` mode it instead builds the
  `Probe::Liveness(&target)` and the `Target` from the liveness host/port — no judge lookup.
- `attempt` (`checker.rs:198`) already cleanly separates connect+negotiate (the request half)
  from validation+classification (the classification half). Change its `judge: &Judge` param to
  `probe: &Probe<'_>`. Connect + negotiate are identical. Then:
  - `Probe::Judged(judge)` → today's path: `build_request` from the judge, `response_is_valid`,
    `anonymity_level` (`checker.rs:223-238`).
  - `Probe::Liveness(target)` → build a plain `GET target.path`, send, read, and check only
    `get_status_code(head, 9, 12) == 200` (`checker.rs:225`); return `Attempt::Working` with
    `level: None`. No marker/IP/anonymity assertions — there is no judge to reflect them.
  - `Proto::Connect25` (`checker.rs:213`) is unchanged: a granted tunnel is still the whole
    check, in either mode.
- `types_passed` (`checker.rs:289`) needs no change: a liveness-confirmed HTTP type has level
  `None`, so any request that carried an explicit `--lvl` filter (`Some(levels)` at
  `checker.rs:307`) naturally drops it. Documented consequence: **liveness mode + an anonymity
  filter yields nothing**, because unclassifiable proxies can't satisfy a level requirement.

**Offline test plan.** New `tests/liveness.rs`, reusing the `echo_server` mock pattern from
`tests/check_http.rs:19` (a 127.0.0.1 TCP server returning `HTTP/1.1 200 OK`).
- **First failing test** — `all_judges_down_degrades_to_liveness`: a "bad judge" mock that never
  echoes the real IP (so `JudgePool` is empty, as in `check_http.rs:137`'s
  `no_judges_is_an_error`), a mock liveness server returning 200, and a mock HTTP proxy (echo
  server). With `liveness_url = Some(<liveness addr>)`, `Checker::new` **succeeds**, and
  `check(&mut proxy)` returns `true` with `proxy.types()[&Proto::Http] == Some(None)`.
- `no_liveness_url_still_errors`: judges down + `liveness_url = None` → `Error::NoJudges`
  (guards parity; mirrors the existing integration test).
- `liveness_bad_status_is_not_working`: liveness server returns `503` → `check` returns `false`.
- `liveness_ignores_anon_filter`: request `TypeSpec { proto: Http, levels: Some([High]) }` in
  liveness mode → proxy does not pass (level `None` can't satisfy `High`).

**Acceptance criteria.**
- [ ] `liveness_url: None` preserves `Error::NoJudges` exactly (parity).
- [ ] With a liveness URL and an empty judge pool, `Checker::new` succeeds and `check` validates
      via a 200 from the liveness endpoint, setting level `None`.
- [ ] Malformed/unresolvable liveness URL → `Error::NoJudges`.
- [ ] `--liveness-url` plumbs through `FindQuery` into `CheckerConfig`.

**Risks / deviations / principle-flags.**
- ⚠ *Deviation from proxybroker2*, which has no such mode (it simply produces nothing). Recorded
  in `decisions.md` as a deliberate additive capability, gated behind an opt-in flag so default
  behavior is unchanged.
- ⚠ The probe-target enum touches the hot `attempt` path. Mitigation: the enum branch is only in
  the classification tail; the connect/negotiate half is byte-identical, and the existing
  `check_http.rs` suite pins that half.

**Effort.** M (1–3 days).

---

## A5 — Configurable retry/backoff policy

**Goal.** Replace the single global `max_tries` (which retries **only** `Timeout` —
`check_one`, `checker.rs:183-186`) with a `RetryPolicy` that declares *which* errors retry and a
backoff/jitter schedule between attempts.

**Public surface.**

```rust
// checker.rs (or a small checker submodule).
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total attempts per protocol (>= 1). Replaces CheckerConfig.max_tries.
    pub max_tries: usize,
    /// Which per-proxy errors are retryable. Default: just Timeout (parity).
    pub retry_on: std::collections::HashSet<ProxyError>,
    /// Base delay before the first retry. Zero = no delay (parity).
    pub backoff: Duration,
    /// Per-attempt multiplier on the delay (exponential). 1.0 = constant.
    pub factor: f64,
    /// Symmetric jitter fraction applied to each delay, 0.0..=1.0.
    pub jitter: f64,
    /// Upper bound on any single delay.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    // max_tries: 3, retry_on: {Timeout}, backoff: 0, factor: 1.0, jitter: 0.0, max_backoff: 0.
}

impl RetryPolicy {
    /// {Timeout, Reset, ConnFailed, EmptyRecv} — the transient set.
    pub fn transient(max_tries: usize) -> Self;
    /// Delay before retry number `i` (0-based): min(max_backoff, backoff * factor^i) ± jitter.
    pub fn backoff_for(&self, i: usize) -> Duration;
}

// checker.rs — CheckerConfig.max_tries (checker.rs:58) becomes:
pub struct CheckerConfig { /* ... */ pub retry: RetryPolicy }
```

`ProxyError` is `Copy + Eq + Hash` (`error.rs:21`), so a `HashSet<ProxyError>` is the natural,
allocation-cheap retry set. `FindQuery.max_tries` (`broker.rs:80`) becomes
`retry: RetryPolicy`; `Broker::find` (`broker.rs:206`) passes it straight through.

CLI (`FindArgs`, `bin/proxybroker.rs:163`, and `PoolArgs`, `bin/proxybroker.rs:105`):

```
--max-tries <N>          Total attempts per protocol.                       [default: 3]
--retry-on <SET>         timeout | transient | all.                         [default: timeout]
--backoff-ms <N>         Base backoff before a retry, milliseconds.         [default: 0]
```

`--retry-on` maps `timeout → {Timeout}`, `transient → RetryPolicy::transient`,
`all → every ProxyError variant`. `factor`/`jitter`/`max_backoff` stay library-only (a config
knob for a value almost nobody tunes is exactly the "no config for a constant" trap; expose them
only if a consumer asks).

**Design.** Rewrite the `check_one` loop (`checker.rs:169-193`):

```rust
for i in 0..self.retry.max_tries {
    let start = Instant::now();
    match self.attempt(proxy, proto, &probe, &target).await {
        Ok(Attempt::Working(obs)) => { proxy.record_attempt(Some(start.elapsed().as_secs_f64()), None);
                                       proxy.add_type(proto, obs.level); /* A4/A6 fold obs */ return true; }
        Ok(Attempt::Invalid)      => { proxy.record_attempt(None, Some(ProxyError::BadResponse)); return false; }
        Err(e) => {
            proxy.record_attempt(None, Some(e));
            if self.retry.retry_on.contains(&e) && i + 1 < self.retry.max_tries {
                tokio::time::sleep(self.retry.backoff_for(i)).await;   // zero-duration = no-op today
                continue;
            }
            return false;
        }
    }
}
false
```

The default (`retry_on = {Timeout}`, `backoff = 0`) reproduces today's control flow exactly:
retry on timeout with no sleep, break on everything else. `backoff_for` uses `rand::rng()`
(already a dependency, `Cargo.toml:63`; used in `judge.rs:176`/`utils.rs:50`) for jitter.

**Offline test plan.**
- **First failing test** — `checker::tests::retry_policy_backoff_schedule` (pure fn):
  `RetryPolicy { backoff: 100ms, factor: 1.0, jitter: 0.0, .. }.backoff_for(0..=2)` is constant
  100ms; `factor: 2.0` gives 100/200/400ms; `max_backoff` caps it; `jitter: 0.5` stays within
  `[0.5x, 1.5x]`.
- `checker::tests::default_policy_retries_only_timeout`: assert `retry_on == {Timeout}` and that
  `RetryPolicy::default().max_tries == 3`.
- Integration `tests/retry.rs::reset_is_retried_when_policy_includes_it`: a mock proxy backed by
  an `Arc<AtomicUsize>` connection counter — the first connection is dropped immediately
  (→ `Reset`/`EmptyRecv`), the second behaves as a normal echo proxy. With
  `retry_on = {Reset, EmptyRecv}` the check passes; with the default `{Timeout}` it fails on the
  first attempt. Uses `tokio::time::pause()` (dev-dep `tokio` has `test-util`, `Cargo.toml:71`)
  so any backoff is virtual-time and the test stays instant.

**Acceptance criteria.**
- [ ] Default `RetryPolicy` reproduces today's timeout-only, no-delay behavior (all existing
      `check_http.rs` tests green).
- [ ] `retry_on` controls which errors retry; `backoff`/`factor`/`jitter`/`max_backoff` shape the
      delay; `backoff_for` is a tested pure function.
- [ ] `--max-tries` / `--retry-on` / `--backoff-ms` plumb into `RetryPolicy`.
- [ ] Backoff is virtual-time testable (no wall-clock sleeps in tests).

**Risks / deviations / principle-flags.**
- ⚠ Replacing `CheckerConfig.max_tries` changes the *derived* `Default` (old `max_tries: 0`
  → new `retry.max_tries: 3`). Only `..Default::default()` callers that never set `max_tries`
  are affected; every real caller sets it. Benign; noted in the commit message.
- ⚠ `--retry-on all` can waste time hammering genuinely-dead proxies. Mitigation: `max_tries`
  still caps total attempts; default stays `timeout`.

**Effort.** S/M.

---

## A4 — Extra check dimensions (capability profile)

**Goal.** `response_is_valid` (`checker.rs:407`) already inspects the judge round-trip for the
Referer (`https://www.google.com/`) and Cookie (`cookie=ok`) echoes as one combined boolean.
Expose them as *individual* capability flags, add CONNECT:25 (SMTP tunnel) capability, store the
profile on `Proxy`, and make it filterable.

**Public surface.**

```rust
// types.rs — shared vocabulary (checker produces, proxy stores).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Caps {
    /// The proxy passed our Cookie header through to the judge unmodified.
    pub cookie_echo: bool,
    /// The proxy passed our Referer header through unmodified.
    pub referer_echo: bool,
    // connect25 is derived from confirmed types, not stored here (see Design).
}

// proxy.rs
impl Proxy {
    /// The capability profile, OR-accumulated across confirmed protocols.
    pub fn caps(&self) -> Caps;
    /// Full profile including derived CONNECT:25 support.
    pub fn capabilities(&self) -> Capabilities;   // { cookie_echo, referer_echo, connect25 }
    /// Fold one attempt's observed caps into the stored profile (called on a working attempt).
    pub fn record_caps(&mut self, c: Caps);
}

// checker.rs — CheckerConfig gains an opt-in that lets caps actually vary (see Design).
pub struct CheckerConfig {
    // ...
    /// Relax response validity to marker+IP only, demoting Referer/Cookie from validity gates
    /// to recorded capability signals. Default false = parity (all four required).
    pub relaxed_validity: bool,
}
```

CLI filters (`FindArgs`):

```
--relaxed-validity       Accept proxies that forward the request (marker+IP echo) even if they
                         strip Referer/Cookie; record what they pass through.   [default: off]
--require-cookie         Keep only proxies that pass Cookie through.            [default: off]
--require-referer        Keep only proxies that pass Referer through.           [default: off]
--require-connect25      Keep only proxies with a confirmed CONNECT:25 tunnel.  [default: off]
```

**Design.**
- **Shared `Observation` seam.** Change `Attempt::Working(Option<AnonLevel>)` (`checker.rs:327`)
  to `Attempt::Working(Observation)` with `struct Observation { level: Option<AnonLevel>,
  caps: Caps }`. `check_one` reads `obs.level` for `add_type` and calls
  `proxy.record_caps(obs.caps)`.
- **Cap extraction.** In `attempt`'s classification tail (`checker.rs:230-238`), after
  `decompress`, compute `Caps { cookie_echo: content.contains("cookie=ok"),
  referer_echo: content.contains("https://www.google.com/") }`. Factor the literals so
  `response_is_valid` and `Caps::from_content` share them (a small
  `fn caps_from_content(content: &str) -> Caps`).
- **Validity coupling.** Keep `response_is_valid` (`checker.rs:407`) exactly as-is when
  `relaxed_validity == false` (parity: marker + IP + referer + cookie all required — so a valid
  proxy trivially has both flags set). When `relaxed_validity == true`, validity requires only
  marker + non-empty IP set; Referer/Cookie become *recorded* signals that can now differ per
  proxy. This is what makes the profile a filter worth having, without changing default behavior.
- **CONNECT:25.** Not stored in `Caps`; derived in `capabilities()` from
  `self.types().contains_key(&Proto::Connect25)` (`proxy.rs:82`). A granted SMTP tunnel is
  already recorded as a confirmed type (`checker.rs:213`), so storing a second bit would
  duplicate state.
- **Storage.** Add `caps: Caps` to `Proxy` (`proxy.rs:26`), OR-folded in `record_caps` so a
  proxy confirmed across multiple protocols keeps every capability it ever demonstrated.
- **Filtering.** `FindQuery` gains `require_cookie/require_referer/require_connect25: bool`,
  applied in the `find` pipeline next to the existing country filter (`broker.rs:171`,
  `country_ok`). A tiny `caps_ok(&proxy, &query)` predicate.
- **Serialization.** `Caps` is **not** added to `Proxy`'s parity `Serialize`
  (`proxy.rs:180`) — that shape mirrors `proxy.py:as_json`, which has no caps. Exposed via the
  library getters + CLI filters. *Open question:* whether to add a `caps` object to the JSON
  output is deferred to Wave 4's schema-versioning work (C4). Recorded, not guessed.

**Offline test plan.**
- **First failing test** — `checker::tests::caps_extracted_from_content` (pure fn):
  a content string with both echoes → `Caps { cookie_echo: true, referer_echo: true }`; drop the
  cookie substring → `cookie_echo: false`; drop both → both false.
- `proxy::tests::record_caps_or_accumulates`: fold `{cookie:true, referer:false}` then
  `{cookie:false, referer:true}` → `{cookie:true, referer:true}`.
- `proxy::tests::capabilities_derives_connect25`: `add_type(Connect25, None)` →
  `capabilities().connect25 == true`.
- Integration `tests/check_caps.rs::cookie_stripping_proxy_is_profiled` (relaxed_validity=true,
  `echo_server` pattern): a mock proxy whose echoed page omits `cookie=ok` but keeps the marker,
  an IP, and the Referer → `check` returns `true`, `proxy.caps() == { cookie_echo: false,
  referer_echo: true }`.
- `check_caps.rs::require_cookie_filters`: same proxy with `require_cookie` → filtered out.

**Acceptance criteria.**
- [ ] `relaxed_validity: false` (default) leaves `response_is_valid` and every existing
      `check_http.rs` assertion unchanged.
- [ ] `Caps` extracted from judge content; `Proxy` stores an OR-folded profile;
      `capabilities()` derives CONNECT:25 from confirmed types.
- [ ] `--require-cookie` / `--require-referer` / `--require-connect25` filter the stream.
- [ ] `Proxy` parity JSON shape unchanged.

**Risks / deviations / principle-flags.**
- ⚠ The capability profile only *varies* under `--relaxed-validity`; in default mode every
  working HTTP proxy has both flags set (they're validity gates). This is intentional: default
  stays byte-parity with proxybroker2, and the opt-in unlocks the signal. Flagged so the CLI
  help makes the coupling explicit.
- ⚠ Adding a struct field (`caps`) to `Proxy`. No abstraction, no trait — a plain data field,
  consistent with the "Proxy is plain data" doctrine (`proxy.rs:2`).

**Effort.** S/M.

---

## A6 — Honeypot / hostile-proxy detection (`trust` verdict)

**Goal.** Derive a `trust` verdict from the existing judge round-trip: a **canary round-trip**
assertion (our nonce must return unmutated) plus an **injected-header scan** (the echoed request
must not contain headers we didn't send), with an optional cert-pin. Report *which signal fired*,
never a bare boolean — and stay robust to benign false positives (gzip re-encode, transparent
caches).

**Public surface.**

```rust
// checker.rs (submodule `trust`), re-exported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustSignal {
    /// Our canary nonce did not survive the round-trip unmutated (content tampering).
    CanaryMismatch,
    /// The echoed request carried a header we never sent (injection).
    InjectedHeader,
    /// (optional) The HTTPS judge presented a cert that did not match the pin (MITM).
    CertMismatch,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrustReport { pub signals: Vec<TrustSignal> }   // empty = trusted

impl TrustReport {
    pub fn trusted(&self) -> bool { self.signals.is_empty() }
    /// Assess a decompressed judge response against what we sent. Pure; the unit under test.
    pub fn assess(sent_header_names: &[&str], canary: &str, content: &str) -> TrustReport;
}

// checker.rs — CheckerConfig opt-in (trust adds work + risks false-positive drops).
pub struct CheckerConfig { /* ... */ pub trust_check: bool }   // default false

// proxy.rs
impl Proxy {
    pub fn trust(&self) -> &TrustReport;
    pub fn set_trust(&mut self, r: TrustReport);
}
```

CLI (`FindArgs`):

```
--trust-check        Run honeypot detection on each proxy and record the verdict.  [default: off]
--require-trusted    Keep only proxies whose trust verdict is clean (implies --trust-check).
```

**Design.**
- **Observation extension.** A6 grows A4's `Observation` to
  `{ level, caps, trust: TrustReport }`. `check_one` calls `proxy.set_trust(obs.trust)` on a
  working attempt. When `trust_check == false`, `assess` is never called and the report is
  `Default` (empty/trusted) — zero cost, zero behavior change.
- **Canary round-trip.** The checker already plants a random marker in the `User-Agent`
  (`build_request`, `checker.rs:245`; `request_headers`, `utils.rs:32`) and greps for it in
  `response_is_valid`. Reuse that marker as the canary. The assertion: on a response where the
  marker is present (i.e. fresh — a stale cache without the marker is already `Invalid`, not
  `Suspect`), the canary must appear **verbatim**. Comparison happens **after** `decompress`
  (`checker.rs:228`), so a proxy that re-gzips a byte-identical body is *not* flagged — this is
  the primary false-positive guard. We deliberately do **not** hash the whole body (legitimate
  re-chunking / cache headers would false-positive); "body hash round-trip" is scoped to the
  canary token's integrity.
- **Injected-header scan.** Azenv/httpbin-class judges echo the *request* headers back in the
  body. We know the exact set we sent (the keys of `request_headers`, `utils.rs:32`, in wire
  order). `assess` scans the echoed request block for header names outside our sent set ∪ a
  small benign-hop allowlist (`Host`, `Connection`, `Content-Length`, `X-Forwarded-For`, `Via` —
  the last two are already the *anonymity* signal, not a trust signal, so they're allow-listed
  here to avoid double-counting). A header like `X-Ad-Inject` or an auth header we never sent →
  `InjectedHeader`.
- **Cert-pin (optional).** For HTTPS judges only, an optional pinned fingerprint. Requires cert
  access from the negotiated TLS stream (`negotiator::Stream`) — a real but separable extension.
  *Open question:* keep it behind a `trust-tls` cargo feature with a `sha2`/`ring` fingerprint,
  or defer entirely. The core (canary + header scan) ships **dependency-free** and unconditional;
  cert-pin is flagged as follow-up so A6 doesn't block on TLS-introspection plumbing.
- **Storage + filter.** `TrustReport` stored on `Proxy`; `--require-trusted` filters in the
  `find` pipeline (beside A4's `caps_ok`). Not added to the parity `Serialize` (same reasoning as
  A4); library getter + CLI filter only.

**Offline test plan.** Recorded fixtures under
`tests/fixtures/trust/` (constraint C5, honoring the roadmap's ⚠ "offline-first with recorded
fixtures"). `assess` is a pure function → the bulk of coverage needs no sockets.
- **First failing test** — `checker::trust::tests::clean_response_is_trusted`: load
  `fixtures/trust/clean.txt` (a normal azenv-style echo containing our sent headers + canary) →
  `TrustReport::assess(...).trusted() == true`.
- `injected_header_is_flagged`: `fixtures/trust/injected_header.txt` (adds `X-Ad-Inject: 1` to
  the echoed request block) → `signals == [InjectedHeader]`.
- `canary_mutation_is_flagged`: `fixtures/trust/canary_mutated.txt` (nonce altered) →
  `signals == [CanaryMismatch]`.
- **False-positive guard (load-bearing)** — `gzip_reencode_is_trusted`: a fixture whose body was
  re-gzipped by the proxy but decompresses to a byte-identical canary → **trusted** (asserts we
  compare post-`decompress`, not raw bytes).
- **False-positive guard** — `forwarded_via_headers_are_not_injection`: an echoed request
  carrying `Via`/`X-Forwarded-For` → **trusted** (those are the anonymity signal, allow-listed).
- Integration `tests/trust.rs::injecting_proxy_is_recorded`: a mock proxy (echo server) that adds
  a bogus header to its echoed page; with `trust_check: true`, `proxy.trust().signals` contains
  `InjectedHeader`, and with `--require-trusted` it's filtered out.

**Acceptance criteria.**
- [ ] `trust_check: false` (default) skips assessment entirely — no behavior/perf change.
- [ ] `TrustReport::assess` is a pure function driven by recorded fixtures; canary compared
      post-decompress; injected-header scan uses the exact sent-header set + benign allowlist.
- [ ] The verdict reports **which** signal(s) fired, not a bare boolean.
- [ ] gzip re-encode and `Via`/`XFF` do **not** produce false positives (guard tests green).
- [ ] `--require-trusted` filters suspect proxies; `Proxy` parity JSON unchanged.

**Risks / deviations / principle-flags.**
- ⚠ *Offline-testable* (register entry A6): satisfied via recorded fixtures + a pure `assess`;
  no live honeypot needed.
- ⚠ *False positives* (register entry A6): explicitly mitigated — canary is compared after
  decompression (gzip re-encode safe), stale caches fall through to `Invalid` not `Suspect`, and
  `Via`/`XFF` are allow-listed. Two guard tests pin these.
- ⚠ Cert-pin is left as an Open Question (feature-gated `trust-tls` vs. defer) to avoid coupling
  the shippable core to TLS-introspection plumbing. No speculative abstraction lands for it.

**Effort.** M.

---

## What must stay green (no regressions)

- **`tests/check_http.rs`** — `high_anonymity_proxy_is_confirmed`,
  `transparent_proxy_is_detected`, `invalid_response_fails_the_check`, `no_judges_is_an_error`.
  A2 must keep `NoJudges` when `liveness_url` is `None`; A4 with default `relaxed_validity: false`
  must keep `response_is_valid` requiring Referer+Cookie so the cookie-omitting proxy still fails;
  A5's default `RetryPolicy` must reproduce timeout-only, no-delay retry (the `cfg()` in that
  file sets `max_tries: 2` → becomes `retry.max_tries: 2`).
- **`src/checker.rs` unit tests** — `split_head_body_splits_on_blank_line`,
  `dnsbl_query_reverses_ipv4_octets`, `response_valid_requires_all_markers` (A4 must not change
  default validity).
- **`src/proxy.rs` unit tests** — `record_attempt_tracks_requests_errors_runtimes`,
  `timeout_runtime_is_excluded_like_python`, `avg_resp_time`, and especially
  `serializes_to_python_as_json_shape` — **no caps/trust/percentile fields may enter the parity
  `Serialize`.**
- **`src/stats.rs` unit tests** — A3 rewrites `StatsCollector` internals (`rt_sum`/`rt_n` →
  `resp_times: Vec<f64>`); `aggregates_errors_and_avg_time` (avg == 0.4),
  `collector_records_failed_proxies_too`, and `empty_batch_is_all_zero` must stay green with
  identical `avg_resp_time` output.
- **`src/error.rs`** — `errmsg_strings_match_python_byte_for_byte` and the `ProxyError`
  histogram-key tests: A5 uses `ProxyError` as `HashSet` members but adds no variants and changes
  no strings.
- **`src/types.rs`** — untouched except the additive `Caps` struct (A4); the existing
  protocol/anon/`TypeSpec` tests must pass unchanged.
- **`src/broker.rs`** — `FindQuery::Default` must keep constructing (new fields via `Default`);
  `Broker::find`'s error ordering (`NoTypes` → `ExtIpUnknown` → `NoJudges`) preserved, with A2
  adding the liveness branch *after* the judge probe.
