# proxybroker-rs — feature roadmap

Everything the feature-research pass surfaced (Python-tracker gaps, competitor features, API
extensions, differentiators), sequenced into **waves**. Ordering optimizes for:

1. **Dependencies** — a feature never precedes what it needs (`Deserialize` before save/load;
   SQLite after file-based save/load; retry-failover pairs with status-gating).
2. **Module batching** — features touching the same file ship as one campaign, so we open
   `server.rs` / `checker.rs` / the output path once, not eight times.
3. **Value × feasibility** — the biggest genuine gap and the cheapest-isolated wins go first.
4. **Principle friction last** — features that fight the project's stated principles
   (`ephemeral by design`, `no speculative abstraction`, offline-testable, CC BY 4.0 data
   hygiene) are deferred until demand pulls them, and carry a ⚠ with the mitigation.

Effort is honest single-maintainer effort: **S** ≈ an afternoon, **M** ≈ 1–3 days, **L** ≈ a
week+. Every feature must stay offline-testable (constraint C5) and one-commit-per-item.

Per-wave specs live beside this file: `wave-1-inputs-and-foundation.md` …
`wave-8-distribution-and-ecosystem.md`.

---

## Wave 1 — Inputs & foundation ✅ *shipped*
*The biggest capability gap, plus the refactors everything downstream needs.*

Shipped on `feat/wave-1-inputs`: Step 0 refactor (`3709226`, extract `build_checker` +
`check_stream` from `find`), A1 (`5396689`), C1 (`0c69d69`), C2 (`07b1473`), E2 (`a5bce0c`).

| # | Feature | Effort | Notes |
|---|---|---|---|
| A1 | **`check` a user-supplied proxy list** (`Broker::check` + `check` subcommand) | M | The #1 gap. Factor check-orchestration out of `find_task` into `check_stream(impl Stream<Item=Proxy>)`; `find` feeds provider candidates, `check` feeds parsed stdin/file input. |
| C1 | **`Deserialize for Proxy`** | S/M | Round-trip the `Serialize` shape. Prereq for C2/save-load and BYO-pool. |
| C2 | **Save/load a checked pool** (`--save` / `--load`) | S | Warm restart. `Pool::from_proxies` already consumes `Vec<Proxy>`; only a disk loader is missing. Depends on C1. |
| E2 | **`FindQuery` builder** | S | 10-field struct with `Default`; every example uses `..Default::default()`. Matches `BrokerBuilder`. Done while in `broker.rs`. |

## Wave 2 — Serving: selection & resilience
*Make the rotating server actually good. All touch `server.rs` `Pool`/`best_for`/`handle_client`.*

| # | Feature | Effort | Notes |
|---|---|---|---|
| B3 | **Serve filter passthrough** (`--lvl/--strict/--post/--dnsbl`) | S | Fixes a real hole — anonymity-filtered serving is currently impossible. Plumbs fields already on `FindArgs`. |
| B4 | **Country filter on serve + check** (`--only-cc`) | S | Builds on geo already resolved. |
| B1 | **Selection strategy + sticky sessions** (`Best/RoundRobin/Random/Sticky`) | M | Standout feature; #1 rotating-proxy ask. `best_for` is one isolated index-returning fn. |
| B5 | **Health-scored selection + re-probe timer** | M | Generalizes `priority()`; `failTimeout` re-entry. Extends B1. |
| B2 | **Inline rotate-on-error / retry failover** | M | Makes serving *feel* reliable; reactive per-request, reusing the eviction hook. |
| B11 | **`--http-allowed-codes`** (retry on bad upstream status) | S/M | Dodges captcha/block pages. Pairs with B2's retry loop. |
| B10 | **`--prefer-connect`** selection bias | S | Python parity; a tie-break in the comparator. |
| B13 | **`--min-queue` / `--backlog`** startup controls | S | Python parity; startup-under-load behavior. |

## Wave 3 — Serving: auth, control, protocols
*Complete the server as a production tool.*

| # | Feature | Effort | Notes |
|---|---|---|---|
| B6 | **`proxycontrol` control API** (`/api/history`, `/api/remove`) | S/M | Introspect/steer a live server without restart. Python parity. |
| B7 | **`X-Proxy-Info` header injection** | S | Client sees which upstream served each request. Python parity. |
| B8 | **Upstream proxy auth** (SOCKS5 RFC 1929 + HTTP `Proxy-Authorization`) | M | Unlocks paid/authenticated pools. `tokio-socks` already exposes authed connect. |
| B9 | **Local-server client auth** (`--auth user:pass`) | S/M | Safely expose the server on a shared host. |
| B12 | **SOCKS5 front-end for the local server** | M/L | Clients speak SOCKS5, not only HTTP/CONNECT. Relay core is scheme-agnostic; only the request parser needs a branch. |

## Wave 4 — Output & integration
*Make the CLI a universal building block. All touch the `Format` enum / `write_stream`.*

| # | Feature | Effort | Notes |
|---|---|---|---|
| C3 | **`--format url` / `csv`** | S | `find_and_save.rs` already hand-rolls URL output — proof it belongs in the enum. |
| C4 | **NDJSON / JSON-array toggle + stream mode** | S | Fixes "why won't `jq .` parse this". ⚠ version the JSON schema before consumers depend on it. |
| C5 | **`Serialize for Stats` + `--stats-format json`** | S | One derive; machine-readable summary for CI/dashboards. |
| C6 | **Custom output template** (`--output-format "{{proxy}}/{{country}}"`) | S | mubeng parity; render against `Proxy` getters. |
| C7 | **Region/city from a user-supplied City DB** | S | ⚠ CC BY 4.0: read richer fields only from a user's `--geo-db`; keep bundled DB Country-Lite, bundle no City data. |
| C8 | **ASN attribution from a user-supplied ASN DB** | S | ⚠ CC BY 4.0: read ASN only from a user's `--asn-db` (a separate mmdb from `--geo-db`); bundle no ASN data. Additive `asn` field on the v1 JSON (null unless resolved) + `{{asn}}`/`{{asn_org}}` template tokens. |

## Wave 5 — Checking depth
*Deepen the check engine. All touch `checker.rs` `attempt`/`check_one` + `proxy.rs`.*

| # | Feature | Effort | Notes |
|---|---|---|---|
| A2 | **Judge-less liveness mode** (graceful degradation) | M | Today all-judges-down → `NoJudges` → `find` returns nothing. Degrade to a liveness URL, skip anonymity. The truest "lazy-that-holds". |
| A3 | **Timing percentiles** (p50/p90/p95) | S | `runtimes: Vec<f64>` is already retained; only the mean is exposed. |
| A4 | **Extra check dimensions** (cookie/referer/SMTP capability profile) | S/M | Filter for proxies that do the thing you need. |
| A5 | **Configurable retry/backoff policy** | S/M | Replace the single global `max_tries`; retry logic is one localized loop. |
| A6 | **Honeypot / hostile-proxy detection** (`trust` verdict) | M | ⚠ offline-first with recorded fixtures; report *why*, not a bare boolean (false positives from gzip re-encode / transparent caches). |

## Wave 6 — Observability
*Instrument what's built.*

| # | Feature | Effort | Notes |
|---|---|---|---|
| F1 | **Prometheus metrics endpoint** (`serve --metrics`) | S/M | Every number already lives in `Pool`/`Stats`. Feature-gated. Cheapest credibility win. |
| F2 | **`--progress`** live bar during `find` | S/M | The shared `StatsCollector` already records every check; poll it. |
| F3 | **Structured `tracing` events per check** | S | Consistent structured events at the outcome points → JSON-log observability. |
| F5 | **Benchmark harness** (`criterion`, mock sockets) | M | ⚠ must mock sockets — network-bound work hides the CPU win and a naive wall-clock test backfires. |
| F4 | **`ratatui` TUI dashboard** (`proxybroker top`) | M | ⚠ do after persistence (W7) — a dashboard over an ephemeral pool has little to show. |

## Wave 7 — Persistence & adaptive
*The deliberate category change: durable state. Do only after W1's file-based save/load proves insufficient.*

| # | Feature | Effort | Notes |
|---|---|---|---|
| D2 | **SQLite state** (`--state proxies.db`, `persist` feature) | M | ⚠ fights "ephemeral by design" + "no speculative abstraction": one denormalized table, `PRAGMA user_version`, no ORM, feature-gated. |
| D3 | **Adaptive re-checking + decay scheduler** | M | ⚠ re-check traffic can get IP-blocked — global rate ceiling + jitter, must not outrun `max_conn`. Needs D2. |
| E3 | **Watch / live-reload the source file** | S | `notify` on the input path; feed changes into the pool. Pairs with the re-check loop. |

## Wave 8 — Distribution & ecosystem
*Packaging and integrations. The two "wait for a real consumer" ideas live here on purpose.*

| # | Feature | Effort | Notes |
|---|---|---|---|
| D1 | **Static musl binary + shell installer + `FROM scratch` Docker** | S | Rust's free win over `pip install`. ⚠ musl + ring/rustls cross-compile yak-shaving; `cargo dist` mitigates. |
| E1 | **`RotatingProxyConnector`** — drop-in reqwest/hyper connector | M | The library headline; what Python structurally can't do. ⚠ hyper 1.x `Connect` + TLS is version-coupled — pin one version, build only against a concrete consumer. |
| E4 | **MCP server** exposing the live pool (`proxybroker mcp`) | M | Agent tooling. ⚠ `rmcp` churn — pin it, keep the tool surface tiny, thin veneer over `Pool`. |

## Cross-cutting (ongoing, not a wave)

| # | Feature | Effort | Notes |
|---|---|---|---|
| P1 | **More bundled providers (12 → ~50) + dead-source curation** | M, ongoing | ⚠ offline-testable: add each with a recorded-fixture parse test; treat liveness as a periodic CI audit, not a unit test. Slot into any wave. |

---

## Principle-conflict register

| Feature(s) | Principle | Mitigation |
|---|---|---|
| C7, C8(ASN) | CC BY 4.0 data hygiene | Ship the hook (accept a user mmdb); bundle no non-Country data. |
| D2, D3 | no speculative abstraction / ephemeral-by-design | One denormalized table, `persist` feature gate; do C2 (file) first. |
| E1, E4 | no speculative abstraction / version churn | Pin one dep version; build only against a real consumer. |
| A6, F5, P1 | offline-testable | Recorded fixtures / mock sockets; liveness as CI audit, not unit test. |
| C4 | data hygiene (not licensing) | Version the JSON schema before downstream depends on it. |

## Module map (where each wave lands)

- `broker.rs` — W1 (A1 `check_stream`, E2 builder), the `find`/`check` orchestration.
- `checker.rs` — W5 (`attempt`/`check_one`, `NoJudges`).
- `server.rs` — W2 + W3 (`Pool`, `best_for`, `handle_client` — the seam most features touch).
- `proxy.rs` — W1/W4/W5 (`Serialize`/`Deserialize`, `priority`, `record_attempt`, `runtimes`).
- `stats.rs` — W4/W6 (numbers for metrics + JSON summary).
- `bin/proxybroker.rs` — W4 (`Format` enum), every CLI-flag feature.
- new: `persist.rs` (W7), `connector.rs` (W8 E1), `mcp.rs` (W8 E4).
