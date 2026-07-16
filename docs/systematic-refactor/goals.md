# Refactor Goals — proxybroker2 (Python) → proxybroker-rs (Rust)

> Engagement mode: **full auto**. The user delegated every design call. Each goal
> and constraint below that was not stated verbatim by the user is marked
> **[assumed]** and is restated in the final Assumptions report.

## Prior art (verified 2026-07-15, before any code was written)

The user asked for a landscape check first. Result: **no equivalent exists**, but
near-misses do. Verified via the crates.io and GitHub APIs, not from memory:

| crate | latest | published | `has_lib` | downloads | scope |
|---|---|---|---|---|---|
| `proxy-rs` | 0.3.7 | 2023-10-24 | **false** | 12,245 | closest analogue: scraper + checker + serve. Repo `zevtyardt/proxy.rs` 301-redirects (moved/renamed). Dead ~2.7 years. |
| `proxy-scraper-checker` | 0.1.3 | 2024-06-14 | false | 5,366 | single source (checkerproxy.net archive) |
| `open_proxies` | 0.1.1 | 2022-11-15 | true | 2,677 | checker only, minimal |
| `proxy-scraper` | 0.2.0 | 2024-05-03 | true | 2,729 | different domain — MTProxy/Shadowsocks/VMess link parsing |

**Conclusion:** `proxy-rs` is the one a search should surface, and it is
genuinely ProxyBroker-shaped — but it publishes **no library target on any
version** and has been unmaintained since Oct 2023. The "library on crates.io"
niche this port targets is open. Crate names `proxybroker` and `proxybroker-rs`
are both unclaimed.

## Drivers (Phase 2 checklist)

Selected from the standard checklist:

- [x] **Language/runtime change** — Python 3.10+/asyncio → Rust/tokio. *Stated by user.*
- [x] **API surface change** — must ship a library (crates.io) **and** a CLI binary. *Stated by user.*
- [x] **Performance** — [assumed] a Rust rewrite is only worth doing if it beats the original on the axes that hurt: memory under high concurrency and per-check latency.
- [x] **Dependency reduction** — [assumed] a natural consequence; tracked so it does not silently regress into a 200-crate tree.
- [ ] Readability / maintainability — not a driver; the Python is not the problem.
- [ ] Testability — not a driver, but see constraint C5.
- [ ] Architecture — not a driver; the pipeline shape is sound and should survive.
- [ ] Team handoff / onboarding — n/a, single maintainer.

### Probes

**Language change → why Rust, what replaces what, interop?**
- Target chosen by the user; not re-litigated.
- Library replacement is the substance of the port. `aiohttp` splits into a client
  and a server story; `aiodns` → `hickory-resolver`; `maxminddb` → `maxminddb`;
  `click` → `clap`; `attrs` → plain structs; `cachetools` → `moka` or a plain map.
  Resolved with evidence in `research.md`.
- **No Python interop.** [assumed] No PyO3 bindings, no attempt to be importable
  from Python. The user asked for a Rust rewrite with a Rust library consumer, and
  bindings are a separate product with their own release surface. Revisit only on request.

**Performance → bottleneck? profiled? target metric?**
- Not profiled — [assumed] and honestly, no profiling of the Python is planned,
  because the port is not justified by a specific measured bottleneck. It is
  justified by the user wanting a Rust library.
- **Therefore: no performance target is claimed.** This is deliberate. Inventing a
  "10× faster" goal with no baseline would be fiction, and every later claim
  against it would be unfalsifiable. If a benchmark is wanted, it is a separate
  task with a real methodology.
- What *is* committed: no accidental pessimisation — the concurrency model must
  keep the same shape (bounded queues, capped in-flight checks) rather than
  degrading to unbounded spawn.

**API surface → breaking? versioned?**
- Greenfield crate at `0.1.0`. Nothing to break.
- [assumed] The Rust API should be **idiomatic Rust, not transliterated Python**.
  Feature parity is measured in *behaviour*, not in matching method names. A
  faithful port of `Proxy.as_json()` is `impl Serialize`, not a method returning a map.

## Constraints

- **C1 — License: Apache-2.0, non-negotiable.** proxybroker2 is Apache-2.0
  (confirmed: `pyproject.toml` `license = "Apache-2.0"`, and the header in
  `proxybroker/__init__.py`). A port is a derivative work. The Rust crate must
  carry Apache-2.0, a `NOTICE` crediting both Constverum (2015–2018) and BlueT –
  Matthew Lien – 練喆明 (2018–2025), and a statement of changes. Flagged to the
  user; unless they say otherwise this is settled. *Not [assumed] — this is a
  legal requirement, not a preference.*
- **C2 — GeoIP data is an open question, not a given.** The Python vendors
  `GeoLite2-Country.mmdb` (3.1 MB) inside its distributed package. Whether a
  crates.io crate may do the same is a **licensing question with a real answer**,
  researched in `research.md` before any code depends on it. Do not assume
  vendoring is legal because the Python does it.
- **C3 — Must build on stable Rust.** Host default is `nightly-2026-06-10`;
  stable `1.96.1` is installed. A crates.io library that needs nightly is a
  library most people cannot use. Pin stable via `rust-toolchain.toml`.
- **C4 — Migration strategy: big-bang.** [assumed] The target repo is empty (one
  commit, a `.gitattributes`). There is no incremental path and nothing to
  strangle. The Python project stays where it is, untouched.
- **C5 — Network-dependent behaviour must be testable offline.** [assumed but
  load-bearing] Every interesting path in this codebase is I/O against the open
  internet: ~50 scraped sites, judges, live proxies. A test suite that needs the
  internet is a test suite that fails in CI for reasons unrelated to the code.
  The port needs a local mock server, mirroring what `tests/mock_server.py` does
  for the Python.
- **C6 — Scope: feature parity with the real `__all__`, not the docs.** The
  readthedocs API page is stale. The live export list adds `SimpleProvider`,
  `PaginatedProvider`, `APIProvider`, `ConfigurableProvider`, and four
  `load_*_from_directory` functions. Parity is judged against the source.
- **C7 — Team size 1, target-stack familiarity high.** No timeline given; full
  auto means drive to a working, verified milestone rather than to a date.

## Explicit non-goals

- Python interop / PyO3 bindings (see probe above).
- Beating `proxy-rs` on benchmarks. It is dead; it is not the bar.
- Preserving Python method names, module layout, or the `attrs` object model.
- A GUI, a web dashboard, or a hosted service.
- Reproducing bugs for bug-compatibility. Where the Python is wrong, the port is
  right, and the deviation is recorded in `map.md`.
