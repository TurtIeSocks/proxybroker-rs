# P1 — Provider expansion: spec + plan

**Goal:** grow the bundled provider registry from 12 → **50** with curated, fresh, low-overlap
free-proxy sources, an offline parse-test guard, and a scheduled liveness audit.

**Research:** [`p1-provider-research.md`](p1-provider-research.md) — 82 curl-verified live sources;
Tier A = 38 curated entries across 11 actively-maintained sources.

## Decisions (resolved, full-auto)

| # | Decision | Choice | Why |
|---|---|---|---|
| 1 | Count/philosophy | Tier A curated → **exactly 50** | breadth without redundant near-duplicate dumps |
| 2 | Staleness | **fresh repos only** (pushed 2026-07-17, hourly Actions) | curl yield hides dead-repo staleness |
| 3 | PR scope | **all 38 in one PR** | homogeneous URL additions + one test |
| 4 | Parse test | **format-archetype fixtures + registry test** | 38 sources → 3 body formats under one scanner; per-source fixtures re-test the same 3 formats 38× |
| 5 | Liveness audit | **new scheduled workflow** (weekly, non-blocking) | roadmap: "liveness as a periodic CI audit, not a unit test" |

## Assumptions / deviations

- **Per-source recorded fixtures → per-format archetypes.** The roadmap says "add each with a
  recorded-fixture parse test." Tier A's 38 sources use only **3** body formats (plain `ip:port`;
  `ip:port:country`; `scheme://ip:port`) and all go through the one generic `find_addrs_global`
  scanner, so a per-source fixture only re-tests its format. The real per-source failure modes are
  handled elsewhere: a typo'd/malformed entry by the **registry integrity test** (offline), and a
  dead/format-changed live source by the **scheduled audit**. Net: 3 archetype fixtures instead of
  38, same coverage, far less repo weight.
- **proxyscrape v2 kept alongside v4.** The existing 3 v2 `getproxies` endpoints stay; the 4 new
  v4 endpoints are additive (different pool, v2 may deprecate).
- **No custom `pattern`s.** Every Tier-A format is scanner-parseable as-is (verified).

## Architecture

A provider is one `simple` GET; adding one is a `data/providers.yaml` entry (`url` + `protocols`).
No code path changes — the fetch/extract pipeline is unchanged. The work is data + tests + CI.

## Tasks

### Task 1 — Registry entries (data)
- **Files:** `data/providers.yaml` (append 38 Tier-A entries + refreshed header comment).
- Update the header: provenance, curation policy (fresh-only, no mirrors), 2026-07-17 date, count.

### Task 2 — Format-archetype fixtures + registry parse test (offline, TDD)
- **Files:** `tests/data/providers/fmt-{plain-ipport,colon-country,scheme-prefixed}.txt` (real
  trimmed samples), `tests/provider_registry.rs`.
- Test A (archetypes): each fixture, through `ProviderSpec::extract`, yields ≥5 candidates tagged
  with the declared protocol. Locks the 3 formats the scanner must handle.
- Test B (registry integrity): `bundled_registry()` has **≥50** entries; every URL is unique and
  http(s)://; no entry declares an unsupported `kind`; protocol lists are valid. Catches typos,
  dupes, and malformed additions offline.
- Extend `provider_fetch.rs::bundled_registry_parses_and_is_nonempty` lower bound 10 → 50.

### Task 3 — Scheduled liveness audit (non-blocking)
- **Files:** `tests/provider_audit.rs` (an `#[ignore]`d test fetching every bundled URL, collecting
  zero-yield sources, panicking with the dead list), `.github/workflows/provider-audit.yml`
  (`on: schedule` weekly + `workflow_dispatch`; runs the ignored test with `--ignored`). Scheduled
  only — never gates a PR.

## Global constraints
- Stable toolchain (`rustup run stable cargo …`).
- Offline unit/integration suite stays network-free (the audit is `#[ignore]`d + separate workflow).
- CI `test` job must stay green across `--all-features`, `--no-default-features`, cli-only.
- Fixtures are test-only (`include` excludes `tests/**`) — nothing ships in the crate.
