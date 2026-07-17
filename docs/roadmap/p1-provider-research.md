# P1 — Provider expansion research (12 → ~50)

**Date:** 2026-07-17 · **Status:** research complete, awaiting build decisions.

Roadmap P1: *"More bundled providers (12 → ~50) + dead-source curation. Offline-testable: add
each with a recorded-fixture parse test; treat liveness as a periodic CI audit, not a unit test."*

## Method

A multi-modal discovery sweep (GitHub-raw lists, single-GET txt APIs, HTML-table sites, and mining
other proxy tools' source configs) — every candidate **curl-verified** for a real measured
`ip:port` yield, cross-checked against an independent 42-URL hand-vetted seed list. 89 raw
candidates → 82 verified live (≥5 pairs) across ~31 sources.

## What qualifies (the model dictates sourcing)

A provider is **one plain HTTP GET**; the generic `find_addrs_global` scanner extracts `ip:port`
pairs from the body (plaintext, per-line, or HTML tables). So:

- **Best fit:** GitHub-raw auto-updated `.txt` lists — one GET, `ip:port` per line, split per
  protocol (http/https/socks4/socks5), refreshed hourly by a GitHub Action.
- **No custom `pattern` needed** for any recommended source: the scanner cleanly handles trailing
  junk (`ip:port:country`, hideip.me), pipe rows (`ip:port | …`, roosterkid), and scheme prefixes
  (`http://ip:port`, proxifly). Verified against the real extractor.
- **Disqualified by the model:** anything needing pagination, an API key, POST, or JS rendering;
  and JSON APIs with separated `ip`/`port` fields (geonode) — would need a `pattern`, low priority.

## Key findings

1. **Supply is abundant** — reaching 50 is trivial; the real work is **curation, not quantity**.
2. **Staleness is the decisive quality filter, and curl yield hides it.** Several high-yield raw
   files are served from *abandoned* repos: `jetkai/proxy-list` (2023, 1.8k pairs), `MuRongPIG`
   (101k pairs, ~11mo stale), `casals-ar` (68k, stale), `mishakorzik` (~1M-line garbage dump,
   2023). The file returns thousands of pairs — all long-dead. **Filter by repo freshness (recent
   commits), not just by curl yield.**
3. **Overlap/redundancy** — GitHub lists scrape overlapping pools, and some are exact mirrors
   (`TheSpeedX/SOCKS-List` ≡ `TheSpeedX/PROXY-List`; `vakhov.github.io` ≡ its raw files;
   `openproxylist.xyz` host-variants). Bundling all wastes fetches for little marginal coverage.
4. **Quality caveat** — free proxies are mostly dead on arrival regardless of source; these are raw
   *candidate* feeds. The checker filters. Yield ≠ usable proxies. The value is a broad, fresh feed
   into `find`.
5. **Confirmed dead / rejected** — `www.proxy-list.download` (persistent 502), `mmpx12/proxy-list`
   (empty on both branches), `proxyscan.io`, `openproxy.space` (now a JS SPA), plus the stale repos
   above and JS-obfuscated table sites (proxynova, hidemy.name, spys.one tables).

## Recommendation — Tier A (curated, fresh, low-overlap): 38 entries → **exactly 50 total**

Eleven actively-maintained sources (all pushed 2026-07-17), protocol-split, no patterns:

| Source | Protocols | Access | Notes |
|---|---|---|---|
| `TheSpeedX/PROXY-List` | http, socks4, socks5 | GitHub raw (master) | canonical high-volume, hourly |
| `monosans/proxy-list` | http, socks4, socks5 | GitHub raw (main) | pre-*validated* (smaller, higher quality) |
| `proxifly/free-proxy-list` | http, socks4, socks5 | GitHub raw (main) | ~5-min refresh |
| `zloi-user/hideip.me` | http, https, socks4, socks5 | GitHub raw (main) | `ip:port:country` lines |
| `ErcinDedeoglu/proxies` | http, https, socks4, socks5 | GitHub raw (main) | very large dumps |
| `vakhov/fresh-proxy-list` | http, https, socks4, socks5 | GitHub raw (master) | frequent |
| `roosterkid/openproxylist` | https, socks4, socks5 | GitHub raw (main) | pipe-delimited rows |
| `hookzof/socks5_list` | socks5 | GitHub raw (master) | plain `ip:port` |
| `api.proxyscrape.com` v4 | http, https, socks4, socks5 | txt API | newer v4 endpoint |
| `proxyspace.pro` | http, https, socks4, socks5 | txt API | high yield |
| `api.openproxylist.xyz` | http, https, socks4, socks5 | txt API | high yield |

The exact 38 URLs are captured in the discovery data; they slot into `data/providers.yaml` as
plain `url` + `protocols` entries.

### Tier B — additional verified-fresh (swap/pad, higher overlap or needs a format re-check)
`zevtyardt/proxy-list`, `dpangestuw/Free-Proxy`, `Anonym0usWork1221/Free-Proxies`,
`Zaeem20/FREE_PROXIES_LIST`, `sunny9577/proxy-scraper`, `spys.me/proxy.txt`, `ObcbO/getproxy`,
`elliottophellia/proxylist`. (~24 sources / 68 files total are available if we want more than 50.)

### Excluded
Stale-but-serving (`jetkai`, `MuRongPIG`, `casals-ar`, `ShiftyTR`, `prxchk`, `clarketm`,
`mishakorzik`, `proxy4parsing`, `rdavydov`, `B4RC0DE-TM`), dead (`proxy-list.download`, `mmpx12`,
`proxyscan.io`, `openproxy.space`), mirrors (`TheSpeedX/SOCKS-List`, `vakhov.github.io`,
`openproxylist.xyz` host-variants), JSON-needs-pattern (`geonode`).

## Test / audit plan (matches the roadmap's offline constraint)

1. **Recorded-fixture parse tests (blocking, offline).** For each new source, save a *small trimmed
   sample* of its real body to `tests/data/providers/<slug>.txt` (~15–20 lines — keep the repo
   light) and a data-driven test asserting `ProviderSpec{url, protocols}.extract(fixture)` yields
   ≥N candidates with the right protocol tag. Locks parse behavior without touching the network.
2. **Liveness audit (non-blocking, scheduled).** A separate CI job (weekly `schedule:`) fetches
   every bundled provider URL and reports zero-yield/dead sources — drift detection, *not* a gate.
   None exists yet (`tests/liveness.rs` is the unrelated A2 checker feature).
3. **Registry test** already asserts `bundled_registry()` parses + is non-empty; extend its lower
   bound.

## Open decisions (for the build spec)

1. **Count/philosophy:** Tier A curated-fresh (→ exactly 50) vs max-breadth (→ ~80, include Tier B)?
   *Rec: Tier A.*
2. **Staleness policy:** fresh-repos-only (rec) vs any-that-serves-data?
3. **Scope for the first PR:** all 38 at once, or a first batch + the audit infra, iterating (P1 is
   "ongoing")? *Rec: all 38 Tier A in one PR — they're homogeneous URL additions + one data-driven
   test.*
4. **Fixture size:** trimmed samples (rec, keeps the crate/repo light) vs fuller captures?
5. **Build the weekly liveness-audit CI job now** as part of P1? *Rec: yes — it's what keeps the
   list honest over time.*
