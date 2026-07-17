# Roadmap & Waves

proxybroker-rs was built past 1.0 in **waves**: each wave batches features that
touch the same module, respect the same dependency order, and can ship as one
campaign of one-commit-per-item changes. The full roadmap, with per-item effort
estimates and design notes, lives in the repository under
[`docs/roadmap/`](https://github.com/TurtIeSocks/proxybroker-rs/tree/main/docs/roadmap).

## The wave model

Ordering optimizes for four things, in priority order:

1. **Dependencies** — a feature never precedes what it needs (`Deserialize` before
   save/load; SQLite after file-based save/load; retry-failover with status-gating).
2. **Module batching** — features touching the same file ship together, so
   `server.rs` / `checker.rs` / the output path is opened once, not eight times.
3. **Value × feasibility** — the biggest genuine gaps and cheapest isolated wins go
   first.
4. **Principle friction last** — features that fight the project's stated principles
   (ephemeral-by-design, no speculative abstraction, offline-testable, CC BY 4.0
   data hygiene) are deferred until demand pulls them.

Every feature must stay offline-testable (constraint C5 — see
[The Systematic Refactor](./systematic-refactor.md)).

## Waves

| Wave | Theme | Highlights | Spec |
|---|---|---|---|
| 1 | Inputs & foundation | `check` subcommand, `Deserialize`, save/load, `FindQuery` builder | [wave-1](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-1-inputs-and-foundation.md) |
| 2 | Serving: selection & resilience | selection strategies, sticky sessions, rotate-on-error, `--http-allowed-codes`, `--min-queue`/`--backlog` | [wave-2](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-2-serving-selection-resilience.md) |
| 3 | Serving: auth, control, protocols | `proxycontrol` API, `X-Proxy-Info`, upstream proxy auth, `--auth`, SOCKS5 front-end | [wave-3](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-3-serving-auth-control-protocols.md) |
| 4 | Output & integration | `--format url`/`csv`, NDJSON/JSON-array, `Serialize for Stats`, output templates, City & ASN DBs | [wave-4](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-4-output-and-integration.md) |
| 5 | Checking depth | judge-less liveness, timing percentiles, capability profile, retry policy, honeypot verdict | [wave-5](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-5-checking-depth.md) |
| 6 | Observability | Prometheus `--metrics`, `--progress`, structured `tracing`, benchmark harness, `top` TUI | [wave-6](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-6-observability.md) |
| 7 | Persistence & adaptive | SQLite `--state`, adaptive re-checking, watch/live-reload | [wave-7](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-7-persistence-and-adaptive.md) |
| 8 | Distribution & ecosystem | static musl binary + installer + Docker, rotating connector, MCP server | [wave-8](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/wave-8-distribution-and-ecosystem.md) |
| 9 | Redis backend | `store-redis` — a Redis backend for `--state` alongside SQLite | — |

The committed roadmap (Waves 1–9, all A/B/C/D/E/F items plus `store-redis` and the
`top` TUI) is **shipped**, along with C8 (ASN attribution) and P1 (provider
expansion).

## Feature families

Each feature carries a letter-family prefix. What shipped, by family:

| Family | Scope | Shipped |
|---|---|---|
| **A** | Check engine depth | `check` a user list (A1), judge-less liveness (A2), timing percentiles p50/p90/p95 (A3), cookie/referer/SMTP capability profile (A4), configurable retry/backoff (A5), honeypot/trust verdict (A6) |
| **B** | The rotating server | filter passthrough (B3), country filter (B4), selection strategies + sticky (B1), health-scored selection + re-probe (B5), rotate-on-error failover (B2), `proxycontrol` API (B6), `X-Proxy-Info` (B7), upstream auth (B8), client `--auth` (B9), `--prefer-connect` (B10), `--http-allowed-codes` (B11), SOCKS5 front-end (B12), `--min-queue`/`--backlog` (B13) |
| **C** | Inputs, outputs & geo/ASN | `Deserialize` (C1), save/load (C2), `--format url`/`csv` (C3), NDJSON/JSON-array (C4), `Serialize for Stats` (C5), output templates (C6), City DB (C7), **ASN attribution (C8)** |
| **D** | Distribution & persistence | static musl binary + installer + Docker (D1), SQLite `--state` (D2), adaptive re-checking + decay (D3) |
| **E** | Library & ecosystem | `FindQuery` builder (E2), watch/live-reload (E3), rotating connector (E1), MCP server (E4) |
| **F** | Observability | Prometheus metrics (F1), `--progress` (F2), structured `tracing` (F3), `top` TUI dashboard (F4), benchmark harness (F5) |

See [Observability](../operations/observability.md) for the F-family runtime
surface, and [Feature Flags](../architecture/feature-flags.md) for how the optional
features gate into the build.

## Cross-cutting: providers (P1)

Provider expansion is tracked outside the wave sequence because it is ongoing. The
bundled registry grew from the Python original's 12 sources to **50 curated live
sources**. Its shape is guarded offline by format-archetype fixtures and a registry
integrity test; its liveness is guarded by a scheduled audit workflow (see
[Contributing](./contributing.md)). The research and expansion notes live in
[`p1-provider-research.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/p1-provider-research.md)
and
[`p1-provider-expansion.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/p1-provider-expansion.md).

## What was deliberately not built

Several features were scoped, understood, and consciously deferred with concrete
triggers rather than shipped on speculation. See the
[Deferred Backlog](./deferred-backlog.md).
