# Deferred backlog

Deliberate **YAGNI deferrals** — features that were scoped, understood, and consciously *not* built
because no consumer needs them yet. Each ships nothing on speculation; each has a concrete trigger
that would justify building it. Consolidated from the wave specs so they live in one place instead
of scattered `⚠` notes.

Status as of 2026-07-17: the committed roadmap (Waves 1–9, all A/B/C/D/E/F items + `store-redis` +
F4 TUI) plus C8 (ASN) and P1 (provider expansion) are **shipped**. A backlog sweep then built four
of the six deferrals and consciously kept two deferred (see below). Nothing below blocks anything.

## Shipped from the backlog (2026-07-17 sweep)

| Item | What shipped |
|---|---|
| **D1 Docker registry auto-push** | A `docker` job in `release.yml` pushes the FROM-scratch image to GHCR on a `v*` tag (built-in `GITHUB_TOKEN`, no secret). |
| **F1 judge-probe latency metric** | `proxybroker_pool_probe_latency_avg_seconds` gauge — the check-time judge-probe RTT, recorded on a dedicated unserialized `Proxy` field, kept distinct from the serve-blended `avg_resp_time`. |
| **D3 memory-only re-check** | `serve --recheck` works without `--state` via an in-memory `MemoryStore` (`Store` impl gated on `persist`, no backend); its EWMA fold mirrors `SqliteStore`. |
| **E1 `broker.rotating()` sugar** | `Broker::rotating(query, cfg)` composes `find` → `Pool::spawn` → `RotatingProxyConnector::from_pool` in one call. |

## Consciously kept deferred (reviewed, not built)

| Item | Why it stays deferred |
|---|---|
| **A6 cert-pinning** | There is no known-good certificate to *pin against* for arbitrary scraped proxies, and the check path already accepts any cert by design. A real version would be a user-supplied expected fingerprint or a bare "expose the fingerprint" — both add `sha2` + a `trust-tls` feature for thin value. The wave-5 spec itself flagged "or defer entirely." **Trigger:** a user who pins specific known proxies and wants a fingerprint-mismatch `TrustSignal`. |
| **E1 TLS-to-target** | Redundant with standard hyper composition: a caller wanting validated `https://` through the connector wraps it in a `hyper-rustls` `HttpsConnector` (which does proper cert-validated TLS-to-target). Building it *into* `RotatingProxyConnector` reimplements the ecosystem's blessed layering; the connector's raw-tunnel design is correct. **Trigger:** a genuine consumer that needs a self-contained https:// connector without composing hyper-rustls. |

## Softer ideas (planning-era, not formal roadmap deferrals)

- **async `Store` trait** — the persistence trait is sync by design (blocking SQLite/Redis behind
  the D2 observer). An async variant would only pay off for a high-throughput fully-async backend.
- **F4 TUI persisted sparklines** — historical timeseries in `proxybroker top` (today it renders a
  live snapshot only). Would need a ring-buffer of `PoolSnapshot`s.

## Opportunistic hygiene (done)

- ~~Reject the unspecified IP (`0.0.0.0`) sentinel~~ — done 2026-07-17 in `ProviderSpec::extract`
  (surfaced by the P1 review). `canonicalize_ip` stays Python-parity-faithful; the filter lives at
  the provider-candidate layer.
