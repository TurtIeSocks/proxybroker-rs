# Deferred Backlog

Deliberate **YAGNI deferrals** — features that were scoped, understood, and
consciously *not* built because no consumer needs them yet. Each ships nothing on
speculation; each has a concrete trigger that would justify building it. The
canonical list lives in the repository at
[`docs/roadmap/deferred-backlog.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/deferred-backlog.md).

As of 2026-07-17, the committed [roadmap](./roadmap.md) (Waves 1–9, all
A/B/C/D/E/F items plus `store-redis` and the `top` TUI), C8 (ASN), and P1 (provider
expansion) are shipped. A backlog sweep then built four of the six deferrals and
consciously kept two deferred (see below). Nothing below blocks anything.

## Shipped from the backlog (2026-07-17 sweep)

| Item | What shipped |
|---|---|
| **D1 Docker registry auto-push** | A `docker` job in `release.yml` pushes the `FROM scratch` image to GHCR on a `v*` tag (built-in `GITHUB_TOKEN`, no secret). See [Installation](../getting-started/installation.md). |
| **F1 judge-probe latency metric** | A `proxybroker_pool_probe_latency_avg_seconds` gauge — the check-time judge-probe RTT, recorded on a dedicated unserialized `Proxy` field, kept distinct from the serve-blended `avg_resp_time`. See [Observability](../operations/observability.md). |
| **D3 memory-only re-check** | `serve --recheck` works without `--state` via an in-memory `MemoryStore` (a `Store` impl gated on `persist`, no backend); its EWMA fold mirrors `SqliteStore`. See [Persistence](../library/persistence.md). |
| **E1 `broker.rotating()` sugar** | `Broker::rotating(query, cfg)` composes `find` → `Pool::spawn` → `RotatingProxyConnector::from_pool` in one call. See [Connector](../library/connector.md). |

## Consciously kept deferred (reviewed, not built)

| Item | Why it stays deferred |
|---|---|
| **A6 cert-pinning** | There is no known-good certificate to *pin against* for arbitrary scraped proxies, and the check path already accepts any cert by design. A real version would be a user-supplied expected fingerprint or a bare "expose the fingerprint" — both add `sha2` + a `trust-tls` feature for thin value. The wave-5 spec itself flagged "or defer entirely." **Trigger:** a user who pins specific known proxies and wants a fingerprint-mismatch `TrustSignal`. |
| **E1 TLS-to-target** | Redundant with standard hyper composition: a caller wanting validated `https://` through the [connector](../library/connector.md) wraps it in a `hyper-rustls` `HttpsConnector` (which does proper cert-validated TLS-to-target). Building it *into* `RotatingProxyConnector` reimplements the ecosystem's blessed layering; the connector's raw-tunnel design is correct. **Trigger:** a genuine consumer that needs a self-contained `https://` connector without composing hyper-rustls. |

## Softer ideas

Planning-era notions, not formal roadmap deferrals:

- **async `Store` trait** — the [persistence](../library/persistence.md) trait is
  synchronous by design (blocking SQLite/Redis behind the observer). An async variant
  would only pay off for a high-throughput fully-async backend.
- **`top` TUI persisted sparklines** — historical timeseries in
  [`proxybroker top`](../cli/top.md) (today it renders a live snapshot only). Would
  need a ring-buffer of pool snapshots.

## Opportunistic hygiene (done)

- Reject the unspecified IP (`0.0.0.0`) sentinel — done 2026-07-17 in
  `ProviderSpec::extract` (surfaced by the P1 review). `canonicalize_ip` stays
  Python-parity-faithful; the filter lives at the provider-candidate layer.

## On the deferral discipline

Every kept-deferred entry above is a decision, not an omission. The project's
principle is that speculative abstraction is a liability: an unused feature is code to
maintain, a larger dependency tree, and a wider API to keep stable — all paid for
before any consumer benefits. Recording the trigger keeps each idea cheap to revive
without carrying its cost in the meantime. The same discipline shaped the
[systematic refactor](./systematic-refactor.md) that produced the port.
