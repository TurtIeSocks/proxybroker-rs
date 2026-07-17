# Deferred backlog

Deliberate **YAGNI deferrals** — features that were scoped, understood, and consciously *not* built
because no consumer needs them yet. Each ships nothing on speculation; each has a concrete trigger
that would justify building it. Consolidated from the wave specs so they live in one place instead
of scattered `⚠` notes.

Status as of 2026-07-17: the committed roadmap (Waves 1–9, all A/B/C/D/E/F items + `store-redis` +
F4 TUI) plus C8 (ASN) and P1 (provider expansion) are **shipped**. Nothing below blocks anything.

## Documented deferrals

| Item | What's deferred | Source | Trigger to build |
|---|---|---|---|
| **A6 cert-pinning** | TLS certificate-fingerprint check in the honeypot/trust verdict. The core (canary + header scan) ships dependency-free; cert-pin would read the fingerprint off `negotiator::Stream`. Open question: gate behind a `trust-tls` feature (sha2/ring) vs. drop. | [wave-5](wave-5-checking-depth.md) | A user needs honeypot detection via TLS introspection. |
| **E1 `broker.rotating()` sugar** | A `Broker::rotating(query) -> RotatingProxyConnector` convenience that spawns a pool via `find(query)` and wraps it. The connector itself ships; only the one-liner is deferred. | [wave-8](wave-8-distribution-and-ecosystem.md) | A consumer wants the wrapper instead of hand-assembling pool + connector. |
| **E1 TLS-to-target** | The rotating connector returns the raw tunnel to `host:port`; for an `https://` URL the caller layers their own TLS. | [wave-8](wave-8-distribution-and-ecosystem.md) | A v2 connector that terminates TLS to the target host. |
| **D3 memory-only re-check** | Adaptive re-check decay is gated on `persist` (it needs a durable `last_seen`). No in-memory-only score map exists for a DB-less adaptive mode. | [wave-7](wave-7-persistence-and-adaptive.md) | A user wants adaptive re-check without a `--state` backend. |
| **F1 judge-probe latency metric** | Prometheus exposes the serve-time `avg_resp_time` (relayed-request RTT) as the latency signal; actual judge-probe latency is not plumbed into a separate metric. | [wave-6](wave-6-observability.md) | A consumer needs true probe latency reported distinctly from serve RTT. |
| **D1 Docker registry auto-push** | The `Dockerfile` (FROM-scratch) + a `docker build` CI smoke test ship; the image is documented in the README but not auto-pushed to a registry. | [wave-8](wave-8-distribution-and-ecosystem.md) | A registry secret is wired and published images are wanted. |

## Softer ideas (planning-era, not formal roadmap deferrals)

- **async `Store` trait** — the persistence trait is sync by design (blocking SQLite/Redis behind
  the D2 observer). An async variant would only pay off for a high-throughput fully-async backend.
- **F4 TUI persisted sparklines** — historical timeseries in `proxybroker top` (today it renders a
  live snapshot only). Would need a ring-buffer of `PoolSnapshot`s.

## Opportunistic hygiene (done)

- ~~Reject the unspecified IP (`0.0.0.0`) sentinel~~ — done 2026-07-17 in `ProviderSpec::extract`
  (surfaced by the P1 review). `canonicalize_ip` stays Python-parity-faithful; the filter lives at
  the provider-candidate layer.
