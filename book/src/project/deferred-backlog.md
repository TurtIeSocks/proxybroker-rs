# Deferred Backlog

These are deliberate **YAGNI deferrals** — features that were scoped, understood,
and consciously *not* built because no consumer needs them yet. Each ships nothing
on speculation, and each has a concrete trigger that would justify building it. The
canonical list lives in the repository at
[`docs/roadmap/deferred-backlog.md`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/docs/roadmap/deferred-backlog.md).

As of the current release, the committed [roadmap](./roadmap.md) (Waves 1–9, all
A/B/C/D/E/F items plus `store-redis` and the `top` TUI), C8 (ASN), and P1 (provider
expansion) are shipped. Nothing below blocks anything.

## Documented deferrals

| Item | What's deferred | Trigger to build |
|---|---|---|
| **A6 cert-pinning** | A TLS certificate-fingerprint check in the honeypot/trust verdict. The core (canary + header scan) ships dependency-free; cert-pinning would read the fingerprint off the negotiator's stream. Open question: gate it behind a `trust-tls` feature (sha2/ring) or drop it. | A user needs honeypot detection via TLS introspection. |
| **E1 `broker.rotating()` sugar** | A `Broker::rotating(query) -> RotatingProxyConnector` convenience that spawns a pool via `find(query)` and wraps it. The [connector](../library/connector.md) itself ships; only the one-liner is deferred. | A consumer wants the wrapper instead of hand-assembling pool + connector. |
| **E1 TLS-to-target** | The [rotating connector](../library/connector.md) returns the raw tunnel to `host:port`; for an `https://` URL the caller layers their own TLS. | A v2 connector that terminates TLS to the target host. |
| **D3 memory-only re-check** | Adaptive re-check decay is gated on persistence (it needs a durable `last_seen`). There is no in-memory-only score map for a DB-less adaptive mode. | A user wants adaptive re-check without a `--state` backend. |
| **F1 judge-probe latency metric** | Prometheus exposes the serve-time `avg_resp_time` (relayed-request RTT) as the latency signal; actual judge-probe latency is not plumbed into a separate metric. See [Observability](../operations/observability.md). | A consumer needs true probe latency reported distinctly from serve RTT. |
| **D1 Docker registry auto-push** | The `FROM scratch` `Dockerfile` and a `docker build` CI smoke test ship; the image is documented but not auto-pushed to a registry. | A registry secret is wired and published images are wanted. |

## Softer ideas

Planning-era notions, not formal roadmap deferrals:

- **async `Store` trait** — the [persistence](../library/persistence.md) trait is
  synchronous by design (blocking SQLite/Redis behind the observer). An async variant
  would only pay off for a high-throughput fully-async backend.
- **`top` TUI persisted sparklines** — historical timeseries in
  [`proxybroker top`](../cli/top.md) (today it renders a live snapshot only). Would
  need a ring-buffer of pool snapshots.

## On the deferral discipline

Every entry above is a decision, not an omission. The project's principle is that
speculative abstraction is a liability: an unused feature is code to maintain, a
larger dependency tree, and a wider API to keep stable — all paid for before any
consumer benefits. Recording the trigger keeps each idea cheap to revive without
carrying its cost in the meantime. The same discipline shaped the
[systematic refactor](./systematic-refactor.md) that produced the port.
