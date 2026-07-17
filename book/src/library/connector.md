# Rotating connector (`connector`)

`RotatingProxyConnector` is a drop-in [`tower_service::Service<Uri>`] that routes
every outbound connection through a rotating pooled proxy — so a Rust program
gets pooled, self-healing proxy rotation with **no local server and no listening
port**. It is gated behind the `connector` feature (internally "E1").

Per connection it checks out a healthy proxy from a [`Pool`](./pool.md),
negotiates the tunnel with the shared negotiator, retries a different proxy on
failure (dead ones self-eject via the pool's existing health thresholds), and
hands hyper the negotiated byte stream.

## `RotateConfig`

```rust
#[derive(Debug, Clone)]
pub struct RotateConfig {
    /// Proxies to try (each a different checkout) before returning an error.
    pub max_tries: usize,
    /// Per-connection negotiation timeout.
    pub timeout: Duration,
}
```

`RotateConfig::default()` is `max_tries: 3`, `timeout: 8s`.

## Plugging into hyper-util

reqwest 0.13 exposes no custom-connector hook, so the intended drop-in target is
`hyper_util::client::legacy::Client`, not `reqwest::Client`. You build the
connector from an already-fed pool with `from_pool`, then hand it to the client
builder:

```rust
use std::sync::Arc;
use std::time::Duration;
use proxybroker::connector::{RotatingProxyConnector, RotateConfig};
use proxybroker::resolver::Resolver;
use proxybroker::server::{Pool, PoolConfig};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

// The pool must already be populated — via Pool::spawn(find_stream, ..) or
// Pool::from_proxies(..). The connector wraps an existing pool + resolver.
let resolver = Arc::new(Resolver::new(Duration::from_secs(8))?);
let connector = RotatingProxyConnector::from_pool(
    pool,          // Arc<Pool>
    resolver,
    RotateConfig::default(),
);

let client: Client<_, http_body_util::Empty<bytes::Bytes>> =
    Client::builder(TokioExecutor::new()).build(connector);
// Every request this client makes now dials through a rotating pooled proxy.
```

`from_pool` is the honest seam: the pool must already be populated (there is no
hidden `find` inside the connector). The service is always ready — checkout
happens per call, in `Service::call`, which runs the retry loop over up to
`max_tries` proxies. Each failed dial records the error so the pool benches or
ejects the proxy through its normal thresholds; the response type is `ProxyConn`,
a bare negotiated tunnel wrapped for hyper.

## Scope (v1): tunnel-only

The connector gives hyper a transparent byte stream to the **target**; hyper then
speaks origin-form HTTP over it. That is correct for CONNECT/SOCKS tunnels, where
the stream really reaches the target. Concretely, `tunnel_proto` prefers
SOCKS5 → SOCKS4 → CONNECT:80, and only falls back to plain-HTTP passthrough when
the target scheme is `http`.

A plain forward-HTTP proxy (which needs absolute-form requests) is **not** the
intended fit — prefer CONNECT/SOCKS proxies. An HTTPS-only proxy for a given
connection is skipped and another is tried.

### Deferred: TLS-to-target and `broker.rotating()` sugar

For an `https://` URL the connector returns the tunnel and the **caller layers
its own end-to-end TLS**. Terminate-and-verify TLS to the target is a later
feature with its own consumer, and a `Broker::rotating()` convenience
constructor is deferred until a consumer wants it. See the
[deferred backlog](../project/deferred-backlog.md).

## Security note

The checker uses a liveness-only `AcceptAllVerifier` when it probes a proxy's
TLS — that is fine for deciding whether a proxy is alive, but it accepts any
certificate. The connector deliberately **never** reuses it for real client
traffic: doing so would be a silent MITM hole. The server's protocol picker can
return `Proto::Https` (which upgrades TLS to the target with the accept-all
verifier) and the SMTP-specific `Connect25`; both are **excluded** from the
connector's `tunnel_proto`, so it only ever hands hyper a plain, un-terminated
byte stream. The caller's own TLS stack — not the checker's — validates the
target certificate.

## Feature dependencies

The `connector` feature pulls in `server` (for [`Pool`](./pool.md)),
`tower-service`, and `hyper-util/client-legacy`. See
[feature flags](../architecture/feature-flags.md).
