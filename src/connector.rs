//! E1 — a drop-in [`tower_service::Service`] connector that routes every outbound connection
//! through a rotating pooled proxy, so a Rust program gets pooled, self-healing proxy rotation with
//! no local server and no port. Gated `feature = "connector"`.
//!
//! Per connection it checks out a healthy proxy from a [`Pool`], negotiates the tunnel with the
//! shared [`negotiate`], retries a different proxy on failure (dead ones self-eject via the pool's
//! existing health thresholds), and hands hyper the negotiated stream.
//!
//! # Scope (v1)
//! - **Tunnelling proxies.** The connector gives hyper a transparent byte stream to the *target*;
//!   hyper then speaks origin-form HTTP over it. That is correct for CONNECT/SOCKS tunnels (the
//!   stream really reaches the target). A plain forward-HTTP proxy (which needs absolute-form
//!   requests) is not the intended fit — prefer CONNECT/SOCKS proxies.
//! - **No TLS-to-target.** For an `https://` URL the connector returns the tunnel; the caller
//!   layers its own TLS. We deliberately do **not** reuse the checker's liveness-only
//!   `AcceptAllVerifier` — that would be a security hole in real client traffic. Terminate-and-
//!   verify TLS is a later feature with its own consumer.
//! - **hyper-util, not reqwest.** reqwest 0.13 exposes no custom-connector hook, so the drop-in
//!   target is `hyper_util::client::legacy::Client::builder(..).build(connector)`, not
//!   `reqwest::Client`. A `Broker::rotating()` convenience is deferred until a consumer wants it.

use crate::error::ProxyError;
use crate::negotiator::{negotiate, Stream, Target};
use crate::proxy::Proxy;
use crate::resolver::Resolver;
use crate::server::{choose_proto, ClientKey, Pool};
use crate::types::Scheme;
use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpStream;

/// Tuning for the rotating connector.
#[derive(Debug, Clone)]
pub struct RotateConfig {
    /// Proxies to try (each a different checkout) before returning an error.
    pub max_tries: usize,
    /// Per-connection negotiation timeout.
    pub timeout: Duration,
}

impl Default for RotateConfig {
    fn default() -> Self {
        RotateConfig {
            max_tries: 3,
            timeout: Duration::from_secs(8),
        }
    }
}

/// A `tower_service::Service<Uri>` that dials each connection through a rotating pooled proxy.
#[derive(Clone)]
pub struct RotatingProxyConnector {
    pool: Arc<Pool>,
    resolver: Arc<Resolver>,
    cfg: RotateConfig,
}

impl RotatingProxyConnector {
    /// Wrap an existing, already-fed pool + resolver. The honest seam: the pool must already be
    /// populated (via [`Pool::spawn`]/[`Pool::from_proxies`]).
    pub fn from_pool(pool: Arc<Pool>, resolver: Arc<Resolver>, cfg: RotateConfig) -> Self {
        RotatingProxyConnector {
            pool,
            resolver,
            cfg,
        }
    }
}

/// The negotiated tunnel handed to hyper: wraps [`negotiator::Stream`](Stream) in [`TokioIo`] and
/// reports a bare [`Connected`] (no proxy/ALPN metadata).
pub struct ProxyConn(TokioIo<Stream>);

impl hyper::rt::Read for ProxyConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for ProxyConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Connection for ProxyConn {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl tower_service::Service<Uri> for RotatingProxyConnector {
    type Response = ProxyConn;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<ProxyConn>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(())) // checkout is per-call; the service itself is always ready
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let pool = self.pool.clone();
        let resolver = self.resolver.clone();
        let cfg = self.cfg.clone();
        Box::pin(async move { connect(&pool, &resolver, &cfg, uri).await })
    }
}

/// The per-connection retry loop — the same shape as the server's relay path: check out a proxy,
/// negotiate, and on failure record the error (so the pool self-ejects it) and try the next.
async fn connect(
    pool: &Pool,
    resolver: &Resolver,
    cfg: &RotateConfig,
    uri: Uri,
) -> io::Result<ProxyConn> {
    let scheme = match uri.scheme_str() {
        Some("https") => Scheme::Https,
        _ => Scheme::Http,
    };
    let host = uri
        .host()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "uri has no host"))?
        .to_string();
    let port = uri
        .port_u16()
        .unwrap_or(if scheme == Scheme::Https { 443 } else { 80 });
    let ip = resolver.resolve(&host).await.ok();
    let target = Target { host, ip, port };
    // Non-sticky: the connector has no client identity, so any key resolves the same pool.
    let key = ClientKey::Ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    let mut last: Option<ProxyError> = None;
    for _ in 0..cfg.max_tries {
        let Some(mut proxy) = pool.get(scheme, &key).await else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no proxy available in the pool",
            ));
        };
        let proto = choose_proto(&proxy, scheme);
        match dial(&proxy, proto, &target, cfg.timeout).await {
            Ok(stream) => {
                proxy.record_attempt(Some(0.0), None);
                pool.put_ok(proxy);
                return Ok(ProxyConn(TokioIo::new(stream)));
            }
            Err(e) => {
                proxy.record_attempt(None, Some(e));
                pool.put_failed(proxy); // benches / ejects it via the pool's thresholds
                last = Some(e);
            }
        }
    }
    Err(io::Error::other(
        last.map(|e| e.as_str()).unwrap_or("all proxies failed"),
    ))
}

/// Connect the TCP socket to the proxy and negotiate the tunnel — mirrors the server's relay
/// connect step, classifying failures into the same [`ProxyError`] the eviction logic feeds on.
async fn dial(
    proxy: &Proxy,
    proto: crate::types::Proto,
    target: &Target,
    timeout: Duration,
) -> Result<Stream, ProxyError> {
    let tcp =
        match tokio::time::timeout(timeout, TcpStream::connect((proxy.host, proxy.port))).await {
            Err(_) => return Err(ProxyError::Timeout),
            Ok(Err(_)) => return Err(ProxyError::ConnFailed),
            Ok(Ok(t)) => t,
        };
    negotiate(proto, tcp, target, timeout, proxy.auth()).await
}
