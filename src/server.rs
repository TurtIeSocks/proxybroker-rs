//! A local rotating proxy server: accepts client connections and relays each through a pool
//! of checked proxies, retrying on a different proxy when one fails. `server.py`.
//!
//! Behind the `server` feature — pure library users mostly do not want a listening socket.
//!
//! A forward proxy is mostly byte-splicing (`copy_bidirectional`) plus a CONNECT/negotiate
//! handshake, so this is built on raw tokio rather than hyper: hyper is structured around
//! parsed HTTP messages and is awkward for the CONNECT tunnel upgrade this needs.
//!
//! The pool avoids `server.py`'s `heapq.heappush((avg_resp_time, proxy))`, which raises
//! `TypeError` on tied `f64` (Python compares the `Proxy` objects, which define no `__lt__`).
//! Selection here uses `f64::total_cmp`, so ties are ordered deterministically, never fatal.
//! Imports are served by one dedicated task feeding a [`Notify`], not a per-waiter mutex over
//! the receiver (design critique #22).

use crate::broker::ProxyStream;
use crate::negotiator::{negotiate, Stream, Target};
use crate::proxy::Proxy;
use crate::resolver::Resolver;
use crate::types::{Proto, Scheme};
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Tuning for proxy eviction (`server.py:ProxyPool`).
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Attempts (with different proxies) to satisfy one client request.
    pub max_tries: usize,
    /// Drop a proxy once its error rate exceeds this (after `min_req` requests).
    pub max_error_rate: f64,
    /// Drop a proxy once its average response time (seconds) exceeds this.
    pub max_resp_time: f64,
    /// Grace: a proxy is not evicted until it has handled this many requests.
    pub min_req: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            max_tries: 3,
            max_error_rate: 0.5,
            max_resp_time: 8.0,
            min_req: 5,
        }
    }
}

/// A pool of checked proxies, refilled from a [`ProxyStream`] by a background importer.
pub struct Pool {
    state: Mutex<Vec<Proxy>>,
    notify: Notify,
    config: PoolConfig,
    exhausted: std::sync::atomic::AtomicBool,
}

impl Pool {
    /// A pool over an already-known set of proxies (bring-your-own, or for tests). No importer;
    /// the pool is considered exhausted immediately, so `get` returns `None` once it drains.
    pub fn from_proxies(proxies: Vec<Proxy>, config: PoolConfig) -> Arc<Pool> {
        Pool {
            state: Mutex::new(proxies),
            notify: Notify::new(),
            config,
            exhausted: std::sync::atomic::AtomicBool::new(true),
        }
        .into()
    }

    /// Create a pool and spawn the importer that drains `stream` into it. The importer is the
    /// single owner of the receiver, so waiters never serialize behind a mutex over it.
    pub fn spawn(stream: ProxyStream, config: PoolConfig) -> Arc<Pool> {
        let pool = Arc::new(Pool {
            state: Mutex::new(Vec::new()),
            notify: Notify::new(),
            config,
            exhausted: std::sync::atomic::AtomicBool::new(false),
        });
        {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut stream = stream;
                while let Some(proxy) = stream.next().await {
                    pool.state.lock().unwrap().push(proxy);
                    pool.notify.notify_waiters();
                }
                // Source exhausted: wake anyone waiting so they stop instead of hanging.
                pool.exhausted
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                pool.notify.notify_waiters();
            });
        }
        pool
    }

    /// Check out the best available proxy that supports `scheme`, waiting for the importer if
    /// the pool is momentarily empty. Returns `None` once the source is exhausted and no
    /// suitable proxy remains. "Best" = lowest priority `(error_rate, avg_resp_time)`, ordered
    /// with `total_cmp` so tied response times never panic (the `server.py` heapq bug).
    pub async fn get(&self, scheme: Scheme) -> Option<Proxy> {
        loop {
            // `notified()` must be created before we inspect the pool, so a push between the
            // check and the await is not missed.
            let waker = self.notify.notified();
            {
                let mut pool = self.state.lock().unwrap();
                if let Some(idx) = best_for(&pool, scheme) {
                    return Some(pool.swap_remove(idx));
                }
            }
            if self.exhausted.load(std::sync::atomic::Ordering::SeqCst) {
                // One last look, in case a proxy arrived just before exhaustion was set.
                let mut pool = self.state.lock().unwrap();
                return best_for(&pool, scheme).map(|i| pool.swap_remove(i));
            }
            waker.await;
        }
    }

    /// Return a proxy after use, dropping it if it has become too slow or too error-prone.
    /// `server.py:ProxyPool.put`.
    pub fn put(&self, proxy: Proxy) {
        let unhealthy = proxy.requests() >= self.config.min_req
            && (proxy.error_rate() > self.config.max_error_rate
                || proxy.avg_resp_time() > self.config.max_resp_time);
        if unhealthy {
            tracing::debug!(addr = %proxy.addr(), "evicted from pool");
            return;
        }
        self.state.lock().unwrap().push(proxy);
        self.notify.notify_waiters();
    }
}

/// Index of the best proxy supporting `scheme`, by ascending priority.
fn best_for(pool: &[Proxy], scheme: Scheme) -> Option<usize> {
    pool.iter()
        .enumerate()
        .filter(|(_, p)| p.schemes().contains(&scheme))
        .min_by(|(_, a), (_, b)| {
            let (ae, at) = a.priority();
            let (be, bt) = b.priority();
            ae.total_cmp(&be).then(at.total_cmp(&bt))
        })
        .map(|(i, _)| i)
}

/// A running server. Dropping the handle (or calling [`ServerHandle::shutdown`]) stops it.
#[derive(Debug)]
pub struct ServerHandle {
    addr: SocketAddr,
    cancel: CancellationToken,
}

impl ServerHandle {
    /// The address the server is listening on (useful when bound to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
    /// Stop accepting new connections and shut down.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Start the local proxy server on `addr`, relaying through `pool`. Returns once it is bound
/// and accepting; the accept loop runs in a background task.
pub async fn serve(
    addr: SocketAddr,
    pool: Arc<Pool>,
    resolver: Arc<Resolver>,
    timeout: Duration,
) -> std::io::Result<ServerHandle> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let max_tries = pool.config.max_tries;

    let accept_cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            let client = tokio::select! {
                _ = accept_cancel.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((s, _)) => s,
                    Err(_) => continue,
                },
            };
            let pool = pool.clone();
            let resolver = resolver.clone();
            tokio::spawn(async move {
                let _ = handle_client(client, pool, resolver, timeout, max_tries).await;
            });
        }
    });

    Ok(ServerHandle {
        addr: local,
        cancel,
    })
}

/// The client's intent, parsed from its first request.
struct ClientRequest {
    /// `HTTPS` for a `CONNECT`, else `HTTP`.
    scheme: Scheme,
    /// Target host and port.
    host: String,
    port: u16,
    /// The raw request bytes to forward (HTTP only; empty for CONNECT).
    raw: Vec<u8>,
}

async fn handle_client(
    mut client: TcpStream,
    pool: Arc<Pool>,
    resolver: Arc<Resolver>,
    timeout: Duration,
    max_tries: usize,
) -> std::io::Result<()> {
    let Some(req) = parse_client_request(&mut client, timeout).await else {
        return Ok(());
    };
    let ip = resolver.resolve(&req.host).await.ok();
    let target = Target {
        host: req.host.clone(),
        ip,
        port: req.port,
    };

    for _ in 0..max_tries {
        let Some(mut proxy) = pool.get(req.scheme).await else {
            // No proxy available and the source is exhausted.
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
            return Ok(());
        };
        let proto = choose_proto(&proxy, req.scheme);

        match relay(&mut client, &proxy, proto, &target, &req, timeout).await {
            Ok(()) => {
                proxy.record_attempt(Some(0.0), None);
                pool.put(proxy);
                return Ok(());
            }
            Err(e) => {
                proxy.record_attempt(None, Some(e));
                pool.put(proxy);
                // try the next proxy
            }
        }
    }
    let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
    Ok(())
}

/// Which proxy protocol to use for the client's scheme. `server.py:_choice_proto`: for an
/// HTTPS (CONNECT) client, prefer a tunnelling protocol; for HTTP, a plain one.
fn choose_proto(proxy: &Proxy, scheme: Scheme) -> Proto {
    let types: Vec<Proto> = proxy.types().keys().copied().collect();
    let pick = |candidates: &[Proto]| candidates.iter().find(|c| types.contains(c)).copied();
    match scheme {
        Scheme::Https => pick(&[Proto::Https, Proto::Socks5, Proto::Socks4, Proto::Connect80])
            .unwrap_or(Proto::Https),
        Scheme::Http => pick(&[Proto::Http, Proto::Connect80, Proto::Socks5, Proto::Socks4])
            .unwrap_or(Proto::Http),
    }
}

/// Relay one client request through `proxy` using `proto`.
async fn relay(
    client: &mut TcpStream,
    proxy: &Proxy,
    proto: Proto,
    target: &Target,
    req: &ClientRequest,
    timeout: Duration,
) -> Result<(), crate::error::ProxyError> {
    use crate::error::ProxyError;

    let tcp = tokio::time::timeout(timeout, TcpStream::connect((proxy.host, proxy.port)))
        .await
        .map_err(|_| ProxyError::Timeout)?
        .map_err(|_| ProxyError::ConnFailed)?;
    let mut upstream = negotiate(proto, tcp, target, timeout).await?;

    match req.scheme {
        Scheme::Https => {
            // The client sent CONNECT; the tunnel is up, so acknowledge and splice.
            client
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .map_err(|_| ProxyError::Reset)?;
        }
        Scheme::Http => {
            // Forward the buffered request to the upstream proxy first.
            upstream
                .write_all(&req.raw)
                .await
                .map_err(|_| ProxyError::Reset)?;
        }
    }

    let mut stream: &mut Stream = &mut upstream;
    tokio::io::copy_bidirectional(client, &mut stream)
        .await
        .map_err(|_| ProxyError::ErrorOnStream)?;
    Ok(())
}

/// Read and parse the client's first request line + Host, enough to route it.
async fn parse_client_request(client: &mut TcpStream, timeout: Duration) -> Option<ClientRequest> {
    let mut buf = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    let deadline = tokio::time::timeout(timeout, async {
        loop {
            let n = client.read(&mut byte).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.push(byte[0]);
            if buf.ends_with(b"\r\n\r\n") || buf.len() > 65536 {
                return Some(());
            }
        }
    });
    deadline.await.ok()??;

    let text = String::from_utf8_lossy(&buf);
    let mut lines = text.lines();
    let first = lines.next()?;
    let mut parts = first.split_whitespace();
    let method = parts.next()?;
    let uri = parts.next()?;

    if method.eq_ignore_ascii_case("CONNECT") {
        // `CONNECT host:port HTTP/1.1`
        let (host, port) = split_host_port(uri, 443);
        Some(ClientRequest {
            scheme: Scheme::Https,
            host,
            port,
            raw: Vec::new(),
        })
    } else {
        // Plain HTTP: target host comes from the absolute URI or the Host header.
        let host_hdr = lines
            .clone()
            .find(|l| l.to_ascii_lowercase().starts_with("host:"))
            .map(|l| l[5..].trim().to_string());
        let host_port = uri
            .strip_prefix("http://")
            .and_then(|rest| rest.split('/').next())
            .map(str::to_string)
            .or(host_hdr)?;
        let (host, port) = split_host_port(&host_port, 80);
        Some(ClientRequest {
            scheme: Scheme::Http,
            host,
            port,
            raw: buf,
        })
    }
}

/// Split `host:port`, bracketed IPv6 aware, with a default port.
fn split_host_port(s: &str, default: u16) -> (String, u16) {
    if let Some(rest) = s.strip_prefix('[') {
        // [v6]:port
        if let Some((h, p)) = rest.split_once("]:") {
            return (h.to_string(), p.parse().unwrap_or(default));
        }
        if let Some(h) = rest.strip_suffix(']') {
            return (h.to_string(), default);
        }
    }
    match s.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() => (h.to_string(), p.parse().unwrap_or(default)),
        _ => (s.to_string(), default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn proxy_with(rt: f64, scheme: Proto) -> Proxy {
        let mut p = Proxy::new("1.2.3.4".parse().unwrap(), 80, BTreeSet::new());
        p.add_type(scheme, None);
        // Give it a runtime so avg_resp_time reflects `rt`.
        p.record_attempt(Some(rt), None);
        p
    }

    #[test]
    fn best_for_picks_lowest_response_time() {
        let pool = vec![
            proxy_with(0.5, Proto::Http),
            proxy_with(0.1, Proto::Http),
            proxy_with(0.3, Proto::Http),
        ];
        let idx = best_for(&pool, Scheme::Http).unwrap();
        assert_eq!(pool[idx].avg_resp_time(), 0.1);
    }

    #[test]
    fn best_for_respects_scheme() {
        let pool = vec![proxy_with(0.1, Proto::Socks5)]; // SOCKS5 → both schemes
        assert!(best_for(&pool, Scheme::Http).is_some());
        assert!(best_for(&pool, Scheme::Https).is_some());

        let http_only = vec![proxy_with(0.1, Proto::Http)]; // HTTP → HTTP scheme only
        assert!(best_for(&http_only, Scheme::Http).is_some());
        assert!(best_for(&http_only, Scheme::Https).is_none());
    }

    #[test]
    fn tied_response_times_do_not_panic() {
        // The bug this fixes: server.py's heapq compares Proxy on tied f64 and raises
        // TypeError. total_cmp orders ties deterministically instead.
        let pool = vec![proxy_with(0.2, Proto::Http), proxy_with(0.2, Proto::Http)];
        assert!(best_for(&pool, Scheme::Http).is_some());
    }

    #[test]
    fn split_host_port_variants() {
        assert_eq!(
            split_host_port("example.com:8080", 80),
            ("example.com".into(), 8080)
        );
        assert_eq!(
            split_host_port("example.com", 80),
            ("example.com".into(), 80)
        );
        assert_eq!(
            split_host_port("[2001:db8::1]:443", 80),
            ("2001:db8::1".into(), 443)
        );
        assert_eq!(
            split_host_port("[2001:db8::1]", 80),
            ("2001:db8::1".into(), 80)
        );
    }
}
