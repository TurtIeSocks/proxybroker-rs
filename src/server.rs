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
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// How the pool picks an upstream for each client request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Strategy {
    /// Lowest `(error_rate, avg_resp_time)` — the historical behaviour.
    #[default]
    Best,
    /// Rotate through the scheme-eligible proxies in pool order.
    RoundRobin,
    /// Uniform pick among the scheme-eligible proxies.
    Random,
    /// Pin a client to one upstream while it stays in the pool; fall back to `Best` for a new
    /// client or when the pin is gone.
    Sticky,
}

/// The identity a [`Strategy::Sticky`] session is keyed on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientKey {
    /// The client's peer IP (the default).
    Ip(IpAddr),
    /// The value of the configured sticky header (`--sticky-header`), HTTP requests only.
    Header(String),
}

/// Tuning for proxy eviction and selection (`server.py:ProxyPool`).
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
    /// Admission allow-list of **uppercased** ISO country codes. `None` = admit any country.
    /// Applied when a proxy enters the pool (import or `from_proxies`), so a warm/BYO pool that
    /// never went through `find`'s country filter is screened too. A proxy with no geo is rejected
    /// when a filter is set (it cannot match a country).
    pub countries: Option<std::collections::BTreeSet<String>>,
    /// How to pick an upstream per request. Default [`Strategy::Best`].
    pub strategy: Strategy,
    /// For [`Strategy::Sticky`], key sessions on this request header instead of the client IP.
    /// HTTP only (a CONNECT tunnel has no forwardable headers); `None` = always key on client IP.
    pub sticky_header: Option<String>,
    /// Upper bound on the sticky-session map, so a long-lived server cannot grow it without limit.
    pub max_sessions: usize,
    /// How long a proxy is benched (skipped unless it is the only option) after a failure, before
    /// it is re-probed. Default 30s (`server.py` parity).
    pub fail_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            max_tries: 3,
            max_error_rate: 0.5,
            max_resp_time: 8.0,
            min_req: 5,
            countries: None,
            strategy: Strategy::Best,
            sticky_header: None,
            max_sessions: 10_000,
            fail_timeout: Duration::from_secs(30),
        }
    }
}

/// A pooled proxy plus its bench state. The `blocked_until` timestamp is server-only and lives
/// here rather than on [`Proxy`] — `Proxy` is the serialized value type shared with `find`/`check`
/// and must not carry a transient selection timestamp (keeps the JSON contract stable).
struct Pooled {
    proxy: Proxy,
    /// `Some(t)` = benched (skipped unless it is the only option) until `t`; `None` = ready.
    blocked_until: Option<tokio::time::Instant>,
}

/// A pool of checked proxies, refilled from a [`ProxyStream`] by a background importer.
pub struct Pool {
    state: Mutex<Vec<Pooled>>,
    /// [`Strategy::Sticky`] pins: client → the **address** (not index — indices churn under
    /// `swap_remove`) of the proxy it is bound to. Only written by `Sticky`.
    sessions: Mutex<HashMap<ClientKey, (IpAddr, u16)>>,
    /// [`Strategy::RoundRobin`] cursor. A monotonic counter; the pick is `cursor % eligible.len()`.
    round_robin: AtomicUsize,
    notify: Notify,
    config: PoolConfig,
    exhausted: std::sync::atomic::AtomicBool,
}

impl Pool {
    /// A pool over an already-known set of proxies (bring-your-own, or for tests). No importer;
    /// the pool is considered exhausted immediately, so `get` returns `None` once it drains.
    pub fn from_proxies(proxies: Vec<Proxy>, config: PoolConfig) -> Arc<Pool> {
        let proxies = proxies
            .into_iter()
            .filter(|p| crate::broker::country_ok(p, config.countries.as_ref()))
            .map(|proxy| Pooled {
                proxy,
                blocked_until: None,
            })
            .collect();
        Pool {
            state: Mutex::new(proxies),
            sessions: Mutex::new(HashMap::new()),
            round_robin: AtomicUsize::new(0),
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
            sessions: Mutex::new(HashMap::new()),
            round_robin: AtomicUsize::new(0),
            notify: Notify::new(),
            config,
            exhausted: std::sync::atomic::AtomicBool::new(false),
        });
        {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut stream = stream;
                while let Some(proxy) = stream.next().await {
                    // Screen imports too, so --load / a live find that skipped country filtering
                    // still honors the pool's allow-list.
                    if !crate::broker::country_ok(&proxy, pool.config.countries.as_ref()) {
                        continue;
                    }
                    pool.state.lock().unwrap().push(Pooled {
                        proxy,
                        blocked_until: None,
                    });
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

    /// Check out a proxy supporting `scheme`, chosen by the configured [`Strategy`], waiting for
    /// the importer if the pool is momentarily empty. `key` identifies the client for
    /// [`Strategy::Sticky`] (ignored by the others). Returns `None` once the source is exhausted
    /// and no suitable proxy remains. Ties are ordered with `total_cmp` so equal response times
    /// never panic (the `server.py` heapq bug).
    pub async fn get(&self, scheme: Scheme, key: &ClientKey) -> Option<Proxy> {
        loop {
            // `notified()` must be created before we inspect the pool, so a push between the
            // check and the await is not missed.
            let waker = self.notify.notified();
            if let Some(proxy) = self.try_select(scheme, key) {
                return Some(proxy);
            }
            if self.exhausted.load(Ordering::SeqCst) {
                // One last look, in case a proxy arrived just before exhaustion was set.
                return self.try_select(scheme, key);
            }
            waker.await;
        }
    }

    /// One selection attempt against the current pool contents. Resolves the sticky pin (if any),
    /// runs the strategy, removes the chosen proxy, and updates the round-robin cursor / session
    /// map. Returns `None` when no eligible proxy is present right now.
    fn try_select(&self, scheme: Scheme, key: &ClientKey) -> Option<Proxy> {
        // Resolve the sticky pin before touching the pool lock (separate short critical sections,
        // never nested, so the two mutexes cannot deadlock).
        let sticky = if self.config.strategy == Strategy::Sticky {
            self.sessions.lock().unwrap().get(key).copied()
        } else {
            None
        };
        let ctx = SelectCtx {
            scheme,
            strategy: self.config.strategy,
            sticky,
            round_robin_cursor: self.round_robin.load(Ordering::SeqCst),
            now: tokio::time::Instant::now(),
        };
        let chosen = {
            let mut pool = self.state.lock().unwrap();
            let idx = best_for(&pool, &ctx)?;
            pool.swap_remove(idx).proxy
        };
        if self.config.strategy == Strategy::RoundRobin {
            self.round_robin.fetch_add(1, Ordering::SeqCst);
        }
        if self.config.strategy == Strategy::Sticky {
            self.record_session(key.clone(), (chosen.host, chosen.port));
        }
        Some(chosen)
    }

    /// Record a sticky pin, keeping the map bounded by `max_sessions`.
    fn record_session(&self, key: ClientKey, addr: (IpAddr, u16)) {
        let mut s = self.sessions.lock().unwrap();
        if s.len() >= self.config.max_sessions && !s.contains_key(&key) {
            // ponytail: bounded map, arbitrary eviction — upgrade to an LRU only if pin-churn
            // fairness ever matters. Drops one existing pin to admit the new client.
            if let Some(k) = s.keys().next().cloned() {
                s.remove(&k);
            }
        }
        s.insert(key, addr);
    }

    /// Return a proxy that served successfully — ready for immediate reselection.
    pub fn put_ok(&self, proxy: Proxy) {
        self.put_inner(proxy, None);
    }

    /// Return a proxy that just failed — benched for `fail_timeout` (skipped unless it is the only
    /// eligible proxy) so a transient failure neither instantly re-selects it nor permanently
    /// demotes it. `server.py`'s `fail_timeout` re-entry.
    pub fn put_failed(&self, proxy: Proxy) {
        let until = tokio::time::Instant::now() + self.config.fail_timeout;
        self.put_inner(proxy, Some(until));
    }

    /// Return a proxy to the pool with the given bench state, dropping it outright if it has become
    /// too slow or too error-prone (`server.py:ProxyPool.put`). Hard eviction is unchanged by B5's
    /// benching — a *persistently* unhealthy proxy is removed, not merely benched.
    fn put_inner(&self, proxy: Proxy, blocked_until: Option<tokio::time::Instant>) {
        let unhealthy = proxy.requests() >= self.config.min_req
            && (proxy.error_rate() > self.config.max_error_rate
                || proxy.avg_resp_time() > self.config.max_resp_time);
        if unhealthy {
            tracing::debug!(addr = %proxy.addr(), "evicted from pool");
            return;
        }
        self.state.lock().unwrap().push(Pooled {
            proxy,
            blocked_until,
        });
        self.notify.notify_waiters();
    }
}

/// Selection inputs for one `get`: the strategy plus the fields it needs — the resolved sticky
/// pin and the round-robin cursor. The single seam every serving feature extends (B10 adds a
/// prefer-connect field here).
struct SelectCtx {
    scheme: Scheme,
    strategy: Strategy,
    /// The address this client is pinned to, for [`Strategy::Sticky`]; `None` = no pin yet.
    sticky: Option<(IpAddr, u16)>,
    round_robin_cursor: usize,
    /// Reference time for the bench check (B5): a proxy is ready iff `blocked_until` is `None`
    /// or `<= now`.
    now: tokio::time::Instant,
}

/// Index of the proxy to serve, per `ctx.strategy`, among the scheme-eligible proxies. Two tiers
/// (B5): rank the **ready** proxies (never benched, or bench window elapsed) by the strategy; only
/// if none are ready fall back to the **benched** ones as a backup (better than a 502). The single
/// isolated selection point every serving feature extends. `None` when nothing is eligible.
fn best_for(pool: &[Pooled], ctx: &SelectCtx) -> Option<usize> {
    let tier_of = |ready: bool| -> Vec<usize> {
        pool.iter()
            .enumerate()
            .filter(|(_, p)| p.proxy.schemes().contains(&ctx.scheme))
            .filter(|(_, p)| p.blocked_until.is_none_or(|t| t <= ctx.now) == ready)
            .map(|(i, _)| i)
            .collect()
    };
    let ready = tier_of(true);
    let tier = if ready.is_empty() {
        tier_of(false)
    } else {
        ready
    };
    if tier.is_empty() {
        return None;
    }
    match ctx.strategy {
        Strategy::Best => best_by_priority(pool, &tier),
        Strategy::RoundRobin => Some(tier[ctx.round_robin_cursor % tier.len()]),
        Strategy::Random => Some(tier[next_rand(tier.len())]),
        Strategy::Sticky => {
            // Reuse the pinned proxy if it is still in this tier; otherwise fall back to Best (a
            // fresh client, or the pin was evicted — self-healing, the caller re-pins).
            if let Some(addr) = ctx.sticky {
                if let Some(&i) = tier
                    .iter()
                    .find(|&&i| (pool[i].proxy.host, pool[i].proxy.port) == addr)
                {
                    return Some(i);
                }
            }
            best_by_priority(pool, &tier)
        }
    }
}

/// Lowest `(error_rate, avg_resp_time)` among `eligible`, `total_cmp`-ordered so tied `f64`s
/// never panic (the `server.py` heapq bug). This is `Strategy::Best`.
fn best_by_priority(pool: &[Pooled], eligible: &[usize]) -> Option<usize> {
    eligible.iter().copied().min_by(|&a, &b| {
        let (ae, at) = pool[a].proxy.priority();
        let (be, bt) = pool[b].proxy.priority();
        ae.total_cmp(&be).then(at.total_cmp(&bt))
    })
}

/// A tiny xorshift over a process-global state, for [`Strategy::Random`]. Spreads load across
/// eligible proxies without pulling `rand` for one call.
/// ponytail: a load spreader, not a cryptographic RNG — deterministic seed is fine (and gives
/// reproducible tests); upgrade to a real RNG only if unpredictability is ever required.
fn next_rand(n: usize) -> usize {
    use std::sync::atomic::AtomicU64;
    static STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    // Relaxed: concurrent stepping only lowers spread quality, never correctness.
    let mut x = STATE.load(Ordering::Relaxed);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    STATE.store(x, Ordering::Relaxed);
    (x as usize) % n.max(1)
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
            let (client, peer) = tokio::select! {
                _ = accept_cancel.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((s, peer)) => (s, peer),
                    Err(_) => continue,
                },
            };
            let pool = pool.clone();
            let resolver = resolver.clone();
            tokio::spawn(async move {
                let _ = handle_client(client, peer.ip(), pool, resolver, timeout, max_tries).await;
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
    peer_ip: IpAddr,
    pool: Arc<Pool>,
    resolver: Arc<Resolver>,
    timeout: Duration,
    max_tries: usize,
) -> std::io::Result<()> {
    let Some(req) = parse_client_request(&mut client, timeout).await else {
        return Ok(());
    };
    let key = client_key(&pool.config, peer_ip, &req);
    let ip = resolver.resolve(&req.host).await.ok();
    let target = Target {
        host: req.host.clone(),
        ip,
        port: req.port,
    };

    for _ in 0..max_tries {
        let Some(mut proxy) = pool.get(req.scheme, &key).await else {
            // No proxy available and the source is exhausted.
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
            return Ok(());
        };
        let proto = choose_proto(&proxy, req.scheme);

        match relay(&mut client, &proxy, proto, &target, &req, timeout).await {
            RelayOutcome::Ok => {
                proxy.record_attempt(Some(0.0), None);
                pool.put_ok(proxy);
                return Ok(());
            }
            RelayOutcome::RetryableFailure(e) => {
                proxy.record_attempt(None, Some(e));
                pool.put_failed(proxy); // bench it, then try the next proxy
            }
            RelayOutcome::ClientCommitted(e) => {
                // The client already saw an ack or bytes — a retry would corrupt it. Record the
                // failure, bench the proxy, and stop.
                proxy.record_attempt(None, Some(e));
                pool.put_failed(proxy);
                return Ok(());
            }
        }
    }
    let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
    Ok(())
}

/// The sticky session key for this request: the configured header's value (HTTP only, when
/// `--sticky-header` is set and the header is present), else the client's peer IP. A CONNECT
/// tunnel has no forwardable headers, so it always keys on IP (B1 open question, resolved).
fn client_key(config: &PoolConfig, peer_ip: IpAddr, req: &ClientRequest) -> ClientKey {
    if req.scheme == Scheme::Http {
        if let Some(name) = &config.sticky_header {
            if let Some(v) = header_value(&req.raw, name) {
                return ClientKey::Header(v);
            }
        }
    }
    ClientKey::Ip(peer_ip)
}

/// Value of the first `name:` header (case-insensitive) in a buffered HTTP request, trimmed.
fn header_value(raw: &[u8], name: &str) -> Option<String> {
    let text = String::from_utf8_lossy(raw);
    let prefix = format!("{}:", name.to_ascii_lowercase());
    text.lines()
        .skip(1) // the request line, not a header
        .find(|l| l.to_ascii_lowercase().starts_with(&prefix))
        .map(|l| l[name.len() + 1..].trim().to_string())
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

/// Where a relay attempt ended — the discriminant B2's retry loop needs. Classification is
/// **positional** (decided by how far the relay got), so it is exhaustive by construction: a new
/// [`ProxyError`] variant cannot accidentally become retryable-past-a-commit.
enum RelayOutcome {
    /// The request completed successfully.
    Ok,
    /// Failed before any byte reached the client — safe to try the next proxy.
    RetryableFailure(crate::error::ProxyError),
    /// The client already received an ack or spliced bytes — retrying would corrupt it, so abort.
    ClientCommitted(crate::error::ProxyError),
}

/// Relay one client request through `proxy` using `proto`, reporting where it ended so the caller
/// only retries a failure the client has not yet seen (B2's commit boundary).
async fn relay(
    client: &mut TcpStream,
    proxy: &Proxy,
    proto: Proto,
    target: &Target,
    req: &ClientRequest,
    timeout: Duration,
) -> RelayOutcome {
    use crate::error::ProxyError;
    use RelayOutcome::{ClientCommitted, RetryableFailure};

    // Connect + negotiate: nothing has reached the client, so every failure here is retryable.
    let tcp =
        match tokio::time::timeout(timeout, TcpStream::connect((proxy.host, proxy.port))).await {
            Err(_) => return RetryableFailure(ProxyError::Timeout),
            Ok(Err(_)) => return RetryableFailure(ProxyError::ConnFailed),
            Ok(Ok(t)) => t,
        };
    let mut upstream = match negotiate(proto, tcp, target, timeout).await {
        Err(e) => return RetryableFailure(e),
        Ok(u) => u,
    };

    match req.scheme {
        Scheme::Https => {
            // Acknowledging the CONNECT tunnel is the commit point: after this the client believes
            // it is talking to the target, so a later failure must not re-ack through another proxy.
            if client
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .is_err()
            {
                // The client write failed → the client is already gone; not worth another proxy.
                return ClientCommitted(ProxyError::Reset);
            }
        }
        Scheme::Http => {
            // The buffered request goes upstream first — the client has still received nothing, so
            // a write failure here is retryable.
            if upstream.write_all(&req.raw).await.is_err() {
                return RetryableFailure(ProxyError::Reset);
            }
        }
    }

    // Splicing has begun. For HTTPS the ack is out; for HTTP the response now flows to the client.
    // Either way a failure may already have been seen by the client, so it is not retryable.
    let mut stream: &mut Stream = &mut upstream;
    match tokio::io::copy_bidirectional(client, &mut stream).await {
        Ok(_) => RelayOutcome::Ok,
        Err(_) => ClientCommitted(ProxyError::ErrorOnStream),
    }
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

    fn proxy_with(rt: f64, scheme: Proto) -> Pooled {
        proxy_at(rt, scheme, "1.2.3.4")
    }

    /// A ready (never-benched) pooled proxy at `ip` with response time `rt`. Distinct addr per
    /// call so sticky/round-robin picks are distinguishable.
    fn proxy_at(rt: f64, scheme: Proto, ip: &str) -> Pooled {
        let mut p = Proxy::new(ip.parse().unwrap(), 80, BTreeSet::new());
        p.add_type(scheme, None);
        // Give it a runtime so avg_resp_time reflects `rt`.
        p.record_attempt(Some(rt), None);
        Pooled {
            proxy: p,
            blocked_until: None,
        }
    }

    /// A default `Best` selection context for `scheme`.
    fn ctx(scheme: Scheme) -> SelectCtx {
        SelectCtx {
            scheme,
            strategy: Strategy::Best,
            sticky: None,
            round_robin_cursor: 0,
            now: tokio::time::Instant::now(),
        }
    }

    #[test]
    fn best_for_picks_lowest_response_time() {
        let pool = vec![
            proxy_with(0.5, Proto::Http),
            proxy_with(0.1, Proto::Http),
            proxy_with(0.3, Proto::Http),
        ];
        let idx = best_for(&pool, &ctx(Scheme::Http)).unwrap();
        assert_eq!(pool[idx].proxy.avg_resp_time(), 0.1);
    }

    #[test]
    fn best_for_prefers_ready_over_benched() {
        // Two-tier ranking: a ready but slower proxy beats a faster but benched one; when only
        // the benched proxy is eligible, it is served as the backup tier.
        let now = tokio::time::Instant::now();
        let mut benched = proxy_at(0.1, Proto::Http, "2.2.2.2"); // faster
        benched.blocked_until = Some(now + Duration::from_secs(30));
        let pool = vec![proxy_at(0.9, Proto::Http, "1.1.1.1"), benched]; // index 0 ready, 1 benched
        let mut c = ctx(Scheme::Http);
        c.now = now;
        assert_eq!(
            best_for(&pool, &c),
            Some(0),
            "ready proxy beats a faster benched one"
        );

        // Only a benched proxy present → backup tier still serves it.
        let mut lone = proxy_at(0.1, Proto::Http, "3.3.3.3");
        lone.blocked_until = Some(now + Duration::from_secs(30));
        assert_eq!(
            best_for(&[lone], &c),
            Some(0),
            "benched proxy is the backup tier"
        );
    }

    #[test]
    fn best_for_respects_scheme() {
        let pool = vec![proxy_with(0.1, Proto::Socks5)]; // SOCKS5 → both schemes
        assert!(best_for(&pool, &ctx(Scheme::Http)).is_some());
        assert!(best_for(&pool, &ctx(Scheme::Https)).is_some());

        let http_only = vec![proxy_with(0.1, Proto::Http)]; // HTTP → HTTP scheme only
        assert!(best_for(&http_only, &ctx(Scheme::Http)).is_some());
        assert!(best_for(&http_only, &ctx(Scheme::Https)).is_none());
    }

    #[test]
    fn tied_response_times_do_not_panic() {
        // The bug this fixes: server.py's heapq compares Proxy on tied f64 and raises
        // TypeError. total_cmp orders ties deterministically instead.
        let pool = vec![proxy_with(0.2, Proto::Http), proxy_with(0.2, Proto::Http)];
        assert!(best_for(&pool, &ctx(Scheme::Http)).is_some());
    }

    #[test]
    fn round_robin_cycles_through_pool() {
        // Three eligible proxies; advancing the cursor visits 0,1,2,0.
        let pool = vec![
            proxy_at(0.1, Proto::Http, "1.1.1.1"),
            proxy_at(0.1, Proto::Http, "2.2.2.2"),
            proxy_at(0.1, Proto::Http, "3.3.3.3"),
        ];
        let pick = |cursor| {
            let mut c = ctx(Scheme::Http);
            c.strategy = Strategy::RoundRobin;
            c.round_robin_cursor = cursor;
            best_for(&pool, &c).unwrap()
        };
        assert_eq!([pick(0), pick(1), pick(2), pick(3)], [0, 1, 2, 0]);
    }

    #[test]
    fn random_stays_in_eligible_set() {
        // Only index 1 is HTTP-eligible; 100 Random draws must all land on it.
        let pool = vec![
            proxy_at(0.1, Proto::Https, "1.1.1.1"), // HTTPS only
            proxy_at(0.1, Proto::Http, "2.2.2.2"),  // HTTP eligible
        ];
        let mut c = ctx(Scheme::Http);
        c.strategy = Strategy::Random;
        for _ in 0..100 {
            assert_eq!(best_for(&pool, &c), Some(1));
        }
    }

    #[test]
    fn sticky_reuses_pin_when_present_else_best() {
        let pool = vec![
            proxy_at(0.1, Proto::Http, "1.1.1.1"), // the Best pick (lowest rt)
            proxy_at(0.9, Proto::Http, "2.2.2.2"),
        ];
        let mut c = ctx(Scheme::Http);
        c.strategy = Strategy::Sticky;
        // Pinned to 2.2.2.2 → reuse it even though it is slower.
        c.sticky = Some(("2.2.2.2".parse().unwrap(), 80));
        assert_eq!(best_for(&pool, &c), Some(1));
        // Pin points at an addr not in the pool → fall back to Best (fastest).
        c.sticky = Some(("9.9.9.9".parse().unwrap(), 80));
        assert_eq!(best_for(&pool, &c), Some(0));
        // No pin → Best.
        c.sticky = None;
        assert_eq!(best_for(&pool, &c), Some(0));
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
