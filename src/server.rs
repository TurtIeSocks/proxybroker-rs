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

use crate::negotiator::{negotiate, Stream, Target};
use crate::proxy::Proxy;
use crate::resolver::Resolver;
use crate::types::{Proto, Scheme};
use futures_util::StreamExt;
use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
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
    /// B10: prefer `CONNECT:80`-capable proxies. A tie-break for `Best`/`Sticky` (health still
    /// dominates), a primary filter for `RoundRobin`/`Random`. Default `false`.
    pub prefer_connect: bool,
    /// B11: for HTTP requests, retry through another proxy when the upstream status is outside
    /// this set (e.g. dodge a `403`/`503` block page). `None` = accept any status (no peek, blind
    /// splice — today's behaviour). CONNECT tunnels are opaque and never status-gated.
    pub http_allowed_codes: Option<Vec<u16>>,
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
            prefer_connect: false,
            http_allowed_codes: None,
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
    /// Cumulative count of proxies hard-evicted from the pool (F1 metrics).
    evictions: AtomicU64,
    /// Cumulative count of mid-request rotations to a different proxy (F1 metrics).
    rotations: AtomicU64,
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
            evictions: AtomicU64::new(0),
            rotations: AtomicU64::new(0),
        }
        .into()
    }

    /// Create a pool and spawn the importer that drains `stream` into it. The importer is the
    /// single owner of the receiver, so waiters never serialize behind a mutex over it. Generic
    /// over the source stream so a [`ProxyStream`] (from `find`), or any other `Stream<Item =
    /// Proxy>` (a BYO feed, or a test's channel), can fill the pool.
    pub fn spawn<S>(stream: S, config: PoolConfig) -> Arc<Pool>
    where
        S: futures_util::Stream<Item = Proxy> + Send + 'static,
    {
        let pool = Arc::new(Pool {
            state: Mutex::new(Vec::new()),
            sessions: Mutex::new(HashMap::new()),
            round_robin: AtomicUsize::new(0),
            notify: Notify::new(),
            config,
            exhausted: std::sync::atomic::AtomicBool::new(false),
            evictions: AtomicU64::new(0),
            rotations: AtomicU64::new(0),
        });
        {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut stream = std::pin::pin!(stream);
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
            prefer_connect: self.config.prefer_connect,
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
            self.evictions.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(addr = %proxy.addr(), "evicted from pool");
            return;
        }
        self.state.lock().unwrap().push(Pooled {
            proxy,
            blocked_until,
        });
        self.notify.notify_waiters();
    }

    /// How many proxies are currently pooled.
    pub fn len(&self) -> usize {
        self.state.lock().unwrap().len()
    }

    /// True when the pool holds no proxies.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every proxy matching `(host, port)` from the pool (the `proxycontrol` remove API, B6).
    /// Returns whether any were removed.
    pub fn remove(&self, host: IpAddr, port: u16) -> bool {
        let mut state = self.state.lock().unwrap();
        let before = state.len();
        state.retain(|p| !(p.proxy.host == host && p.proxy.port == port));
        before != state.len()
    }

    /// Add a checked proxy to a running pool, deduped on `(host, port)` (no-op if already present).
    /// Used by the D3 re-check loop and the E3 file watcher to mutate a live pool.
    pub fn add(&self, proxy: Proxy) {
        let mut state = self.state.lock().unwrap();
        if state
            .iter()
            .any(|p| p.proxy.host == proxy.host && p.proxy.port == proxy.port)
        {
            return;
        }
        state.push(Pooled {
            proxy,
            blocked_until: None,
        });
        self.notify.notify_waiters();
    }

    /// Remove any pooled proxy at this address; returns whether one was removed. (Alias of
    /// [`Pool::remove`] under the D3/E3 vocabulary.)
    pub fn remove_addr(&self, host: IpAddr, port: u16) -> bool {
        self.remove(host, port)
    }

    /// Snapshot the current `(host, port)` set, for re-check scheduling (D3) and reconciliation (E3).
    pub fn addrs(&self) -> BTreeSet<(IpAddr, u16)> {
        self.state
            .lock()
            .unwrap()
            .iter()
            .map(|p| (p.proxy.host, p.proxy.port))
            .collect()
    }

    /// A non-consuming clone of every pooled proxy — for the E4 MCP `pool_status` snapshot (feed to
    /// [`crate::Stats::from_proxies`]) and status/selection previews. Does not check anything out.
    pub fn proxies(&self) -> Vec<Proxy> {
        self.state
            .lock()
            .unwrap()
            .iter()
            .map(|p| p.proxy.clone())
            .collect()
    }

    /// Check out the best proxy for `scheme` (optionally filtered to an ISO country code,
    /// case-insensitive), by priority — like [`Pool::get`] but non-blocking: returns `None` at once
    /// instead of waiting on the importer. The E4 MCP `get_proxy` tool's selection seam.
    pub fn try_get(&self, scheme: Scheme, country: Option<&str>) -> Option<Proxy> {
        let mut state = self.state.lock().unwrap();
        let eligible: Vec<usize> = state
            .iter()
            .enumerate()
            .filter(|(_, p)| p.proxy.schemes().contains(&scheme))
            .filter(|(_, p)| match country {
                Some(cc) => p
                    .proxy
                    .geo
                    .as_ref()
                    .is_some_and(|g| g.code.eq_ignore_ascii_case(cc)),
                None => true,
            })
            .map(|(i, _)| i)
            .collect();
        let idx = best_by_priority(&state, &eligible, self.config.prefer_connect)?;
        Some(state.remove(idx).proxy)
    }

    /// Wait until at least `n` proxies are pooled, or the source is exhausted — so a too-small
    /// source can never hang startup forever (B13's `--min-queue`). Reuses the importer's
    /// [`Notify`] exactly as `get` does. Returns immediately when `n == 0`.
    pub async fn wait_ready(&self, n: usize) {
        if n == 0 {
            return;
        }
        loop {
            // Create the waker before checking, so a push between the check and the await is not
            // missed.
            let waker = self.notify.notified();
            if self.len() >= n || self.exhausted.load(Ordering::SeqCst) {
                return;
            }
            waker.await;
        }
    }

    /// A cheap live view of the pool for metrics (F1) — counts by scheme and mean health, taken
    /// under a single `state` lock. Shared with the (deferred) F4 dashboard.
    pub fn snapshot(&self) -> PoolSnapshot {
        let state = self.state.lock().unwrap();
        let total = state.len();
        let mut snap = PoolSnapshot::default();
        let (mut err_sum, mut rt_sum, mut probe_sum) = (0.0_f64, 0.0_f64, 0.0_f64);
        for p in state.iter() {
            let schemes = p.proxy.schemes();
            if schemes.contains(&Scheme::Http) {
                snap.http += 1;
            }
            if schemes.contains(&Scheme::Https) {
                snap.https += 1;
            }
            err_sum += p.proxy.error_rate();
            rt_sum += p.proxy.avg_resp_time();
            probe_sum += p.proxy.probe_latency();
        }
        snap.total = total;
        if total > 0 {
            snap.avg_error_rate = err_sum / total as f64;
            snap.avg_resp_time = rt_sum / total as f64;
            snap.probe_latency_avg = probe_sum / total as f64;
        }
        snap
    }

    /// Cumulative proxies hard-evicted from the pool (F1 metrics).
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Cumulative mid-request rotations to a different proxy (F1 metrics).
    pub fn rotations(&self) -> u64 {
        self.rotations.load(Ordering::Relaxed)
    }
}

/// A cheap, allocation-light snapshot of the pool's live state (F1). One producer, shared with the
/// (deferred) F4 dashboard.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PoolSnapshot {
    /// Proxies serving `Scheme::Http`.
    pub http: usize,
    /// Proxies serving `Scheme::Https`.
    pub https: usize,
    pub total: usize,
    /// Mean of `Proxy::error_rate()` over the pool.
    pub avg_error_rate: f64,
    /// Mean of `Proxy::avg_resp_time()` over the pool, seconds. Blends check-time probe RTT and
    /// serve-time relayed-request RTT (both feed `Proxy::record_attempt`).
    pub avg_resp_time: f64,
    /// Mean of `Proxy::probe_latency()` over the pool, seconds (F1) — the check-time judge-probe
    /// RTT only, kept distinct from the serve-blended `avg_resp_time`.
    pub probe_latency_avg: f64,
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
    /// B10: bias selection toward `CONNECT:80`-capable proxies — a tie-break for `Best`/`Sticky`
    /// (health dominates), a primary filter for `RoundRobin`/`Random`.
    prefer_connect: bool,
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
        Strategy::Best => best_by_priority(pool, &tier, ctx.prefer_connect),
        // Rotation/random treat prefer-connect as a primary filter: rotate only among CONNECT-
        // capable proxies if any exist, else fall back to the whole tier.
        Strategy::RoundRobin => {
            let pick = connect_biased(pool, &tier, ctx.prefer_connect);
            Some(pick[ctx.round_robin_cursor % pick.len()])
        }
        Strategy::Random => {
            let pick = connect_biased(pool, &tier, ctx.prefer_connect);
            Some(pick[next_rand(pick.len())])
        }
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
            best_by_priority(pool, &tier, ctx.prefer_connect)
        }
    }
}

/// Does the proxy at `i` expose `CONNECT:80`?
fn has_connect(pool: &[Pooled], i: usize) -> bool {
    pool[i].proxy.types().contains_key(&Proto::Connect80)
}

/// Narrow `tier` to only `CONNECT:80`-capable proxies when `prefer` is set and any exist; else
/// return `tier` unchanged (the bias is a preference, not a hard requirement). Used by the
/// rotate/random strategies, where prefer-connect is a primary filter (B10).
fn connect_biased(pool: &[Pooled], tier: &[usize], prefer: bool) -> Vec<usize> {
    if !prefer {
        return tier.to_vec();
    }
    let connect: Vec<usize> = tier
        .iter()
        .copied()
        .filter(|&i| has_connect(pool, i))
        .collect();
    if connect.is_empty() {
        tier.to_vec()
    } else {
        connect
    }
}

/// Lowest `(error_rate, avg_resp_time)` among `eligible`, `total_cmp`-ordered so tied `f64`s
/// never panic (the `server.py` heapq bug). This is `Strategy::Best`. When `prefer_connect` is
/// set, `CONNECT:80` support breaks ties **after** health (health dominates, per B10) — so a
/// faster non-CONNECT proxy still wins.
fn best_by_priority(pool: &[Pooled], eligible: &[usize], prefer_connect: bool) -> Option<usize> {
    eligible.iter().copied().min_by(|&a, &b| {
        let (ae, at) = pool[a].proxy.priority();
        let (be, bt) = pool[b].proxy.priority();
        // 0 sorts before 1: a CONNECT-capable proxy wins only when health is otherwise tied.
        let ac = u8::from(prefer_connect && !has_connect(pool, a));
        let bc = u8::from(prefer_connect && !has_connect(pool, b));
        ae.total_cmp(&be).then(at.total_cmp(&bt)).then(ac.cmp(&bc))
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

/// Start the local proxy server on `addr`, relaying through `pool`. Binds immediately and returns
/// the handle (so `local_addr` works at once); the accept loop runs in a background task and, when
/// `min_queue > 0`, does not start serving until the pool holds that many proxies (B13). `backlog`
/// is the TCP listen backlog. `auth` (`user:pass`) gates clients: when `Some`, a client without a
/// matching `Proxy-Authorization: Basic <b64>` gets `407` (B9).
pub async fn serve(
    addr: SocketAddr,
    pool: Arc<Pool>,
    resolver: Arc<Resolver>,
    timeout: Duration,
    min_queue: usize,
    backlog: u32,
    auth: Option<String>,
) -> std::io::Result<ServerHandle> {
    // TcpListener::bind does not expose the backlog; go through TcpSocket to set it. Pick the
    // socket family from the bind address.
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(backlog)?;
    let local = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let max_tries = pool.config.max_tries;
    // Encode the expected header once at startup, not per request. `Arc` so each connection shares
    // one copy.
    let expected: Option<Arc<AuthExpect>> = auth.map(|up| {
        let (user, pass) = up.split_once(':').unwrap_or((up.as_str(), ""));
        Arc::new(AuthExpect {
            user: user.to_string(),
            pass: pass.to_string(),
            basic: format!("Basic {}", crate::utils::base64_encode(up.as_bytes())),
        })
    });
    // Per-connection context, cloned cheaply (all Arc/Copy) for each accepted client.
    let ctx = ConnCtx {
        pool,
        resolver,
        timeout,
        max_tries,
        expected_auth: expected,
        history: Arc::new(History::new()), // the proxycontrol map, shared across connections (B6)
    };

    let accept_cancel = cancel.clone();
    tokio::spawn(async move {
        // Startup gate: wait for the pool to fill to min_queue before accepting (clients queue in
        // the backlog meanwhile). wait_ready also returns on source exhaustion, so a too-small
        // source cannot hang startup forever.
        tokio::select! {
            _ = accept_cancel.cancelled() => return,
            _ = ctx.pool.wait_ready(min_queue) => {}
        }
        loop {
            let (client, peer) = tokio::select! {
                _ = accept_cancel.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((s, peer)) => (s, peer),
                    Err(_) => continue,
                },
            };
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let _ = handle_client(client, peer.ip(), ctx).await;
            });
        }
    });

    Ok(ServerHandle {
        addr: local,
        cancel,
    })
}

/// Render the pool's live state in Prometheus text exposition format (0.0.4) (F1). Hand-rolled —
/// the surface is tiny and stable; adding the `prometheus` crate would bloat the build for no gain.
/// Floats use `{:.2}` to match `Proxy`'s 2-dp rounding convention. Error rate is an **aggregate**
/// gauge, not per-address (per-proxy labels are unbounded cardinality for a rotating pool — see
/// `decisions.md`); per-proxy detail lives behind the control API.
#[cfg(feature = "metrics")]
pub fn render_metrics(pool: &Pool) -> String {
    let s = pool.snapshot();
    format!(
        "# HELP proxybroker_pool_size Proxies currently available in the pool.\n\
         # TYPE proxybroker_pool_size gauge\n\
         proxybroker_pool_size{{scheme=\"http\"}} {http}\n\
         proxybroker_pool_size{{scheme=\"https\"}} {https}\n\
         # HELP proxybroker_pool_error_rate_avg Mean proxy error rate over the pool.\n\
         # TYPE proxybroker_pool_error_rate_avg gauge\n\
         proxybroker_pool_error_rate_avg {err:.2}\n\
         # HELP proxybroker_pool_resp_time_avg_seconds Mean proxy response time over the pool.\n\
         # TYPE proxybroker_pool_resp_time_avg_seconds gauge\n\
         proxybroker_pool_resp_time_avg_seconds {rt:.2}\n\
         # HELP proxybroker_pool_probe_latency_avg_seconds Mean judge-probe latency (check-time) over the pool.\n\
         # TYPE proxybroker_pool_probe_latency_avg_seconds gauge\n\
         proxybroker_pool_probe_latency_avg_seconds {probe:.2}\n\
         # HELP proxybroker_evictions_total Proxies hard-evicted from the pool.\n\
         # TYPE proxybroker_evictions_total counter\n\
         proxybroker_evictions_total {evict}\n\
         # HELP proxybroker_rotations_total Mid-request rotations to a different proxy.\n\
         # TYPE proxybroker_rotations_total counter\n\
         proxybroker_rotations_total {rot}\n",
        http = s.http,
        https = s.https,
        err = s.avg_error_rate,
        rt = s.avg_resp_time,
        probe = s.probe_latency_avg,
        evict = pool.evictions(),
        rot = pool.rotations(),
    )
}

/// Serve [`render_metrics`] over a tiny HTTP `GET` responder on `addr` (F1). Any request returns the
/// current metrics. Mirrors `serve`'s raw-tokio accept loop + `CancellationToken` shutdown; no hyper.
#[cfg(feature = "metrics")]
pub async fn serve_metrics(addr: SocketAddr, pool: Arc<Pool>) -> std::io::Result<ServerHandle> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let accept_cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            let mut client = tokio::select! {
                _ = accept_cancel.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((s, _)) => s,
                    Err(_) => continue,
                },
            };
            let pool = pool.clone();
            tokio::spawn(async move {
                // Drain the request (any GET returns metrics); ignore the request line.
                let mut buf = [0u8; 1024];
                let _ = client.read(&mut buf).await;
                let body = render_metrics(&pool);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = client.write_all(resp.as_bytes()).await;
                let _ = client.flush().await;
            });
        }
    });
    Ok(ServerHandle {
        addr: local,
        cancel,
    })
}

/// How the client spoke to us — drives the *ack* the relay sends back, independently of the
/// target [`Scheme`] (which still drives pool selection + `choose_proto`). R0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Frontend {
    /// Plain HTTP forward proxy (absolute-URI request): forward the buffered request upstream.
    HttpForward,
    /// HTTP `CONNECT`: acknowledge with `200 Connection established`, then tunnel.
    HttpConnect,
    /// SOCKS5 front-end (B12): acknowledge with a SOCKS5 success frame, then tunnel. Auth (RFC
    /// 1929) is handled during parsing, not by the HTTP `407` gate.
    Socks5,
}

/// The client's intent, parsed from its first request.
struct ClientRequest {
    /// `HTTPS` for a `CONNECT`/SOCKS5 tunnel, else `HTTP`. Drives pool selection + `choose_proto`.
    scheme: Scheme,
    /// How the client addressed us — drives the relay's client-ack. R0.
    frontend: Frontend,
    /// Target host and port.
    host: String,
    port: u16,
    /// The raw request bytes to forward (HTTP forward only; empty for CONNECT/SOCKS5).
    raw: Vec<u8>,
    /// The client's `Proxy-Authorization` header value, if present (B9 client auth).
    proxy_auth: Option<String>,
    /// The request-target token (B6): the history key uses it, and `proxycontrol` parses it.
    path: String,
}

async fn handle_client(
    mut client: TcpStream,
    peer_ip: IpAddr,
    ctx: ConnCtx,
) -> std::io::Result<()> {
    let ConnCtx {
        pool,
        resolver,
        timeout,
        max_tries,
        expected_auth,
        history,
    } = ctx;
    let Some(req) = parse_client_request(&mut client, timeout, expected_auth.as_deref()).await
    else {
        return Ok(());
    };
    // B9: gate HTTP clients on credentials before consuming any pool proxy. (A SOCKS5 client was
    // already authed via RFC 1929 during parsing, so it is not re-gated here — an HTTP 407 to a
    // SOCKS5 client would be a protocol error.) A plain `==` is fine — the secret is a shared
    // static string. Auth is checked BEFORE the control API (a deliberate deviation from Python),
    // so introspection cannot reveal pool membership on an authenticated server.
    if req.frontend != Frontend::Socks5 {
        if let Some(expected) = &expected_auth {
            if req.proxy_auth.as_deref() != Some(expected.basic.as_str()) {
                let _ = client
                    .write_all(
                        b"HTTP/1.1 407 Proxy Authentication Required\r\n\
                          Proxy-Authenticate: Basic realm=\"proxybroker\"\r\n\r\n",
                    )
                    .await;
                return Ok(());
            }
        }
    }
    // B6: `proxycontrol` requests are handled locally and never consume a pool proxy.
    if req.host == "proxycontrol" {
        return serve_control(&mut client, &req, &pool, &history, peer_ip).await;
    }
    let key = client_key(&pool.config, peer_ip, &req);
    let ip = resolver.resolve(&req.host).await.ok();
    let target = Target {
        host: req.host.clone(),
        ip,
        port: req.port,
    };

    for attempt in 0..max_tries {
        let Some(mut proxy) = pool.get(req.scheme, &key).await else {
            // No proxy available and the source is exhausted — tell the client in its own protocol.
            let _ = client.write_all(terminal_failure(req.frontend)).await;
            return Ok(());
        };
        // F1: reaching attempt > 0 means the previous attempt failed and benched its proxy, and
        // this `get` acquired a *different* one — that is the rotation. Counting at the failure
        // instead would over-count the final failure before a 502 (no actual switch follows it).
        if attempt > 0 {
            pool.rotations.fetch_add(1, Ordering::Relaxed);
        }
        let proto = choose_proto(&proxy, req.scheme);

        match relay(
            &mut client,
            &proxy,
            proto,
            &target,
            &req,
            timeout,
            pool.config.http_allowed_codes.as_deref(),
        )
        .await
        {
            RelayOutcome::Ok => {
                proxy.record_attempt(Some(0.0), None);
                // B6: record which upstream served this request-target for this client. The addr is
                // v6-unbracketed to match Python's control-API format.
                history.record(
                    format!("{peer_ip}-{}", req.path),
                    format!("{}:{}", proxy.host, proxy.port),
                );
                pool.put_ok(proxy);
                return Ok(());
            }
            RelayOutcome::RetryableFailure(e) => {
                proxy.record_attempt(None, Some(e));
                pool.put_failed(proxy); // bench it, then try the next proxy (counted on next get)
            }
            RelayOutcome::ClientCommitted(e) => {
                // The client already saw an ack or bytes — a retry would corrupt it. Record the
                // failure, bench the proxy, and stop.
                proxy.record_attempt(None, Some(e));
                pool.put_failed(proxy);
                return Ok(());
            }
            RelayOutcome::ClientGone => {
                // The client vanished at the commit point; the proxy was fine. Return it healthy
                // (no error charged, no bench) and stop — a retry cannot reach the client.
                proxy.record_attempt(Some(0.0), None);
                pool.put_ok(proxy);
                return Ok(());
            }
        }
    }
    let _ = client.write_all(terminal_failure(req.frontend)).await;
    Ok(())
}

/// The client-visible "could not serve" reply, in the client's own protocol (B12): an HTTP `502`
/// for HTTP/CONNECT front-ends, or a SOCKS5 general-failure reply for a SOCKS5 client (an HTTP body
/// would be garbage to a SOCKS5 client mid-handshake).
fn terminal_failure(frontend: Frontend) -> &'static [u8] {
    match frontend {
        Frontend::Socks5 => &[0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0], // REP=01 general failure
        _ => b"HTTP/1.1 502 Bad Gateway\r\n\r\n",
    }
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

/// Per-connection context handed to `handle_client` — bundles the shared server state (all cheap
/// to clone: `Arc`s + `Copy`) so the handler keeps a small, stable argument list.
#[derive(Clone)]
struct ConnCtx {
    pool: Arc<Pool>,
    resolver: Arc<Resolver>,
    timeout: Duration,
    max_tries: usize,
    expected_auth: Option<Arc<AuthExpect>>,
    history: Arc<History>,
}

/// The server's expected client credentials (B9/B12), precomputed once at startup: the raw
/// user/pass for the SOCKS5 RFC 1929 comparison, and the HTTP `Basic <b64>` header form.
struct AuthExpect {
    user: String,
    pass: String,
    basic: String,
}

/// Per-client record of which upstream last served a request-target — the `proxycontrol` history
/// API (B6). Bounded by a hard cap and cleared wholesale when exceeded (no time-based TTL — an
/// "ephemeral by design" deviation from Python's `TTLCache`; see `decisions.md`).
struct History {
    map: Mutex<HashMap<String, String>>,
}

impl History {
    const CAP: usize = 10_000;

    fn new() -> Self {
        History {
            map: Mutex::new(HashMap::new()),
        }
    }

    fn record(&self, key: String, proxy: String) {
        let mut m = self.map.lock().unwrap();
        if m.len() >= Self::CAP && !m.contains_key(&key) {
            m.clear(); // hard cap, drop-all — ephemeral by design
        }
        m.insert(key, proxy);
    }

    fn get(&self, key: &str) -> Option<String> {
        self.map.lock().unwrap().get(key).cloned()
    }
}

/// Handle a `Host: proxycontrol` request: introspect/steer the live server without a pool proxy.
/// `GET .../api/remove/<ip:port>` evicts a proxy (always 204); `GET .../api/history/url:<url>`
/// reports the upstream that last served `<url>` for this client (200 + JSON, or 204 on a miss).
/// Mirrors `server.py:320`.
async fn serve_control(
    client: &mut TcpStream,
    req: &ClientRequest,
    pool: &Pool,
    history: &History,
    peer_ip: IpAddr,
) -> std::io::Result<()> {
    // path is `http://proxycontrol/api/<operation>/<params>`; strip the authority, split the rest.
    let rest = req
        .path
        .strip_prefix("http://proxycontrol")
        .unwrap_or(&req.path)
        .trim_start_matches('/');
    let parts: Vec<&str> = rest.splitn(3, '/').collect();
    match parts.as_slice() {
        [_api, "remove", params] => {
            let (h, p) = split_host_port(params, 0);
            if let Ok(ip) = h.parse::<IpAddr>() {
                pool.remove(ip, p);
            }
            // Parity: always 204, regardless of whether anything matched.
            client.write_all(b"HTTP/1.1 204 No Content\r\n\r\n").await
        }
        [_api, "history", params] => {
            let url = params.strip_prefix("url:").unwrap_or("");
            match history.get(&format!("{peer_ip}-{url}")) {
                Some(proxy) => {
                    let body = format!("{{\"proxy\": \"{proxy}\"}}\r\n");
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nAccess-Control-Allow-Origin: *\r\n\
                         Access-Control-Allow-Credentials: true\r\n\r\n{body}",
                        body.len()
                    );
                    client.write_all(resp.as_bytes()).await
                }
                None => client.write_all(b"HTTP/1.1 204 No Content\r\n\r\n").await,
            }
        }
        _ => client.write_all(b"HTTP/1.1 204 No Content\r\n\r\n").await,
    }
}

/// Insert `header_line` (which must end with CRLF) right after the request/status line — i.e. after
/// the first CRLF — in a buffered HTTP head. Returns `raw` unchanged if there is no CRLF. Shared by
/// B8 (inject `Proxy-Authorization` upstream) and B7 (inject `X-Proxy-Info` downstream).
fn inject_header(raw: &[u8], header_line: &[u8]) -> Vec<u8> {
    match raw.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => {
            let cut = pos + 2;
            let mut out = Vec::with_capacity(raw.len() + header_line.len());
            out.extend_from_slice(&raw[..cut]);
            out.extend_from_slice(header_line);
            out.extend_from_slice(&raw[cut..]);
            out
        }
        None => raw.to_vec(),
    }
}

/// Remove every `name:` header line (case-insensitive) from a buffered HTTP head, preserving the
/// request/status line (never dropped) and the terminating blank line. Used to strip the client's
/// hop-by-hop `Proxy-Authorization` before forwarding upstream (B9 secret hygiene).
fn strip_header(raw: &[u8], name: &str) -> Vec<u8> {
    let prefix = format!("{}:", name.to_ascii_lowercase());
    let mut out = Vec::with_capacity(raw.len());
    let mut rest = raw;
    let mut first = true; // the first line is the request line — never a header
    while let Some(pos) = rest.windows(2).position(|w| w == b"\r\n") {
        let advance = pos + 2;
        let is_target = !first
            && String::from_utf8_lossy(&rest[..pos])
                .to_ascii_lowercase()
                .starts_with(&prefix);
        if !is_target {
            out.extend_from_slice(&rest[..advance]);
        }
        rest = &rest[advance..];
        first = false;
    }
    out.extend_from_slice(rest); // trailing bytes after the last CRLF (usually empty)
    out
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
    /// The client already received an ack or spliced bytes and a splice error followed — the
    /// upstream is implicated, so abort (a retry would corrupt the client) and penalize the proxy.
    ClientCommitted(crate::error::ProxyError),
    /// The client went away at the commit point while the upstream was proven good (negotiate/peek
    /// succeeded). Abort — a retry cannot reach the departed client — but do NOT penalize the
    /// (blameless) proxy.
    ClientGone,
}

/// Relay one client request through `proxy` using `proto`, reporting where it ended so the caller
/// only retries a failure the client has not yet seen (B2's commit boundary). When `allowed_codes`
/// is set, an HTTP response whose status is outside the set is a **pre-commit** retryable failure
/// (B11), so the block page never reaches the client.
async fn relay(
    client: &mut TcpStream,
    proxy: &Proxy,
    proto: Proto,
    target: &Target,
    req: &ClientRequest,
    timeout: Duration,
    allowed_codes: Option<&[u16]>,
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
    // Pass the proxy's upstream credentials (B8) into the negotiation (SOCKS5 RFC 1929 / CONNECT
    // Proxy-Authorization). `None` for scraped proxies.
    let mut upstream = match negotiate(proto, tcp, target, timeout, proxy.auth()).await {
        Err(e) => return RetryableFailure(e),
        Ok(u) => u,
    };

    // Ack the client per how it addressed us (R0), independently of the target scheme.
    match req.frontend {
        Frontend::HttpConnect => {
            // Acknowledging the CONNECT tunnel is the commit point: after this the client believes
            // it is talking to the target, so a later failure must not re-ack through another proxy.
            // B7: carry X-Proxy-Info on the ACK head — the one place a CONNECT client can see it
            // (the tunnel body is opaque; Python's in-stream rewrite is a no-op on real TLS).
            let ack = format!(
                "HTTP/1.1 200 Connection established\r\nX-Proxy-Info: {}:{}\r\n\r\n",
                proxy.host, proxy.port
            );
            if client.write_all(ack.as_bytes()).await.is_err() {
                // The client write failed → the client is gone, but the upstream tunnel was fine;
                // abort without blaming the proxy.
                return RelayOutcome::ClientGone;
            }
        }
        Frontend::Socks5 => {
            // SOCKS5 success reply (B12): VER=05 REP=00 RSV=00 ATYP=01 BND.ADDR=0.0.0.0 BND.PORT=0.
            // A stub bound address, so we do not leak which upstream served the tunnel. No
            // X-Proxy-Info — a SOCKS5 tunnel is opaque.
            if client
                .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await
                .is_err()
            {
                return RelayOutcome::ClientGone;
            }
        }
        Frontend::HttpForward => {
            // The buffered request goes upstream first — the client has still received nothing, so
            // a write failure here is retryable.
            //
            // Secret hygiene: drop the client's Proxy-Authorization before forwarding. It carries
            // the LOCAL server's access secret (B9), is hop-by-hop (RFC 7235 §4.4), and must not be
            // handed to the untrusted upstream proxy — an authenticating proxy strips it. THEN add
            // the upstream's own credentials (B8), if any.
            let sanitized = strip_header(&req.raw, "Proxy-Authorization");
            let to_send: Vec<u8> = match proxy.auth() {
                Some(c) => inject_header(
                    &sanitized,
                    format!(
                        "Proxy-Authorization: Basic {}\r\n",
                        crate::utils::base64_encode(
                            format!("{}:{}", c.username, c.password).as_bytes()
                        )
                    )
                    .as_bytes(),
                ),
                None => sanitized,
            };
            if upstream.write_all(&to_send).await.is_err() {
                return RetryableFailure(ProxyError::Reset);
            }
            // Read the upstream response status line BEFORE splicing, to (B11) gate on
            // --http-allowed-codes and (B7) inject X-Proxy-Info after the status line. Both replay
            // the (rewritten) line to the client, which becomes the commit point.
            //
            // Skip this for a body-bearing request (POST/PUT/…): the client's body is still unread
            // in its socket and is only pumped by the copy_bidirectional below, so reading the
            // response first would deadlock an origin that waits for the whole body before
            // answering. Those requests are non-idempotent anyway (the body is consumed on the
            // first upstream, so a retry could not replay it) — splice straight through, no inject.
            if !request_has_body(&req.raw) {
                match read_status_line(&mut upstream, timeout).await {
                    Ok(line) => {
                        // B11: a disallowed status is a pre-commit retryable failure — nothing has
                        // reached the client yet, so the block page never does.
                        if let Some(allowed) = allowed_codes {
                            let code = parse_http_status(&line);
                            if !allowed.contains(&code) {
                                return RetryableFailure(ProxyError::DisallowedStatus(code));
                            }
                        }
                        // B7: X-Proxy-Info after the status line; the rest of the response splices.
                        let head = inject_header(
                            &line,
                            format!("X-Proxy-Info: {}:{}\r\n", proxy.host, proxy.port).as_bytes(),
                        );
                        if client.write_all(&head).await.is_err() {
                            return RelayOutcome::ClientGone;
                        }
                    }
                    // Could not read the status line (upstream closed / timed out) — pre-commit, so
                    // the next proxy can be tried.
                    Err(e) => return RetryableFailure(e),
                }
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

/// Read an HTTP response's status line from `upstream` (bounded), returning the raw bytes read
/// (including the trailing CRLF) so the caller can gate on the status (B11) and/or rewrite + replay
/// them to the client (B7). Errors if the upstream closes or the read times out — both pre-commit,
/// so the caller can retry another proxy. Reads byte-by-byte so a status line split across TCP
/// segments still parses.
async fn read_status_line(
    upstream: &mut Stream,
    deadline: Duration,
) -> Result<Vec<u8>, crate::error::ProxyError> {
    use crate::error::ProxyError;
    let mut head = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    let read = async {
        loop {
            let n = upstream
                .read(&mut byte)
                .await
                .map_err(|_| ProxyError::Reset)?;
            if n == 0 {
                return Err(ProxyError::EmptyRecv);
            }
            head.push(byte[0]);
            // The status line ends at the first LF (a CRLF also ends with LF, so this subsumes it
            // and tolerates a non-conformant bare-LF terminator); cap the scan so a header-less
            // stream cannot buffer without bound.
            if head.ends_with(b"\n") || head.len() >= 256 {
                return Ok(head);
            }
        }
    };
    tokio::time::timeout(deadline, read)
        .await
        .map_err(|_| ProxyError::Timeout)?
}

/// The status code from an HTTP status line (`HTTP/1.1 200 OK` → `200`); `0` on anything
/// unparseable (which no `allowed` set contains, so it is treated as disallowed).
fn parse_http_status(head: &[u8]) -> u16 {
    std::str::from_utf8(head)
        .ok()
        .and_then(|s| s.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

/// Whether the buffered request declares a body (chunked, or a non-zero `Content-Length`). The
/// B11 status peek must be skipped for these — the origin will not answer until the body arrives,
/// but the body is still unread in the client socket (only `copy_bidirectional` pumps it), so
/// peeking first would deadlock.
fn request_has_body(raw: &[u8]) -> bool {
    if header_value(raw, "Transfer-Encoding").is_some() {
        return true;
    }
    match header_value(raw, "Content-Length") {
        // Unparseable length → assume a body (be safe: skip the peek rather than risk a hang).
        Some(v) => v.trim().parse::<u64>().map_or(true, |n| n > 0),
        None => false,
    }
}

/// Read and parse the client's first request, auto-detecting the front-end protocol (B12): a first
/// byte of `0x05` is a SOCKS5 greeting; anything else is HTTP (forward or CONNECT).
async fn parse_client_request(
    client: &mut TcpStream,
    timeout: Duration,
    expected_auth: Option<&AuthExpect>,
) -> Option<ClientRequest> {
    // Sniff the first byte.
    let mut first = [0u8; 1];
    tokio::time::timeout(timeout, client.read_exact(&mut first))
        .await
        .ok()?
        .ok()?;
    if first[0] == 0x05 {
        return parse_socks5_request(client, timeout, expected_auth).await;
    }

    // HTTP: seed the buffer with the sniffed byte, then read to the blank line.
    let mut buf = vec![first[0]];
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
    // Own the request-target now (B6): the HTTP branch moves `buf` into `raw`, after which `uri`
    // (a borrow of `buf`) would be invalid.
    let path = uri.to_string();

    // The client's Proxy-Authorization header (B9), present on either method.
    let proxy_auth = header_value(&buf, "Proxy-Authorization");

    if method.eq_ignore_ascii_case("CONNECT") {
        // `CONNECT host:port HTTP/1.1`
        let (host, port) = split_host_port(uri, 443);
        Some(ClientRequest {
            scheme: Scheme::Https,
            frontend: Frontend::HttpConnect,
            host,
            port,
            raw: Vec::new(),
            proxy_auth,
            path,
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
            frontend: Frontend::HttpForward,
            host,
            port,
            raw: buf,
            proxy_auth,
            path,
        })
    }
}

/// Parse a SOCKS5 client handshake (B12): greeting → method select (no-auth, or RFC 1929 when the
/// server has `--auth`) → CONNECT request. The `0x05` version byte was already consumed. Returns a
/// tunnel [`ClientRequest`] (`Scheme::Https`, `Frontend::Socks5`), or `None` after writing the
/// appropriate SOCKS5 rejection. Only `CMD=CONNECT` is supported; all three address types are.
async fn parse_socks5_request(
    client: &mut TcpStream,
    timeout: Duration,
    expected_auth: Option<&AuthExpect>,
) -> Option<ClientRequest> {
    use std::net::{Ipv4Addr, Ipv6Addr};
    tokio::time::timeout(timeout, async {
        // Greeting: NMETHODS, then the offered method bytes.
        let mut nmethods = [0u8; 1];
        client.read_exact(&mut nmethods).await.ok()?;
        let mut methods = vec![0u8; nmethods[0] as usize];
        client.read_exact(&mut methods).await.ok()?;

        match expected_auth {
            None => {
                // No-auth: require the client to offer 0x00.
                if !methods.contains(&0x00) {
                    let _ = client.write_all(&[0x05, 0xFF]).await; // no acceptable methods
                    return None;
                }
                client.write_all(&[0x05, 0x00]).await.ok()?;
            }
            Some(exp) => {
                // Require username/password (0x02), symmetric with the HTTP 407 gate.
                if !methods.contains(&0x02) {
                    let _ = client.write_all(&[0x05, 0xFF]).await;
                    return None;
                }
                client.write_all(&[0x05, 0x02]).await.ok()?;
                // RFC 1929: VER=01, ULEN, user, PLEN, pass.
                let mut vu = [0u8; 2];
                client.read_exact(&mut vu).await.ok()?;
                let mut user = vec![0u8; vu[1] as usize];
                client.read_exact(&mut user).await.ok()?;
                let mut pl = [0u8; 1];
                client.read_exact(&mut pl).await.ok()?;
                let mut pass = vec![0u8; pl[0] as usize];
                client.read_exact(&mut pass).await.ok()?;
                if user == exp.user.as_bytes() && pass == exp.pass.as_bytes() {
                    client.write_all(&[0x01, 0x00]).await.ok()?; // auth success
                } else {
                    let _ = client.write_all(&[0x01, 0x01]).await; // auth failure
                    return None;
                }
            }
        }

        // CONNECT request: VER=05, CMD, RSV, ATYP.
        let mut hdr = [0u8; 4];
        client.read_exact(&mut hdr).await.ok()?;
        if hdr[0] != 0x05 {
            return None;
        }
        if hdr[1] != 0x01 {
            // Only CONNECT; reject BIND/UDP with REP=07 (command not supported).
            let _ = client
                .write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await;
            return None;
        }
        let (host, port) = match hdr[3] {
            0x01 => {
                let mut a = [0u8; 4];
                client.read_exact(&mut a).await.ok()?;
                let mut p = [0u8; 2];
                client.read_exact(&mut p).await.ok()?;
                (Ipv4Addr::from(a).to_string(), u16::from_be_bytes(p))
            }
            0x04 => {
                let mut a = [0u8; 16];
                client.read_exact(&mut a).await.ok()?;
                let mut p = [0u8; 2];
                client.read_exact(&mut p).await.ok()?;
                (Ipv6Addr::from(a).to_string(), u16::from_be_bytes(p))
            }
            0x03 => {
                let mut l = [0u8; 1];
                client.read_exact(&mut l).await.ok()?;
                let mut d = vec![0u8; l[0] as usize];
                client.read_exact(&mut d).await.ok()?;
                let mut p = [0u8; 2];
                client.read_exact(&mut p).await.ok()?;
                (
                    String::from_utf8_lossy(&d).into_owned(),
                    u16::from_be_bytes(p),
                )
            }
            _ => {
                let _ = client
                    .write_all(&[0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0]) // addr type unsupported
                    .await;
                return None;
            }
        };
        Some(ClientRequest {
            // An opaque SOCKS5 CONNECT tunnel → Https scheme, so choose_proto prefers a tunnelling
            // upstream proto.
            scheme: Scheme::Https,
            frontend: Frontend::Socks5,
            path: format!("{host}:{port}"),
            host,
            port,
            raw: Vec::new(),
            proxy_auth: None,
        })
    })
    .await
    .ok()?
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
            prefer_connect: false,
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
    fn prefer_connect_biases_toward_connect80() {
        // Two HTTP proxies with identical priority; prefer_connect breaks the tie toward the one
        // that also supports CONNECT:80.
        let a = proxy_at(0.1, Proto::Http, "1.1.1.1");
        let mut b = proxy_at(0.1, Proto::Http, "2.2.2.2");
        b.proxy.add_type(Proto::Connect80, None);
        let pool = vec![a, b];
        let mut c = ctx(Scheme::Http);
        c.prefer_connect = true;
        assert_eq!(best_for(&pool, &c), Some(1), "CONNECT-capable wins the tie");
        c.prefer_connect = false;
        assert_eq!(
            best_for(&pool, &c),
            Some(0),
            "no bias → tie resolves to first"
        );
    }

    #[test]
    fn prefer_connect_does_not_override_health() {
        // A faster non-CONNECT proxy beats a slower CONNECT one — prefer_connect is only a
        // tie-break for Best; health dominates (the resolved open question).
        let fast = proxy_at(0.1, Proto::Http, "1.1.1.1");
        let mut slow = proxy_at(0.9, Proto::Http, "2.2.2.2");
        slow.proxy.add_type(Proto::Connect80, None);
        let pool = vec![fast, slow];
        let mut c = ctx(Scheme::Http);
        c.prefer_connect = true;
        assert_eq!(
            best_for(&pool, &c),
            Some(0),
            "health dominates the CONNECT bias"
        );
    }

    #[test]
    fn parse_http_status_extracts_code() {
        assert_eq!(parse_http_status(b"HTTP/1.1 200 OK\r\n"), 200);
        assert_eq!(parse_http_status(b"HTTP/1.0 403 Forbidden\r\n"), 403);
        assert_eq!(
            parse_http_status(b"HTTP/1.1 301 Moved Permanently\r\n"),
            301
        );
        assert_eq!(parse_http_status(b"garbage"), 0); // unparseable → 0 (in no allow-list)
        assert_eq!(parse_http_status(b"HTTP/1.1 \r\n"), 0);
    }

    #[test]
    fn strip_header_removes_only_the_named_header() {
        let raw =
            b"GET / HTTP/1.1\r\nHost: x\r\nProxy-Authorization: Basic zzz\r\nAccept: */*\r\n\r\n";
        let s = String::from_utf8(strip_header(raw, "Proxy-Authorization")).unwrap();
        assert!(!s.contains("Proxy-Authorization"), "{s:?}");
        assert!(s.contains("Host: x") && s.contains("Accept: */*"), "{s:?}");
        assert!(
            s.starts_with("GET / HTTP/1.1\r\n"),
            "request line kept: {s:?}"
        );
        assert!(s.ends_with("\r\n\r\n"), "blank line kept: {s:?}");
    }

    #[test]
    fn pool_remove_drops_matching() {
        let mk = |ip: &str| Proxy::new(ip.parse().unwrap(), 80, BTreeSet::new());
        let pool = Pool::from_proxies(
            vec![mk("1.1.1.1"), mk("2.2.2.2"), mk("3.3.3.3")],
            PoolConfig::default(),
        );
        assert!(
            pool.remove("2.2.2.2".parse().unwrap(), 80),
            "should remove a match"
        );
        assert_eq!(pool.len(), 2);
        assert!(
            !pool.remove("9.9.9.9".parse().unwrap(), 80),
            "no match → false"
        );
        assert_eq!(pool.len(), 2);
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

    fn http_at(ip: &str, cc: Option<&str>) -> Proxy {
        let mut p = Proxy::new(ip.parse().unwrap(), 80, BTreeSet::new());
        p.add_type(Proto::Http, None);
        if let Some(cc) = cc {
            p.geo = Some(crate::proxy::Country {
                code: cc.to_string(),
                ..Default::default()
            });
        }
        p
    }

    #[test]
    fn try_get_filters_by_country() {
        let pool = Pool::from_proxies(
            vec![
                http_at("1.1.1.1", Some("US")),
                http_at("2.2.2.2", Some("GB")),
            ],
            PoolConfig::default(),
        );
        let got = pool.try_get(Scheme::Http, Some("us")).expect("a US proxy"); // case-insensitive
        assert_eq!(got.addr(), "1.1.1.1:80");
        assert!(
            pool.try_get(Scheme::Http, Some("FR")).is_none(),
            "no FR proxy to hand out"
        );
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn render_metrics_emits_probe_latency_distinct_from_resp_time() {
        // F1: a proxy whose check recorded a judge-probe latency, then served slower traffic.
        // The check records the probe RTT into BOTH the blended runtimes and the probe signal;
        // the later serve request blends only into avg_resp_time. So the two series diverge.
        let mut p = Proxy::new("1.2.3.4".parse().unwrap(), 80, BTreeSet::new());
        p.add_type(Proto::Http, None);
        p.record_probe_latency(0.10); // check-time judge probe
        p.record_attempt(Some(0.10), None); // same check attempt, blended in
        p.record_attempt(Some(0.90), None); // later serve-time relayed request
        let pool = Pool::from_proxies(vec![p], PoolConfig::default());
        let out = render_metrics(&pool);

        assert!(
            out.contains("# TYPE proxybroker_pool_probe_latency_avg_seconds gauge"),
            "missing probe-latency HELP/TYPE block:\n{out}"
        );
        // Probe latency = pure check-time 0.10; resp_time = blended (0.10 + 0.90)/2 = 0.50.
        assert!(
            out.contains("proxybroker_pool_probe_latency_avg_seconds 0.10"),
            "probe latency should be the pure check-time value:\n{out}"
        );
        assert!(
            out.contains("proxybroker_pool_resp_time_avg_seconds 0.50"),
            "resp_time should be the serve-blended value:\n{out}"
        );
    }

    #[test]
    fn proxies_snapshot_does_not_consume() {
        let pool = Pool::from_proxies(
            vec![http_at("1.1.1.1", None), http_at("2.2.2.2", None)],
            PoolConfig::default(),
        );
        assert_eq!(pool.proxies().len(), 2, "snapshot sees both");
        assert!(
            pool.try_get(Scheme::Http, None).is_some(),
            "still checkoutable"
        );
        assert_eq!(pool.proxies().len(), 1, "checkout removed one");
    }
}
