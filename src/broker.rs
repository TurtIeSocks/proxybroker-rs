//! The [`Broker`]: orchestrates providers into a stream of proxies.
//!
//! `grab` scrapes providers without checking; `find` scrapes and checks. (`serve` builds on
//! `find` and lands with the server.)
//!
//! Delivery is a [`ProxyStream`], not proxybroker2's fire-and-forget-onto-a-queue. Termination
//! is the channel closing (drop the sender), not a `None` poison pill — broadcast- and
//! multi-consumer-safe by construction. Dropping the stream drops the receiver, so the source
//! task's next `send` fails and it stops: cancellation for free, without a sentinel or a
//! detached-task leak (design critique #14). See `decisions.md`.
//!
//! `find`'s concurrency is the delicate part (`decisions.md` §1). proxybroker2's `_on_check`
//! is a queue impersonating **two** primitives, which must stay separate here:
//!
//! - a [`Semaphore`](tokio::sync::Semaphore) bounds in-flight checks (the concurrency cap);
//! - a [`TaskTracker`](tokio_util::task::TaskTracker) is the wait-group we drain before
//!   ending the stream, so no check is silently dropped.
//!
//! A [`CancellationToken`] fired when the consumer drops the stream aborts in-flight checks —
//! not a detached-task leak (critique #14).

use crate::checker::{Checker, CheckerConfig};
use crate::provider::{fetch, ProviderSpec};
use crate::proxy::Proxy;
use crate::resolver::Resolver;
use crate::types::TypeSpec;
use futures_util::stream::{Stream, StreamExt};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[cfg(feature = "geo")]
use crate::geo::GeoDb;

use crate::error::Error;
use crate::stats::StatsCollector;

/// The maximum number of providers fetched concurrently. `api.py:MAX_CONCURRENT_PROVIDERS`.
const MAX_CONCURRENT_PROVIDERS: usize = 3;

/// Channel depth between the source task and the consumer. Bounds memory and provides
/// backpressure: when the consumer is slow, the source task's `send` blocks.
const CHANNEL_CAPACITY: usize = 256;

/// What to gather. `limit: None` means unlimited (the CLI maps `--limit 0` to `None`, since
/// proxybroker2 relies on integer underflow for "unlimited" — see `decisions.md`).
#[derive(Debug, Clone, Default)]
pub struct GrabQuery {
    /// Keep only proxies located in these ISO country codes. `None` = no filter.
    pub countries: Option<Vec<String>>,
    /// Stop after this many proxies. `None` = unlimited.
    pub limit: Option<usize>,
}

/// What to find and check. `types` is required (empty is [`Error::NoTypes`]).
#[derive(Debug, Clone)]
pub struct FindQuery {
    /// Protocols (and optional anonymity levels) a proxy must support.
    pub types: Vec<TypeSpec>,
    /// Keep only proxies in these ISO country codes. `None` = no filter.
    pub countries: Option<Vec<String>>,
    /// Stop after this many working proxies. `None` = unlimited.
    pub limit: Option<usize>,
    /// Judge URLs to probe. Empty = the bundled defaults.
    pub judges: Vec<String>,
    /// DNS blocklist zones; a proxy whose IP is listed in any is rejected. Empty disables.
    pub dnsbl: Vec<String>,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Max concurrent checks in flight. `api.py:max_conn`.
    pub max_conn: usize,
    /// Attempts per protocol before giving up. `api.py:max_tries`.
    pub max_tries: usize,
    /// Use `POST` for the test request.
    pub post: bool,
    /// Require the anonymity level to match exactly.
    pub strict: bool,
}

impl Default for FindQuery {
    fn default() -> Self {
        FindQuery {
            types: Vec::new(),
            countries: None,
            limit: None,
            judges: Vec::new(),
            dnsbl: Vec::new(),
            timeout: Duration::from_secs(8),
            max_conn: 200,
            max_tries: 3,
            post: false,
            strict: false,
        }
    }
}

/// Finds proxies from a set of providers.
#[derive(Clone)]
pub struct Broker {
    providers: Arc<Vec<ProviderSpec>>,
    client: reqwest::Client,
    /// Injectable so tests can stub external-IP discovery and DNS offline. `None` = build a
    /// default resolver on first `find`.
    resolver: Option<Arc<Resolver>>,
    #[cfg(feature = "geo")]
    geo: Option<Arc<GeoDb>>,
}

impl Broker {
    /// Start building a broker.
    pub fn builder() -> BrokerBuilder {
        BrokerBuilder::default()
    }

    /// Gather proxies from the providers **without checking** them. `api.py:Broker.grab`.
    ///
    /// Returns immediately with a stream; the work runs in a spawned task. Proxies are
    /// deduplicated by `(host, port)`, optionally country-filtered, and capped at
    /// `query.limit`.
    pub fn grab(&self, query: GrabQuery) -> ProxyStream {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let broker = self.clone();
        tokio::spawn(async move { broker.grab_task(query, tx).await });
        ProxyStream {
            rx,
            _cancel_on_drop: None,
            stats: None,
        }
    }

    async fn grab_task(self, query: GrabQuery, tx: mpsc::Sender<Proxy>) {
        let countries: Option<BTreeSet<String>> = query
            .countries
            .map(|cs| cs.into_iter().map(|c| c.to_uppercase()).collect());

        let mut seen: BTreeSet<(IpAddr, u16)> = BTreeSet::new();
        let mut sent = 0usize;

        // Each future owns its inputs (a `ProviderSpec` clone and a cheap `reqwest::Client`
        // clone — the client is `Arc` internally) so nothing borrows `self` across the
        // buffered stream, which otherwise trips a higher-ranked-lifetime error.
        let client = self.client.clone();
        let specs: Vec<ProviderSpec> = self.providers.as_ref().clone();
        let mut fetches = futures_util::stream::iter(specs)
            .map(|spec| {
                let client = client.clone();
                async move { fetch(&spec, &client).await }
            })
            .buffer_unordered(MAX_CONCURRENT_PROVIDERS);

        while let Some(candidates) = fetches.next().await {
            for cand in candidates {
                if query.limit.is_some_and(|l| sent >= l) {
                    return; // limit reached — drop tx, ending the stream
                }
                let Ok(host) = cand.host.parse::<IpAddr>() else {
                    continue;
                };
                if !seen.insert((host, cand.port)) {
                    continue; // duplicate (host, port)
                }
                let mut proxy = Proxy::new(host, cand.port, cand.protocols.clone());
                self.attach_geo(&mut proxy);
                if !country_ok(&proxy, countries.as_ref()) {
                    continue;
                }
                if tx.send(proxy).await.is_err() {
                    return; // consumer dropped the stream — stop working
                }
                sent += 1;
            }
        }
    }

    /// Gather **and check** proxies. `api.py:Broker.find`.
    ///
    /// Unlike proxybroker2's `find` (fire-and-forget onto a queue), this returns a
    /// `Result<ProxyStream>`: the up-front work that can fail — discovering the host's
    /// external IPs and verifying at least one judge — happens before the stream is returned,
    /// so [`Error::NoTypes`], [`Error::ExtIpUnknown`], and [`Error::NoJudges`] surface here
    /// rather than silently producing an empty stream.
    pub async fn find(&self, query: FindQuery) -> Result<ProxyStream, Error> {
        if query.types.is_empty() {
            return Err(Error::NoTypes);
        }
        let checker = self.build_checker(&query).await?;
        let (tx, rx, cancel, stats) = new_run();
        let broker = self.clone();
        let task_cancel = cancel.clone();
        let task_stats = stats.clone();
        tokio::spawn(async move {
            broker
                .find_task(query, checker, tx, task_cancel, task_stats)
                .await
        });
        Ok(ProxyStream {
            rx,
            _cancel_on_drop: Some(cancel.drop_guard()),
            stats: Some(stats),
        })
    }

    /// Gather **and check** proxies the caller already has, instead of scraping providers.
    /// The counterpart to [`Broker::find`], sharing the same check pipeline — see `check_stream`.
    ///
    /// `proxies` is any stream of unchecked [`Proxy`]s (parse a file/stdin with
    /// [`crate::parse::parse_proxy_lines`], or build them directly). Geo is attached per proxy so
    /// serialized output carries country. The same errors surface up front as `find`
    /// ([`Error::NoTypes`], [`Error::ExtIpUnknown`], [`Error::NoJudges`]).
    pub async fn check<S>(&self, proxies: S, query: FindQuery) -> Result<ProxyStream, Error>
    where
        S: Stream<Item = Proxy> + Send + 'static,
    {
        if query.types.is_empty() {
            return Err(Error::NoTypes);
        }
        let checker = self.build_checker(&query).await?;
        let (tx, rx, cancel, stats) = new_run();
        let broker = self.clone();
        let task_cancel = cancel.clone();
        let task_stats = stats.clone();
        let countries = uppercase_set(query.countries.clone());
        let (max_conn, limit) = (query.max_conn, query.limit);
        tokio::spawn(async move {
            // Attach geo + apply the country filter before checking, mirroring find's source.
            let source = proxies.filter_map(move |mut proxy| {
                broker.attach_geo(&mut proxy);
                let keep = country_ok(&proxy, countries.as_ref()).then_some(proxy);
                std::future::ready(keep)
            });
            check_stream(
                source,
                checker,
                max_conn,
                limit,
                tx,
                task_cancel,
                task_stats,
            )
            .await;
        });
        Ok(ProxyStream {
            rx,
            _cancel_on_drop: Some(cancel.drop_guard()),
            stats: Some(stats),
        })
    }

    /// The resolver + external-IP discovery + [`Checker`] setup shared by `find` and `check`.
    /// Proxy candidates are already IP literals, so the resolver is only for external-IP
    /// discovery and judge-host resolution.
    async fn build_checker(&self, query: &FindQuery) -> Result<Arc<Checker>, Error> {
        let resolver = match &self.resolver {
            Some(r) => r.clone(),
            None => Arc::new(Resolver::new(query.timeout)?),
        };
        let real_ext_ips = resolver.external_ips().await?;
        let checker = Checker::new(
            CheckerConfig {
                judges: query.judges.clone(),
                types: query.types.clone(),
                timeout: query.timeout,
                max_tries: query.max_tries,
                post: query.post,
                strict: query.strict,
                dnsbl: query.dnsbl.clone(),
            },
            resolver,
            &self.client,
            real_ext_ips,
        )
        .await?;
        Ok(Arc::new(checker))
    }

    async fn find_task(
        self,
        query: FindQuery,
        checker: Arc<Checker>,
        tx: mpsc::Sender<Proxy>,
        cancel: CancellationToken,
        stats: Arc<std::sync::Mutex<StatsCollector>>,
    ) {
        let countries = uppercase_set(query.countries.clone());
        let client = self.client.clone();
        let specs: Vec<ProviderSpec> = self.providers.as_ref().clone();
        let fetches = futures_util::stream::iter(specs)
            .map(|spec| {
                let client = client.clone();
                async move { fetch(&spec, &client).await }
            })
            .buffer_unordered(MAX_CONCURRENT_PROVIDERS);

        // Flatten provider batches into a per-proxy stream: dedup on (host, port), attach geo,
        // apply the country filter. `seen` is threaded through the FnMut closure.
        let broker = self.clone();
        let mut seen: BTreeSet<(IpAddr, u16)> = BTreeSet::new();
        let source = fetches
            .flat_map(futures_util::stream::iter)
            .filter_map(move |cand| {
                let keep = match cand.host.parse::<IpAddr>() {
                    Ok(host) if seen.insert((host, cand.port)) => {
                        let mut proxy = Proxy::new(host, cand.port, cand.protocols.clone());
                        broker.attach_geo(&mut proxy);
                        country_ok(&proxy, countries.as_ref()).then_some(proxy)
                    }
                    _ => None,
                };
                std::future::ready(keep)
            });

        check_stream(
            source,
            checker,
            query.max_conn,
            query.limit,
            tx,
            cancel,
            stats,
        )
        .await;
    }

    #[cfg(feature = "geo")]
    fn attach_geo(&self, proxy: &mut Proxy) {
        if let Some(db) = &self.geo {
            proxy.geo = db.lookup(proxy.host);
        }
    }

    #[cfg(not(feature = "geo"))]
    fn attach_geo(&self, _proxy: &mut Proxy) {}
}

/// Has the emit count reached the limit? `None` = unlimited.
fn is_limit_reached(sent: &AtomicUsize, limit: Option<usize>) -> bool {
    limit.is_some_and(|l| sent.load(Ordering::SeqCst) >= l)
}

/// The channel + cancellation + stats triple every run (`find`/`check`) is built from.
type Run = (
    mpsc::Sender<Proxy>,
    mpsc::Receiver<Proxy>,
    CancellationToken,
    Arc<std::sync::Mutex<StatsCollector>>,
);

fn new_run() -> Run {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
    // Shared across every check task so stats cover ALL checked proxies, not just the working
    // ones streamed out (design review F4).
    let stats = Arc::new(std::sync::Mutex::new(StatsCollector::default()));
    (tx, rx, cancel, stats)
}

/// Uppercase an optional country list into a set for case-insensitive matching.
fn uppercase_set(countries: Option<Vec<String>>) -> Option<BTreeSet<String>> {
    countries.map(|v| v.into_iter().map(|c| c.to_uppercase()).collect())
}

/// The per-proxy concurrency pipeline shared by `find` and `check`: a [`Semaphore`] concurrency
/// cap, a [`TaskTracker`] wait-group drained before the stream ends, atomic limit accounting,
/// per-check `stats.record`, and cancel-on-drop. Source-agnostic — `find` feeds provider
/// candidates, `check` feeds user input.
async fn check_stream<S>(
    source: S,
    checker: Arc<Checker>,
    max_conn: usize,
    limit: Option<usize>,
    tx: mpsc::Sender<Proxy>,
    cancel: CancellationToken,
    stats: Arc<std::sync::Mutex<StatsCollector>>,
) where
    S: Stream<Item = Proxy> + Send,
{
    let sem = Arc::new(Semaphore::new(max_conn));
    let tracker = TaskTracker::new();
    let sent = Arc::new(AtomicUsize::new(0));
    let mut source = std::pin::pin!(source);

    while let Some(mut proxy) = source.next().await {
        if cancel.is_cancelled() || is_limit_reached(&sent, limit) {
            break;
        }
        // Acquire a permit BEFORE spawning; move it into the task so its Drop frees the slot —
        // race-free by construction (decisions.md §1).
        let Ok(permit) = sem.clone().acquire_owned().await else {
            break;
        };
        let checker = checker.clone();
        let tx = tx.clone();
        let sent = sent.clone();
        let cancel = cancel.clone();
        let stats = stats.clone();
        tracker.spawn(async move {
            let _permit = permit;
            tokio::select! {
                _ = cancel.cancelled() => {} // consumer gone — abort
                working = checker.check(&mut proxy) => {
                    // Record EVERY checked proxy (working or not) before it is sent or dropped,
                    // so the stats reflect the whole checked set (review F4).
                    stats.lock().unwrap().record(&proxy);
                    if working {
                        // Reserve a slot atomically; emit only if under the limit.
                        let n = sent.fetch_add(1, Ordering::SeqCst);
                        match limit {
                            Some(l) if n >= l => cancel.cancel(),
                            _ => {
                                let _ = tx.send(proxy).await;
                                if limit.is_some_and(|l| n + 1 >= l) {
                                    cancel.cancel();
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // Wait-group: drain every spawned check before dropping tx and ending the stream.
    tracker.close();
    tracker.wait().await;
}

/// A country filter that is a no-op when no countries are requested. Matches `api.py`'s
/// `_geo_passed`: keep the proxy if its country code is in the requested set.
fn country_ok(proxy: &Proxy, countries: Option<&BTreeSet<String>>) -> bool {
    match countries {
        None => true,
        Some(set) => proxy
            .geo
            .as_ref()
            .is_some_and(|c| set.contains(&c.code.to_uppercase())),
    }
}

/// Builds a [`Broker`].
#[derive(Default)]
pub struct BrokerBuilder {
    providers: Option<Vec<ProviderSpec>>,
    client: Option<reqwest::Client>,
    resolver: Option<Arc<Resolver>>,
    #[cfg(feature = "geo")]
    geo: Option<Arc<GeoDb>>,
    #[cfg(feature = "geo")]
    no_geo: bool,
}

impl BrokerBuilder {
    /// Use a specific provider list instead of the bundled registry.
    pub fn providers(mut self, providers: Vec<ProviderSpec>) -> Self {
        self.providers = Some(providers);
        self
    }

    /// Use a specific HTTP client (timeouts, proxy, TLS config).
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Use a specific resolver — mainly for tests, which stub external-IP discovery and DNS
    /// so `find` runs fully offline.
    pub fn resolver(mut self, resolver: Resolver) -> Self {
        self.resolver = Some(Arc::new(resolver));
        self
    }

    /// Attach a geo database for country lookup and filtering, overriding the bundled default.
    #[cfg(feature = "geo")]
    pub fn geo(mut self, db: GeoDb) -> Self {
        self.geo = Some(Arc::new(db));
        self
    }

    /// Do **not** attach any geo database. Country filtering will then reject every proxy (a
    /// proxy with unknown location cannot match a country), and proxies carry no `geo`. Use
    /// this to skip loading the ~8 MB bundled database when you do not need geolocation.
    #[cfg(feature = "geo")]
    pub fn without_geo(mut self) -> Self {
        self.no_geo = true;
        self
    }

    pub fn build(self) -> Broker {
        // Auto-attach the bundled geo database when built with `geo-bundled` (the default) and
        // the caller neither supplied one nor opted out. Without this, country filtering
        // silently rejects everything — a footgun for library users, since a proxy with no
        // known location can never match a requested country.
        #[cfg(feature = "geo")]
        let geo = match (self.geo, self.no_geo) {
            (Some(db), _) => Some(db),
            (None, true) => None,
            #[cfg(feature = "geo-bundled")]
            (None, false) => GeoDb::bundled().ok().map(Arc::new),
            #[cfg(not(feature = "geo-bundled"))]
            (None, false) => None,
        };

        Broker {
            providers: Arc::new(
                self.providers
                    .unwrap_or_else(crate::provider::bundled_registry),
            ),
            client: self.client.unwrap_or_default(),
            resolver: self.resolver,
            #[cfg(feature = "geo")]
            geo,
        }
    }
}

/// A stream of discovered proxies. Ends when the source is exhausted, the limit is reached,
/// or this stream is dropped (which stops the source task).
#[derive(Debug)]
pub struct ProxyStream {
    rx: mpsc::Receiver<Proxy>,
    /// For `find`: dropping the stream fires this token, aborting in-flight check tasks. For
    /// `grab` it is `None` — dropping the receiver already stops the single source task.
    _cancel_on_drop: Option<tokio_util::sync::DropGuard>,
    /// For `find`: the running aggregate over every checked proxy. `None` for `grab` (nothing
    /// is checked). Read it after the stream has been fully drained — by then every check has
    /// completed and recorded (the source task drains its `TaskTracker` before ending).
    stats: Option<Arc<std::sync::Mutex<StatsCollector>>>,
}

impl ProxyStream {
    /// Aggregate statistics over every proxy checked so far. `Some` only for `find`; call it
    /// after the stream is drained for a complete picture. `None` for `grab`.
    pub fn stats(&self) -> Option<crate::stats::Stats> {
        self.stats.as_ref().map(|s| s.lock().unwrap().finish())
    }
}

impl Stream for ProxyStream {
    type Item = Proxy;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Proxy>> {
        self.rx.poll_recv(cx)
    }
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("providers", &self.providers.len())
            .finish_non_exhaustive()
    }
}
