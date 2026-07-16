//! The [`Broker`]: orchestrates providers into a stream of proxies.
//!
//! This module currently implements the `grab` path (scrape providers, no checking). The
//! `find`/`serve` paths build on it and land later.
//!
//! Delivery is a [`ProxyStream`], not proxybroker2's fire-and-forget-onto-a-queue. Termination
//! is the channel closing (drop the sender), not a `None` poison pill — broadcast- and
//! multi-consumer-safe by construction. Dropping the stream drops the receiver, so the source
//! task's next `send` fails and it stops: cancellation for free, without a sentinel or a
//! detached-task leak (design critique #14). See `decisions.md`.

use crate::provider::{fetch, ProviderSpec};
use crate::proxy::Proxy;
use futures_util::stream::{Stream, StreamExt};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

#[cfg(feature = "geo")]
use crate::geo::GeoDb;

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

/// Finds proxies from a set of providers.
#[derive(Clone)]
pub struct Broker {
    providers: Arc<Vec<ProviderSpec>>,
    client: reqwest::Client,
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
        ProxyStream { rx }
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

    #[cfg(feature = "geo")]
    fn attach_geo(&self, proxy: &mut Proxy) {
        if let Some(db) = &self.geo {
            proxy.geo = db.lookup(proxy.host);
        }
    }

    #[cfg(not(feature = "geo"))]
    fn attach_geo(&self, _proxy: &mut Proxy) {}
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
    #[cfg(feature = "geo")]
    geo: Option<Arc<GeoDb>>,
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

    /// Attach a geo database for country lookup and filtering.
    #[cfg(feature = "geo")]
    pub fn geo(mut self, db: GeoDb) -> Self {
        self.geo = Some(Arc::new(db));
        self
    }

    pub fn build(self) -> Broker {
        Broker {
            providers: Arc::new(
                self.providers
                    .unwrap_or_else(crate::provider::bundled_registry),
            ),
            client: self.client.unwrap_or_default(),
            #[cfg(feature = "geo")]
            geo: self.geo,
        }
    }
}

/// A stream of discovered proxies. Ends when the source is exhausted, the limit is reached,
/// or this stream is dropped (which stops the source task).
pub struct ProxyStream {
    rx: mpsc::Receiver<Proxy>,
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
