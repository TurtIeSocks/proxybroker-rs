//! # proxybroker
//!
//! Find, check, and serve public HTTP(S) and SOCKS4/5 proxies.
//!
//! A Rust rewrite of [proxybroker2](https://github.com/bluet/proxybroker2). See `NOTICE`
//! for attribution and a statement of changes.
//!
//! The three core capabilities are complete: [`Broker::grab`](broker::Broker::grab) scrapes
//! providers, [`Broker::find`](broker::Broker::find) checks them and classifies anonymity,
//! and [`server::serve`] runs a local rotating proxy server (behind the `server` feature).
//!
//! ```no_run
//! # async fn f() -> Result<(), Box<dyn std::error::Error>> {
//! use proxybroker::{Broker, FindQuery, Proto, TypeSpec};
//! use futures_util::StreamExt;
//!
//! let broker = Broker::builder().build();
//! let mut stream = broker.find(
//!     FindQuery::builder()
//!         .types(vec![TypeSpec::any(Proto::Http)])
//!         .limit(10)
//!         .build(),
//! ).await?;
//! while let Some(proxy) = stream.next().await {
//!     println!("{}", proxy.addr());
//! }
//! # Ok(()) }
//! ```

pub mod broker;
pub mod checker;
pub mod error;
#[cfg(feature = "geo")]
pub mod geo;
pub mod judge;
pub mod negotiator;
pub mod parse;
#[cfg(feature = "persist")]
pub mod persist;
pub mod provider;
pub mod proxy;
pub mod resolver;
#[cfg(all(feature = "server", feature = "persist"))]
pub mod scheduler;
#[cfg(feature = "server")]
pub mod server;
#[cfg(all(feature = "mcp", feature = "server"))]
pub mod mcp;
pub mod stats;
pub mod types;
pub mod utils;
#[cfg(all(feature = "server", feature = "watch"))]
pub mod watch;

pub use broker::{Broker, FindQuery, FindQueryBuilder, GrabQuery, ProxyStream};
pub use checker::{Checker, CheckerConfig, RetryPolicy, TrustReport, TrustSignal};
pub use error::{Error, ProxyError};
#[cfg(feature = "geo")]
pub use geo::GeoDb;
pub use negotiator::{Stream, Target};
pub use parse::parse_proxy_lines;
#[cfg(feature = "persist")]
pub use persist::Store;
#[cfg(feature = "store-sqlite")]
pub use persist::{SqliteStore, SCHEMA_VERSION};
pub use provider::{config_template, load_provider_dir, Candidate, ProviderSpec};
pub use proxy::{read_ndjson, write_ndjson, Capabilities, Country, Credentials, Proxy};
pub use resolver::Resolver;
#[cfg(all(feature = "server", feature = "persist"))]
pub use scheduler::{decayed_score, next_interval, spawn_rechecker, RecheckConfig, RecheckHandle};
#[cfg(feature = "metrics")]
pub use server::{render_metrics, serve_metrics};
#[cfg(feature = "server")]
pub use server::{serve, ClientKey, Pool, PoolConfig, PoolSnapshot, ServerHandle, Strategy};
pub use stats::Stats;
pub use types::{AnonLevel, Caps, JudgeScheme, ParseProtoError, Proto, Scheme, TypeSpec};
#[cfg(all(feature = "server", feature = "watch"))]
pub use watch::{reconcile, spawn_watch, WatchHandle};
