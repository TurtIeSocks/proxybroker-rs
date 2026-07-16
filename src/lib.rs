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
//! let mut stream = broker.find(FindQuery {
//!     types: vec![TypeSpec::any(Proto::Http)],
//!     limit: Some(10),
//!     ..Default::default()
//! }).await?;
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
pub mod provider;
pub mod proxy;
pub mod resolver;
#[cfg(feature = "server")]
pub mod server;
pub mod stats;
pub mod types;
pub mod utils;

pub use broker::{Broker, FindQuery, GrabQuery, ProxyStream};
pub use checker::{Checker, CheckerConfig};
pub use error::{Error, ProxyError};
#[cfg(feature = "geo")]
pub use geo::GeoDb;
pub use negotiator::{Stream, Target};
pub use provider::{config_template, load_provider_dir, Candidate, ProviderSpec};
pub use proxy::{Country, Proxy};
pub use resolver::Resolver;
#[cfg(feature = "server")]
pub use server::{serve, Pool, PoolConfig, ServerHandle};
pub use stats::Stats;
pub use types::{AnonLevel, JudgeScheme, ParseProtoError, Proto, Scheme, TypeSpec};
