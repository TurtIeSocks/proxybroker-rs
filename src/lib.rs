//! # proxybroker
//!
//! Find, check, and serve public HTTP(S) and SOCKS4/5 proxies.
//!
//! A Rust rewrite of [proxybroker2](https://github.com/bluet/proxybroker2). See `NOTICE`
//! for attribution and a statement of changes.
//!
//! **Status: in development.** The module tree is being built out against
//! `docs/systematic-refactor/map.md`.

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
pub mod types;
pub mod utils;

pub use broker::{Broker, FindQuery, GrabQuery, ProxyStream};
pub use checker::{Checker, CheckerConfig};
pub use error::{Error, ProxyError};
#[cfg(feature = "geo")]
pub use geo::GeoDb;
pub use negotiator::{Stream, Target};
pub use provider::{Candidate, ProviderSpec};
pub use proxy::{Country, Proxy};
pub use resolver::Resolver;
pub use types::{AnonLevel, JudgeScheme, ParseProtoError, Proto, Scheme, TypeSpec};
