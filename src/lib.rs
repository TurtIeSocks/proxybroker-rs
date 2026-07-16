//! # proxybroker
//!
//! Find, check, and serve public HTTP(S) and SOCKS4/5 proxies.
//!
//! A Rust rewrite of [proxybroker2](https://github.com/bluet/proxybroker2). See `NOTICE`
//! for attribution and a statement of changes.
//!
//! **Status: in development.** The module tree is being built out against
//! `docs/systematic-refactor/map.md`.

pub mod error;
pub mod parse;
pub mod types;
pub mod utils;

pub use error::{Error, ProxyError};
pub use types::{AnonLevel, JudgeScheme, ParseProtoError, Proto, Scheme, TypeSpec};

/// Country lookup against a MaxMind-format database.
///
/// Spike: verifies maxminddb 0.29 + the bundled DB-IP database on stable Rust. Will be
/// replaced by `geo.rs` proper. Everything below was checked against the compiler, not
/// recalled:
///
/// - `Reader::lookup(ip) -> Result<LookupResult<'_, S>, MaxMindDbError>`
/// - `LookupResult::decode::<T>() -> Result<Option<T>, MaxMindDbError>` (genuinely two-stage)
/// - `geoip2::Country.country` is **not** an `Option`; only `.iso_code` is.
///   (Closes `research.md` open question #3.)
#[cfg(feature = "geo")]
pub fn country_of(db: &str, ip: std::net::IpAddr) -> Option<String> {
    let reader = maxminddb::Reader::open_readfile(db).ok()?;
    let found = reader.lookup(ip).ok()?;
    let rec: maxminddb::geoip2::Country = found.decode().ok()??;
    rec.country.iso_code.map(str::to_string)
}

/// Spike: hickory-resolver 0.26's import path.
///
/// `research.md` open question #7 called this "the one import worth confirming on the
/// first build", and it was right to. The first research pass fabricated
/// `name_server::TokioConnectionProvider`; that module is **private** in 0.26.1, which
/// the compiler confirms. The adversarial pass caught it. Real path below.
#[allow(dead_code)]
pub fn resolver_spike() -> Result<hickory_resolver::TokioResolver, hickory_resolver::net::NetError>
{
    hickory_resolver::Resolver::builder_tokio()?.build()
}

#[cfg(all(test, feature = "geo"))]
mod geo_spike_tests {
    #[test]
    fn dbip_country_lite_resolves_known_ips() {
        let db = concat!(env!("CARGO_MANIFEST_DIR"), "/data/dbip-country-lite.mmdb");
        for (ip, want) in [
            ("8.8.8.8", "US"),
            ("1.1.1.1", "AU"),
            ("77.88.55.77", "RU"),
            // DB-IP places Google's IPv6 anycast DNS in CA, not US. Not a bug: a live
            // datapoint on the DB-IP-vs-GeoLite2 accuracy delta (research.md open
            // question #9), and why the user-supplied --geo-db override is non-negotiable.
            ("2001:4860:4860::8888", "CA"),
        ] {
            assert_eq!(
                super::country_of(db, ip.parse().unwrap()).as_deref(),
                Some(want),
                "lookup {ip}"
            );
        }
    }
}
