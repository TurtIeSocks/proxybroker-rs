//! Spike: verify maxminddb 0.29 + DB-IP Country Lite end-to-end on stable Rust.
//!
//! Verified against the real crate, not from memory:
//!   - `Reader::lookup(ip) -> Result<LookupResult<'_, S>, MaxMindDbError>`
//!   - `LookupResult::decode::<T>() -> Result<Option<T>, MaxMindDbError>`  (two-stage)
//!   - `geoip2::Country.country` is NOT an Option; `country.iso_code` IS `Option<&str>`.
#[cfg(feature = "geo")]
pub fn country_of(db: &str, ip: std::net::IpAddr) -> Option<String> {
    let reader = maxminddb::Reader::open_readfile(db).ok()?;
    let found = reader.lookup(ip).ok()?;
    let rec: maxminddb::geoip2::Country = found.decode().ok()??;
    rec.country.iso_code.map(str::to_string)
}

#[cfg(all(test, feature = "geo"))]
mod tests {
    #[test]
    fn dbip_country_lite_resolves_known_ips() {
        let db = concat!(env!("CARGO_MANIFEST_DIR"), "/data/dbip-country-lite.mmdb");
        for (ip, want) in [
            ("8.8.8.8", "US"),
            ("1.1.1.1", "AU"),
            ("77.88.55.77", "RU"),
            // DB-IP places Google's IPv6 anycast DNS in CA, not US. Not a bug: a live
            // datapoint on the DB-IP-vs-GeoLite2 accuracy delta, and why the
            // user-supplied --geo-db override is non-negotiable. See research.md Q9.
            ("2001:4860:4860::8888", "CA"),
        ] {
            assert_eq!(super::country_of(db, ip.parse().unwrap()).as_deref(), Some(want), "lookup {ip}");
        }
    }
}

/// Spike: hickory-resolver 0.26 import path. research.md open question #7 flagged this as
/// the one import to verify on the first build, and it was right to: the first research
/// pass fabricated `name_server::TokioConnectionProvider` (that module is PRIVATE in
/// 0.26.1). Verified below against the crate source, confirmed by the compiler:
///   - `hickory_resolver::net::runtime::TokioRuntimeProvider` (via `pub use hickory_net as net`)
///   - `Resolver::builder_tokio() -> Result<ResolverBuilder<TokioRuntimeProvider>, NetError>`
///   - `ResolverBuilder::build(self) -> Result<Resolver<P>, NetError>`  (returns Result)
#[allow(dead_code)]
pub fn resolver_spike() -> Result<hickory_resolver::TokioResolver, hickory_resolver::net::NetError> {
    hickory_resolver::Resolver::builder_tokio()?.build()
}
