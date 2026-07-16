//! Country lookup for IP addresses, backed by a MaxMind-format database.
//!
//! The bundled database is DB-IP Country Lite (CC BY 4.0), **not** MaxMind GeoLite2 — see
//! `LICENSE-DATA` and `docs/systematic-refactor/research.md` for why. Any MMDB the caller
//! supplies works too, including their own lawfully-licensed GeoLite2.
//!
//! Gated behind the `geo` feature; `geo-bundled` additionally embeds the database.

use crate::proxy::Country;
use maxminddb::Reader;
use std::net::IpAddr;
use std::path::Path;

/// An opened country database.
pub struct GeoDb {
    reader: Reader<Vec<u8>>,
}

impl GeoDb {
    /// The database embedded at build time (requires the `geo-bundled` feature).
    ///
    /// Attribution (CC BY 4.0): *IP Geolocation by DB-IP (https://db-ip.com)*.
    #[cfg(feature = "geo-bundled")]
    pub fn bundled() -> Result<Self, maxminddb::MaxMindDbError> {
        const DB: &[u8] = include_bytes!("../data/dbip-country-lite.mmdb");
        Ok(GeoDb {
            reader: Reader::from_source(DB.to_vec())?,
        })
    }

    /// Open a user-supplied MMDB (`--geo-db /path`). Lets the caller bring a more accurate
    /// or differently-licensed database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, maxminddb::MaxMindDbError> {
        let bytes = std::fs::read(path).map_err(maxminddb::MaxMindDbError::from)?;
        Ok(GeoDb {
            reader: Reader::from_source(bytes)?,
        })
    }

    /// The country of `ip`, or `None` if the database has no record for it.
    ///
    /// Two-stage lookup per maxminddb 0.29 (`lookup(ip)?` → `LookupResult`, then
    /// `decode::<T>()?` → `Option<T>`), verified against the crate rather than recalled.
    pub fn lookup(&self, ip: IpAddr) -> Option<Country> {
        let rec: maxminddb::geoip2::Country = self.reader.lookup(ip).ok()?.decode().ok()??;
        let country = rec.country;
        let code = country.iso_code?;
        Some(Country {
            code: code.to_owned(),
            name: country.names.english.unwrap_or(code).to_owned(),
        })
    }
}

#[cfg(all(test, feature = "geo-bundled"))]
mod tests {
    use super::*;

    #[test]
    fn bundled_db_resolves_known_ips() {
        let db = GeoDb::bundled().unwrap();
        assert_eq!(db.lookup("8.8.8.8".parse().unwrap()).unwrap().code, "US");
        assert_eq!(db.lookup("1.1.1.1".parse().unwrap()).unwrap().code, "AU");
        // Name comes from the DB-IP record's English name.
        assert!(!db
            .lookup("8.8.8.8".parse().unwrap())
            .unwrap()
            .name
            .is_empty());
    }
}
