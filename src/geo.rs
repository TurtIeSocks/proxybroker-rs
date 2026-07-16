//! Country lookup for IP addresses, backed by a MaxMind-format database.
//!
//! The bundled database is DB-IP Country Lite (CC BY 4.0), **not** MaxMind GeoLite2 — see
//! `LICENSE-DATA` and `docs/systematic-refactor/research.md` for why. Any MMDB the caller
//! supplies works too, including their own lawfully-licensed GeoLite2.
//!
//! Gated behind the `geo` feature; `geo-bundled` additionally embeds the database.

use crate::proxy::{Country, Region};
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
    ///
    /// One code path serves both DB kinds: `geoip2::City`'s fields are all optional/defaulted, so a
    /// Country-only record (the bundled DB-IP) decodes cleanly with empty `subdivisions`/`city` —
    /// `region`/`city` then fill in only when the caller's DB actually carries them (C7).
    pub fn lookup(&self, ip: IpAddr) -> Option<Country> {
        let rec: maxminddb::geoip2::City = self.reader.lookup(ip).ok()?.decode().ok()??;
        let code = rec.country.iso_code?;
        let name = rec.country.names.english.unwrap_or(code).to_owned();
        // Subdivisions run largest→smallest; the first is the top-level region (state/province).
        let region = rec
            .subdivisions
            .first()
            .map(|s| Region {
                code: s.iso_code.unwrap_or_default().to_owned(),
                name: s.names.english.unwrap_or_default().to_owned(),
            })
            .filter(|r| !(r.code.is_empty() && r.name.is_empty()));
        let city = rec.city.names.english.map(str::to_owned);
        Some(Country {
            code: code.to_owned(),
            name,
            region,
            city,
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

    #[test]
    fn bundled_country_db_has_no_region_city() {
        // The hard C7 constraint, executable: the bundled DB is Country-only, so decoding it as
        // City yields empty subdivisions/city — region/city stay None. No City data is shipped.
        let db = GeoDb::bundled().unwrap();
        let c = db.lookup("8.8.8.8".parse().unwrap()).unwrap();
        assert_eq!(c.region, None);
        assert_eq!(c.city, None);
    }
}
