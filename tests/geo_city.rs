//! C7: a user-supplied MaxMind **City** DB fills `geo.region`/`geo.city`; the bundled Country-Lite
//! DB does not (that constraint is under test in `src/geo.rs::bundled_country_db_has_no_region_city`).
//!
//! Fixture: `tests/data/city-test.mmdb` — MaxMind's synthetic `GeoIP2-City-Test.mmdb` (Apache-2.0,
//! provenance in `tests/data/README.md`). It is **not** in the crate `include` list, so nothing
//! City-shaped is ever published.
#![cfg(feature = "geo")]

use proxybroker::geo::GeoDb;
use proxybroker::{Proto, Proxy};
use std::collections::BTreeSet;

#[test]
fn city_db_populates_region_and_city() {
    let db = GeoDb::open("tests/data/city-test.mmdb").expect("open city fixture");
    // 89.160.20.128 is a known City-Test record: country SE, carrying a subdivision and a city.
    let c = db
        .lookup("89.160.20.128".parse().unwrap())
        .expect("known fixture IP resolves");
    assert_eq!(c.code, "SE");

    let region = c.region.as_ref().expect("City DB carries a region");
    assert!(
        !region.code.is_empty() || !region.name.is_empty(),
        "region should be populated, got {region:?}"
    );
    let city = c.city.as_deref().expect("City DB carries a city");
    assert!(!city.is_empty(), "city should be populated");

    // ...and it flows into the JSON geo block, not the empty country-only shape.
    let mut p = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
    p.geo = Some(c);
    p.add_type(Proto::Http, None);
    let v = serde_json::to_value(&p).unwrap();
    let r = &v["geo"]["region"];
    assert!(
        !r["code"].as_str().unwrap().is_empty() || !r["name"].as_str().unwrap().is_empty(),
        "serialized region should be non-empty: {v}"
    );
    assert!(
        v["geo"]["city"].is_string(),
        "serialized city should be a string: {v}"
    );
    assert!(!v["geo"]["city"].as_str().unwrap().is_empty());
}
