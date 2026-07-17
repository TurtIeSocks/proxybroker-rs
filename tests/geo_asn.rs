//! C8: a user-supplied MaxMind **ASN** DB (`--asn-db`) attributes each proxy to its Autonomous
//! System (`proxy.asn`); the bundled Country-Lite DB carries none (that constraint is under test in
//! `src/geo.rs::bundled_db_carries_no_asn`).
//!
//! Fixture: `tests/data/GeoLite2-ASN-Test.mmdb` — MaxMind's synthetic ASN test database (Apache-2.0,
//! provenance in `tests/data/README.md`), the same upstream as `city-test.mmdb`. It is **not** in
//! the crate `include` list, so nothing ASN-shaped is ever published.
#![cfg(feature = "geo")]

use proxybroker::geo::GeoDb;
use proxybroker::{Asn, Proto, Proxy};
use std::collections::BTreeSet;

#[test]
fn asn_db_resolves_number_and_org() {
    let db = GeoDb::open("tests/data/GeoLite2-ASN-Test.mmdb").expect("open asn fixture");

    // 1.128.0.0 is a known ASN-Test record: AS1221, owned by Telstra.
    let a = db
        .lookup_asn("1.128.0.0".parse().unwrap())
        .expect("known fixture IP resolves");
    assert_eq!(
        a,
        Asn {
            number: 1221,
            org: Some("Telstra Pty Ltd".into()),
        }
    );

    // ...and it flows into the JSON `asn` object.
    let mut p = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
    p.asn = Some(a);
    p.add_type(Proto::Http, None);
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v["asn"]["number"], 1221);
    assert_eq!(v["asn"]["org"], "Telstra Pty Ltd");
}

#[test]
fn asn_db_number_without_org_serializes_org_null() {
    // 216.160.83.56 is a known record carrying a number (AS209) but no organization — the org path
    // must degrade to None, and serialize as null, not an empty string.
    let db = GeoDb::open("tests/data/GeoLite2-ASN-Test.mmdb").expect("open asn fixture");
    let a = db
        .lookup_asn("216.160.83.56".parse().unwrap())
        .expect("known fixture IP resolves");
    assert_eq!(a.number, 209);
    assert_eq!(a.org, None);

    let mut p = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
    p.asn = Some(a);
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v["asn"]["number"], 209);
    assert_eq!(v["asn"]["org"], serde_json::Value::Null);
}

#[test]
fn asn_db_misses_return_none() {
    // An IP with no ASN record resolves to None (no attribution), not an error.
    let db = GeoDb::open("tests/data/GeoLite2-ASN-Test.mmdb").expect("open asn fixture");
    assert_eq!(db.lookup_asn("175.16.199.0".parse().unwrap()), None);
}
