//! P1 — bundled provider registry: offline guards for the curated source list.
//!
//! Two concerns, both network-free:
//!   1. Format archetypes — the 3 body formats the Tier-A sources use all parse under the generic
//!      `find_addrs_global` scanner (recorded real samples in `tests/data/providers/`).
//!   2. Registry integrity — the bundled list reaches the P1 target, and every entry is
//!      well-formed (unique http(s) URL, supported kind, valid protocols). Catches a typo'd or
//!      malformed addition offline; live liveness is the separate scheduled audit
//!      (`provider_audit.rs`), never a unit test.

use proxybroker::provider::{bundled_registry, ProviderSpec};
use proxybroker::types::Proto;
use std::collections::BTreeSet;

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!("tests/data/providers/{name}"))
        .unwrap_or_else(|e| panic!("read fixture {name}: {e}"))
}

/// Each of the 3 Tier-A body formats extracts cleanly under the default scanner, with the declared
/// protocol tagged onto every candidate. If a source's format ever needs a custom `pattern`, one of
/// these archetypes is where that shows up.
#[test]
fn format_archetypes_extract_under_the_default_scanner() {
    let cases = [
        (
            "fmt-plain-ipport.txt",
            "plain ip:port per line (TheSpeedX/monosans/hookzof/APIs)",
        ),
        (
            "fmt-colon-country.txt",
            "ip:port:country colon-delimited (hideip.me)",
        ),
        ("fmt-scheme-prefixed.txt", "scheme://ip:port (proxifly)"),
    ];
    for (file, desc) in cases {
        let body = fixture(file);
        let got = ProviderSpec::new("http://archetype/", &[Proto::Http]).extract(&body);
        assert!(
            got.len() >= 5,
            "{desc}: expected >=5 candidates from {file}, got {}",
            got.len()
        );
        assert!(
            got.iter().all(|c| c.protocols.contains(&Proto::Http)),
            "{desc}: every candidate should carry the declared protocol"
        );
    }
}

/// The bundled registry hits the P1 target and every entry is well-formed. A malformed or duplicate
/// addition trips here offline, before it can silently yield nothing in production.
#[test]
fn bundled_registry_is_curated_and_well_formed() {
    let reg = bundled_registry();

    assert!(
        reg.len() >= 50,
        "P1 target: expected >=50 bundled providers, got {}",
        reg.len()
    );

    // Every URL is a well-formed http(s) endpoint.
    for s in &reg {
        assert!(
            s.url.starts_with("http://") || s.url.starts_with("https://"),
            "provider URL must be http(s): {:?}",
            s.url
        );
    }

    // No duplicate URLs (a copy-paste slip would silently double-fetch one source).
    let unique: BTreeSet<&str> = reg.iter().map(|s| s.url.as_str()).collect();
    assert_eq!(
        unique.len(),
        reg.len(),
        "duplicate provider URL(s) in the registry"
    );

    // Only the supported `simple` kind (None or "simple"); anything else is a silently-broken GET.
    for s in &reg {
        assert!(
            matches!(s.kind.as_deref(), None | Some("simple")),
            "unsupported provider kind {:?} for {}",
            s.kind,
            s.url
        );
    }
}

/// Protocol coverage: the expanded set carries every protocol family, so a `find --types SOCKS5`
/// (etc.) has bundled sources to draw from — not just HTTP.
#[test]
fn registry_covers_all_protocol_families() {
    let reg = bundled_registry();
    let declared: BTreeSet<Proto> = reg
        .iter()
        .flat_map(|s| s.protocols.iter().copied())
        .collect();
    for p in [Proto::Http, Proto::Https, Proto::Socks4, Proto::Socks5] {
        assert!(declared.contains(&p), "no bundled source declares {p:?}");
    }
}
