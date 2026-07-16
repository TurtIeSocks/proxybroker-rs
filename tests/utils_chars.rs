//! Characterization test: `utils` helpers must reproduce proxybroker2's `utils.py` exactly.
//! Oracle (`utils_oracle.json`) generated from Python's stdlib `ipaddress` and the actual
//! `get_status_code` / `get_all_ip` functions.

use proxybroker::utils::{canonicalize_ip, get_all_ip, get_status_code};
use std::collections::BTreeMap;

#[derive(serde::Deserialize)]
struct Oracle {
    canonicalize_ip: BTreeMap<String, Option<String>>,
    get_status_code: BTreeMap<String, u16>,
    get_all_ip: BTreeMap<String, Vec<String>>,
}

fn oracle() -> Oracle {
    serde_json::from_str(include_str!("utils_oracle.json")).unwrap()
}

#[test]
fn canonicalize_ip_matches_python() {
    for (input, want) in oracle().canonicalize_ip {
        assert_eq!(
            canonicalize_ip(&input),
            want,
            "canonicalize_ip({input:?}) — including the leading-zero and zone-id cases"
        );
    }
}

#[test]
fn get_status_code_matches_python() {
    // Python default slice is resp[9:12] (the status in "HTTP/1.1 200 OK").
    for (input, want) in oracle().get_status_code {
        assert_eq!(
            get_status_code(input.as_bytes(), 9, 12),
            want,
            "get_status_code({input:?}) — 400 is the sentinel on unparseable"
        );
    }
}

#[test]
fn get_all_ip_matches_python() {
    for (input, want) in oracle().get_all_ip {
        let got: Vec<String> = get_all_ip(&input).into_iter().collect(); // BTreeSet → sorted
        assert_eq!(got, want, "get_all_ip({input:?})");
    }
}
