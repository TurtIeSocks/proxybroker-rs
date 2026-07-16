//! The [`Proxy`] value type: an address, the protocols it is expected to (and confirmed to)
//! support, timing/error statistics, and geolocation.
//!
//! Deliberately **not** a connection handle. In `proxy.py` the `Proxy` owns its reader/writer
//! and the negotiator holds a back-reference to it — a reference cycle Python's GC absorbs
//! and Rust rejects. Here `Proxy` is plain data plus [`Proxy::record_attempt`]; the socket
//! lives in the checker/negotiator and is passed in. See `docs/systematic-refactor/map.md`
//! (socket ownership) and `decisions.md`.

use crate::error::ProxyError;
use crate::types::{AnonLevel, Proto, Scheme};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::IpAddr;

/// Country of an IP. DB-IP Country Lite is country-resolution only, so region/city — present
/// in proxybroker2's JSON — are always empty here. The shape is preserved for compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Country {
    pub code: String,
    pub name: String,
}

/// A proxy: where it is, what it can do, and how well it has done it.
#[derive(Debug, Clone)]
pub struct Proxy {
    pub host: IpAddr,
    pub port: u16,
    /// Protocols to check (from the provider). `proxy.py:expected_types`.
    pub expected_types: BTreeSet<Proto>,
    /// Country, or `None` when geo is disabled or the lookup missed.
    pub geo: Option<Country>,
    /// Confirmed protocols and, for HTTP, the measured anonymity level.
    types: BTreeMap<Proto, Option<AnonLevel>>,
    /// Total connection attempts. `stat["requests"]`.
    requests: u32,
    /// Error histogram, keyed by the stats bucket. `stat["errors"]` (a `Counter`).
    errors: HashMap<ProxyError, u32>,
    /// Successful round-trip times, seconds. Timeouts are excluded. `_runtimes`.
    runtimes: Vec<f64>,
}

/// `HTTP`-family protocols — a proxy supporting any of these can serve plain HTTP.
const HTTP_PROTOS: [Proto; 4] = [Proto::Http, Proto::Connect80, Proto::Socks4, Proto::Socks5];
/// `HTTPS`-family protocols — any of these can tunnel TLS.
const HTTPS_PROTOS: [Proto; 3] = [Proto::Https, Proto::Socks4, Proto::Socks5];

/// Python's `round(x, 2)`: correctly-rounded to 2 decimals, ties-to-even. The naive
/// `(x*100).round()/100` diverges because `x*100` is not exactly representable (`2.675*100`
/// becomes `267.5`, rounding up to `2.68` where CPython gives `2.67`). Rust's `{:.2}`
/// formatter is correctly-rounded (Ryū) and ties-to-even, matching CPython's decimal
/// rounding — so format-then-parse is the faithful port.
fn round2(x: f64) -> f64 {
    format!("{x:.2}").parse().unwrap()
}

impl Proxy {
    pub fn new(host: IpAddr, port: u16, expected_types: BTreeSet<Proto>) -> Self {
        Proxy {
            host,
            port,
            expected_types,
            geo: None,
            types: BTreeMap::new(),
            requests: 0,
            errors: HashMap::new(),
            runtimes: Vec::new(),
        }
    }

    /// `host:port`, IPv6 bracketed per RFC 3986 (`proxy.py:_format_host_port`). No trailing
    /// newline — `as_text` in Python appended `\n`; that belongs to the output sink, not the
    /// address.
    pub fn addr(&self) -> String {
        match self.host {
            IpAddr::V4(v4) => format!("{v4}:{}", self.port),
            IpAddr::V6(v6) => format!("[{v6}]:{}", self.port),
        }
    }

    /// Confirmed protocols and their anonymity levels.
    pub fn types(&self) -> &BTreeMap<Proto, Option<AnonLevel>> {
        &self.types
    }

    /// Record that `proto` works, with an optional anonymity level (HTTP only).
    pub fn add_type(&mut self, proto: Proto, level: Option<AnonLevel>) {
        self.types.insert(proto, level);
    }

    /// True once any protocol is confirmed. `proxy.py:is_working` (set when types is non-empty).
    pub fn is_working(&self) -> bool {
        !self.types.is_empty()
    }

    /// Error rate in `0.0..=1.0`, rounded to 2 dp. `0.0` before any request. `proxy.py:error_rate`.
    pub fn error_rate(&self) -> f64 {
        if self.requests == 0 {
            return 0.0;
        }
        let errs: u32 = self.errors.values().sum();
        round2(errs as f64 / self.requests as f64)
    }

    /// Mean successful round-trip time, seconds, rounded to 2 dp. `0.0` if none. `avg_resp_time`.
    pub fn avg_resp_time(&self) -> f64 {
        if self.runtimes.is_empty() {
            return 0.0;
        }
        round2(self.runtimes.iter().sum::<f64>() / self.runtimes.len() as f64)
    }

    /// Pool-ordering key `(error_rate, avg_resp_time)`, lower is better. `proxy.py:priority`.
    ///
    /// In Python this feeds `heapq`, which on an `f64` tie compares the `Proxy` objects
    /// themselves and raises `TypeError` (no `__lt__`). Here it is a plain tuple; callers
    /// order with a total comparison (`f64::total_cmp`), so ties are deterministic rather
    /// than fatal. Deviation recorded in `decisions.md`.
    pub fn priority(&self) -> (f64, f64) {
        (self.error_rate(), self.avg_resp_time())
    }

    /// Transport schemes this proxy can serve. `proxy.py:schemes`.
    pub fn schemes(&self) -> Vec<Scheme> {
        let mut out = Vec::new();
        if self.types.keys().any(|t| HTTP_PROTOS.contains(t)) {
            out.push(Scheme::Http);
        }
        if self.types.keys().any(|t| HTTPS_PROTOS.contains(t)) {
            out.push(Scheme::Https);
        }
        out
    }

    /// Total connection attempts made against this proxy.
    pub fn requests(&self) -> u32 {
        self.requests
    }

    /// Record one connection attempt. Mirrors the coupling in `proxy.py`: `connect()`'s
    /// `finally` does `requests += 1` and calls `log(..., err=err)`, which bumps the error
    /// bucket and appends the runtime unless the failure was a timeout.
    ///
    /// - always: `requests += 1`
    /// - `err`: increment that bucket
    /// - `runtime`: append it, **except** on a timeout (Python guards `"timeout" not in msg`)
    pub fn record_attempt(&mut self, runtime: Option<f64>, err: Option<ProxyError>) {
        self.requests += 1;
        if let Some(e) = err {
            *self.errors.entry(e).or_insert(0) += 1;
        }
        if let Some(rt) = runtime {
            if err != Some(ProxyError::Timeout) {
                self.runtimes.push(rt);
            }
        }
    }

    /// The error histogram — the port of `stat["errors"]`, consumed by `show_stats`.
    pub fn errors(&self) -> &HashMap<ProxyError, u32> {
        &self.errors
    }

    /// Confirmed types in proxybroker2's display order (`key=(len(name), name[-1])`).
    fn types_sorted(&self) -> Vec<(Proto, Option<AnonLevel>)> {
        let mut v: Vec<_> = self.types.iter().map(|(p, l)| (*p, *l)).collect();
        v.sort_by_key(|(p, _)| p.display_order_key());
        v
    }
}

/// Serialize matches `proxy.py:as_json` — a nested object, not a flat struct. This is the
/// idiomatic replacement for the `as_json()` method: `serde_json::to_string(&proxy)`.
impl Serialize for Proxy {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("Proxy", 6)?;
        st.serialize_field("host", &self.host.to_string())?;
        st.serialize_field("port", &self.port)?;

        // geo: { country: {code,name}, region:{code,name}, city } — region/city always empty
        // because DB-IP Country Lite is country-only.
        let (code, name) = match &self.geo {
            Some(c) => (c.code.as_str(), c.name.as_str()),
            None => ("", ""),
        };
        st.serialize_field(
            "geo",
            &serde_json::json!({
                "country": { "code": code, "name": name },
                "region":  { "code": "", "name": "" },
                "city": serde_json::Value::Null,
            }),
        )?;

        let types: Vec<_> = self
            .types_sorted()
            .into_iter()
            .map(|(p, l)| {
                serde_json::json!({
                    "type": p.as_str(),
                    "level": l.map(|x| x.as_str()).unwrap_or(""),
                })
            })
            .collect();
        st.serialize_field("types", &types)?;
        st.serialize_field("avg_resp_time", &self.avg_resp_time())?;
        st.serialize_field("error_rate", &self.error_rate())?;
        st.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p() -> Proxy {
        Proxy::new(
            "1.2.3.4".parse().unwrap(),
            8080,
            BTreeSet::from([Proto::Http]),
        )
    }

    #[test]
    fn addr_brackets_ipv6_only() {
        assert_eq!(p().addr(), "1.2.3.4:8080");
        let v6 = Proxy::new("2001:db8::1".parse().unwrap(), 3128, BTreeSet::new());
        assert_eq!(v6.addr(), "[2001:db8::1]:3128");
    }

    #[test]
    fn fresh_proxy_has_zero_stats() {
        let x = p();
        assert_eq!(x.error_rate(), 0.0);
        assert_eq!(x.avg_resp_time(), 0.0);
        assert!(!x.is_working());
    }

    #[test]
    fn record_attempt_tracks_requests_errors_runtimes() {
        let mut x = p();
        x.record_attempt(Some(0.5), None); // success
        x.record_attempt(Some(1.5), None); // success
        x.record_attempt(None, Some(ProxyError::ConnFailed)); // error
        assert_eq!(x.requests(), 3);
        assert_eq!(x.error_rate(), round2(1.0 / 3.0)); // 0.33
        assert_eq!(x.avg_resp_time(), 1.0); // (0.5+1.5)/2
    }

    #[test]
    fn timeout_runtime_is_excluded_like_python() {
        let mut x = p();
        // Python appends runtime only when "timeout" is not in the message — i.e. not on
        // a timeout error. A timeout with a runtime must NOT pollute avg_resp_time.
        x.record_attempt(Some(8.0), Some(ProxyError::Timeout));
        assert_eq!(x.avg_resp_time(), 0.0, "timeout runtime must be excluded");
        assert_eq!(x.requests(), 1);
        assert_eq!(x.error_rate(), 1.0);
    }

    #[test]
    fn round2_matches_python_round() {
        // Cross-checked against CPython `round(v, 2)` for each value (see commit message).
        // Pairs: (input, CPython round(input, 2)).
        let cases = [
            (0.125, 0.12), // ties-to-even, not 0.13
            (2.675, 2.67), // 2.675 is stored as 2.67499… → 2.67, not 2.68
            (0.335, 0.34),
            (1.005, 1.0),
            (0.045, 0.04),
            (0.005, 0.01),
            (1.0 / 3.0, 0.33),
            (2.0 / 3.0, 0.67),
            (99.995, 100.0),
        ];
        for (input, want) in cases {
            assert_eq!(round2(input), want, "round2({input})");
        }
    }

    #[test]
    fn schemes_follow_protocol_families() {
        let mut x = p();
        x.add_type(Proto::Http, Some(AnonLevel::High));
        assert_eq!(x.schemes(), vec![Scheme::Http]); // HTTP only
        x.add_type(Proto::Socks5, None);
        assert_eq!(x.schemes(), vec![Scheme::Http, Scheme::Https]); // SOCKS5 adds HTTPS
        assert!(x.is_working());
    }

    #[test]
    fn serializes_to_python_as_json_shape() {
        let mut x = Proxy::new(
            "1.2.3.4".parse().unwrap(),
            80,
            BTreeSet::from([Proto::Http]),
        );
        x.geo = Some(Country {
            code: "US".into(),
            name: "United States".into(),
        });
        x.add_type(Proto::Http, Some(AnonLevel::High));
        x.add_type(Proto::Connect80, None);
        let v: serde_json::Value = serde_json::to_value(&x).unwrap();

        assert_eq!(v["host"], "1.2.3.4");
        assert_eq!(v["port"], 80);
        assert_eq!(v["geo"]["country"]["code"], "US");
        assert_eq!(v["geo"]["country"]["name"], "United States");
        // display order: HTTP (len 4) before CONNECT:80 (len 10)
        assert_eq!(v["types"][0]["type"], "HTTP");
        assert_eq!(v["types"][0]["level"], "High");
        assert_eq!(v["types"][1]["type"], "CONNECT:80");
        assert_eq!(v["types"][1]["level"], ""); // no level for non-HTTP
    }
}
