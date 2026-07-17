//! The [`Proxy`] value type: an address, the protocols it is expected to (and confirmed to)
//! support, timing/error statistics, and geolocation.
//!
//! Deliberately **not** a connection handle. In `proxy.py` the `Proxy` owns its reader/writer
//! and the negotiator holds a back-reference to it — a reference cycle Python's GC absorbs
//! and Rust rejects. Here `Proxy` is plain data plus [`Proxy::record_attempt`]; the socket
//! lives in the checker/negotiator and is passed in. See `docs/systematic-refactor/map.md`
//! (socket ownership) and `decisions.md`.

use crate::checker::TrustReport;
use crate::error::ProxyError;
use crate::types::{AnonLevel, Caps, Proto, Scheme};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::IpAddr;

/// Country of an IP. The bundled DB-IP Country-Lite is country-resolution only, so `region`/`city`
/// — present in proxybroker2's JSON — stay `None` for it. They populate only when the caller opens
/// a richer MaxMind **City** DB via `--geo-db` (C7); the JSON shape is fixed either way.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Country {
    pub code: String,
    pub name: String,
    /// Subdivision (state/province), from a City DB only.
    pub region: Option<Region>,
    /// City name, from a City DB only.
    pub city: Option<String>,
}

/// A subdivision (ISO 3166-2 state/province), populated only from a City DB.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Region {
    pub code: String,
    pub name: String,
}

/// Autonomous System attribution for an IP (C8): the network that owns it. Populated only from a
/// user-supplied ASN database (`--asn-db`) — a separate MaxMind/DB-IP file from the country/city DB,
/// so no ASN data is bundled (the CC BY 4.0 hygiene constraint: ship the hook, bundle nothing).
/// Orthogonal to [`Country`]: geolocation and network ownership are independent facts about an IP.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Asn {
    /// The autonomous system number, e.g. `15169`.
    pub number: u32,
    /// The organization that owns the ASN, e.g. `"Google LLC"`. `None` when the DB omits it.
    pub org: Option<String>,
}

/// Username/password for an authenticated upstream proxy (B8). Carried on [`Proxy`], applied by
/// the negotiator (SOCKS5 RFC 1929) and the server (HTTP `Proxy-Authorization`). Never serialized
/// (kept out of `--format json`), and its [`std::fmt::Debug`] is redacted so a `Proxy` debug print
/// cannot leak the secret.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials").finish_non_exhaustive()
    }
}

/// A proxy: where it is, what it can do, and how well it has done it.
///
/// `PartialEq` (not `Eq` — the `runtimes: Vec<f64>` field blocks it) lets a save/load round-trip
/// be asserted. The round-trip is **lossy on stats by design**: `Serialize` emits the computed
/// `avg_resp_time`/`error_rate` for humans, but `Deserialize` restores only the persistent
/// identity (host, port, geo, confirmed types) with `requests`/`errors`/`runtimes` empty — a
/// loaded proxy's timing history restarts. `auth` is never serialized (secrets stay out of JSON).
#[derive(Debug, Clone, PartialEq)]
pub struct Proxy {
    pub host: IpAddr,
    pub port: u16,
    /// Protocols to check (from the provider). `proxy.py:expected_types`.
    pub expected_types: BTreeSet<Proto>,
    /// Country, or `None` when geo is disabled or the lookup missed.
    pub geo: Option<Country>,
    /// Autonomous System (C8), or `None` unless a `--asn-db` was supplied and resolved this IP.
    pub asn: Option<Asn>,
    /// Confirmed protocols and, for HTTP, the measured anonymity level.
    types: BTreeMap<Proto, Option<AnonLevel>>,
    /// Total connection attempts. `stat["requests"]`.
    requests: u32,
    /// Error histogram, keyed by the stats bucket. `stat["errors"]` (a `Counter`).
    errors: HashMap<ProxyError, u32>,
    /// Successful round-trip times, seconds. Timeouts are excluded. `_runtimes`.
    runtimes: Vec<f64>,
    /// Upstream proxy credentials (B8), for a paid/authenticated proxy. `None` for scraped
    /// candidates; set only via BYO/URL loading. Never serialized.
    auth: Option<Credentials>,
    /// Capability profile (A4), OR-accumulated across confirmed protocols. Not serialized (stays
    /// out of the parity JSON); exposed via [`Proxy::caps`]/[`Proxy::capabilities`] + CLI filters.
    caps: Caps,
    /// Honeypot/trust verdict (A6). Empty (trusted) unless `--trust-check` ran. Not serialized.
    trust: TrustReport,
}

/// A proxy's full capability profile (A4): the recorded [`Caps`] plus CONNECT:25 support derived
/// from the confirmed types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capabilities {
    pub cookie_echo: bool,
    pub referer_echo: bool,
    /// A confirmed SMTP (CONNECT:25) tunnel.
    pub connect25: bool,
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

/// The `q`-quantile (`q` in `0.0..=1.0`) of `data` by linear interpolation between closest ranks
/// (numpy "linear" / type-7), rounded to 2 dp. Empty → `0.0`. Does not require sorted input.
///
/// Linear (not nearest-rank) so `p50` is the true median: `percentile(&[1.0, 2.0], 0.5) == 1.5`.
pub fn percentile(data: &[f64], q: f64) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_by(f64::total_cmp); // the crate's tie-safe ordering (cf. `priority`)
    let rank = q * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    let frac = rank - lo as f64;
    round2(sorted[lo] + frac * (sorted[hi] - sorted[lo]))
}

impl Proxy {
    pub fn new(host: IpAddr, port: u16, expected_types: BTreeSet<Proto>) -> Self {
        Proxy {
            host,
            port,
            expected_types,
            geo: None,
            asn: None,
            types: BTreeMap::new(),
            requests: 0,
            errors: HashMap::new(),
            runtimes: Vec::new(),
            auth: None,
            caps: Caps::default(),
            trust: TrustReport::default(),
        }
    }

    /// Rebuild a proxy from persisted aggregates (D2 warm start) so `priority()` / `error_rate()` /
    /// `avg_resp_time()` reflect its stored history. Seeds the private stat fields directly — the
    /// only mutating constructor beyond [`Proxy::new`], used only by the `persist` store.
    ///
    /// The reconstruction is **lossy on the error histogram, faithful on the error rate**:
    /// `errors_total` is seeded under a single bucket, so per-bucket breakdowns are gone but
    /// `error_rate()` is exact. `avg_resp_time` is seeded as one runtime sample so `avg_resp_time()`
    /// returns it. Warm start only needs `priority()`, never the per-bucket breakdown (a
    /// fresh-session stat). Recorded in `decisions.md`.
    pub fn restored(
        host: IpAddr,
        port: u16,
        types: BTreeMap<Proto, Option<AnonLevel>>,
        requests: u32,
        errors_total: u32,
        avg_resp_time: f64,
    ) -> Proxy {
        let runtimes = if avg_resp_time > 0.0 {
            vec![avg_resp_time]
        } else {
            Vec::new()
        };
        let mut errors = HashMap::new();
        if errors_total > 0 {
            errors.insert(ProxyError::BadResponse, errors_total);
        }
        Proxy {
            host,
            port,
            expected_types: BTreeSet::new(),
            geo: None,
            asn: None,
            types,
            requests,
            errors,
            runtimes,
            auth: None,
            caps: Caps::default(),
            trust: TrustReport::default(),
        }
    }

    /// Attach upstream credentials (builder-style). `scheme://user:pass@host:port` loading sets
    /// these; scraped proxies never carry them.
    pub fn with_auth(mut self, creds: Credentials) -> Self {
        self.auth = Some(creds);
        self
    }

    /// The upstream credentials, if any (B8).
    pub fn auth(&self) -> Option<&Credentials> {
        self.auth.as_ref()
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

    /// Remove a confirmed protocol. Used by the checker's strict-mode filtering, which drops
    /// types whose anonymity level does not match the request (`checker.py:_types_passed`).
    pub fn remove_type(&mut self, proto: Proto) {
        self.types.remove(&proto);
    }

    /// True once any protocol is confirmed. `proxy.py:is_working` (set when types is non-empty).
    pub fn is_working(&self) -> bool {
        !self.types.is_empty()
    }

    /// The recorded capability profile (A4), OR-accumulated across confirmed protocols.
    pub fn caps(&self) -> Caps {
        self.caps
    }

    /// Fold one working attempt's observed capabilities into the stored profile (OR): a proxy
    /// keeps every capability it ever demonstrated, across protocols.
    pub fn record_caps(&mut self, c: Caps) {
        self.caps.cookie_echo |= c.cookie_echo;
        self.caps.referer_echo |= c.referer_echo;
    }

    /// The full capability profile: the recorded [`Caps`] plus CONNECT:25 support, derived from
    /// the confirmed types (a granted SMTP tunnel is already a confirmed `Connect25`).
    pub fn capabilities(&self) -> Capabilities {
        Capabilities {
            cookie_echo: self.caps.cookie_echo,
            referer_echo: self.caps.referer_echo,
            connect25: self.types.contains_key(&Proto::Connect25),
        }
    }

    /// The honeypot/trust verdict (A6). Empty (trusted) unless `--trust-check` assessed this proxy.
    pub fn trust(&self) -> &TrustReport {
        &self.trust
    }

    /// Fold a working attempt's trust verdict into the stored one (A6): signals **union** across
    /// protocols (deduped), mirroring [`Proxy::record_caps`]. A plain overwrite would let a clean
    /// later protocol — e.g. a CONNECT:25 tunnel, which is never assessed and always "trusted", and
    /// is checked last — erase an earlier `InjectedHeader`, silently admitting a multi-protocol
    /// honeypot past `--require-trusted`.
    pub fn record_trust(&mut self, r: TrustReport) {
        for s in r.signals {
            if !self.trust.signals.contains(&s) {
                self.trust.signals.push(s);
            }
        }
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

    /// The `q`-quantile of this proxy's successful round-trip times, seconds, 2 dp. `0.0` if none.
    pub fn percentile(&self, q: f64) -> f64 {
        percentile(&self.runtimes, q)
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
        let mut st = s.serialize_struct("Proxy", 7)?;
        st.serialize_field("host", &self.host.to_string())?;
        st.serialize_field("port", &self.port)?;

        // geo: { country: {code,name}, region:{code,name}, city }. region/city fill only from a
        // user City DB (C7); the bundled Country-Lite leaves them empty/null — the shape is fixed.
        let (code, name) = match &self.geo {
            Some(c) => (c.code.as_str(), c.name.as_str()),
            None => ("", ""),
        };
        let region = self.geo.as_ref().and_then(|c| c.region.as_ref());
        st.serialize_field(
            "geo",
            &serde_json::json!({
                "country": { "code": code, "name": name },
                "region": {
                    "code": region.map_or("", |r| r.code.as_str()),
                    "name": region.map_or("", |r| r.name.as_str()),
                },
                "city": self.geo.as_ref().and_then(|c| c.city.as_deref()),
            }),
        )?;

        // asn: null when no --asn-db resolved it (the default), else { number, org }. Unlike geo's
        // fixed-empty-shape (proxybroker2 parity), ASN is a new field with no parity duty, so null
        // is the unambiguous "not looked up" marker (ASN 0 is a real reserved number).
        st.serialize_field(
            "asn",
            &self
                .asn
                .as_ref()
                .map(|a| serde_json::json!({ "number": a.number, "org": a.org })),
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

/// Read a `Proxy` back from the JSON its [`Serialize`] emits, so a saved pool reloads without
/// re-checking (Wave 1 C2). Restores the persistent identity — host, port, geo, confirmed
/// types — and leaves `expected_types`/`requests`/`errors`/`runtimes` empty (they are not
/// serialized; the computed `avg_resp_time`/`error_rate` fields are read and discarded).
impl<'de> serde::Deserialize<'de> for Proxy {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;

        #[derive(serde::Deserialize, Default)]
        struct RawCountry {
            #[serde(default)]
            code: String,
            #[serde(default)]
            name: String,
        }
        // region/city sit beside `country` under `geo` in the JSON (see Serialize), not inside it.
        #[derive(serde::Deserialize, Default)]
        struct RawRegion {
            #[serde(default)]
            code: String,
            #[serde(default)]
            name: String,
        }
        #[derive(serde::Deserialize, Default)]
        struct RawGeo {
            #[serde(default)]
            country: RawCountry,
            #[serde(default)]
            region: RawRegion,
            #[serde(default)]
            city: Option<String>,
        }
        // asn sits top-level beside `geo` (network attribution is orthogonal to geolocation).
        // Serialize emits null when absent, so `Option<RawAsn>` maps null → None directly.
        #[derive(serde::Deserialize, Default)]
        struct RawAsn {
            #[serde(default)]
            number: u32,
            #[serde(default)]
            org: Option<String>,
        }
        #[derive(serde::Deserialize)]
        struct RawType {
            #[serde(rename = "type")]
            proto: String,
            #[serde(default)]
            level: String,
        }
        #[derive(serde::Deserialize)]
        struct Raw {
            host: String,
            port: u16,
            #[serde(default)]
            geo: RawGeo,
            #[serde(default)]
            asn: Option<RawAsn>,
            #[serde(default)]
            types: Vec<RawType>,
            // avg_resp_time / error_rate are present in the JSON but computed, not stored —
            // serde ignores them (no field here).
        }

        let raw = Raw::deserialize(d)?;
        let host: IpAddr = raw.host.parse().map_err(D::Error::custom)?;
        // Serialize emits an empty code for "no geo"; map that back to None.
        let code = raw.geo.country.code;
        let geo = if code.is_empty() {
            None
        } else {
            // Round-trip region/city too (a City DB populates them): empty region → None,
            // matching how Serialize renders an absent region as {code:"",name:""}.
            let region = if raw.geo.region.code.is_empty() && raw.geo.region.name.is_empty() {
                None
            } else {
                Some(Region {
                    code: raw.geo.region.code,
                    name: raw.geo.region.name,
                })
            };
            Some(Country {
                code,
                name: raw.geo.country.name,
                region,
                city: raw.geo.city.filter(|s| !s.is_empty()),
            })
        };
        let mut types = BTreeMap::new();
        for t in raw.types {
            let proto: Proto = t.proto.parse().map_err(D::Error::custom)?;
            let level = if t.level.is_empty() {
                None
            } else {
                Some(t.level.parse::<AnonLevel>().map_err(D::Error::custom)?)
            };
            types.insert(proto, level);
        }

        let asn = raw.asn.map(|a| Asn {
            number: a.number,
            org: a.org,
        });

        Ok(Proxy {
            host,
            port: raw.port,
            expected_types: BTreeSet::new(),
            geo,
            asn,
            types,
            requests: 0,
            errors: HashMap::new(),
            runtimes: Vec::new(),
            auth: None, // secrets are never serialized, so never deserialized either
            caps: Caps::default(), // capabilities are not serialized; re-measured on a fresh check
            trust: TrustReport::default(), // trust is not serialized; re-assessed on a fresh check
        })
    }
}

/// Write proxies as NDJSON — one `serde_json` object per line, the exact bytes `Format::Json`
/// already emits to stdout. This is the roadmap's deliberate minimal persistence step (flat file,
/// no schema/index/migration) that must ship before SQLite; see `docs/roadmap` (C2).
pub fn write_ndjson<W: std::io::Write>(mut writer: W, proxies: &[Proxy]) -> std::io::Result<()> {
    for p in proxies {
        // to_writer can't fail on a Proxy (no non-string map keys, no NaN in the wire shape), but
        // the writer can — surface that as the io::Error it already is.
        serde_json::to_writer(&mut writer, p).map_err(std::io::Error::from)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

/// Read proxies from NDJSON. Blank lines are skipped; the first malformed line aborts with an
/// [`std::io::ErrorKind::InvalidData`] error. Reader-generic so tests use an in-memory cursor.
pub fn read_ndjson<R: std::io::BufRead>(reader: R) -> std::io::Result<Vec<Proxy>> {
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let p = serde_json::from_str(&line).map_err(std::io::Error::from)?;
        out.push(p);
    }
    Ok(out)
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
    fn record_caps_or_accumulates() {
        let mut x = p();
        x.record_caps(Caps {
            cookie_echo: true,
            referer_echo: false,
        });
        x.record_caps(Caps {
            cookie_echo: false,
            referer_echo: true,
        });
        // OR-fold: a proxy keeps every capability it ever demonstrated.
        assert_eq!(
            x.caps(),
            Caps {
                cookie_echo: true,
                referer_echo: true
            }
        );
    }

    #[test]
    fn capabilities_derives_connect25() {
        let mut x = p();
        assert!(!x.capabilities().connect25);
        x.add_type(Proto::Connect25, None);
        assert!(x.capabilities().connect25);
    }

    #[test]
    fn record_trust_unions_across_protocols() {
        use crate::checker::TrustSignal;
        let mut x = p();
        x.record_trust(TrustReport {
            signals: vec![TrustSignal::InjectedHeader],
        });
        // A later clean protocol (e.g. CONNECT:25, always trusted, checked last) must NOT erase it.
        x.record_trust(TrustReport::default());
        assert!(!x.trust().trusted());
        assert_eq!(x.trust().signals, vec![TrustSignal::InjectedHeader]);
        // Deduped: re-recording the same signal does not accumulate.
        x.record_trust(TrustReport {
            signals: vec![TrustSignal::InjectedHeader],
        });
        assert_eq!(x.trust().signals.len(), 1);
    }

    #[test]
    fn percentile_linear_interpolation() {
        // numpy "linear" (type-7): rank = q*(n-1), interpolate between the bracketing ranks.
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.5), 2.5); // rank 1.5 → 2 + .5*(3-2)
        assert_eq!(percentile(&[], 0.5), 0.0); // empty
        assert_eq!(percentile(&[5.0], 0.9), 5.0); // single element
                                                  // 1..=10, cross-checked against numpy.percentile(interpolation="linear"):
                                                  //   p90 → rank 8.1 → 9 + .1*(10-9) = 9.1 ; p95 → rank 8.55 → 9 + .55 = 9.55
        assert_eq!(
            percentile(&(1..=10).map(f64::from).collect::<Vec<_>>(), 0.90),
            9.1
        );
        assert_eq!(
            percentile(&(1..=10).map(f64::from).collect::<Vec<_>>(), 0.95),
            9.55
        );
        // Unsorted input must not matter.
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.5), 2.5);
    }

    #[test]
    fn proxy_percentile_uses_runtimes() {
        let mut x = p();
        for rt in [0.1, 0.2, 0.3, 0.4] {
            x.record_attempt(Some(rt), None);
        }
        // A timeout carries a runtime but is excluded from the population (like avg_resp_time).
        x.record_attempt(Some(9.9), Some(ProxyError::Timeout));
        assert_eq!(x.percentile(0.5), 0.25); // median of [.1,.2,.3,.4]
        assert_eq!(p().percentile(0.5), 0.0); // no runtimes → 0.0
    }

    #[test]
    fn deserialize_round_trips_serialized_fields() {
        // A proxy with no recorded attempts (empty stats) round-trips exactly: identity fields
        // (host/port/geo/types) survive; the lossy stats fields are empty on both sides.
        let mut x = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
        x.geo = Some(Country {
            code: "US".into(),
            name: "United States".into(),
            ..Default::default()
        });
        x.add_type(Proto::Http, Some(AnonLevel::High));
        x.add_type(Proto::Connect80, None);

        let json = serde_json::to_string(&x).unwrap();
        let back: Proxy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, x);
    }

    #[test]
    fn no_geo_round_trips_to_none() {
        // Serialize emits an empty country code for no-geo; Deserialize must map it back to None.
        let mut x = Proxy::new("9.9.9.9".parse().unwrap(), 53, BTreeSet::new());
        x.add_type(Proto::Socks5, None);
        let back: Proxy = serde_json::from_str(&serde_json::to_string(&x).unwrap()).unwrap();
        assert_eq!(back.geo, None);
        assert_eq!(back, x);
    }

    #[test]
    fn geo_region_city_round_trip() {
        // A City DB populates region/city; the save/load round-trip must preserve them (they are
        // never re-resolved on --load, so dropping them would be silent, irrecoverable data loss).
        let mut x = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
        x.geo = Some(Country {
            code: "SE".into(),
            name: "Sweden".into(),
            region: Some(Region {
                code: "E".into(),
                name: "Östergötland".into(),
            }),
            city: Some("Linköping".into()),
        });
        x.add_type(Proto::Http, Some(AnonLevel::High));
        let back: Proxy = serde_json::from_str(&serde_json::to_string(&x).unwrap()).unwrap();
        assert_eq!(back.geo, x.geo);
        assert_eq!(back, x);
    }

    #[test]
    fn asn_round_trips_and_absent_serializes_null() {
        // ASN attribution (C8) comes from a user --asn-db; like geo it must survive save/load,
        // since --load never re-resolves it. Absent ASN serializes as null (no --asn-db supplied).
        let mut absent = Proxy::new("9.9.9.9".parse().unwrap(), 53, BTreeSet::new());
        absent.add_type(Proto::Socks5, None);
        let v = serde_json::to_value(&absent).unwrap();
        assert!(v["asn"].is_null(), "absent ASN must serialize as null: {v}");
        let back: Proxy = serde_json::from_value(v).unwrap();
        assert_eq!(back.asn, None);
        assert_eq!(back, absent);

        let mut present = Proxy::new("8.8.8.8".parse().unwrap(), 8080, BTreeSet::new());
        present.asn = Some(Asn {
            number: 15169,
            org: Some("Google LLC".into()),
        });
        present.add_type(Proto::Http, Some(AnonLevel::High));
        let v = serde_json::to_value(&present).unwrap();
        assert_eq!(v["asn"]["number"], 15169);
        assert_eq!(v["asn"]["org"], "Google LLC");
        let back: Proxy = serde_json::from_str(&serde_json::to_string(&present).unwrap()).unwrap();
        assert_eq!(back.asn, present.asn);
        assert_eq!(back, present);
    }

    #[test]
    fn deserialize_ignores_computed_stat_fields() {
        // avg_resp_time/error_rate are present in the JSON (from a checked proxy) but are not
        // restored — the loaded proxy's timing history starts empty.
        let json = r#"{"host":"1.2.3.4","port":80,
            "geo":{"country":{"code":"US","name":"United States"},"region":{"code":"","name":""},"city":null},
            "types":[{"type":"HTTP","level":"High"}],"avg_resp_time":0.42,"error_rate":0.1}"#;
        let p: Proxy = serde_json::from_str(json).unwrap();
        assert_eq!(p.avg_resp_time(), 0.0); // not restored
        assert_eq!(p.error_rate(), 0.0);
        assert_eq!(p.types().get(&Proto::Http), Some(&Some(AnonLevel::High)));
    }

    #[test]
    fn ndjson_round_trips_via_cursor() {
        // Persist a checked pool and reload it — fully in-memory, no temp files (constraint C5).
        let mut a = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
        a.geo = Some(Country {
            code: "US".into(),
            name: "United States".into(),
            ..Default::default()
        });
        a.add_type(Proto::Http, Some(AnonLevel::High));
        let mut b = Proxy::new("5.6.7.8".parse().unwrap(), 3128, BTreeSet::new());
        b.add_type(Proto::Socks5, None);
        let proxies = vec![a, b];

        let mut buf = Vec::new();
        write_ndjson(&mut buf, &proxies).unwrap();
        // One object per line, so N proxies => N newlines.
        assert_eq!(buf.iter().filter(|&&c| c == b'\n').count(), proxies.len());

        let back = read_ndjson(std::io::Cursor::new(buf)).unwrap();
        assert_eq!(back, proxies);
    }

    #[test]
    fn read_ndjson_skips_blank_lines() {
        let p = Proxy::new("1.2.3.4".parse().unwrap(), 80, BTreeSet::new());
        let line = serde_json::to_string(&p).unwrap();
        // Leading, interior, and trailing blank lines must not break parsing.
        let text = format!("\n{line}\n\n{line}\n\n");
        let back = read_ndjson(std::io::Cursor::new(text.into_bytes())).unwrap();
        assert_eq!(back, vec![p.clone(), p]);
    }

    #[test]
    fn read_ndjson_propagates_parse_error() {
        let err = read_ndjson(std::io::Cursor::new(b"not json\n".to_vec())).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
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
            ..Default::default()
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

        // Proxy JSON = v1, FROZEN (see decisions.md). This golden test asserts the COMPLETE shape
        // so region/city (C7) or any future field cannot drift the schema silently. The freeze
        // permits only *additive, always-present, backward-compatible* fields (a consumer reading
        // host/port/geo/types is unaffected); a *breaking* change — removing or retyping a field —
        // must bump the `--format` variant (e.g. json2). `asn` (C8) is such an additive field: null
        // for every proxy unless a `--asn-db` resolved it, so no existing consumer path changes.
        // For a country-only Country (the bundled DB, and this fixture) region is empty and city is
        // null; asn is null because this fixture set no --asn-db.
        assert_eq!(v["geo"]["region"]["code"], "");
        assert_eq!(v["geo"]["region"]["name"], "");
        assert_eq!(v["geo"]["city"], serde_json::Value::Null);
        assert_eq!(v["asn"], serde_json::Value::Null);
        assert_eq!(v["avg_resp_time"], 0.0);
        assert_eq!(v["error_rate"], 0.0);
        let mut top_keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        top_keys.sort_unstable();
        assert_eq!(
            top_keys,
            [
                "asn",
                "avg_resp_time",
                "error_rate",
                "geo",
                "host",
                "port",
                "types"
            ],
            "top-level Proxy JSON keys are frozen at v1 (asn added additively in C8)"
        );

        // A top-level-only check is addition-blind: a new key nested inside geo / geo.country /
        // geo.region / a types[] element (serde_json indexes a missing key to Null, so only
        // *removals* trip a path assertion) would drift the schema silently. Freeze the nested
        // key sets too so any added field must consciously bump the format variant.
        fn sorted_keys(val: &serde_json::Value) -> Vec<&str> {
            let mut k: Vec<&str> = val
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            k.sort_unstable();
            k
        }
        assert_eq!(
            sorted_keys(&v["geo"]),
            ["city", "country", "region"],
            "geo shape frozen"
        );
        assert_eq!(
            sorted_keys(&v["geo"]["country"]),
            ["code", "name"],
            "geo.country frozen"
        );
        assert_eq!(
            sorted_keys(&v["geo"]["region"]),
            ["code", "name"],
            "geo.region frozen"
        );
        assert_eq!(
            sorted_keys(&v["types"][0]),
            ["level", "type"],
            "types[] element frozen"
        );
    }
}
