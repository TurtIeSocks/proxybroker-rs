//! Small helpers shared across modules — the module every other module assumed someone
//! else would write (design critique #39). Ported from `utils.py`.
//!
//! Two things here are load-bearing and easy to get wrong:
//! - [`request_headers`] returns headers in a fixed order via [`IndexMap`], because the
//!   emitted request bytes depend on that order and a `HashMap` randomizes it per process.
//! - [`get_status_code`] returns `400` on unparseable input as a **sentinel** the callers
//!   depend on, not as an error.

use crate::parse::IPV4_RE;
use indexmap::IndexMap;
use rand::Rng;
use regex::Regex;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::LazyLock;

/// The IPv6-candidate tokenizer from `utils.py:_IPV6_CANDIDATE_PATTERN`. No lookaround, so
/// the `regex` crate accepts it. Validation is delegated to [`canonicalize_ip`], not the
/// grammar — exactly as in Python.
static IPV6_CANDIDATE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[0-9A-Fa-f:][0-9A-Fa-f:.]*(?:%[A-Za-z0-9_.\-]+)?").unwrap());

/// Request headers, in the exact order `utils.py:get_headers` emits them.
///
/// Ordered on purpose: the byte sequence a proxy and a judge see is this map serialized in
/// order, and tests assert on it. `IndexMap`, never `HashMap`.
///
/// When `marker` is `Some`, its value is folded into the `User-Agent` (Python's `rv` path):
/// the checker plants a random token in the request and greps the judge's echo for it to
/// detect header injection. Returns the marker actually used so the caller can grep for it.
pub fn request_headers(marker: Option<&str>) -> IndexMap<&'static str, String> {
    let rv = marker.unwrap_or("");
    let mut h = IndexMap::with_capacity(7);
    h.insert(
        "User-Agent",
        format!("PxBroker/{}/{}", env!("CARGO_PKG_VERSION"), rv),
    );
    h.insert("Accept", "*/*".to_owned());
    h.insert("Accept-Encoding", "gzip, deflate".to_owned());
    h.insert("Pragma", "no-cache".to_owned());
    h.insert("Cache-control", "no-cache".to_owned());
    h.insert("Cookie", "cookie=ok".to_owned());
    h.insert("Referer", "https://www.google.com/".to_owned());
    h
}

/// A fresh random request marker, `1000..=9999` as a string. `utils.py`'s `_rv`.
pub fn fresh_marker() -> String {
    (1000 + rand::rng().random_range(0..9000)).to_string()
}

/// RFC 5952 canonical textual form of `s`, or `None` if it is not a valid IP address.
///
/// Mirrors `utils.py:canonicalize_ip` **byte for byte** (asserted against a Python oracle in
/// `tests/utils_chars.rs`) — including accepting the unspecified address `0.0.0.0`/`::`, which
/// Python's `ipaddress` accepts. Dropping non-routable sentinels is the *provider* layer's job
/// (see `ProviderSpec::extract`), not this parity-faithful primitive. IPv4 canonical form equals
/// the input; both Python's `ipaddress` and Rust's `IpAddr` reject leading zeros (`010.1.1.1` →
/// `None`), so the "quiet killer" is neutralized for free. Zone IDs (`fe80::1%eth0`) are preserved
/// to match Python — Rust's `IpAddr` parser rejects `%zone`, so they are split off, the base is
/// canonicalized, and the zone re-appended verbatim.
pub fn canonicalize_ip(s: &str) -> Option<String> {
    if let Some((base, zone)) = s.split_once('%') {
        // Zone IDs attach only to IPv6. Canonicalize the address, keep the zone verbatim.
        let addr: IpAddr = base.parse().ok()?;
        if !addr.is_ipv6() {
            return None;
        }
        return Some(format!("{addr}%{zone}"));
    }
    s.parse::<IpAddr>().ok().map(|ip| ip.to_string())
}

/// Every IPv4 and IPv6 literal in `page`, canonicalized, as a set.
///
/// Mirrors `utils.py:get_all_ip`. IPv4 are taken as raw substrings (not canonicalized — so
/// a leading-zero match stays as-is, matching Python); IPv6 candidates are validated and
/// canonicalized. A `BTreeSet` rather than a `HashSet` so iteration is deterministic.
pub fn get_all_ip(page: &str) -> BTreeSet<String> {
    let mut found: BTreeSet<String> = IPV4_RE
        .find_iter(page)
        .map(|m| m.as_str().to_owned())
        .collect();
    for tok in IPV6_CANDIDATE_RE.find_iter(page) {
        let tok = tok.as_str();
        if !tok.contains(':') {
            continue; // pure IPv4 token — already covered above
        }
        // The tokenizer greedily grabs a trailing '.', e.g. "2001:db8::1." — strip it.
        if let Some(c) = canonicalize_ip(tok.trim_end_matches('.')) {
            found.insert(c);
        }
    }
    found
}

/// The HTTP status code in `resp[start..stop]`, or `400` if that slice is not a number.
///
/// Mirrors `utils.py:get_status_code`. The `400` is a **sentinel**, not an error — callers
/// branch on it. The default slice for a status line is `9..12` ("HTTP/1.1 `200`"); the
/// SMTP negotiator uses `0..3`. Matches Python's `int()` leniency by trimming surrounding
/// ASCII whitespace before parsing (`int(" 200 ") == 200`), but rejecting internal
/// non-digits (`int("5 x")` raises → 400).
pub fn get_status_code(resp: &[u8], start: usize, stop: usize) -> u16 {
    let end = stop.min(resp.len());
    if start >= end {
        return 400;
    }
    match std::str::from_utf8(&resp[start..end]) {
        Ok(s) => s.trim().parse::<u16>().unwrap_or(400),
        Err(_) => 400,
    }
}

/// Encode `input` as standard base64 (RFC 4648 alphabet, `=` padding). Encode-only — all Wave 3
/// needs: the local server compares a client's `Proxy-Authorization` against a pre-encoded expected
/// string (B9), and emits `Basic <b64>` to authenticated upstreams (B8). Hand-rolled (~15 lines) to
/// avoid pulling a base64 crate into an always-compiled module for one header; if a decoder is ever
/// needed, swap in the `base64` crate.
pub fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        // Pack up to 3 bytes into a 24-bit group, then emit 4 sextets.
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(n >> 12) as usize & 0x3f] as char);
        // Pad the sextets that had no input byte with '='.
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_are_in_wire_order() {
        let h = request_headers(None);
        let keys: Vec<_> = h.keys().copied().collect();
        assert_eq!(
            keys,
            [
                "User-Agent",
                "Accept",
                "Accept-Encoding",
                "Pragma",
                "Cache-control",
                "Cookie",
                "Referer"
            ]
        );
    }

    #[test]
    fn marker_is_folded_into_user_agent() {
        let h = request_headers(Some("4242"));
        assert!(h["User-Agent"].ends_with("/4242"), "{}", h["User-Agent"]);
        assert!(request_headers(None)["User-Agent"].ends_with('/'));
    }

    #[test]
    fn fresh_marker_is_four_digits() {
        let m = fresh_marker();
        let n: u32 = m.parse().unwrap();
        assert!((1000..=9999).contains(&n), "{m}");
    }

    #[test]
    fn zone_id_that_is_not_ipv6_is_rejected() {
        // A '%' on a v4 address is nonsense; Python's ip_address rejects it too.
        assert_eq!(canonicalize_ip("1.2.3.4%eth0"), None);
    }

    #[test]
    fn base64_encode_matches_rfc4648() {
        // RFC 4648 §10 test vectors.
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        // The credential case B8/B9 rely on.
        assert_eq!(base64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }
}
