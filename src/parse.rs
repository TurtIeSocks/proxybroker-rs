//! IP:port scanning. The one home for extracting proxy addresses from text.
//!
//! Rust's `regex` crate refuses `IPPortPatternGlobal` — it uses lookahead, and the crate
//! rejects look-around by design (finite automaton → linear-time guarantee). That is a
//! feature, not a limitation: `fancy-regex` would restore lookahead at the cost of the
//! ReDoS risk `utils.py`'s own comments boast of avoiding.
//!
//! So this is a two-pass scanner. Pass 1 uses the `regex` crate with proxybroker2's
//! **exact** IPv4 octet sub-pattern (which has no lookaround), so IP matching — including
//! its leading-zero quirk, `010.1.1.1` and `300.1.2.3 → 00.1.2.3` — is byte-identical.
//! Pass 2 does the IP↔port pairing in code. Verified against a characterization oracle in
//! `tests/ip_scan.rs`.

use regex::Regex;
use std::sync::LazyLock;

/// proxybroker2's exact IPv4 pattern (`utils.py:IPPattern`), verbatim. No lookaround, so
/// the `regex` crate accepts it. The `[01]?\d\d?` alternative is what preserves leading
/// zeros — matching Python's behaviour rather than "fixing" it here; validity is decided
/// later by `canonicalize_ip`, exactly as in the original.
const IPV4: &str = r"(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)";

pub(crate) static IPV4_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(IPV4).unwrap());

/// A 2–5 digit run — Python's `\d{2,5}` port group. `\d` is Unicode in both `re` and the
/// `regex` crate, so the two agree.
static PORT_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d{2,5}").unwrap());

/// One proxy per line: the first IPv4 on a line, paired with the first 2–5 digit run after
/// it on that same line. Mirrors `IPPortPatternLine` (`re.MULTILINE`), used by the raw-data
/// loader (`api.py:_load`). A line with no IP, or an IP with no following 2-digit run, is
/// skipped.
pub fn find_addrs_line(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    // `re.MULTILINE` with `.` not matching `\n` means each `^.*$` is one line. Python's `.`
    // does match `\r`; splitting only on `\n` (not stripping `\r`) reproduces that — a
    // trailing `\r` just becomes part of the between-IP-and-port `.*?`.
    for line in text.split('\n') {
        let Some(ip) = IPV4_RE.find(line) else {
            continue;
        };
        // First 2–5 digit run strictly after the IP.
        if let Some(port) = PORT_RE.find(&line[ip.end()..]) {
            out.push((ip.as_str().to_owned(), port.as_str().to_owned()));
        }
    }
    out
}

/// Every IPv4 in the whole text, each paired with the nearest following token: a 2–5 digit
/// port, or an empty string if the nearest token is another IP. An IP with nothing after it
/// is dropped. Mirrors `IPPortPatternGlobal` (`re.DOTALL`), the default provider pattern.
///
/// The lookahead `(?=.*?(?:IP|port))` means: assert that, somewhere ahead, either another
/// IP or a port begins — and capture the port group only if the port alternative is what
/// matched. Because the alternation lists the IP first, a position where *both* could start
/// (a number that also begins an IP) resolves to the IP, leaving the port empty. That tie
/// rule is why the comparison below is strict (`port.start() < ip.start()`).
pub fn find_addrs_global(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut search_from = 0usize;
    while let Some(ip) = IPV4_RE.find_at(text, search_from) {
        let after = ip.end();
        let next_ip = IPV4_RE.find_at(text, after);
        let next_port = PORT_RE.find_at(text, after);

        let port = match (next_port, next_ip) {
            // A port begins strictly before any following IP → capture it.
            (Some(p), Some(i)) if p.start() < i.start() => Some(p.as_str().to_owned()),
            (Some(p), None) => Some(p.as_str().to_owned()),
            // A following IP is nearer or ties → the IP is emitted with an empty port.
            (_, Some(_)) => Some(String::new()),
            // Nothing follows this IP → Python's lookahead fails, so it is not emitted.
            (None, None) => None,
        };

        if let Some(port) = port {
            out.push((ip.as_str().to_owned(), port));
        }
        // Non-overlapping: resume scanning after this IP, exactly as `findall` does.
        search_from = after;
    }
    out
}
