//! The canonical shared vocabulary.
//!
//! Every type named here has exactly ONE definition and ONE home. This module exists
//! because eleven module designs were produced in parallel against a drifting vocabulary
//! and invented thirteen names for five concepts (`Proto`/`ProxyType`/`NegotiatorKind`,
//! `ProxyError`/`ProxyFailure`, `AnonLevel`/`Anonymity`, `Stream`×2, `JudgePool`×2,
//! `Scheme`×2, `JudgeScheme`×2). See `docs/systematic-refactor/map.md` §Critique.
//!
//! Rule: if a type is used by more than one module, it is defined here or re-exported
//! from here. No module invents a local spelling of a shared concept.

use std::fmt;

/// A proxy protocol. The closed set of six, matching Python's `NGTRS` dict keys.
///
/// One name for what the parallel designs variously called `Proto`, `ProxyType`, and
/// `NegotiatorKind`. `Proto` wins: it is the user-facing spelling in the CLI and in
/// provider configs.
///
/// Users extend *providers*, never protocols — hence a closed enum rather than a trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Proto {
    Http,
    Https,
    Socks4,
    Socks5,
    Connect80,
    Connect25,
}

impl Proto {
    /// Every protocol, in a deterministic order.
    ///
    /// Never iterate a `HashMap` to get this: Rust randomizes `HashMap` iteration per
    /// process (SipHash with a random seed), which would make check order — and therefore
    /// emitted request bytes and test output — nondeterministic.
    pub const ALL: [Proto; 6] = [
        Proto::Http,
        Proto::Https,
        Proto::Socks4,
        Proto::Socks5,
        Proto::Connect80,
        Proto::Connect25,
    ];

    /// The wire/config name. Matches Python's `NGTRS` keys byte-for-byte, colons included.
    pub const fn as_str(self) -> &'static str {
        match self {
            Proto::Http => "HTTP",
            Proto::Https => "HTTPS",
            Proto::Socks4 => "SOCKS4",
            Proto::Socks5 => "SOCKS5",
            Proto::Connect80 => "CONNECT:80",
            Proto::Connect25 => "CONNECT:25",
        }
    }

    /// Only HTTP carries anonymity information — the judge sees the client's headers
    /// directly. Tunnelled protocols hide them, so there is nothing to classify.
    /// (`negotiators.py`: `check_anon_lvl` is True on `HttpNgtr` alone.)
    pub const fn checks_anon_level(self) -> bool {
        matches!(self, Proto::Http)
    }

    /// HTTP sends an absolute-form request URI to the proxy (`GET http://host/path`);
    /// everything else tunnels first and then sends an origin-form path.
    /// (`negotiators.py`: `use_full_path` is True on `HttpNgtr` alone.)
    pub const fn uses_full_path(self) -> bool {
        matches!(self, Proto::Http)
    }

    /// Which judge flavour validates this protocol. (`judge.py:52 get_random`.)
    pub const fn judge_scheme(self) -> JudgeScheme {
        match self {
            Proto::Https => JudgeScheme::Https,
            Proto::Connect25 => JudgeScheme::Smtp,
            _ => JudgeScheme::Http,
        }
    }

    /// Python's display order, from `proxy.py`'s `key=lambda tp: (len(tp), tp[-1])`.
    ///
    /// Note this puts `CONNECT:80` before `CONNECT:25`: both are length 10, and the last
    /// characters compare `'0'` (48) < `'5'` (53). Verified against the interpreter — one
    /// parallel design asserted the opposite.
    pub fn display_order_key(self) -> (usize, u8) {
        let s = self.as_str();
        (s.len(), s.as_bytes()[s.len() - 1])
    }
}

impl fmt::Display for Proto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Proto {
    type Err = ParseProtoError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Proto::ALL
            .into_iter()
            .find(|p| p.as_str().eq_ignore_ascii_case(s))
            .ok_or_else(|| ParseProtoError(s.to_owned()))
    }
}

/// Python raises a bare `KeyError` from `NGTRS[proto]`; this makes the failure typed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown protocol: {0}")]
pub struct ParseProtoError(pub String);

/// How much the proxy reveals about the client. Ordered worst → best, so
/// `level >= AnonLevel::Anonymous` is a meaningful filter.
///
/// One name for what was variously `AnonLevel` and `Anonymity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AnonLevel {
    /// The judge saw our real external IP.
    Transparent,
    /// Our IP is hidden, but the request is marked as proxied (`via` above baseline).
    Anonymous,
    /// Indistinguishable from a direct request.
    High,
}

impl AnonLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            AnonLevel::Transparent => "Transparent",
            AnonLevel::Anonymous => "Anonymous",
            AnonLevel::High => "High",
        }
    }
}

impl fmt::Display for AnonLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AnonLevel {
    type Err = ParseProtoError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for l in [AnonLevel::Transparent, AnonLevel::Anonymous, AnonLevel::High] {
            if l.as_str().eq_ignore_ascii_case(s) {
                return Ok(l);
            }
        }
        Err(ParseProtoError(s.to_owned()))
    }
}

/// Transport scheme for a request through a proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    Http,
    Https,
}

/// Judge flavour. SMTP judges validate `CONNECT:25` and are probed differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JudgeScheme {
    Http,
    Https,
    Smtp,
}

impl JudgeScheme {
    /// `url::Url::port()` returns `None` for non-special schemes such as `smtp://`, so a
    /// default-port rule is required rather than optional.
    pub const fn default_port(self) -> u16 {
        match self {
            JudgeScheme::Http => 80,
            JudgeScheme::Https => 443,
            JudgeScheme::Smtp => 25,
        }
    }
}

/// One requested protocol, optionally narrowed to specific anonymity levels.
///
/// The single spelling of the type/level query. The parallel designs had three
/// (`Vec<TypeSpec>`, `types + http_levels`, and `IntoIterator<(NegotiatorKind, BTreeSet)>`).
///
/// `levels: None` means "any level". Levels only apply to `Proto::Http`
/// (`Proto::checks_anon_level`); for anything else they are ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeSpec {
    pub proto: Proto,
    pub levels: Option<Vec<AnonLevel>>,
}

impl TypeSpec {
    pub fn any(proto: Proto) -> Self {
        Self { proto, levels: None }
    }

    /// Does a measured outcome satisfy this request?
    pub fn accepts(&self, proto: Proto, level: Option<AnonLevel>) -> bool {
        if self.proto != proto {
            return false;
        }
        match (&self.levels, level) {
            (None, _) => true,
            (Some(want), Some(got)) => want.contains(&got),
            (Some(_), None) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_roundtrips_through_its_wire_name() {
        for p in Proto::ALL {
            assert_eq!(p.as_str().parse::<Proto>().unwrap(), p);
        }
        assert_eq!("connect:80".parse::<Proto>().unwrap(), Proto::Connect80);
        assert!("NOPE".parse::<Proto>().is_err());
    }

    #[test]
    fn display_order_matches_python() {
        // Python: sorted(types, key=lambda tp: (len(tp), tp[-1]))
        // Verified against the interpreter:
        //   ['HTTP', 'HTTPS', 'SOCKS4', 'SOCKS5', 'CONNECT:80', 'CONNECT:25']
        let mut all = Proto::ALL;
        all.sort_by_key(|p| p.display_order_key());
        let got: Vec<_> = all.iter().map(|p| p.as_str()).collect();
        assert_eq!(
            got,
            ["HTTP", "HTTPS", "SOCKS4", "SOCKS5", "CONNECT:80", "CONNECT:25"],
            "CONNECT:80 must precede CONNECT:25 ('0' < '5')"
        );
    }

    #[test]
    fn only_http_carries_anonymity() {
        assert!(Proto::Http.checks_anon_level());
        assert!(Proto::Http.uses_full_path());
        for p in Proto::ALL.into_iter().filter(|p| *p != Proto::Http) {
            assert!(!p.checks_anon_level(), "{p} must not check anon level");
            assert!(!p.uses_full_path(), "{p} must not use full path");
        }
    }

    #[test]
    fn judge_scheme_routing_matches_python() {
        // judge.py:52 get_random
        assert_eq!(Proto::Https.judge_scheme(), JudgeScheme::Https);
        assert_eq!(Proto::Connect25.judge_scheme(), JudgeScheme::Smtp);
        for p in [Proto::Http, Proto::Socks4, Proto::Socks5, Proto::Connect80] {
            assert_eq!(p.judge_scheme(), JudgeScheme::Http, "{p}");
        }
    }

    #[test]
    fn anon_levels_order_worst_to_best() {
        assert!(AnonLevel::Transparent < AnonLevel::Anonymous);
        assert!(AnonLevel::Anonymous < AnonLevel::High);
    }

    #[test]
    fn type_spec_accepts() {
        let any = TypeSpec::any(Proto::Socks5);
        assert!(any.accepts(Proto::Socks5, None));
        assert!(!any.accepts(Proto::Socks4, None));

        let strict = TypeSpec {
            proto: Proto::Http,
            levels: Some(vec![AnonLevel::High]),
        };
        assert!(strict.accepts(Proto::Http, Some(AnonLevel::High)));
        assert!(!strict.accepts(Proto::Http, Some(AnonLevel::Transparent)));
        assert!(!strict.accepts(Proto::Http, None));
    }

    /// proxybroker2 has a live bug at providers.py:706 and :710: `proto=("SOCKS4")` has
    /// no trailing comma, so it is a *string*, not a tuple. At api.py:409 the filter
    /// `bool(pr.proto & types.keys())` then intersects the string's CHARACTERS
    /// ({'S','O','C','K','4'}) with the requested protocol names, always yielding empty —
    /// silently dropping both proxyscrape SOCKS providers, which measurement shows are
    /// among the highest-yield sources still alive.
    ///
    /// This test exists to record that Rust makes the bug unrepresentable: a
    /// `Vec<Proto>` cannot be a string, and there is no comma to forget.
    #[test]
    fn missing_comma_bug_is_unrepresentable() {
        let one = vec![Proto::Socks4];
        assert_eq!(one.len(), 1);
        assert!(one.contains(&Proto::Socks4));
    }
}
