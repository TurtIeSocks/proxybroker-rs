//! The error taxonomy: two enums split by whether the caller can act.
//!
//! - [`ProxyError`] — a single proxy failed. This is a **histogram bucket key**: in Python
//!   it is the `errmsg` class attribute read reflectively at `proxy.py:333`
//!   (`stat["errors"][err.errmsg] += 1`). It is `Copy + Eq + Hash` and non-allocating (the one
//!   data-carrying variant, `DisallowedStatus(u16)`, is a plain `Copy` integer), because a
//!   `Counter` key that allocates is a `Counter` key you cannot use cheaply. The `as_str` strings
//!   are preserved **byte-for-byte** from `errors.py` — the stats output is a user-visible
//!   contract (`DisallowedStatus` is a local-server addition with no Python analogue).
//! - [`Error`] — something the caller must handle: no network, no judges, bad config.
//!
//! See `docs/systematic-refactor/decisions.md` §Errors.

use std::fmt;

/// A single proxy failed a check or a relayed request.
///
/// One flat enum replaces nine Python exception classes, because every call site already
/// treats them as one set (`checker.py:210-216` catches six in a tuple; `server.py:415-423`
/// catches seven). Nothing ever catches the bare `ProxyError` base, so the hierarchy is
/// load-bearing at zero sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProxyError {
    /// TCP connect refused/failed. `errors.py:16 ProxyConnError`.
    ConnFailed,
    /// Timed out connecting or receiving. `errors.py:28 ProxyTimeoutError`. The one variant
    /// with distinct control flow: the checker retries (`continue`) on this and `break`s on
    /// all others (`checker.py:208/239`).
    Timeout,
    /// Connection reset. Merges `ProxyRecvError` + `ProxySendError`, which share
    /// `errmsg="connection_is_reset"` in Python and are therefore already ONE bucket.
    /// Splitting them by Rust variant name would silently change `error_rate`; direction
    /// lives in the tracing message, as it did in Python.
    Reset,
    /// A clean zero-byte read with no OS error. `errors.py:32 ProxyEmptyRecvError`.
    EmptyRecv,
    /// Judge/negotiator returned a non-200 status. `errors.py:36 BadStatusError`.
    BadStatus,
    /// Malformed negotiator/judge payload. `errors.py:40 BadResponseError`.
    BadResponse,
    /// Error while relaying in the local server. `errors.py:48 ErrorOnStream`.
    ErrorOnStream,
    /// The upstream returned an HTTP status outside the served pool's `--http-allowed-codes`
    /// set (B11). A local-server-only, retryable failure — no Python analogue.
    DisallowedStatus(u16),
    /// Host did not resolve. `errors.py:12 ResolveError` (which has no `errmsg` in Python —
    /// the string is new). Deliberately never counted: at `api.py:443` the resolve fails
    /// before a `Proxy` exists, so there is no `stat` dict to increment. Exists as a return
    /// type, not a bucket.
    Resolve,
}

impl ProxyError {
    /// The stats-histogram key. Preserved byte-for-byte from `errors.py`, except `Resolve`,
    /// whose Python class carries no `errmsg`.
    pub const fn as_str(self) -> &'static str {
        match self {
            ProxyError::ConnFailed => "connection_failed",
            ProxyError::Timeout => "connection_timeout",
            ProxyError::Reset => "connection_is_reset",
            ProxyError::EmptyRecv => "empty_response",
            ProxyError::BadStatus => "bad_status",
            ProxyError::BadResponse => "bad_response",
            ProxyError::ErrorOnStream => "error_on_stream",
            ProxyError::DisallowedStatus(_) => "disallowed_status",
            ProxyError::Resolve => "resolve_failed",
        }
    }
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for ProxyError {}

/// A fatal or caller-actionable error — the run cannot proceed until it is handled.
///
/// `#[non_exhaustive]` so variants can be added without a breaking change.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// No judges verified. `checker.py:137 raise RuntimeError("Not found judges")`.
    #[error("no working judges were found")]
    NoJudges,

    /// `find()` was called without any protocol to check. `api.py:249 raise ValueError`.
    #[error("at least one proxy type is required")]
    NoTypes,

    /// The provider list resolved to empty.
    #[error("no providers configured")]
    NoProviders,

    /// The machine's own external IP could not be determined, so anonymity cannot be judged.
    #[error("could not determine this machine's external IP address")]
    ExtIpUnknown,

    /// A provider config file was malformed.
    #[error("invalid provider configuration: {0}")]
    Config(String),

    /// Underlying I/O failure (binding the server socket, reading a config file).
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// The errmsg strings are a user-visible stats contract. If any of these changes, the
    /// histogram output silently diverges from proxybroker2. Byte-for-byte from errors.py.
    #[test]
    fn errmsg_strings_match_python_byte_for_byte() {
        assert_eq!(ProxyError::ConnFailed.as_str(), "connection_failed");
        assert_eq!(ProxyError::Timeout.as_str(), "connection_timeout");
        assert_eq!(ProxyError::Reset.as_str(), "connection_is_reset");
        assert_eq!(ProxyError::EmptyRecv.as_str(), "empty_response");
        assert_eq!(ProxyError::BadStatus.as_str(), "bad_status");
        assert_eq!(ProxyError::BadResponse.as_str(), "bad_response");
        assert_eq!(ProxyError::ErrorOnStream.as_str(), "error_on_stream");
    }

    /// Recv and Send collapse to one key in Python (both "connection_is_reset"). The merge
    /// must not resurrect two buckets, or error_rate diverges.
    #[test]
    fn reset_is_a_single_bucket() {
        // There is no separate Recv/Send variant to construct — the type makes the merge
        // structural. This test pins that the one variant maps to the shared string.
        let mut counter: HashMap<ProxyError, u32> = HashMap::new();
        *counter.entry(ProxyError::Reset).or_default() += 1;
        *counter.entry(ProxyError::Reset).or_default() += 1;
        assert_eq!(counter.len(), 1);
        assert_eq!(counter[&ProxyError::Reset], 2);
        assert_eq!(
            counter.keys().next().unwrap().as_str(),
            "connection_is_reset"
        );
    }

    /// It is the Counter key type, exactly as Python's `Counter()` at proxy.py:131.
    #[test]
    fn proxy_error_is_a_cheap_hashmap_key() {
        let mut stats: HashMap<ProxyError, u32> = HashMap::new();
        for e in [
            ProxyError::ConnFailed,
            ProxyError::Timeout,
            ProxyError::Timeout,
            ProxyError::BadStatus,
        ] {
            *stats.entry(e).or_default() += 1;
        }
        assert_eq!(stats[&ProxyError::Timeout], 2);
        assert_eq!(stats[&ProxyError::ConnFailed], 1);
        assert_eq!(stats.get(&ProxyError::Reset), None);
    }

    #[test]
    fn display_delegates_to_as_str() {
        assert_eq!(ProxyError::Timeout.to_string(), "connection_timeout");
    }
}
