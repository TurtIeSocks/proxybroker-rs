//! DNS resolution and this host's external-IP discovery.
//!
//! `resolver.py` hand-rolls a DNS cache with `cachetools`; hickory-resolver 0.26 ships a
//! built-in response cache, so that code is gone (a `replace-with-lib`, per `assessment.md`).
//!
//! [`Resolver::external_ips`] returns a **set**: on a dual-stack host both the IPv4 and IPv6
//! external addresses, because [`crate::checker`]'s anonymity check must pass if *either*
//! appears in a judge's echo. This set semantics is load-bearing, not incidental.

use crate::error::{Error, ProxyError};
use hickory_resolver::TokioResolver;
use std::collections::HashSet;
use std::net::IpAddr;
use std::time::Duration;

/// Default external-IP discovery endpoints (`resolver.py:_ip_hosts`). `api64.ipify.org`
/// resolves to IPv6 where available (v6 echo), the plain `api.ipify.org` is IPv4-only — so
/// hitting both naturally collects both families via DNS, without per-request family pinning
/// (which `research.md` left unverified; this sidesteps it).
const DEFAULT_IP_ENDPOINTS: &[&str] = &[
    "https://api64.ipify.org/",
    "https://api.ipify.org/",
    "https://icanhazip.com/",
];

/// Resolves host names and discovers this machine's external IPs.
pub struct Resolver {
    dns: TokioResolver,
    client: reqwest::Client,
    timeout: Duration,
    ip_endpoints: Vec<String>,
}

impl Resolver {
    /// Build a resolver using the system DNS configuration.
    pub fn new(timeout: Duration) -> Result<Self, Error> {
        let dns = hickory_resolver::Resolver::builder_tokio()
            .map_err(|e| Error::Config(format!("dns resolver init: {e}")))?
            .build()
            .map_err(|e| Error::Config(format!("dns resolver build: {e}")))?;
        Ok(Resolver {
            dns,
            client: reqwest::Client::new(),
            timeout,
            ip_endpoints: DEFAULT_IP_ENDPOINTS.iter().map(|s| s.to_string()).collect(),
        })
    }

    /// Override the external-IP endpoints (used by tests to point at a local server).
    pub fn with_ip_endpoints(mut self, endpoints: Vec<String>) -> Self {
        self.ip_endpoints = endpoints;
        self
    }

    /// Is `host` an IP literal? Mirrors `resolver.py:host_is_ip`, including its acceptance of
    /// leading-zero IPv4 (`127.0.0.001`) — which stdlib and Rust both reject by default, but
    /// which provider feeds occasionally emit and historical proxybroker accepted.
    pub fn host_is_ip(host: &str) -> bool {
        parse_ip_lenient(host).is_some()
    }

    /// Resolve `host` to a single IP. An IP literal passes straight through (no DNS); a name
    /// is looked up via hickory (A/AAAA), whose response cache handles repeats. Returns
    /// [`ProxyError::Resolve`] on failure — the `ResolveError` of `proxy.py:create`.
    pub async fn resolve(&self, host: &str) -> Result<IpAddr, ProxyError> {
        if let Some(ip) = parse_ip_lenient(host) {
            return Ok(ip);
        }
        let lookup = tokio::time::timeout(self.timeout, self.dns.lookup_ip(host))
            .await
            .map_err(|_| ProxyError::Resolve)?
            .map_err(|_| ProxyError::Resolve)?;
        lookup.iter().next().ok_or(ProxyError::Resolve)
    }

    /// Discover this host's external IP addresses — the anonymity baseline. Probes the
    /// endpoints concurrently and collects every distinct valid IP they report. Returns
    /// [`Error::ExtIpUnknown`] if none answer (no network, or every endpoint down).
    pub async fn external_ips(&self) -> Result<HashSet<IpAddr>, Error> {
        let probes = self.ip_endpoints.iter().map(|url| {
            let client = self.client.clone();
            let url = url.clone();
            let timeout = self.timeout;
            async move {
                let body = client
                    .get(&url)
                    .timeout(timeout)
                    .send()
                    .await
                    .ok()?
                    .text()
                    .await
                    .ok()?;
                body.trim().parse::<IpAddr>().ok()
            }
        });
        let found: HashSet<IpAddr> = futures_util::future::join_all(probes)
            .await
            .into_iter()
            .flatten()
            .collect();
        if found.is_empty() {
            return Err(Error::ExtIpUnknown);
        }
        Ok(found)
    }
}

/// Parse an IP literal, accepting leading-zero IPv4 octets (decimal, not octal — matching
/// Python's `int("010") == 10`). Returns the normalized address. Shared with `parse.rs` so
/// user-supplied `host:port` lines normalize the same way provider-scraped ones do.
pub(crate) fn parse_ip_lenient(host: &str) -> Option<IpAddr> {
    if host.is_empty() {
        return None;
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(ip);
    }
    // Legacy IPv4-only path: strip leading zeros per octet, then parse. `resolver.py` guards
    // this with `"." in host and ":" not in host`.
    if host.contains('.') && !host.contains(':') {
        let octets: Vec<&str> = host.split('.').collect();
        if octets.len() == 4 {
            let normalized: Option<Vec<u8>> = octets.iter().map(|o| o.parse::<u8>().ok()).collect();
            if let Some(o) = normalized {
                return Some(IpAddr::from([o[0], o[1], o[2], o[3]]));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_is_ip_matches_python() {
        assert!(Resolver::host_is_ip("1.2.3.4"));
        assert!(Resolver::host_is_ip("2001:db8::1"));
        assert!(Resolver::host_is_ip("::1"));
        // leading-zero IPv4 accepted (Python normalizes; stdlib alone rejects)
        assert!(Resolver::host_is_ip("127.0.0.001"));
        assert!(Resolver::host_is_ip("010.1.1.1"));
        // not IPs
        assert!(!Resolver::host_is_ip("example.com"));
        assert!(!Resolver::host_is_ip(""));
        assert!(!Resolver::host_is_ip("999.1.1.1"));
        assert!(!Resolver::host_is_ip("1.2.3"));
    }

    #[test]
    fn leading_zero_ipv4_is_decimal_not_octal() {
        // Python: int("010") == 10, so 010.0.0.1 → 10.0.0.1 (not octal 8).
        assert_eq!(
            parse_ip_lenient("010.0.0.1"),
            Some("10.0.0.1".parse().unwrap())
        );
        assert_eq!(
            parse_ip_lenient("127.0.0.001"),
            Some("127.0.0.1".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn resolve_passes_ip_literals_through_without_dns() {
        let r = Resolver::new(Duration::from_secs(5)).unwrap();
        assert_eq!(
            r.resolve("8.8.8.8").await.unwrap(),
            "8.8.8.8".parse::<IpAddr>().unwrap()
        );
        // leading-zero literal normalizes, still no DNS
        assert_eq!(
            r.resolve("010.0.0.1").await.unwrap(),
            "10.0.0.1".parse::<IpAddr>().unwrap()
        );
    }
}
