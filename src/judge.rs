//! Judges: endpoints that echo back the request headers and the client IP, used to measure
//! a proxy's anonymity.
//!
//! In `judge.py` the working-judge lists and their readiness `Event`s are **class**
//! attributes — process-global state shared across every `Broker`, which is why
//! `Checker.__init__` must call `Judge.clear()`. Here a [`JudgePool`] is instance state owned
//! by the checker, probed eagerly at construction. No globals, no `clear()`, and — crucially
//! — no `asyncio.Event`: the level-vs-edge-triggering trap that would deadlock a naive tokio
//! port simply does not exist, because there is no event to wait on. See `decisions.md`.

use crate::resolver::Resolver;
use crate::types::JudgeScheme;
use crate::utils::{fresh_marker, get_all_ip, request_headers};
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// A judge endpoint. `marks` is the anonymity **baseline**: how many times `via`/`proxy`
/// appear in a direct response, so a proxy that adds a `Via` header can be detected as
/// merely Anonymous rather than High.
#[derive(Debug, Clone)]
pub struct Judge {
    pub url: String,
    pub scheme: JudgeScheme,
    pub host: String,
    /// Path + query, e.g. `/get?show_env` — sent as the request target (origin-form when
    /// tunnelled, absolute-form for plain HTTP).
    pub path: String,
    pub ip: Option<IpAddr>,
    pub marks: Marks,
}

/// Baseline occurrence counts in a verified judge response.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Marks {
    pub via: usize,
    pub proxy: usize,
}

impl Judge {
    /// Parse a judge URL into scheme/host. `http`/`https`/`smtp` map to the [`JudgeScheme`]s;
    /// anything else is rejected.
    pub fn parse(url: &str) -> Option<Judge> {
        let parsed = Url::parse(url).ok()?;
        let scheme = match parsed.scheme() {
            "http" => JudgeScheme::Http,
            "https" => JudgeScheme::Https,
            "smtp" => JudgeScheme::Smtp,
            _ => return None,
        };
        let host = parsed.host_str()?.to_owned();
        let path = match parsed.query() {
            Some(q) => format!("{}?{}", parsed.path(), q),
            None => parsed.path().to_owned(),
        };
        Some(Judge {
            url: url.to_owned(),
            scheme,
            host,
            path,
            ip: None,
            marks: Marks::default(),
        })
    }

    /// Probe this judge and decide whether it is usable, recording the `via`/`proxy` baseline
    /// on success. Mirrors `judge.py:Judge.check`:
    ///
    /// - resolve the host (fail → unusable);
    /// - SMTP judges are accepted on resolution alone (no page to fetch);
    /// - HTTP/HTTPS judges are fetched with a random marker in the `User-Agent`; usable iff
    ///   the response is 200 **and** echoes one of the host's real external IPs **and**
    ///   echoes the marker (proving it reflects our request).
    pub async fn probe(
        &mut self,
        resolver: &Resolver,
        client: &reqwest::Client,
        real_ext_ips: &HashSet<IpAddr>,
        timeout: Duration,
    ) -> bool {
        let Ok(ip) = resolver.resolve(&self.host).await else {
            return false;
        };
        self.ip = Some(ip);

        if self.scheme == JudgeScheme::Smtp {
            return true; // SMTP judges have no page to verify
        }

        let marker = fresh_marker();
        let mut req = client.get(&self.url).timeout(timeout);
        for (k, v) in request_headers(Some(&marker)) {
            req = req.header(k, v);
        }
        let Ok(resp) = req.send().await else {
            return false;
        };
        if resp.status() != reqwest::StatusCode::OK {
            return false;
        }
        let Ok(body) = resp.text().await else {
            return false;
        };
        let page = body.to_lowercase();

        // Pass if ANY of the host's real external IPs appears (dual-stack: the judge may echo
        // whichever family the connection used), and our marker round-tripped.
        let page_ips = get_all_ip(&page);
        let real_visible = real_ext_ips
            .iter()
            .any(|ip| page_ips.contains(&ip.to_string()));
        let marker_visible = page.contains(&marker);

        if real_visible && marker_visible {
            self.marks = Marks {
                via: page.matches("via").count(),
                proxy: page.matches("proxy").count(),
            };
            true
        } else {
            false
        }
    }
}

/// The working judges, grouped by scheme. Owned by value by the checker (it is already behind
/// an `Arc`, so a second `Arc<JudgePool>` would be redundant — critique #13).
#[derive(Debug, Default)]
pub struct JudgePool {
    http: Vec<Arc<Judge>>,
    https: Vec<Arc<Judge>>,
    smtp: Vec<Arc<Judge>>,
}

impl JudgePool {
    /// Probe all `candidates` concurrently and keep the working ones, grouped by scheme.
    ///
    /// Uses `join_all`, not a `JoinSet`: the probe futures borrow `resolver`/`client`, so they
    /// are not `'static` and cannot be `spawn`ed (critique #11).
    pub async fn probe_all(
        candidates: Vec<Judge>,
        resolver: &Resolver,
        client: &reqwest::Client,
        real_ext_ips: &HashSet<IpAddr>,
        timeout: Duration,
    ) -> JudgePool {
        let probes = candidates.into_iter().map(|mut j| async move {
            j.probe(resolver, client, real_ext_ips, timeout)
                .await
                .then_some(j)
        });
        let working = futures_util::future::join_all(probes).await;

        let mut pool = JudgePool::default();
        for judge in working.into_iter().flatten() {
            let arc = Arc::new(judge);
            match arc.scheme {
                JudgeScheme::Http => pool.http.push(arc),
                JudgeScheme::Https => pool.https.push(arc),
                JudgeScheme::Smtp => pool.smtp.push(arc),
            }
        }
        pool
    }

    /// A random working judge for `scheme`, or `None` if there are none.
    pub fn random(&self, scheme: JudgeScheme) -> Option<Arc<Judge>> {
        use rand::seq::IndexedRandom;
        let bucket = match scheme {
            JudgeScheme::Http => &self.http,
            JudgeScheme::Https => &self.https,
            JudgeScheme::Smtp => &self.smtp,
        };
        bucket.choose(&mut rand::rng()).cloned()
    }

    /// True if no judge of any scheme verified.
    pub fn is_empty(&self) -> bool {
        self.http.is_empty() && self.https.is_empty() && self.smtp.is_empty()
    }

    /// Count of working judges per scheme (for logging/tests).
    pub fn counts(&self) -> (usize, usize, usize) {
        (self.http.len(), self.https.len(), self.smtp.len())
    }
}

/// The default judge URLs (`judge.py:get_judges`).
pub fn default_judges() -> Vec<String> {
    [
        "http://httpbin.org/get?show_env",
        "https://httpbin.org/get?show_env",
        "smtp://smtp.gmail.com",
        "smtp://aspmx.l.google.com",
        "http://azenv.net/",
        "https://www.proxy-listen.de/azenv.php",
        "http://proxyjudge.us/azenv.php",
        "http://ip.spys.ru/",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_routes_scheme() {
        assert_eq!(
            Judge::parse("http://azenv.net/").unwrap().scheme,
            JudgeScheme::Http
        );
        assert_eq!(
            Judge::parse("https://x.com/j").unwrap().scheme,
            JudgeScheme::Https
        );
        assert_eq!(
            Judge::parse("smtp://smtp.gmail.com").unwrap().scheme,
            JudgeScheme::Smtp
        );
        assert_eq!(Judge::parse("http://azenv.net/").unwrap().host, "azenv.net");
        assert!(Judge::parse("ftp://x.com").is_none());
        assert!(Judge::parse("not a url").is_none());
    }

    #[test]
    fn empty_pool_is_empty() {
        assert!(JudgePool::default().is_empty());
        assert!(JudgePool::default().random(JudgeScheme::Http).is_none());
    }

    #[test]
    fn default_judges_present() {
        let j = default_judges();
        assert!(j.iter().any(|u| u.starts_with("http://")));
        assert!(j.iter().any(|u| u.starts_with("smtp://")));
    }
}
