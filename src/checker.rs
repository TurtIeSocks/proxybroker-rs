//! The [`Checker`]: validate one proxy across the requested protocols, and classify its
//! anonymity. `checker.py:Checker`.
//!
//! The judges are probed **eagerly** when the checker is built and owned by it — so
//! `Checker::new` returns [`Error::NoJudges`] if none verify (`checker.py:137`), and
//! `check` is simply unconstructible before the baseline exists. This turns the
//! probe-before-check ordering into a type fact and removes the process-global judge state
//! (and the deadlock-prone `asyncio.Event`) entirely. See `decisions.md`.

use crate::error::{Error, ProxyError};
use crate::judge::{Judge, JudgePool};
use crate::negotiator::{negotiate, Target};
use crate::proxy::Proxy;
use crate::resolver::Resolver;
use crate::types::{AnonLevel, Proto, TypeSpec};
use crate::utils::{fresh_marker, get_all_ip, get_status_code, request_headers};
use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Everything needed to run checks against one proxy.
#[derive(Debug)]
pub struct Checker {
    judges: JudgePool,
    real_ext_ips: HashSet<IpAddr>,
    requested: Vec<TypeSpec>,
    timeout: Duration,
    max_tries: usize,
    post: bool,
    strict: bool,
}

/// Configuration for [`Checker::new`].
#[derive(Debug, Clone)]
pub struct CheckerConfig {
    /// Judge URLs to probe. Empty → the bundled defaults.
    pub judges: Vec<String>,
    /// Protocols (and optional anonymity levels) to check. Required — empty is an error.
    pub types: Vec<TypeSpec>,
    pub timeout: Duration,
    pub max_tries: usize,
    /// Use `POST` instead of `GET` for the test request.
    pub post: bool,
    /// Require the proxy's anonymity level to match exactly (`--strict`).
    pub strict: bool,
}

impl Checker {
    /// Build a checker, probing the judges eagerly. Errors:
    /// - [`Error::NoTypes`] if no protocol was requested (`api.py:249`);
    /// - [`Error::NoJudges`] if no judge verifies (`checker.py:137`).
    pub async fn new(
        cfg: CheckerConfig,
        resolver: &Resolver,
        client: &reqwest::Client,
        real_ext_ips: HashSet<IpAddr>,
    ) -> Result<Checker, Error> {
        if cfg.types.is_empty() {
            return Err(Error::NoTypes);
        }
        let urls = if cfg.judges.is_empty() {
            crate::judge::default_judges()
        } else {
            cfg.judges.clone()
        };
        let candidates: Vec<Judge> = urls.iter().filter_map(|u| Judge::parse(u)).collect();
        let judges =
            JudgePool::probe_all(candidates, resolver, client, &real_ext_ips, cfg.timeout).await;
        if judges.is_empty() {
            return Err(Error::NoJudges);
        }
        Ok(Checker {
            judges,
            real_ext_ips,
            requested: cfg.types,
            timeout: cfg.timeout,
            max_tries: cfg.max_tries,
            post: cfg.post,
            strict: cfg.strict,
        })
    }

    /// Check a proxy across the protocols it is expected to support (intersected with the
    /// requested set), record its working types, and return whether it passes. `checker.py:check`.
    pub async fn check(&self, proxy: &mut Proxy) -> bool {
        let requested: HashSet<Proto> = self.requested.iter().map(|t| t.proto).collect();

        // ngtrs = expected ∩ requested, iterated in Proto::ALL order (never HashMap order,
        // which is randomized and would make check order nondeterministic). An empty
        // expected set means "unknown", so check all requested (api.py's else branch).
        let ngtrs: Vec<Proto> = Proto::ALL
            .into_iter()
            .filter(|p| {
                requested.contains(p)
                    && (proxy.expected_types.is_empty() || proxy.expected_types.contains(p))
            })
            .collect();

        let mut any = false;
        for proto in ngtrs {
            if self.check_one(proxy, proto).await {
                any = true;
            }
        }

        any && self.types_passed(proxy)
    }

    /// Check one protocol, retrying on timeout up to `max_tries`. Mirrors `checker.py:_check`:
    /// a timeout retries (`continue`), any other proxy error stops (`break`).
    async fn check_one(&self, proxy: &mut Proxy, proto: Proto) -> bool {
        let scheme = proto.judge_scheme();
        let Some(judge) = self.judges.random(scheme) else {
            return false; // no judge for this scheme
        };
        let target = Target {
            host: judge.host.clone(),
            ip: judge.ip,
            port: scheme.default_port(),
        };

        for _ in 0..self.max_tries {
            let start = Instant::now();
            match self.attempt(proxy, proto, &judge, &target).await {
                Ok(Attempt::Working(level)) => {
                    proxy.record_attempt(Some(start.elapsed().as_secs_f64()), None);
                    proxy.add_type(proto, level);
                    return true;
                }
                Ok(Attempt::Invalid) => {
                    // Response did not validate — the proxy does not work for this protocol.
                    // Python breaks here (no retry).
                    proxy.record_attempt(None, Some(ProxyError::BadResponse));
                    return false;
                }
                Err(ProxyError::Timeout) => {
                    proxy.record_attempt(None, Some(ProxyError::Timeout));
                    continue; // retry with a fresh connection
                }
                Err(e) => {
                    proxy.record_attempt(None, Some(e));
                    return false; // break
                }
            }
        }
        false
    }

    /// One connection attempt: connect, negotiate, and (for non-`CONNECT:25`) run the test
    /// request. Returns the anonymity outcome, or a [`ProxyError`] to drive the retry logic.
    async fn attempt(
        &self,
        proxy: &Proxy,
        proto: Proto,
        judge: &Judge,
        target: &Target,
    ) -> Result<Attempt, ProxyError> {
        let tcp = tokio::time::timeout(self.timeout, TcpStream::connect((proxy.host, proxy.port)))
            .await
            .map_err(|_| ProxyError::Timeout)?
            .map_err(|_| ProxyError::ConnFailed)?;

        let mut stream = negotiate(proto, tcp, target, self.timeout).await?;

        // CONNECT:25 has no test request — a granted tunnel is the whole check.
        if proto == Proto::Connect25 {
            return Ok(Attempt::Working(None));
        }

        let (request, marker) = self.build_request(judge, proto);
        tokio::time::timeout(self.timeout, stream.write_all(&request))
            .await
            .map_err(|_| ProxyError::Timeout)?
            .map_err(|_| ProxyError::Reset)?;

        let raw = read_response(&mut stream, self.timeout).await?;
        let (head, body) = split_head_body(&raw);
        if get_status_code(head, 9, 12) != 200 {
            return Err(ProxyError::BadStatus);
        }
        let content = decompress(head, body);

        if !response_is_valid(&content, &marker) {
            return Ok(Attempt::Invalid);
        }
        let level = if proto.checks_anon_level() {
            Some(self.anonymity_level(&content, judge))
        } else {
            None
        };
        Ok(Attempt::Working(level))
    }

    /// Build the test request. HTTP uses an absolute-form request URI (it goes to the proxy,
    /// which forwards it); tunnelled protocols use origin-form. Mirrors `checker.py:_request`.
    fn build_request(&self, judge: &Judge, proto: Proto) -> (Vec<u8>, String) {
        let marker = fresh_marker();
        let mut hdrs = request_headers(Some(&marker));
        hdrs.insert("Host", judge.host.clone());
        hdrs.insert("Connection", "close".to_owned());
        hdrs.insert("Content-Length", "0".to_owned());
        let method = if self.post { "POST" } else { "GET" };
        let path = if proto.uses_full_path() {
            format!("http://{}{}", judge.host, judge.path)
        } else {
            judge.path.clone()
        };
        let headers: String = hdrs
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join("\r\n");
        let req = format!("{method} {path} HTTP/1.1\r\n{headers}\r\n\r\n").into_bytes();
        (req, marker)
    }

    /// Classify anonymity. `checker.py:_get_anonymity_lvl`: Transparent if any of the host's
    /// real external IPs appears in the (lowercased) page; else Anonymous if `via`/`proxy`
    /// counts exceed the judge's baseline; else High.
    fn anonymity_level(&self, content: &str, judge: &Judge) -> AnonLevel {
        let lower = content.to_lowercase();
        let found = get_all_ip(&lower);
        let real_visible = self
            .real_ext_ips
            .iter()
            .any(|ip| found.contains(&ip.to_string()));
        let via = lower.matches("via").count() > judge.marks.via
            || lower.matches("proxy").count() > judge.marks.proxy;

        if real_visible {
            AnonLevel::Transparent
        } else if via {
            AnonLevel::Anonymous
        } else {
            AnonLevel::High
        }
    }

    /// `checker.py:_types_passed`. Non-strict: pass if any confirmed type matches the request
    /// (by protocol, and level if one was requested). Strict: drop confirmed types whose level
    /// does not match, then pass if any survive.
    fn types_passed(&self, proxy: &mut Proxy) -> bool {
        if self.requested.is_empty() {
            return true;
        }
        let requested: BTreeMap<Proto, Option<Vec<AnonLevel>>> = self
            .requested
            .iter()
            .map(|t| (t.proto, t.levels.clone()))
            .collect();

        let confirmed: Vec<(Proto, Option<AnonLevel>)> =
            proxy.types().iter().map(|(p, l)| (*p, *l)).collect();
        let mut to_remove = Vec::new();
        for (proto, lvl) in confirmed {
            let matches = match requested.get(&proto) {
                // proto not requested, or requested with no/empty level filter → matches any
                None | Some(None) => true,
                Some(Some(levels)) if levels.is_empty() => true,
                Some(Some(levels)) => lvl.is_some_and(|l| levels.contains(&l)),
            };
            if matches {
                if !self.strict {
                    return true;
                }
            } else if self.strict {
                to_remove.push(proto);
            }
        }
        for p in to_remove {
            proxy.remove_type(p);
        }
        self.strict && !proxy.types().is_empty()
    }
}

/// Outcome of one connection attempt.
enum Attempt {
    /// The proxy works for this protocol; carries the anonymity level (HTTP only).
    Working(Option<AnonLevel>),
    /// Connected and responded, but the response failed validation.
    Invalid,
}

/// Read the whole response. We send `Connection: close`, so the judge closes the connection
/// after the body — read-to-end is correct and captures the full page.
async fn read_response(
    stream: &mut crate::negotiator::Stream,
    deadline: Duration,
) -> Result<Vec<u8>, ProxyError> {
    let mut buf = Vec::with_capacity(4096);
    let n = tokio::time::timeout(deadline, stream.read_to_end(&mut buf))
        .await
        .map_err(|_| ProxyError::Timeout)?
        .map_err(|_| ProxyError::Reset)?;
    if n == 0 {
        return Err(ProxyError::EmptyRecv);
    }
    Ok(buf)
}

/// Split at the first `\r\n\r\n`. If there is none, everything is head and the body is empty.
fn split_head_body(raw: &[u8]) -> (&[u8], &[u8]) {
    match raw.windows(4).position(|w| w == b"\r\n\r\n") {
        Some(i) => (&raw[..i], &raw[i + 4..]),
        None => (raw, &[]),
    }
}

/// Decompress the body per `Content-Encoding`, matching `checker.py:_decompress_content`.
/// gzip/deflate are inflated; anything else (or a decode failure) falls back to a lossy UTF-8
/// read of the raw bytes.
fn decompress(head: &[u8], body: &[u8]) -> String {
    use flate2::read::{DeflateDecoder, GzDecoder};
    use std::io::Read;

    let head_lower = String::from_utf8_lossy(head).to_lowercase();
    let enc = head_lower
        .lines()
        .find_map(|l| l.strip_prefix("content-encoding:"))
        .map(|v| v.trim());

    let decoded = match enc {
        Some("gzip") => {
            let mut out = String::new();
            GzDecoder::new(body)
                .read_to_string(&mut out)
                .ok()
                .map(|_| out)
        }
        Some("deflate") => {
            let mut out = String::new();
            DeflateDecoder::new(body)
                .read_to_string(&mut out)
                .ok()
                .map(|_| out)
        }
        _ => None,
    };
    decoded.unwrap_or_else(|| String::from_utf8_lossy(body).into_owned())
}

/// A valid judge response echoes our marker, at least one IP, and the constant Referer and
/// Cookie header values — proving the proxy forwarded our request intact.
/// `checker.py:_check_test_response`. Case-sensitive (Python does not lowercase here).
fn response_is_valid(content: &str, marker: &str) -> bool {
    content.contains(marker)
        && !get_all_ip(content).is_empty()
        && content.contains("https://www.google.com/") // Referer
        && content.contains("cookie=ok") // Cookie
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_head_body_splits_on_blank_line() {
        let (h, b) = split_head_body(b"HTTP/1.1 200 OK\r\nX: y\r\n\r\nbody here");
        assert_eq!(h, b"HTTP/1.1 200 OK\r\nX: y");
        assert_eq!(b, b"body here");
    }

    #[test]
    fn response_valid_requires_all_markers() {
        let good = "REMOTE_ADDR=1.2.3.4 UA=PxBroker/x/5555 \
                    Referer=https://www.google.com/ Cookie=cookie=ok";
        assert!(response_is_valid(good, "5555"));
        // missing cookie
        let no_cookie = "1.2.3.4 5555 https://www.google.com/";
        assert!(!response_is_valid(no_cookie, "5555"));
        // missing marker
        assert!(!response_is_valid(good, "9999"));
    }
}
