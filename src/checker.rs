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
use crate::types::{AnonLevel, Caps, JudgeScheme, Proto, TypeSpec};
use crate::utils::{fresh_marker, get_all_ip, get_status_code, request_headers};
use rand::Rng;
use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Everything needed to run checks against one proxy.
pub struct Checker {
    judges: JudgePool,
    real_ext_ips: HashSet<IpAddr>,
    requested: Vec<TypeSpec>,
    timeout: Duration,
    post: bool,
    strict: bool,
    relaxed_validity: bool,
    trust_check: bool,
    /// DNS blocklist zones; empty disables the check. Kept with the resolver so `check` can
    /// reject a listed proxy before doing any real work.
    dnsbl: Vec<String>,
    resolver: std::sync::Arc<Resolver>,
    /// Judged (a verified judge pool) or Liveness (graceful degradation, A2).
    mode: CheckMode,
    retry: RetryPolicy,
}

/// How the checker validates a proxy: against a verified judge, or — when no judge came up and the
/// caller supplied a liveness URL — by a plain fetch-through-the-proxy 200 check (A2).
enum CheckMode {
    Judged,
    Liveness(LivenessTarget),
}

/// A resolved liveness endpoint: a plain URL the checker GETs through the proxy, expecting 200.
struct LivenessTarget {
    host: String,
    path: String,
    ip: IpAddr,
    scheme: JudgeScheme,
}

impl LivenessTarget {
    /// Parse + resolve a liveness URL. Reuses [`Judge::parse`] for scheme/host/path. `None` if the
    /// URL is malformed, is an SMTP URL (liveness is an HTTP GET), or the host does not resolve.
    async fn resolve(url: &str, resolver: &Resolver) -> Option<LivenessTarget> {
        let judge = Judge::parse(url)?;
        if judge.scheme == JudgeScheme::Smtp {
            return None;
        }
        let ip = resolver.resolve(&judge.host).await.ok()?;
        Some(LivenessTarget {
            host: judge.host,
            path: judge.path,
            ip,
            scheme: judge.scheme,
        })
    }
}

/// What a single [`Checker::attempt`] validates against — a judge (with anonymity classification)
/// or a liveness target (200 = working, level `None`).
enum Probe<'a> {
    Judged(&'a Judge),
    Liveness(&'a LivenessTarget),
}

impl std::fmt::Debug for Checker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Resolver holds a hickory resolver that is not Debug; summarize instead.
        f.debug_struct("Checker")
            .field("judges", &self.judges.counts())
            .field("requested", &self.requested)
            .field("strict", &self.strict)
            .field("dnsbl", &self.dnsbl)
            .finish_non_exhaustive()
    }
}

/// Which errors retry and how long to wait between attempts (A5). Replaces a bare `max_tries`.
///
/// The [`Default`] reproduces the historical behavior exactly: 3 attempts, retry **only**
/// `Timeout`, no delay — so existing callers are unaffected.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    /// Total attempts per protocol (`>= 1`).
    pub max_tries: usize,
    /// Which per-proxy errors are retryable. Default: just `Timeout` (parity).
    pub retry_on: HashSet<ProxyError>,
    /// Base delay before the first retry. Zero = no delay (parity).
    pub backoff: Duration,
    /// Per-attempt multiplier on the delay (exponential). `1.0` = constant.
    pub factor: f64,
    /// Symmetric jitter fraction applied to each delay, `0.0..=1.0`.
    pub jitter: f64,
    /// Upper bound on any single delay. Zero = uncapped.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_tries: 3,
            retry_on: HashSet::from([ProxyError::Timeout]),
            backoff: Duration::ZERO,
            factor: 1.0,
            jitter: 0.0,
            max_backoff: Duration::ZERO,
        }
    }
}

impl RetryPolicy {
    /// `max_tries` attempts, otherwise the default policy (retry only `Timeout`, no delay).
    pub fn tries(max_tries: usize) -> Self {
        RetryPolicy {
            max_tries,
            ..Default::default()
        }
    }

    /// Retry the transient set — `{Timeout, Reset, ConnFailed, EmptyRecv}` — for `max_tries`.
    pub fn transient(max_tries: usize) -> Self {
        RetryPolicy {
            max_tries,
            retry_on: HashSet::from([
                ProxyError::Timeout,
                ProxyError::Reset,
                ProxyError::ConnFailed,
                ProxyError::EmptyRecv,
            ]),
            ..Default::default()
        }
    }

    /// The delay before retry number `i` (0-based): `min(max_backoff, backoff * factor^i)`, then
    /// symmetric jitter of `±jitter` fraction. Zero base → zero (no wall-clock sleep on the
    /// parity path).
    pub fn backoff_for(&self, i: usize) -> Duration {
        if self.backoff.is_zero() {
            return Duration::ZERO;
        }
        let mut d = self.backoff.as_secs_f64() * self.factor.powi(i as i32);
        if !self.max_backoff.is_zero() {
            d = d.min(self.max_backoff.as_secs_f64());
        }
        if self.jitter > 0.0 {
            let r: f64 = rand::rng().random_range(-1.0..=1.0);
            d *= 1.0 + self.jitter * r;
        }
        Duration::from_secs_f64(d.max(0.0))
    }
}

/// Configuration for [`Checker::new`].
#[derive(Debug, Clone, Default)]
pub struct CheckerConfig {
    /// Judge URLs to probe. Empty → the bundled defaults.
    pub judges: Vec<String>,
    /// Protocols (and optional anonymity levels) to check. Required — empty is an error.
    pub types: Vec<TypeSpec>,
    pub timeout: Duration,
    /// Retry policy: attempt count, which errors retry, and the backoff schedule (A5).
    pub retry: RetryPolicy,
    /// Use `POST` instead of `GET` for the test request.
    pub post: bool,
    /// Require the proxy's anonymity level to match exactly (`--strict`).
    pub strict: bool,
    /// Relax response validity to marker + IP only, demoting Referer/Cookie from validity gates to
    /// recorded capability signals (A4). Default `false` = parity (all four required).
    pub relaxed_validity: bool,
    /// Run honeypot detection on each proxy and record the trust verdict (A6). Default `false` =
    /// no assessment, zero cost.
    pub trust_check: bool,
    /// DNS blocklist zones (`--dnsbl`); a proxy whose IP is listed in any zone is rejected.
    pub dnsbl: Vec<String>,
    /// When the judge pool is empty, fall back to a plain liveness check against this URL instead
    /// of failing with `NoJudges`. `None` keeps the strict judge-required behavior. Proxies
    /// confirmed this way carry anonymity level `None` (unclassifiable without a judge).
    pub liveness_url: Option<String>,
}

impl Checker {
    /// Build a checker, probing the judges eagerly. Errors:
    /// - [`Error::NoTypes`] if no protocol was requested (`api.py:249`);
    /// - [`Error::NoJudges`] if no judge verifies (`checker.py:137`).
    pub async fn new(
        cfg: CheckerConfig,
        resolver: std::sync::Arc<Resolver>,
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
            JudgePool::probe_all(candidates, &resolver, client, &real_ext_ips, cfg.timeout).await;
        // No judge came up: fail as before, unless a liveness URL enables graceful degradation
        // (A2). A malformed/unresolvable liveness URL still errors — there is nothing to check.
        let mode = if judges.is_empty() {
            match &cfg.liveness_url {
                Some(url) => CheckMode::Liveness(
                    LivenessTarget::resolve(url, &resolver)
                        .await
                        .ok_or(Error::NoJudges)?,
                ),
                None => return Err(Error::NoJudges),
            }
        } else {
            CheckMode::Judged
        };
        Ok(Checker {
            judges,
            real_ext_ips,
            requested: cfg.types,
            timeout: cfg.timeout,
            retry: cfg.retry,
            post: cfg.post,
            strict: cfg.strict,
            relaxed_validity: cfg.relaxed_validity,
            trust_check: cfg.trust_check,
            dnsbl: cfg.dnsbl,
            resolver,
            mode,
        })
    }

    /// Check a proxy across the protocols it is expected to support (intersected with the
    /// requested set), record its working types, and return whether it passes. `checker.py:check`.
    pub async fn check(&self, proxy: &mut Proxy) -> bool {
        // A proxy listed in a configured DNS blocklist is rejected before any real work
        // (`checker.py:167` runs this first).
        if !self.dnsbl.is_empty() && self.in_dnsbl(proxy.host).await {
            tracing::debug!(addr = %proxy.addr(), "rejected: found in DNSBL");
            return false;
        }

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

    /// Is the proxy's IP listed in any configured DNS blocklist? Mirrors `checker.py:_in_DNSBL`:
    /// reverse the address, prepend it to each zone, and if ANY such name resolves, the IP is
    /// listed. Queries run concurrently. IPv6 is not checked (see [`dnsbl_query`]).
    async fn in_dnsbl(&self, host: IpAddr) -> bool {
        let queries: Vec<String> = self
            .dnsbl
            .iter()
            .filter_map(|zone| dnsbl_query(host, zone))
            .collect();
        if queries.is_empty() {
            return false;
        }
        let probes = queries.iter().map(|q| self.resolver.resolve(q));
        futures_util::future::join_all(probes)
            .await
            .iter()
            .any(|r| r.is_ok())
    }

    /// Check one protocol, retrying on timeout up to `max_tries`. Mirrors `checker.py:_check`:
    /// a timeout retries (`continue`), any other proxy error stops (`break`).
    async fn check_one(&self, proxy: &mut Proxy, proto: Proto) -> bool {
        let scheme = proto.judge_scheme();
        // Resolve the probe + target for this protocol. Judged mode routes to a random judge for
        // the scheme; Liveness mode always probes the single liveness endpoint.
        let judge; // holds the Arc alive across the retry loop in Judged mode
        let (probe, target) = match &self.mode {
            CheckMode::Judged => {
                let Some(j) = self.judges.random(scheme) else {
                    return false; // no judge for this scheme
                };
                judge = j;
                let target = Target {
                    host: judge.host.clone(),
                    ip: judge.ip,
                    port: scheme.default_port(),
                };
                (Probe::Judged(&judge), target)
            }
            CheckMode::Liveness(lt) => {
                let target = Target {
                    host: lt.host.clone(),
                    ip: Some(lt.ip),
                    port: lt.scheme.default_port(),
                };
                (Probe::Liveness(lt), target)
            }
        };

        for i in 0..self.retry.max_tries {
            let start = Instant::now();
            match self.attempt(proxy, proto, &probe, &target).await {
                Ok(Attempt::Working(obs)) => {
                    proxy.record_attempt(Some(start.elapsed().as_secs_f64()), None);
                    proxy.add_type(proto, obs.level);
                    proxy.record_caps(obs.caps);
                    proxy.set_trust(obs.trust);
                    return true;
                }
                Ok(Attempt::Invalid) => {
                    // Response did not validate — the proxy does not work for this protocol.
                    // Python breaks here (no retry).
                    proxy.record_attempt(None, Some(ProxyError::BadResponse));
                    return false;
                }
                Err(e) => {
                    proxy.record_attempt(None, Some(e));
                    // Retry only errors the policy marks retryable, and only if attempts remain.
                    // Default policy = {Timeout}, zero backoff → the historical timeout-only,
                    // no-delay retry, exactly.
                    if self.retry.retry_on.contains(&e) && i + 1 < self.retry.max_tries {
                        tokio::time::sleep(self.retry.backoff_for(i)).await;
                        continue;
                    }
                    return false;
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
        probe: &Probe<'_>,
        target: &Target,
    ) -> Result<Attempt, ProxyError> {
        let tcp = tokio::time::timeout(self.timeout, TcpStream::connect((proxy.host, proxy.port)))
            .await
            .map_err(|_| ProxyError::Timeout)?
            .map_err(|_| ProxyError::ConnFailed)?;

        // The checker probes public candidates; it has no upstream credentials (B8 auth is inert
        // on the check path).
        let mut stream = negotiate(proto, tcp, target, self.timeout, None).await?;

        // CONNECT:25 has no test request — a granted tunnel is the whole check.
        if proto == Proto::Connect25 {
            return Ok(Attempt::Working(Observation::default()));
        }

        // Build + send the request; the connect/negotiate/send/read/status handling is identical
        // for both modes — only what we assert about the 200 response differs.
        let (request, marker) = match probe {
            Probe::Judged(judge) => {
                let (req, marker) = self.build_request(judge, proto);
                (req, Some(marker))
            }
            Probe::Liveness(lt) => (self.build_liveness_request(lt, proto), None),
        };
        tokio::time::timeout(self.timeout, stream.write_all(&request))
            .await
            .map_err(|_| ProxyError::Timeout)?
            .map_err(|_| ProxyError::Reset)?;

        let raw = read_response(&mut stream, self.timeout).await?;
        let (head, body) = split_head_body(&raw);
        if get_status_code(head, 9, 12) != 200 {
            return Err(ProxyError::BadStatus);
        }

        match probe {
            // Liveness: a 200 through the proxy is the whole check; there is no judge to reflect
            // markers/IPs, so anonymity is unclassifiable → level None, no capability profile.
            Probe::Liveness(_) => Ok(Attempt::Working(Observation::default())),
            Probe::Judged(judge) => {
                let content = decompress(head, body);
                let marker = marker.expect("judged mode always builds a marker");
                let caps = caps_from_content(&content);
                // Default (strict): Referer + Cookie are validity gates (parity). Relaxed: only
                // marker + a non-empty IP set are required, and the echo flags become recorded
                // capability signals that can vary per proxy (A4).
                let valid = if self.relaxed_validity {
                    content.contains(&marker) && !get_all_ip(&content).is_empty()
                } else {
                    response_is_valid(&content, &marker)
                };
                if !valid {
                    return Ok(Attempt::Invalid);
                }
                let level = if proto.checks_anon_level() {
                    Some(self.anonymity_level(&content, judge))
                } else {
                    None
                };
                // Honeypot verdict (A6): only when opted in — the marker doubles as the canary.
                let trust = if self.trust_check {
                    TrustReport::assess(&sent_header_names(), &marker, &content)
                } else {
                    TrustReport::default()
                };
                Ok(Attempt::Working(Observation { level, caps, trust }))
            }
        }
    }

    /// A plain `GET` for the liveness endpoint (A2), routed like the test request: absolute-form
    /// for plain HTTP (it goes to the proxy), origin-form when tunnelled. No marker/cookie/referer
    /// — there is no judge to reflect them; only the 200 matters.
    fn build_liveness_request(&self, lt: &LivenessTarget, proto: Proto) -> Vec<u8> {
        let mut hdrs = request_headers(None);
        hdrs.insert("Host", lt.host.clone());
        hdrs.insert("Connection", "close".to_owned());
        let path = if proto.uses_full_path() {
            format!("http://{}{}", lt.host, lt.path)
        } else {
            lt.path.clone()
        };
        let headers: String = hdrs
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join("\r\n");
        format!("GET {path} HTTP/1.1\r\n{headers}\r\n\r\n").into_bytes()
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
    /// The proxy works for this protocol; carries the classification [`Observation`].
    Working(Observation),
    /// Connected and responded, but the response failed validation.
    Invalid,
}

/// What a working attempt observed about the proxy: its anonymity level (HTTP only), the
/// capability profile (A4), and the trust verdict (A6).
#[derive(Debug, Default)]
struct Observation {
    level: Option<AnonLevel>,
    caps: Caps,
    trust: TrustReport,
}

/// A specific way a proxy's judge round-trip looked hostile (A6). Reported individually — never a
/// bare "untrusted" boolean — so the caller sees *which* signal fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustSignal {
    /// Our canary nonce did not survive the round-trip unmutated (content tampering).
    CanaryMismatch,
    /// The echoed request carried a header we never sent (injection).
    InjectedHeader,
    /// The HTTPS judge presented a certificate that did not match the pin (MITM). Reserved for the
    /// optional cert-pin follow-up; the dependency-free core never emits it.
    CertMismatch,
}

/// The honeypot/hostility verdict for a proxy (A6). Empty = trusted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrustReport {
    pub signals: Vec<TrustSignal>,
}

impl TrustReport {
    /// No signal fired.
    pub fn trusted(&self) -> bool {
        self.signals.is_empty()
    }

    /// Assess a **decompressed** judge response against what we sent. Pure — the unit under test.
    ///
    /// - **Canary:** our nonce must appear verbatim (content tampering else).
    /// - **Injected headers:** the echoed request (scanned as `Name: value` lines) must carry no
    ///   header name outside the set we sent ∪ a benign-hop allowlist. `Via`/`X-Forwarded-For` are
    ///   the *anonymity* signal, not trust, so they are allow-listed to avoid double-counting.
    ///
    /// The scan splits on `": "` (colon-space), so a URL value like `https://…` on its own line is
    /// not mistaken for a header. It is a heuristic: opt-in, with documented false-positive guards.
    pub fn assess(sent_header_names: &[&str], canary: &str, content: &str) -> TrustReport {
        let mut signals = Vec::new();
        if !content.contains(canary) {
            signals.push(TrustSignal::CanaryMismatch);
        }
        const BENIGN: &[&str] = &[
            "host",
            "connection",
            "content-length",
            "x-forwarded-for",
            "via",
        ];
        let sent: HashSet<String> = sent_header_names
            .iter()
            .map(|h| h.to_ascii_lowercase())
            .collect();
        for line in content.lines() {
            let Some((name, _)) = line.split_once(": ") else {
                continue;
            };
            let name = name.trim().to_ascii_lowercase();
            // Only plausible header names (letters/digits/hyphen) — skips `REMOTE_ADDR = …` etc.
            if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
                continue;
            }
            if !sent.contains(&name) && !BENIGN.contains(&name.as_str()) {
                signals.push(TrustSignal::InjectedHeader);
                break;
            }
        }
        TrustReport { signals }
    }
}

/// The header names the checker sends on the test request (A6 injected-header baseline): the
/// constant request headers plus the three `build_request` adds.
fn sent_header_names() -> Vec<&'static str> {
    request_headers(None)
        .keys()
        .copied()
        .chain(["Host", "Connection", "Content-Length"])
        .collect()
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

/// The DNSBL query name for `ip` in `zone`: the reversed IPv4 octets, then the zone
/// (`1.2.3.4` + `zen.spamhaus.org` → `4.3.2.1.zen.spamhaus.org`). Returns `None` for IPv6:
/// DNSBLs use a nibble-reversed format for v6 that `checker.py`'s simple `split(".")` reversal
/// does not produce, so — like the Python — v6 is effectively unsupported and skipped here.
fn dnsbl_query(ip: IpAddr, zone: &str) -> Option<String> {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            Some(format!("{}.{}.{}.{}.{}", o[3], o[2], o[1], o[0], zone))
        }
        IpAddr::V6(_) => None,
    }
}

/// The capability signals in a judge response (A4): whether the proxy forwarded our constant
/// Referer and Cookie header values through. Shared with [`response_is_valid`] so the marker
/// literals live in one place.
fn caps_from_content(content: &str) -> Caps {
    Caps {
        cookie_echo: content.contains("cookie=ok"),
        referer_echo: content.contains("https://www.google.com/"),
    }
}

/// A valid judge response echoes our marker, at least one IP, and the constant Referer and
/// Cookie header values — proving the proxy forwarded our request intact.
/// `checker.py:_check_test_response`. Case-sensitive (Python does not lowercase here).
fn response_is_valid(content: &str, marker: &str) -> bool {
    let caps = caps_from_content(content);
    content.contains(marker)
        && !get_all_ip(content).is_empty()
        && caps.referer_echo
        && caps.cookie_echo
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
    fn dnsbl_query_reverses_ipv4_octets() {
        // checker.py: ".".join(reversed(host.split("."))) + "." + zone
        assert_eq!(
            dnsbl_query("1.2.3.4".parse().unwrap(), "zen.spamhaus.org").as_deref(),
            Some("4.3.2.1.zen.spamhaus.org")
        );
        // IPv6 is skipped (Python's dot-split reversal does not produce a valid v6 query).
        assert_eq!(
            dnsbl_query("2001:db8::1".parse().unwrap(), "zen.spamhaus.org"),
            None
        );
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

    #[test]
    fn caps_extracted_from_content() {
        let both = "REMOTE_ADDR=1.2.3.4 cookie=ok Referer=https://www.google.com/";
        assert_eq!(
            caps_from_content(both),
            Caps {
                cookie_echo: true,
                referer_echo: true
            }
        );
        // drop the cookie echo
        let no_cookie = "REMOTE_ADDR=1.2.3.4 Referer=https://www.google.com/";
        assert_eq!(
            caps_from_content(no_cookie),
            Caps {
                cookie_echo: false,
                referer_echo: true
            }
        );
        // neither
        assert_eq!(caps_from_content("nothing here"), Caps::default());
    }

    #[test]
    fn default_policy_retries_only_timeout() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_tries, 3);
        assert_eq!(p.retry_on, HashSet::from([ProxyError::Timeout]));
        assert!(!p.retry_on.contains(&ProxyError::Reset));
        assert!(p.backoff.is_zero());
    }

    #[test]
    fn retry_policy_backoff_schedule() {
        // Constant (factor 1.0): 100ms every attempt.
        let c = RetryPolicy {
            backoff: Duration::from_millis(100),
            factor: 1.0,
            ..Default::default()
        };
        for i in 0..=2 {
            assert_eq!(c.backoff_for(i).as_millis(), 100);
        }
        // Exponential (factor 2.0): 100 / 200 / 400.
        let e = RetryPolicy {
            backoff: Duration::from_millis(100),
            factor: 2.0,
            ..Default::default()
        };
        assert_eq!(e.backoff_for(0).as_millis(), 100);
        assert_eq!(e.backoff_for(1).as_millis(), 200);
        assert_eq!(e.backoff_for(2).as_millis(), 400);
        // max_backoff caps the delay.
        let cap = RetryPolicy {
            backoff: Duration::from_millis(100),
            factor: 2.0,
            max_backoff: Duration::from_millis(250),
            ..Default::default()
        };
        assert_eq!(cap.backoff_for(2).as_millis(), 250);
        // jitter 0.5 keeps each delay within [0.5x, 1.5x].
        let j = RetryPolicy {
            backoff: Duration::from_millis(100),
            jitter: 0.5,
            ..Default::default()
        };
        for _ in 0..64 {
            let ms = j.backoff_for(0).as_secs_f64() * 1000.0;
            assert!((50.0..=150.0).contains(&ms), "jitter out of band: {ms}ms");
        }
        // Zero base → zero: the parity path never sleeps.
        assert_eq!(RetryPolicy::default().backoff_for(0), Duration::ZERO);
    }

    // The header names the checker sends (the injected-header baseline).
    const SENT: &[&str] = &[
        "User-Agent",
        "Accept",
        "Accept-Encoding",
        "Pragma",
        "Cache-control",
        "Cookie",
        "Referer",
        "Host",
        "Connection",
        "Content-Length",
    ];

    #[test]
    fn sent_header_names_matches_the_request() {
        // The baseline must equal what build_request actually sends, or the scan false-positives.
        let mut got = sent_header_names();
        got.sort_unstable();
        let mut want = SENT.to_vec();
        want.sort_unstable();
        assert_eq!(got, want);
    }

    #[test]
    fn trust_clean_response_is_trusted() {
        let content = "User-Agent: PxBroker/0.1/CANARY42\n\
             Cookie: cookie=ok\n\
             Referer: https://www.google.com/\n\
             Host: judge.example\n\
             REMOTE_ADDR = 8.8.8.8\n";
        let r = TrustReport::assess(SENT, "CANARY42", content);
        assert!(r.trusted(), "unexpected signals: {:?}", r.signals);
    }

    #[test]
    fn trust_injected_header_is_flagged() {
        let content = "User-Agent: PxBroker/0.1/CANARY42\n\
             Cookie: cookie=ok\n\
             X-Ad-Inject: buy now\n";
        let r = TrustReport::assess(SENT, "CANARY42", content);
        assert_eq!(r.signals, vec![TrustSignal::InjectedHeader]);
    }

    #[test]
    fn trust_canary_mutation_is_flagged() {
        let content = "User-Agent: PxBroker/0.1/MUTATED\n\
             Cookie: cookie=ok\n\
             Host: judge.example\n";
        let r = TrustReport::assess(SENT, "CANARY42", content);
        assert_eq!(r.signals, vec![TrustSignal::CanaryMismatch]);
    }

    #[test]
    fn trust_forwarded_via_headers_are_not_injection() {
        // Via / X-Forwarded-For are the anonymity signal, allow-listed so they don't double-count.
        let content = "User-Agent: PxBroker/0.1/CANARY42\n\
             Via: 1.1 someproxy\n\
             X-Forwarded-For: 8.8.8.8\n\
             Host: judge.example\n";
        let r = TrustReport::assess(SENT, "CANARY42", content);
        assert!(r.trusted(), "unexpected signals: {:?}", r.signals);
    }

    #[test]
    fn trust_url_value_on_its_own_is_not_a_header() {
        // A bare URL must not be parsed as a `https:` header (the `": "` split guards this).
        let content = "User-Agent: PxBroker/0.1/CANARY42\nhttps://www.google.com/\n";
        let r = TrustReport::assess(SENT, "CANARY42", content);
        assert!(r.trusted(), "unexpected signals: {:?}", r.signals);
    }
}
