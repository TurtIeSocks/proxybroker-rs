//! D3 — adaptive re-checking + decay scheduler (`all(server, persist)`).
//!
//! Re-probe pooled proxies on a cadence proportional to their stability (stable → rarely, flaky →
//! often), and stop wasting re-checks on a proxy whose score has decayed away. Keeps a served pool
//! fresh without a human re-running `find`. Drives the server [`Pool`] and folds each re-check
//! outcome into the D2 [`Store`].

use crate::checker::Checker;
use crate::persist::Store;
use crate::proxy::Proxy;
use crate::server::Pool;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// A pooled proxy's identity: the `(host, port)` the scheduler tracks it by.
type Key = (IpAddr, u16);

/// Tuning for the re-check loop.
#[derive(Debug, Clone)]
pub struct RecheckConfig {
    /// Shortest re-check interval (a brand-new/flaky proxy).
    pub min_interval: Duration,
    /// Longest re-check interval (a rock-solid proxy).
    pub max_interval: Duration,
    /// Global ceiling on re-check *starts* per second, across all proxies. Kept well below the
    /// judges' tolerance so the host is not IP-blocked.
    pub rate_per_sec: f64,
    /// Score half-life: a proxy unseen for this long has its effective score halved.
    pub decay_halflife: Duration,
}

impl Default for RecheckConfig {
    fn default() -> Self {
        RecheckConfig {
            min_interval: Duration::from_secs(60),
            max_interval: Duration::from_secs(3600),
            rate_per_sec: 5.0,
            decay_halflife: Duration::from_secs(21_600), // 6h
        }
    }
}

/// The next-check delay for a proxy, from its rolling success `ewma` (`0..=1`): linear between
/// `min_interval` (flaky, `ewma≈0`) and `max_interval` (stable, `ewma≈1`).
pub fn next_interval(ewma: f64, cfg: &RecheckConfig) -> Duration {
    let e = ewma.clamp(0.0, 1.0);
    let min = cfg.min_interval.as_secs_f64();
    let max = cfg.max_interval.as_secs_f64();
    Duration::from_secs_f64(min + e * (max - min))
}

/// A proxy's decayed standing (higher = better): a base goodness from success rate and latency,
/// halved for every `decay_halflife` it has gone unseen. Used to evict stale-flaky proxies. A
/// non-positive half-life disables decay (rather than dividing by zero into a NaN).
pub fn decayed_score(ewma: f64, latency: f64, age: Duration, cfg: &RecheckConfig) -> f64 {
    let base = ewma.clamp(0.0, 1.0) / (1.0 + latency.max(0.0));
    let hl = cfg.decay_halflife.as_secs_f64();
    let decay = if hl > 0.0 {
        0.5f64.powf(age.as_secs_f64() / hl)
    } else {
        1.0
    };
    base * decay
}

/// Whether a failed proxy is still worth retrying (`true`) or should be evicted (`false`): its
/// decayed standing — folded score, latency, and time since it last worked — is still above the
/// floor. Splitting this out keeps the decay/eviction decision unit-testable.
fn should_retry(ewma: f64, latency: f64, age: Duration, cfg: &RecheckConfig) -> bool {
    decayed_score(ewma, latency, age, cfg) >= EVICT_FLOOR
}

/// Handle to a running re-check loop; `shutdown` or drop stops it.
pub struct RecheckHandle {
    cancel: CancellationToken,
}

impl RecheckHandle {
    /// Stop the re-check loop.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

impl Drop for RecheckHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// A due re-check, ordered by `due` so the soonest pops first (min-heap via [`Reverse`]).
struct Scheduled {
    due: Instant,
    host: std::net::IpAddr,
    port: u16,
}
impl PartialEq for Scheduled {
    fn eq(&self, o: &Self) -> bool {
        self.due == o.due
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.due.cmp(&o.due)
    }
}

/// Below this decayed score a proxy is dropped instead of re-scheduled — it has been flaky and
/// unseen long enough that further re-checks are wasted load.
const EVICT_FLOOR: f64 = 0.02;

/// Spawn the re-check loop: pops due proxies off a next-check heap, re-probes each through the
/// shared [`Checker`], upserts the outcome into the [`Store`] (D2), and returns survivors to the
/// pool — evicting a proxy that fails hard or whose score has decayed below the floor. Honors a
/// global start-rate ceiling with jitter and bounded concurrency, and stops when `cancel` fires.
pub fn spawn_rechecker(
    pool: Arc<Pool>,
    checker: Arc<Checker>,
    store: Arc<dyn Store>,
    cfg: RecheckConfig,
) -> RecheckHandle {
    let cancel = CancellationToken::new();
    let handle_cancel = cancel.clone();
    tokio::spawn(async move {
        // Per-address bookkeeping for this run (the durable score lives in the store):
        //   ewma      folded success rate — drives cadence and eviction
        //   last_ok   last time this address re-checked WORKING — the decay clock for eviction
        //   retrying  failed + pulled from the pool but still scheduled, so a watcher-removed
        //             address can be told apart from one we are mid-retry on
        let mut heap: BinaryHeap<Reverse<Scheduled>> = BinaryHeap::new();
        let mut ewma: HashMap<Key, f64> = HashMap::new();
        let mut last_ok: HashMap<Key, Instant> = HashMap::new();
        let mut retrying: HashSet<Key> = HashSet::new();
        // Token-bucket: at most one re-check START per `1/rate_per_sec`, regardless of backlog.
        let gap = Duration::from_secs_f64(1.0 / cfg.rate_per_sec.max(0.001));
        let mut next_start = Instant::now();

        loop {
            // Enroll any pool address we are not yet tracking. This runs every pass, so proxies
            // added after startup (watcher live-reload, find top-up) enter the schedule promptly —
            // enrollment never waits for the heap to drain to empty. Jitter staggers a batch
            // inserted together so it does not thundering-herd the judges.
            let now = Instant::now();
            for (host, port) in pool.addrs() {
                if let std::collections::hash_map::Entry::Vacant(slot) = ewma.entry((host, port)) {
                    slot.insert(1.0); // assume healthy until a re-check says otherwise
                    last_ok.insert((host, port), now);
                    heap.push(Reverse(Scheduled {
                        due: now + jitter(cfg.min_interval),
                        host,
                        port,
                    }));
                }
            }

            let Some(Reverse(top)) = heap.peek() else {
                // Empty schedule (pool still filling / everything evicted): wait a beat, re-enroll.
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(cfg.min_interval) => {}
                }
                continue;
            };
            // Wait for the soonest due, but wake at least every min_interval to re-enroll newcomers
            // even when every scheduled proxy is an hour out.
            let due = top.due;
            let wake = due.min(Instant::now() + cfg.min_interval);
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep_until(wake) => {}
            }
            if Instant::now() < due {
                continue; // woke only to re-enroll; nothing due yet
            }
            // Rate ceiling: never start faster than the token bucket allows.
            let start_at = next_start.max(Instant::now());
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep_until(start_at) => {}
            }
            next_start = start_at + gap;

            let Reverse(job) = heap.pop().expect("peeked");
            let key = (job.host, job.port);
            // If the pool no longer holds this address and we are not mid-retry on it, the watcher
            // removed it — drop it, never re-probe or resurrect a proxy the user just deleted.
            if !retrying.contains(&key) && !pool.addrs().contains(&key) {
                ewma.remove(&key);
                last_ok.remove(&key);
                continue;
            }

            // Re-probe a fresh proxy for this address; the checker records attempts internally.
            let mut proxy = Proxy::new(job.host, job.port, std::collections::BTreeSet::new());
            let working = checker.check(&mut proxy).await;
            let _ = store.upsert(&proxy);

            // Fold this outcome into the in-memory cadence EWMA (per-run; the durable one lives in
            // the store). 0.3 new + 0.7 prior, matching the store's alpha.
            let sample = if working { 1.0 } else { 0.0 };
            let prev = ewma.get(&key).copied().unwrap_or(1.0);
            let e = 0.3 * sample + 0.7 * prev;
            ewma.insert(key, e);
            let now = Instant::now();
            let next_due = now + next_interval(e, &cfg) + jitter(cfg.min_interval);

            if working {
                last_ok.insert(key, now);
                retrying.remove(&key);
                pool.add(proxy); // dedup on (host,port)
                heap.push(Reverse(Scheduled {
                    due: next_due,
                    host: job.host,
                    port: job.port,
                }));
            } else {
                pool.remove_addr(job.host, job.port);
                // Evict once the score, decayed by how long since it last worked, falls below the
                // floor; else keep retrying. The `age` from last_ok is what gives --decay-halflife
                // its effect — the eviction site used to pass a constant zero age.
                let age = now.saturating_duration_since(*last_ok.get(&key).unwrap_or(&now));
                if should_retry(e, proxy.avg_resp_time(), age, &cfg) {
                    retrying.insert(key);
                    heap.push(Reverse(Scheduled {
                        due: next_due,
                        host: job.host,
                        port: job.port,
                    }));
                } else {
                    ewma.remove(&key);
                    last_ok.remove(&key);
                    retrying.remove(&key);
                }
            }
        }
    });
    RecheckHandle {
        cancel: handle_cancel,
    }
}

/// Uniform ± up to 50% jitter on `base`, so proxies scheduled together spread out.
fn jitter(base: Duration) -> Duration {
    let f = 1.0 + rand::rng().random_range(-0.5..=0.5);
    Duration::from_secs_f64(base.as_secs_f64() * f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cadence_grows_with_stability() {
        let cfg = RecheckConfig {
            min_interval: Duration::from_secs(60),
            max_interval: Duration::from_secs(3600),
            ..Default::default()
        };
        assert_eq!(next_interval(0.0, &cfg), Duration::from_secs(60)); // flaky → min
        assert_eq!(next_interval(1.0, &cfg), Duration::from_secs(3600)); // stable → max
        assert!(next_interval(1.0, &cfg) > next_interval(0.0, &cfg));
        // Monotonic: a steadier proxy is always re-checked no sooner.
        assert!(next_interval(0.9, &cfg) >= next_interval(0.4, &cfg));
        // Clamped: out-of-range ewma does not overshoot.
        assert_eq!(next_interval(2.0, &cfg), Duration::from_secs(3600));
    }

    #[test]
    fn decay_halves_score_at_one_half_life() {
        let cfg = RecheckConfig {
            decay_halflife: Duration::from_secs(3600),
            ..Default::default()
        };
        let fresh = decayed_score(1.0, 0.5, Duration::ZERO, &cfg);
        let one_hl = decayed_score(1.0, 0.5, Duration::from_secs(3600), &cfg);
        assert!(
            (one_hl - fresh * 0.5).abs() < 1e-9,
            "half-life must halve the score"
        );
        // Two half-lives → a quarter.
        let two_hl = decayed_score(1.0, 0.5, Duration::from_secs(7200), &cfg);
        assert!((two_hl - fresh * 0.25).abs() < 1e-9);
    }

    #[test]
    fn staleness_drives_eviction() {
        // A failed proxy with a healthy score is retried; the same score gone stale for many
        // half-lives decays below the floor and is evicted. This is what makes --decay-halflife live.
        let cfg = RecheckConfig {
            decay_halflife: Duration::from_secs(60),
            ..Default::default()
        };
        assert!(
            should_retry(0.5, 0.0, Duration::ZERO, &cfg),
            "a decent, just-seen score keeps being retried"
        );
        assert!(
            !should_retry(0.5, 0.0, Duration::from_secs(600), &cfg),
            "ten half-lives of staleness must decay below the evict floor"
        );
    }

    #[test]
    fn zero_halflife_does_not_nan() {
        // --decay-halflife 0 must not produce NaN (0.5^(age/0)); a non-positive half-life disables
        // decay rather than evicting a proxy on its first failure.
        let cfg = RecheckConfig {
            decay_halflife: Duration::ZERO,
            ..Default::default()
        };
        let s0 = decayed_score(1.0, 0.0, Duration::ZERO, &cfg);
        let s1 = decayed_score(1.0, 0.0, Duration::from_secs(3600), &cfg);
        assert!(
            s0.is_finite() && s1.is_finite(),
            "no NaN/Inf from a zero half-life"
        );
        assert!(should_retry(0.5, 0.0, Duration::from_secs(3600), &cfg));
    }
}
