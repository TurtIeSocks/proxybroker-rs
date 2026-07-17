//! D2/Wave 9 — the Redis `Store` backend round-trips proxy history across runs, atomically, and
//! across two independent connections (the shared-fleet case). Gated on a real Redis: each test
//! reads `REDIS_URL` and skips (stays green) if unset, per the crate's zero-network default
//! (constraint C5). A real Redis is expected at `redis://127.0.0.1:6379/` in dev/CI for this file.
//!
//! Each test flushes its own key prefix at the start so reruns against the same persistent Redis
//! stay deterministic — the DB is shared across the whole test binary run.
#![cfg(feature = "store-redis")]

use proxybroker::persist::RedisStore;
use proxybroker::proxy::Proxy;
use proxybroker::types::{AnonLevel, Proto};
use proxybroker::ProxyError;
use proxybroker::Store; // the trait: upsert/load
use std::collections::BTreeSet;
use std::sync::{Mutex, OnceLock};

/// The fixed key layout (`SET_KEY = "proxybroker:proxies"`, no per-test prefix) is shared by every
/// `RedisStore` — by design, it's what lets two independent connections see one another's upserts
/// (`ewma_folds_across_two_connections`). That means these three tests are NOT isolated from each
/// other in Redis, and Rust's default test harness runs them on separate threads concurrently. This
/// lock serializes them within this process so `flush_test_keys` + the test body run as one unit —
/// without it, two tests interleave their SADD/DEL and both flake with a wrong `SMEMBERS` count.
fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Reads `REDIS_URL`, or prints a skip notice and returns `None` — the no-Redis-available path
/// that keeps a plain `cargo test` green.
fn redis_url() -> Option<String> {
    match std::env::var("REDIS_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skipped: no REDIS_URL");
            None
        }
    }
}

/// Delete every `proxybroker:*` key so each test starts from a clean slate against the shared,
/// persistent dev Redis.
fn flush_test_keys(url: &str) {
    let client = redis::Client::open(url).expect("open raw client for cleanup");
    let mut conn = client.get_connection().expect("connect for cleanup");
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("proxybroker:*")
        .query(&mut conn)
        .expect("KEYS proxybroker:*");
    if !keys.is_empty() {
        let mut del = redis::cmd("DEL");
        for k in &keys {
            del.arg(k);
        }
        let _: i64 = del.query(&mut conn).expect("DEL proxybroker:*");
    }
}

fn working_http(ip: &str) -> Proxy {
    let mut p = Proxy::new(ip.parse().unwrap(), 8080, BTreeSet::new());
    p.add_type(Proto::Http, Some(AnonLevel::High));
    p.record_attempt(Some(0.5), None); // one successful attempt
    p
}

#[test]
fn upsert_then_load_roundtrips() {
    let Some(url) = redis_url() else { return };
    let _guard = test_lock().lock().unwrap_or_else(|e| e.into_inner());
    flush_test_keys(&url);

    {
        let store = RedisStore::open(&url).unwrap();
        store.upsert(&working_http("1.2.3.4")).unwrap();
    } // drop, reopen a FRESH store below (history lives in Redis, not the handle)

    let store = RedisStore::open(&url).unwrap();
    let loaded = store.load().unwrap();
    assert_eq!(loaded.len(), 1);
    let p = &loaded[0];
    assert_eq!(p.addr(), "1.2.3.4:8080");
    assert!(p.is_working());
    assert_eq!(p.types().get(&Proto::Http), Some(&Some(AnonLevel::High)));
    assert!(p.avg_resp_time() > 0.0, "stored latency reproduced");
}

#[test]
fn ewma_folds_across_two_connections() {
    let Some(url) = redis_url() else { return };
    let _guard = test_lock().lock().unwrap_or_else(|e| e.into_inner());
    flush_test_keys(&url);

    // TWO RedisStores on the same URL — the shared-fleet case: two broker processes checking the
    // same proxy pool concurrently.
    let store_a = RedisStore::open(&url).unwrap();
    let store_b = RedisStore::open(&url).unwrap();

    store_a.upsert(&working_http("5.5.5.5")).unwrap(); // sample 1.0 -> ewma 1.0 on first insert

    let mut bad = Proxy::new("5.5.5.5".parse().unwrap(), 8080, BTreeSet::new());
    bad.record_attempt(None, Some(ProxyError::Timeout)); // no confirmed type -> sample 0.0
    store_b.upsert(&bad).unwrap();

    let client = redis::Client::open(url).expect("open raw client for assertion");
    let mut conn = client.get_connection().expect("connect for assertion");
    let ewma: f64 = redis::cmd("HGET")
        .arg("proxybroker:proxy:5.5.5.5:8080")
        .arg("ewma_success")
        .query(&mut conn)
        .expect("HGET ewma_success");
    // fold_ewma(1.0, 0.0, 0.3) = 0.3*0.0 + 0.7*1.0 = 0.7
    assert!((ewma - 0.7).abs() < 1e-9, "ewma={ewma}");

    // The failing re-check carried latency 0.0, so the avg_latency zero-guard must KEEP the prior
    // 0.5 (matching SqliteStore's `CASE WHEN excluded.avg_latency > 0 …`), not fold the 0.0 in.
    let avg_latency: f64 = redis::cmd("HGET")
        .arg("proxybroker:proxy:5.5.5.5:8080")
        .arg("avg_latency")
        .query(&mut conn)
        .expect("HGET avg_latency");
    assert!(
        (avg_latency - 0.5).abs() < 1e-9,
        "avg_latency zero-guard should keep the prior 0.5, got {avg_latency}"
    );

    // requests/errors accumulate across upserts (not overwrite): 1 + 1 requests, 0 + 1 errors.
    let (requests, errors): (u32, u32) = redis::cmd("HMGET")
        .arg("proxybroker:proxy:5.5.5.5:8080")
        .arg("requests")
        .arg("errors")
        .query(&mut conn)
        .expect("HMGET requests/errors");
    assert_eq!(requests, 2, "requests accumulate across upserts");
    assert_eq!(errors, 1, "errors accumulate across upserts");
}

#[test]
fn failing_recheck_preserves_confirmed_types() {
    let Some(url) = redis_url() else { return };
    let _guard = test_lock().lock().unwrap_or_else(|e| e.into_inner());
    flush_test_keys(&url);

    {
        let store = RedisStore::open(&url).unwrap();
        store.upsert(&working_http("9.9.9.9")).unwrap(); // Http@High, working
    }
    {
        let store = RedisStore::open(&url).unwrap();
        let mut bad = Proxy::new("9.9.9.9".parse().unwrap(), 8080, BTreeSet::new());
        bad.record_attempt(None, Some(ProxyError::Timeout)); // no confirmed types -> upserts "[]"
        store.upsert(&bad).unwrap();
    }

    let loaded = RedisStore::open(&url).unwrap().load().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(
        loaded[0].types().get(&Proto::Http),
        Some(&Some(AnonLevel::High)),
        "a failing re-check must not erase previously confirmed types"
    );
}
