//! D2 — the SQLite store round-trips proxy history across runs. All against a temp-dir DB, zero
//! network (constraint C5). Raw-column assertions use rusqlite directly (a `persist`-feature dep).
#![cfg(feature = "persist")]

use proxybroker::persist::{Store, SCHEMA_VERSION};
use proxybroker::proxy::Proxy;
use proxybroker::types::{AnonLevel, Proto};
use proxybroker::ProxyError;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn tmp_db() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("pxb-persist-{}-{n}.db", std::process::id()))
}

fn working_http(ip: &str) -> Proxy {
    let mut p = Proxy::new(ip.parse().unwrap(), 8080, BTreeSet::new());
    p.add_type(Proto::Http, Some(AnonLevel::High));
    p.record_attempt(Some(0.5), None); // one successful attempt
    p
}

#[test]
fn upsert_then_load_roundtrips_a_proxy() {
    let path = tmp_db();
    {
        let store = Store::open(&path).unwrap();
        store.upsert(&working_http("1.2.3.4")).unwrap();
    } // drop → close
    let store = Store::open(&path).unwrap(); // reopen: history survives
    let loaded = store.load().unwrap();
    assert_eq!(loaded.len(), 1);
    let p = &loaded[0];
    assert_eq!(p.addr(), "1.2.3.4:8080");
    assert!(p.is_working());
    assert_eq!(p.types().get(&Proto::Http), Some(&Some(AnonLevel::High)));
    assert!(p.avg_resp_time() > 0.0, "stored latency reproduced");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn user_version_is_set_on_open() {
    let path = tmp_db();
    let _store = Store::open(&path).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let v: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, SCHEMA_VERSION);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ewma_folds_across_two_runs() {
    let path = tmp_db();
    {
        let store = Store::open(&path).unwrap();
        store.upsert(&working_http("2.2.2.2")).unwrap(); // sample 1.0 → ewma 1.0 on insert
    }
    {
        let store = Store::open(&path).unwrap();
        let mut bad = Proxy::new("2.2.2.2".parse().unwrap(), 8080, BTreeSet::new());
        bad.record_attempt(None, Some(ProxyError::Timeout)); // no confirmed type → not working (0.0)
        store.upsert(&bad).unwrap();
    }
    let conn = rusqlite::Connection::open(&path).unwrap();
    let ewma: f64 = conn
        .query_row(
            "SELECT ewma_success FROM proxies WHERE host='2.2.2.2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // 0.3*0.0 + 0.7*1.0
    assert!((ewma - 0.7).abs() < 1e-9, "ewma={ewma}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn requests_and_errors_accumulate() {
    let path = tmp_db();
    {
        let store = Store::open(&path).unwrap();
        let mut p = Proxy::new("3.3.3.3".parse().unwrap(), 80, BTreeSet::new());
        p.add_type(Proto::Http, None);
        p.record_attempt(Some(0.1), None); // req 1, err 0
        p.record_attempt(None, Some(ProxyError::Timeout)); // req 2, err 1
        store.upsert(&p).unwrap();
    }
    {
        let store = Store::open(&path).unwrap();
        let mut p = Proxy::new("3.3.3.3".parse().unwrap(), 80, BTreeSet::new());
        p.add_type(Proto::Http, None);
        p.record_attempt(None, Some(ProxyError::Timeout)); // req 1, err 1
        store.upsert(&p).unwrap();
    }
    let conn = rusqlite::Connection::open(&path).unwrap();
    let (req, err): (i64, i64) = conn
        .query_row(
            "SELECT requests, errors FROM proxies WHERE host='3.3.3.3'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(req, 3, "2 + 1 requests");
    assert_eq!(err, 2, "1 + 1 errors");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn migration_from_v0_creates_table() {
    let path = tmp_db();
    let store = Store::open(&path).unwrap(); // fresh path: version 0 → migrate → 1
    assert!(store.load().unwrap().is_empty());
    let conn = rusqlite::Connection::open(&path).unwrap();
    let v: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 1);
    let has_table: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='proxies'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(has_table, 1);
    let _ = std::fs::remove_file(&path);
}
