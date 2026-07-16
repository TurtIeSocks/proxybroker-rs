//! E3 — live-reload the `serve --load` file. `reconcile` is the deterministic, offline-tested core
//! (pure over `Pool`'s public API); the two integration tests prove the real `notify` watcher wires
//! into it. `notify` is OS-driven, so those use bounded real-time polls, never paused time. No
//! network anywhere — `Pool` + a local temp file only (constraint C5).
#![cfg(all(feature = "server", feature = "watch"))]

use proxybroker::proxy::Proxy;
use proxybroker::server::{Pool, PoolConfig};
use proxybroker::types::Proto;
use proxybroker::{reconcile, spawn_watch};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

static FN: AtomicU32 = AtomicU32::new(0);
fn tmp_file() -> PathBuf {
    let n = FN.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("pxb-watch-{}-{n}.ndjson", std::process::id()))
}

fn proxy(ip: &str, port: u16) -> Proxy {
    Proxy::new(ip.parse().unwrap(), port, BTreeSet::from([Proto::Http]))
}
fn key(ip: &str, port: u16) -> (IpAddr, u16) {
    (ip.parse().unwrap(), port)
}
fn write_file(path: &Path, proxies: &[Proxy]) {
    let mut buf = Vec::new();
    proxybroker::write_ndjson(&mut buf, proxies).unwrap();
    std::fs::write(path, buf).unwrap();
}

#[test]
fn reconcile_adds_and_removes() {
    let pool = Pool::from_proxies(
        vec![proxy("1.1.1.1", 1111), proxy("2.2.2.2", 2222)],
        PoolConfig::default(),
    );
    // Desired drops A, keeps B, adds C.
    reconcile(&pool, vec![proxy("2.2.2.2", 2222), proxy("3.3.3.3", 3333)]);
    assert_eq!(
        pool.addrs(),
        BTreeSet::from([key("2.2.2.2", 2222), key("3.3.3.3", 3333)])
    );
}

#[test]
fn reconcile_is_idempotent() {
    let pool = Pool::from_proxies(
        vec![proxy("1.1.1.1", 1111), proxy("2.2.2.2", 2222)],
        PoolConfig::default(),
    );
    let before = pool.addrs();
    reconcile(&pool, vec![proxy("1.1.1.1", 1111), proxy("2.2.2.2", 2222)]);
    assert_eq!(
        pool.addrs(),
        before,
        "reconciling to the current set is a no-op"
    );
}

#[tokio::test]
async fn watch_reparses_on_file_change() {
    let path = tmp_file();
    write_file(&path, &[proxy("1.1.1.1", 1111)]);
    let pool = Pool::from_proxies(vec![proxy("1.1.1.1", 1111)], PoolConfig::default());
    let _h = spawn_watch(pool.clone(), path.clone()).unwrap();

    // Rewrite the file with B added; the watcher should reconcile it into the pool.
    write_file(&path, &[proxy("1.1.1.1", 1111), proxy("2.2.2.2", 2222)]);

    let deadline = Instant::now() + Duration::from_secs(3); // notify is OS-driven; poll for it
    loop {
        if pool.addrs().contains(&key("2.2.2.2", 2222)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "watcher did not pick up the new proxy within the deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn watch_ignores_a_malformed_write() {
    let path = tmp_file();
    write_file(&path, &[proxy("1.1.1.1", 1111)]);
    let pool = Pool::from_proxies(vec![proxy("1.1.1.1", 1111)], PoolConfig::default());
    let _h = spawn_watch(pool.clone(), path.clone()).unwrap();

    // A half-written / garbage file must never empty the pool — parse error is swallowed.
    std::fs::write(&path, b"{ this is not valid ndjson\n").unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await; // past the debounce window

    assert_eq!(
        pool.addrs(),
        BTreeSet::from([key("1.1.1.1", 1111)]),
        "a malformed write must leave the pool untouched"
    );
    let _ = std::fs::remove_file(&path);
}
