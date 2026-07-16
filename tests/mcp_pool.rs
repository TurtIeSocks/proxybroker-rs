//! E4 — the MCP tool handlers are thin free functions over a live `Pool`, tested directly (no
//! stdio transport, no subprocess): the rmcp glue is nearly logic-free, so the handlers are the
//! contract. Pure pool manipulation, offline (constraint C5).
#![cfg(all(feature = "mcp", feature = "server"))]

use proxybroker::mcp::{handle_get_proxy, handle_pool_status, handle_report_dead};
use proxybroker::proxy::Proxy;
use proxybroker::server::{Pool, PoolConfig};
use proxybroker::types::{Proto, Scheme};
use std::collections::BTreeSet;

fn http(ip: &str, rt: f64) -> Proxy {
    let mut p = Proxy::new(ip.parse().unwrap(), 80, BTreeSet::new());
    p.add_type(Proto::Http, None);
    p.record_attempt(Some(rt), None); // a runtime so avg_resp_time / priority reflect `rt`
    p
}

#[test]
fn get_proxy_returns_best_and_report_dead_removes_it() {
    let pool = Pool::from_proxies(
        vec![http("1.1.1.1", 0.1), http("2.2.2.2", 0.9)],
        PoolConfig::default(),
    );

    // get_proxy hands out the best (fastest) and puts it back, so the pool is unchanged.
    let info = handle_get_proxy(&pool, Scheme::Http, None).expect("a proxy");
    assert_eq!(info.proxy, "1.1.1.1:80", "fastest is best");
    assert_eq!(
        handle_pool_status(&pool).total,
        2,
        "get_proxy is non-consuming"
    );

    // report_dead removes it, so the next get_proxy returns the slower one.
    assert!(handle_report_dead(&pool, "1.1.1.1:80"), "removed");
    assert_eq!(handle_pool_status(&pool).total, 1);
    let info2 = handle_get_proxy(&pool, Scheme::Http, None).expect("a proxy");
    assert_eq!(info2.proxy, "2.2.2.2:80");

    // report_dead on an address that is not pooled is a no-op → false.
    assert!(!handle_report_dead(&pool, "9.9.9.9:80"), "absent addr");
}
