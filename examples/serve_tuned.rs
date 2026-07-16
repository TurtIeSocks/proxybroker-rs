//! A production-tuned rotating server: selection strategy + sticky sessions, country filtering,
//! failure benching, prefer-connect, block-page dodging, and startup gating — all through
//! [`PoolConfig`] and the [`serve`] parameters.
//!
//! ```sh
//! cargo run --example serve_tuned
//! ```
//!
//! Compiling this shows the knobs; running it needs network (it scrapes + checks proxies).

use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig, Strategy};
use proxybroker::{Broker, FindQuery, Proto, TypeSpec};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(8);

    let config = PoolConfig {
        // B1 — pin each client to one upstream; key the session on a request header (else client IP).
        strategy: Strategy::Sticky,
        sticky_header: Some("X-Session-Id".into()),
        // B4 — admit only proxies located in these countries (also filtered at find time below).
        countries: Some(BTreeSet::from(["US".to_string(), "DE".to_string()])),
        // B5 — bench a proxy for 60s after a failure before re-probing it.
        fail_timeout: Duration::from_secs(60),
        // B10 — tie-break toward CONNECT:80-capable proxies.
        prefer_connect: true,
        // B11 — retry through another proxy when the upstream status is a block page (not in the set).
        http_allowed_codes: Some(vec![200, 204, 301, 302]),
        ..Default::default()
    };

    let broker = Broker::builder().build();
    let stream = broker
        .find(FindQuery {
            types: vec![TypeSpec::any(Proto::Http)],
            countries: Some(vec!["US".to_string(), "DE".to_string()]),
            limit: Some(20),
            timeout,
            ..Default::default()
        })
        .await?;
    let pool = Pool::spawn(stream, config);

    let resolver = Arc::new(Resolver::new(timeout)?);
    let handle = serve(
        "127.0.0.1:8888".parse()?,
        pool,
        resolver,
        timeout,
        5,    // B13 — do not accept clients until the pool holds 5 proxies
        2048, // B13 — TCP listen backlog
        None,
    )
    .await?;
    println!("tuned server on {} — Ctrl-C to stop", handle.local_addr());

    tokio::signal::ctrl_c().await?;
    handle.shutdown();
    Ok(())
}
