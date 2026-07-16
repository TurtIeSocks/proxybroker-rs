//! Bring your own proxies: fill a [`Pool`] from any `Stream<Item = Proxy>` — a database, an mpsc
//! channel, a saved list — without running `find`, then use the pool-management API.
//!
//! ```sh
//! cargo run --example byo_pool
//! ```
//!
//! Shows [`Pool::spawn`] (generic over the source stream, B13), [`Pool::wait_ready`],
//! [`Pool::len`], and [`Pool::remove`] (the same eviction the `proxycontrol` API uses, B6). Fully
//! self-contained — no network.

use futures_util::stream;
use proxybroker::server::{Pool, PoolConfig};
use proxybroker::{Proto, Proxy};
use std::collections::BTreeSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mk = |ip: &str| {
        let mut p = Proxy::new(ip.parse().unwrap(), 8080, BTreeSet::from([Proto::Http]));
        p.add_type(Proto::Http, None); // mark it confirmed-working for HTTP
        p
    };

    // Your own source of proxies — here a fixed list, but it could be any `Stream`.
    let source = stream::iter(vec![
        mk("203.0.113.1"),
        mk("203.0.113.2"),
        mk("203.0.113.3"),
    ]);

    // Pool::spawn drains the stream in the background. wait_ready blocks until the pool is warm (or
    // the source is exhausted), so startup never hangs on a too-small source.
    let pool = Pool::spawn(source, PoolConfig::default());
    pool.wait_ready(1).await;
    println!("pool warmed: {} proxies", pool.len());

    // Evict one by address — this is exactly what `GET http://proxycontrol/api/remove/<ip:port>`
    // does to a live server (B6).
    let removed = pool.remove("203.0.113.2".parse()?, 8080);
    println!(
        "removed 203.0.113.2:8080 → {removed}; pool now has {}",
        pool.len()
    );

    Ok(())
}
