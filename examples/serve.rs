//! Run a local rotating proxy server, then make a request through it.
//!
//! ```sh
//! cargo run --example serve
//! ```
//!
//! The Rust equivalent of proxybroker2's `proxy_server.py` + `use_existing_proxy.py`. `find`
//! seeds a [`Pool`]; [`serve`] listens and relays each client connection through the best
//! available proxy, retrying on a different one when a proxy fails. Requires the `server`
//! feature (on by default).

use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig};
use proxybroker::{Broker, FindQuery, Proto, TypeSpec};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(8);
    let broker = Broker::builder().build();

    // Seed the pool with a handful of working HTTP proxies.
    println!("finding proxies to fill the pool…");
    let stream = broker
        .find(FindQuery {
            types: vec![TypeSpec::any(Proto::Http)],
            limit: Some(10),
            timeout,
            ..Default::default()
        })
        .await?;

    let pool = Pool::spawn(stream, PoolConfig::default());
    let resolver = Arc::new(Resolver::new(timeout)?);
    // min_queue 0 (serve as soon as a proxy arrives) and the default 1024 listen backlog.
    let handle = serve("127.0.0.1:8888".parse()?, pool, resolver, timeout, 0, 1024).await?;
    println!("serving on {}", handle.local_addr());

    // Use it: fetch a page through the local proxy, exactly as any client would.
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!(
            "http://{}",
            handle.local_addr()
        ))?)
        .build()?;

    match client
        .get("http://example.com/")
        .timeout(Duration::from_secs(20))
        .send()
        .await
    {
        Ok(resp) => println!("fetched example.com via the pool → HTTP {}", resp.status()),
        Err(e) => println!("fetch failed (public proxies are flaky, try again): {e}"),
    }

    handle.shutdown();
    Ok(())
}
