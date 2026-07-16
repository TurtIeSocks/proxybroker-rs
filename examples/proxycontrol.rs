//! The `proxycontrol` control API (B6): introspect and steer a live server without a restart, by
//! sending it requests (as its own client) addressed to the magic `proxycontrol` host.
//!
//! ```sh
//! cargo run --example proxycontrol
//! ```
//!
//! The server intercepts `Host: proxycontrol` before selection, so these calls never consume a pool
//! proxy. Self-contained — the `remove` call needs no upstream.

use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig};
use proxybroker::{Proto, Proxy};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut proxy = Proxy::new("203.0.113.5".parse()?, 3128, BTreeSet::from([Proto::Http]));
    proxy.add_type(Proto::Http, None);
    let pool = Pool::from_proxies(vec![proxy], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(8))?);
    let handle = serve(
        "127.0.0.1:0".parse()?,
        pool,
        resolver,
        Duration::from_secs(8),
        0,
        1024,
        None,
    )
    .await?;
    let addr = handle.local_addr();
    println!("server on {addr}");

    // Reach the control API by pointing a client at the server as its HTTP proxy and requesting the
    // `proxycontrol` host. reqwest sends the absolute-URI request straight to the proxy.
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{addr}"))?)
        .build()?;

    // Evict a proxy from the live pool: `GET .../api/remove/<ip:port>` → always 204.
    let resp = client
        .get("http://proxycontrol/api/remove/203.0.113.5:3128")
        .send()
        .await?;
    println!("remove 203.0.113.5:3128 → HTTP {}", resp.status());

    // Ask which upstream last served a URL for this client:
    //   `GET http://proxycontrol/api/history/url:<url>` → 200 {"proxy":"<ip:port>"}, or 204 on a miss.
    // (Here it is a miss — nothing has been relayed yet.)
    let resp = client
        .get("http://proxycontrol/api/history/url:http://example.com/")
        .send()
        .await?;
    println!("history for http://example.com/ → HTTP {}", resp.status());

    handle.shutdown();
    Ok(())
}
