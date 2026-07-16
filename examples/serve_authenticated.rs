//! Authentication both ways: gate clients of the local server (B9), and relay through
//! authenticated/paid upstream proxies (B8).
//!
//! ```sh
//! cargo run --example serve_authenticated
//! ```
//!
//! A client without a matching `Proxy-Authorization` gets `407` and no pool proxy is touched. The
//! pool's upstreams carry their own [`Credentials`], applied automatically by the negotiator
//! (SOCKS5 RFC 1929) or as `Proxy-Authorization` on CONNECT/forward — and never serialized, so the
//! secrets stay out of `--format json`. The client's own gate credential is stripped before
//! forwarding, so it never leaks to the upstream.

use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig};
use proxybroker::{Credentials, Proto, Proxy};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(8);

    // B8: a paid upstream proxy that requires username/password.
    let mut upstream = Proxy::new("203.0.113.10".parse()?, 3128, BTreeSet::from([Proto::Http]));
    upstream.add_type(Proto::Http, None);
    let upstream = upstream.with_auth(Credentials {
        username: "paid-user".into(),
        password: "paid-secret".into(),
    });

    let pool = Pool::from_proxies(vec![upstream], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(timeout)?);

    // B9: require `Proxy-Authorization: Basic base64("gate-user:gate-pass")` from every client.
    let handle = serve(
        "127.0.0.1:0".parse()?,
        pool,
        resolver,
        timeout,
        0,    // min_queue
        1024, // backlog
        Some("gate-user:gate-pass".into()),
    )
    .await?;
    let addr = handle.local_addr();
    println!("gated server on {addr}");

    // A client WITHOUT credentials is rejected before any upstream is used. Self-contained — needs
    // no live upstream (the 407 is returned before selection).
    let anon = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{addr}"))?)
        .build()?;
    match anon.get("http://example.com/").send().await {
        Ok(r) if r.status() == reqwest::StatusCode::PROXY_AUTHENTICATION_REQUIRED => {
            println!("no-credential client correctly rejected with 407")
        }
        Ok(r) => println!("HTTP {}", r.status()),
        Err(e) => println!("request error: {e}"),
    }

    // A real client authenticates by embedding creds in the proxy URL — the request then relays via
    // the paid upstream:
    //   curl -x http://gate-user:gate-pass@{addr} http://example.com/
    println!("authenticate with: curl -x http://gate-user:gate-pass@{addr} http://example.com/");

    handle.shutdown();
    Ok(())
}
