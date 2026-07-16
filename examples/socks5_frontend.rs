//! The SOCKS5 front-end (B12): a client speaks SOCKS5 to the local server (auto-detected from the
//! first byte), which tunnels through a pooled upstream.
//!
//! ```sh
//! cargo run --example socks5_frontend
//! ```
//!
//! Compiling shows the client usage; completing the tunnel needs a reachable SOCKS5 upstream.

use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig};
use proxybroker::{Proto, Proxy};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_socks::tcp::Socks5Stream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A SOCKS5-capable upstream is needed to serve an (opaque) tunnel.
    let mut upstream = Proxy::new(
        "203.0.113.7".parse()?,
        1080,
        BTreeSet::from([Proto::Socks5]),
    );
    upstream.add_type(Proto::Socks5, None);
    let pool = Pool::from_proxies(vec![upstream], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(8))?);
    let handle = serve(
        "127.0.0.1:0".parse()?,
        pool,
        resolver,
        Duration::from_secs(8),
        0,
        1024,
        None, // pass Some("user:pass") to require SOCKS5 RFC 1929 auth (symmetric with HTTP 407)
    )
    .await?;
    let addr = handle.local_addr();
    println!("SOCKS5 front-end on {addr}");

    // Speak SOCKS5 to the local server. The same listener also accepts plain HTTP / CONNECT — the
    // protocol is sniffed from the first byte (0x05 ⇒ SOCKS5). With --auth, use
    // `Socks5Stream::connect_with_password(addr, target, user, pass)` instead.
    match Socks5Stream::connect(addr, ("example.com", 80u16)).await {
        Ok(_tunnel) => println!("SOCKS5 tunnel to example.com:80 established through the pool"),
        Err(e) => println!("tunnel not completed (needs a live SOCKS5 upstream): {e}"),
    }

    handle.shutdown();
    Ok(())
}
