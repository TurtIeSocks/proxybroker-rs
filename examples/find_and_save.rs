//! Find working proxies and save them to `proxies.txt`, one scheme-prefixed URL per line
//! (`http://host:port` / `https://host:port`).
//!
//! ```sh
//! cargo run --example find_and_save
//! ```
//!
//! The Rust equivalent of proxybroker2's `find_and_save.py`.

use futures_util::StreamExt;
use proxybroker::{Broker, FindQuery, Proto, Scheme, TypeSpec};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::builder().build();

    let mut proxies = broker
        .find(FindQuery {
            types: vec![TypeSpec::any(Proto::Http), TypeSpec::any(Proto::Https)],
            limit: Some(10),
            ..Default::default()
        })
        .await?;

    let mut file = tokio::fs::File::create("proxies.txt").await?;
    let mut count = 0u32;
    while let Some(proxy) = proxies.next().await {
        let scheme = if proxy.schemes().contains(&Scheme::Https) {
            "https"
        } else {
            "http"
        };
        file.write_all(format!("{scheme}://{}\n", proxy.addr()).as_bytes())
            .await?;
        count += 1;
    }
    file.flush().await?;
    println!("saved {count} proxies to proxies.txt");
    Ok(())
}
