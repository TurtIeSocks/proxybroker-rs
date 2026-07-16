//! Find working HTTP/HTTPS proxies and print each as it is confirmed.
//!
//! ```sh
//! cargo run --example find
//! ```
//!
//! The Rust equivalent of proxybroker2's `basic.py`. `Broker::find` returns a stream that
//! yields proxies as they pass checking — you consume it like any other `Stream`.

use futures_util::StreamExt;
use proxybroker::{Broker, FindQuery, Proto, TypeSpec};

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

    while let Some(proxy) = proxies.next().await {
        // `schemes()` is HTTP/HTTPS support; `types()` has the per-protocol anonymity level.
        println!(
            "Found proxy: {:<21} {:?}  {:.2}s",
            proxy.addr(),
            proxy.schemes(),
            proxy.avg_resp_time(),
        );
    }
    Ok(())
}
