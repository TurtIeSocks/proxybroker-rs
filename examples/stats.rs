//! Find proxies, then print an aggregate summary — counts by protocol, anonymity level, and
//! country, plus the error histogram over **every** proxy checked (not only the ones that
//! passed).
//!
//! ```sh
//! cargo run --example stats
//! ```

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
        println!("{}", proxy.addr());
    }

    // `stats()` is complete once the stream is drained — every check has finished and been
    // recorded. It covers all checked proxies, so `working` and the error histogram are real.
    if let Some(stats) = proxies.stats() {
        eprintln!("\n{stats}");
    }
    Ok(())
}
