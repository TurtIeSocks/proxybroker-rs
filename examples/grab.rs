//! Gather proxies from the providers **without checking** them — fast, but the results are
//! unverified. Filtered to the US or GB.
//!
//! ```sh
//! cargo run --example grab
//! ```
//!
//! The Rust equivalent of proxybroker2's `only_grab.py`.

use futures_util::StreamExt;
use proxybroker::{Broker, GrabQuery};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::builder().build();

    let mut proxies = broker.grab(GrabQuery {
        countries: Some(vec!["US".into(), "GB".into()]),
        limit: Some(10),
    });

    while let Some(proxy) = proxies.next().await {
        let country = proxy.geo.as_ref().map(|c| c.code.as_str()).unwrap_or("--");
        println!("{:<21} {country}", proxy.addr());
    }
    Ok(())
}
