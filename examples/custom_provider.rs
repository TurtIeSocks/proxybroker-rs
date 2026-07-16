//! Use your **own** proxy source instead of (or alongside) the bundled providers.
//!
//! ```sh
//! cargo run --example custom_provider
//! ```
//!
//! A [`ProviderSpec`] is just data — a URL, the protocols it yields, and an optional
//! 2-capture-group `(host, port)` regex. Without a regex the default whole-text scanner picks
//! up any `ip:port` on the page (plain text or HTML tables alike). This is the Rust
//! equivalent of proxybroker2's `custom_providers/`.

use futures_util::StreamExt;
use proxybroker::{Broker, GrabQuery, Proto, ProviderSpec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A source that serves a plain `ip:port`-per-line list. The default scanner handles it.
    let plain = ProviderSpec::new(
        "https://api.proxyscrape.com/?request=getproxies&proxytype=http",
        &[Proto::Http],
    );

    // A source needing a bespoke pattern: a 2-group regex captures (host, port). (Illustrative
    // — point it at a page whose markup this pattern matches.)
    let mut custom = ProviderSpec::new("https://example.com/proxies", &[Proto::Socks5]);
    custom.pattern = Some(r"(\d+\.\d+\.\d+\.\d+)</td><td>(\d+)".to_owned());

    // `providers(...)` replaces the bundled registry; drop `custom` here since example.com has
    // no proxies. To ADD to the bundled set instead, start from `bundled_registry()`.
    let broker = Broker::builder().providers(vec![plain]).build();
    let _ = custom; // shown for reference

    let mut proxies = broker.grab(GrabQuery {
        limit: Some(5),
        ..Default::default()
    });
    while let Some(proxy) = proxies.next().await {
        println!("{}", proxy.addr());
    }
    Ok(())
}
