//! Deep checking (Wave 5): configure the retry policy, a liveness fallback, and relaxed validity,
//! then read the enriched per-proxy signals — tail latency (A3), the capability profile (A4), and
//! the trust verdict (A6). Every knob is opt-in; a plain `FindQuery::default()` behaves as before.
//!
//! ```sh
//! cargo run --example checking_depth
//! ```

use futures_util::StreamExt;
use proxybroker::{Broker, FindQuery, Proto, RetryPolicy, TypeSpec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::builder().build();

    let query = FindQuery {
        types: vec![TypeSpec::any(Proto::Http)],
        limit: Some(5),
        // A5 — retry the whole transient set (timeout/reset/conn-failed/empty-recv), not just
        // timeouts. `RetryPolicy` also carries backoff/factor/jitter/max_backoff for the library.
        retry: RetryPolicy::transient(3),
        // A4 — accept proxies that forward the request (marker + IP) even if they strip Referer or
        // Cookie, recording what they *did* pass through as capabilities instead of failing them.
        relaxed_validity: true,
        // A6 — record a honeypot/trust verdict per proxy. The injected-header scan only bites
        // against a judge that echoes raw request headers; with the bundled judges it stays inert
        // (see the `--trust-check` help), so `trust()` will read "clean" here.
        trust_check: true,
        // A2 — if no judge verifies, fall back to a plain 200 check against this URL instead of
        // failing with NoJudges (such proxies report anonymity level None).
        liveness_url: Some("https://httpbin.org/ip".to_string()),
        ..Default::default()
    };

    let mut proxies = broker.find(query).await?;
    while let Some(p) = proxies.next().await {
        let caps = p.capabilities();
        println!(
            "{:<21} p90={:>5.2}s  cookie={} referer={} connect25={}  trust={}",
            p.addr(),
            p.percentile(0.90), // A3 — 90th-percentile round-trip time
            caps.cookie_echo,
            caps.referer_echo,
            caps.connect25,
            if p.trust().trusted() {
                "clean"
            } else {
                "SUSPECT"
            },
        );
    }
    Ok(())
}
