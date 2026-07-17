//! P1 — provider liveness audit. NOT part of the offline suite: this is `#[ignore]`d and hits the
//! network, run only by the scheduled `provider-audit.yml` workflow (weekly). It fetches every
//! bundled provider and reports any that yield zero candidates, so dead/changed sources surface as
//! a maintenance signal — never as a blocking unit test (roadmap P1: "liveness as a periodic CI
//! audit, not a unit test").
//!
//! Run locally: `cargo test --test provider_audit -- --ignored --nocapture`

use proxybroker::provider::{bundled_registry, fetch};
use std::time::Duration;
use tokio::task::JoinSet;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "network audit; run via the scheduled provider-audit workflow"]
async fn all_bundled_providers_yield_proxies() {
    proxybroker::install_default_crypto_provider();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build audit client");

    let mut set = JoinSet::new();
    for spec in bundled_registry() {
        let client = client.clone();
        set.spawn(async move {
            let n = fetch(&spec, &client).await.len();
            (spec.url, n)
        });
    }

    let mut results = Vec::new();
    while let Some(joined) = set.join_next().await {
        results.push(joined.expect("audit task panicked"));
    }
    results.sort_by_key(|r| std::cmp::Reverse(r.1));

    println!(
        "\n=== provider liveness audit ({} sources) ===",
        results.len()
    );
    for (url, n) in &results {
        println!("  {n:>7}  {url}");
    }

    let dead: Vec<&String> = results
        .iter()
        .filter(|(_, n)| *n == 0)
        .map(|(u, _)| u)
        .collect();
    println!("\nzero-yield: {} / {}", dead.len(), results.len());
    assert!(
        dead.is_empty(),
        "these bundled providers yielded 0 proxies (dead or format-changed):\n{}",
        dead.iter()
            .map(|u| format!("  - {u}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
