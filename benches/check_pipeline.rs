//! F5 — a reproducible criterion benchmark of the check pipeline's CPU cost on deterministic
//! loopback input. The mock judge + mock HTTP proxy live on 127.0.0.1, so the measured work is the
//! parse/validate/classify pipeline, not real network latency. Run with `cargo bench`; the offline
//! determinism of this fixture is guarded by `tests/bench_pipeline.rs`.
//!
//! "Mock sockets" is realised as loopback fixtures rather than an injected in-memory duplex: the
//! checker connects internally, and adding a one-impl `Connect` trait would fight "no speculative
//! abstraction". Loopback stays in-kernel and deterministic (see decisions.md).

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use proxybroker::checker::{Checker, CheckerConfig, RetryPolicy};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::types::{Proto, TypeSpec};
use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn echo_server(body: &'static str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let marker = req
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("user-agent:"))
                    .and_then(|l| l.rsplit('/').next())
                    .map(|m| m.trim().to_string())
                    .unwrap_or_default();
                let body = body.replace("{marker}", &marker);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    (addr, h)
}

const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";
const HIGH_PAGE: &str = "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

async fn make_checker(judge: SocketAddr) -> Checker {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from(["203.0.113.9".parse().unwrap()]);
    let cfg = CheckerConfig {
        judges: vec![format!("http://{judge}/")],
        types: vec![TypeSpec::any(Proto::Http)],
        timeout: Duration::from_secs(3),
        retry: RetryPolicy::tries(1), // deterministic: no retries
        ..Default::default()
    };
    Checker::new(cfg, resolver, &client, real).await.unwrap()
}

fn bench_check_pipeline(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Setup once, OUTSIDE the measured loop: mock judge + proxy and a Checker whose judge pool is
    // already verified. The server tasks keep running on `rt` for the duration of the benchmark.
    let (checker, proxy_addr, _judge, _proxy) = rt.block_on(async {
        let (judge, jh) = echo_server(JUDGE_PAGE).await;
        let (proxy_addr, ph) = echo_server(HIGH_PAGE).await;
        let checker = make_checker(judge).await;
        (checker, proxy_addr, jh, ph)
    });

    let mut group = c.benchmark_group("check_pipeline");
    group.throughput(Throughput::Elements(1)); // elements/sec == proxies/sec
    group.bench_function("http_check", |b| {
        b.to_async(&rt).iter(|| async {
            // A fresh proxy each iteration so per-proxy stats never accumulate.
            let mut proxy = Proxy::new(proxy_addr.ip(), proxy_addr.port(), BTreeSet::new());
            let _ = checker.check(&mut proxy).await;
        });
    });
    group.finish();

    print_peak_rss();
}

/// Peak resident set size — criterion does not measure memory, so print it separately (advisory,
/// not asserted). `ru_maxrss` units are bytes on macOS, kilobytes on Linux.
#[cfg(unix)]
fn print_peak_rss() {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } == 0 {
        eprintln!("peak RSS (ru_maxrss, platform units): {}", usage.ru_maxrss);
    }
}
#[cfg(not(unix))]
fn print_peak_rss() {
    eprintln!("peak RSS: unavailable on this platform");
}

criterion_group!(benches, bench_check_pipeline);
criterion_main!(benches);
