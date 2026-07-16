//! E4 — MCP stdio server (`proxybroker mcp`), a thin veneer over a live [`Pool`].
//!
//! Exposes exactly three tools — `get_proxy`, `pool_status`, `report_dead` — so agent tooling can
//! pull healthy proxies and feed failures back into the same eviction machinery. All the logic
//! lives in the free `handle_*` functions (offline-testable); the rmcp glue below is a near
//! logic-free adapter, so an rmcp bump only touches the transport surface. Gated
//! `all(feature = "mcp", feature = "server")`.

use crate::server::Pool;
use crate::stats::Stats;
use crate::types::Scheme;
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The `get_proxy` result: a checked-out proxy's address and health metadata.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ProxyInfo {
    /// `host:port`.
    pub proxy: String,
    /// Confirmed protocols (e.g. `["HTTP", "HTTPS"]`).
    pub types: Vec<String>,
    /// Mean response time, seconds.
    pub avg_resp_time: f64,
    /// Rolling error rate, `0..=1`.
    pub error_rate: f64,
}

/// The `pool_status` result: a [`Stats`] snapshot of the live pool, flattened to string keys.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PoolStatus {
    pub total: usize,
    pub working: usize,
    pub by_protocol: BTreeMap<String, usize>,
    pub by_country: BTreeMap<String, usize>,
    pub avg_resp_time: f64,
    pub errors: BTreeMap<String, u32>,
}

/// The `report_dead` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RemovedResult {
    pub removed: bool,
}

/// `get_proxy` arguments.
#[derive(Debug, Deserialize, JsonSchema, Default)]
pub struct GetProxyParams {
    /// `"http"` or `"https"`.
    pub scheme: String,
    /// Optional ISO country code filter (case-insensitive).
    #[serde(default)]
    pub country: Option<String>,
}

/// `report_dead` arguments.
#[derive(Debug, Deserialize, JsonSchema, Default)]
pub struct ReportDeadParams {
    /// The dead proxy's `host:port`.
    pub proxy: String,
}

/// Check out the best proxy for `scheme` (optional country filter), read its metadata, and return
/// it to the pool so it stays available and rotates by priority — the "thin veneer" contract.
pub fn handle_get_proxy(pool: &Pool, scheme: Scheme, country: Option<&str>) -> Option<ProxyInfo> {
    let proxy = pool.try_get(scheme, country)?;
    let info = ProxyInfo {
        proxy: proxy.addr(),
        types: proxy.types().keys().map(|p| p.as_str().to_string()).collect(),
        avg_resp_time: proxy.avg_resp_time(),
        error_rate: proxy.error_rate(),
    };
    pool.add(proxy); // put it back (dedup on host,port); no synthetic success recorded
    Some(info)
}

/// A [`Stats`] snapshot of the live pool.
pub fn handle_pool_status(pool: &Pool) -> PoolStatus {
    let stats = Stats::from_proxies(&pool.proxies());
    PoolStatus {
        total: stats.total,
        working: stats.working,
        by_protocol: stats
            .by_protocol
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), *v))
            .collect(),
        by_country: stats.by_country.clone(),
        avg_resp_time: stats.avg_resp_time,
        errors: stats
            .errors
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect(),
    }
}

/// Remove a dead proxy (`host:port`) from the pool; `true` if one was present. The failure happened
/// out-of-process, so the honest action is removal — not synthesizing an error into the histogram.
pub fn handle_report_dead(pool: &Pool, addr: &str) -> bool {
    match addr.parse::<std::net::SocketAddr>() {
        Ok(s) => pool.remove(s.ip(), s.port()),
        Err(_) => false,
    }
}

fn parse_scheme(s: &str) -> Option<Scheme> {
    match s.to_ascii_lowercase().as_str() {
        "http" => Some(Scheme::Http),
        "https" => Some(Scheme::Https),
        _ => None,
    }
}

// ---- rmcp glue: the near-logic-free adapter over the handlers above ----

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::{tool, tool_router};

/// The MCP tool set, owning the shared pool.
#[derive(Clone)]
pub struct ProxyTools {
    pool: Arc<Pool>,
}

#[tool_router(server_handler)]
impl ProxyTools {
    #[tool(
        name = "get_proxy",
        description = "Check out the best healthy proxy for a scheme (\"http\" or \"https\"), optionally filtered to an ISO country code. Returns its host:port and health metadata; the proxy stays in the pool. Null if none is available."
    )]
    fn get_proxy(&self, Parameters(p): Parameters<GetProxyParams>) -> Json<Option<ProxyInfo>> {
        let info = parse_scheme(&p.scheme)
            .and_then(|s| handle_get_proxy(&self.pool, s, p.country.as_deref()));
        Json(info)
    }

    #[tool(
        name = "pool_status",
        description = "Snapshot of the live proxy pool: totals, per-protocol and per-country counts, mean latency, and the error histogram."
    )]
    fn pool_status(&self) -> Json<PoolStatus> {
        Json(handle_pool_status(&self.pool))
    }

    #[tool(
        name = "report_dead",
        description = "Report a proxy (host:port) as dead so it is removed from the pool and no longer handed out."
    )]
    fn report_dead(&self, Parameters(p): Parameters<ReportDeadParams>) -> Json<RemovedResult> {
        Json(RemovedResult {
            removed: handle_report_dead(&self.pool, &p.proxy),
        })
    }
}

/// Serve the three tools over MCP stdio until the peer disconnects.
pub async fn serve_stdio(pool: Arc<Pool>) -> Result<(), Box<dyn std::error::Error>> {
    use rmcp::ServiceExt;
    let service = ProxyTools { pool }
        .serve(rmcp::transport::io::stdio())
        .await?;
    service.waiting().await?;
    Ok(())
}
