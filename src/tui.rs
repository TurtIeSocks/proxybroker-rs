//! Pure core of the `proxybroker top` terminal dashboard (F4).
//!
//! [`DashboardState`] holds everything the dashboard needs to draw a frame; [`DashboardState::apply`]
//! folds in a fresh [`Pool`](crate::server::Pool) snapshot, [`render`] draws it with ratatui, and
//! [`DashboardState::on_key`] turns a keypress into a state mutation. All three are pure/offline —
//! no terminal, no I/O, no tokio — so they are unit-tested directly (`render` via ratatui's
//! `TestBackend`). The event loop, raw-mode terminal guard, and the `top` CLI subcommand that drive
//! these are a separate, non-pure controller layer built on top of this module.

use crate::proxy::Proxy;
use crate::server::PoolSnapshot;
use ratatui::layout::{Constraint, Layout};
use ratatui::widgets::{Block, Paragraph, Row as TableRow, Sparkline, Table};
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;

/// One row of the dashboard's proxy table — a display-ready projection of a [`Proxy`].
pub struct Row {
    pub addr: String,
    pub protos: String,
    pub error_rate: f64,
    pub resp_time: f64,
    pub country: String,
}

/// Which column [`DashboardState::rows`] is sorted by. `ErrorRate`/`RespTime` sort ascending
/// (best/fastest first); `Addr`/`Country` sort lexicographically.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    Addr,
    ErrorRate,
    RespTime,
    Country,
}

/// Ring-buffer capacity for a proxy's response-time history (one sparkline's worth).
const RING_CAP: usize = 60;

/// Everything [`render`] needs to draw one frame. Rebuilt from a live `Pool` by [`Self::apply`];
/// mutated in place by [`Self::on_key`].
pub struct DashboardState {
    pub rows: Vec<Row>,
    pub snapshot: PoolSnapshot,
    pub sort: SortKey,
    /// Per-proxy response-time history, keyed by `(host, port)` so it survives a proxy dropping
    /// out and back into the pool. Capped at [`RING_CAP`] — oldest sample drops first.
    pub history: HashMap<(IpAddr, u16), VecDeque<f64>>,
    pub selected: usize,
}

impl Default for DashboardState {
    fn default() -> Self {
        DashboardState {
            rows: Vec::new(),
            snapshot: PoolSnapshot::default(),
            sort: SortKey::RespTime,
            history: HashMap::new(),
            selected: 0,
        }
    }
}

impl DashboardState {
    /// Rebuild [`Self::rows`] from a live pool snapshot: one [`Row`] per proxy, each proxy's
    /// current `avg_resp_time()` pushed onto its history ring, then re-sorted by [`Self::sort`]
    /// and [`Self::selected`] clamped to the new row count.
    pub fn apply(&mut self, proxies: &[Proxy], snapshot: PoolSnapshot) {
        self.rows = proxies
            .iter()
            .map(|p| Row {
                addr: p.addr(),
                protos: p
                    .types()
                    .keys()
                    .map(|proto| proto.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
                error_rate: p.error_rate(),
                resp_time: p.avg_resp_time(),
                country: p.geo.as_ref().map(|c| c.code.clone()).unwrap_or_default(),
            })
            .collect();

        for p in proxies {
            let ring = self.history.entry((p.host, p.port)).or_default();
            ring.push_back(p.avg_resp_time());
            while ring.len() > RING_CAP {
                ring.pop_front();
            }
        }

        self.snapshot = snapshot;
        self.sort_rows();
        self.selected = self.selected.min(self.rows.len().saturating_sub(1));
    }

    /// Re-sort [`Self::rows`] by the current [`Self::sort`] key. Factored out of [`Self::apply`]
    /// so [`Self::on_key`] can re-sort without re-fetching the pool.
    fn sort_rows(&mut self) {
        match self.sort {
            SortKey::Addr => self.rows.sort_by(|a, b| a.addr.cmp(&b.addr)),
            SortKey::ErrorRate => self
                .rows
                .sort_by(|a, b| a.error_rate.total_cmp(&b.error_rate)),
            SortKey::RespTime => self
                .rows
                .sort_by(|a, b| a.resp_time.total_cmp(&b.resp_time)),
            SortKey::Country => self.rows.sort_by(|a, b| a.country.cmp(&b.country)),
        }
    }
}

/// Recover the `(host, port)` history key from a rendered [`Row::addr`] (`Proxy::addr()`'s
/// `host:port`, IPv6 bracketed as `[host]:port`). Used only to look up the selected row's
/// sparkline ring — [`Row`] itself carries the display string, not the parsed address.
fn parse_addr(addr: &str) -> Option<(IpAddr, u16)> {
    if let Some(rest) = addr.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        Some((host.parse().ok()?, port.parse().ok()?))
    } else {
        let (host, port) = addr.rsplit_once(':')?;
        Some((host.parse().ok()?, port.parse().ok()?))
    }
}

/// Draw one frame: a one-line pool summary, a `Table` of [`DashboardState::rows`] (sorted per
/// [`DashboardState::sort`]), and a `Sparkline` of the selected row's response-time history. Pure
/// — no I/O, safe to call against a [`ratatui::backend::TestBackend`].
pub fn render(frame: &mut ratatui::Frame, state: &DashboardState) {
    let [header_area, table_area, spark_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(3),
    ])
    .areas(frame.area());

    let header_text = format!(
        "total {} | http {} | https {} | avg err {:.2} | avg resp {:.2}s",
        state.snapshot.total,
        state.snapshot.http,
        state.snapshot.https,
        state.snapshot.avg_error_rate,
        state.snapshot.avg_resp_time,
    );
    frame.render_widget(Paragraph::new(header_text), header_area);

    let widths = [
        Constraint::Length(21),
        Constraint::Length(16),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(8),
    ];
    let header_row = TableRow::new(["Addr", "Protos", "Err%", "Resp(s)", "Country"]);
    let rows = state.rows.iter().map(|r| {
        TableRow::new([
            r.addr.clone(),
            r.protos.clone(),
            format!("{:.2}", r.error_rate),
            format!("{:.2}", r.resp_time),
            r.country.clone(),
        ])
    });
    let table = Table::new(rows, widths)
        .header(header_row)
        .block(Block::bordered().title("Proxies"));
    frame.render_widget(table, table_area);

    let selected_ring = state
        .rows
        .get(state.selected)
        .and_then(|r| parse_addr(&r.addr))
        .and_then(|key| state.history.get(&key));
    let spark_data: Vec<u64> = selected_ring
        .map(|ring| ring.iter().map(|secs| (secs * 1000.0).round() as u64).collect())
        .unwrap_or_default();
    let sparkline = Sparkline::default()
        .block(Block::bordered().title("Selected resp time (ms)"))
        .data(spark_data);
    frame.render_widget(sparkline, spark_area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{Pool, PoolConfig};
    use crate::types::Proto;
    use std::collections::BTreeSet;

    fn proxy(ip: &str, rt: f64) -> Proxy {
        let mut p = Proxy::new(ip.parse().unwrap(), 80, BTreeSet::new());
        p.add_type(Proto::Http, None);
        p.record_attempt(Some(rt), None); // a runtime so avg_resp_time reflects `rt`
        p
    }

    #[test]
    fn apply_snapshot_updates_rings_and_sorts() {
        let pool = Pool::from_proxies(
            vec![proxy("2.2.2.2", 0.9), proxy("1.1.1.1", 0.1)],
            PoolConfig::default(),
        );
        let mut st = DashboardState {
            sort: SortKey::RespTime,
            ..Default::default()
        };
        st.apply(&pool.proxies(), pool.snapshot());
        assert_eq!(st.rows[0].addr, "1.1.1.1:80", "fastest first under RespTime");
        st.apply(&pool.proxies(), pool.snapshot());
        assert_eq!(
            st.history[&("1.1.1.1".parse().unwrap(), 80)].len(),
            2,
            "ring grew per apply"
        );
    }

    #[test]
    fn top_renders_sorted_pool_table() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let pool = Pool::from_proxies(
            vec![proxy("2.2.2.2", 0.9), proxy("1.1.1.1", 0.1)],
            PoolConfig::default(),
        );
        let mut st = DashboardState {
            sort: SortKey::RespTime,
            ..Default::default()
        };
        st.apply(&pool.proxies(), pool.snapshot());

        let mut term = Terminal::new(TestBackend::new(90, 20)).unwrap();
        term.draw(|f| render(f, &st)).unwrap();
        let text: String = term
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(
            text.contains("1.1.1.1:80") && text.contains("2.2.2.2:80"),
            "both addrs rendered"
        );
        assert!(
            text.find("1.1.1.1").unwrap() < text.find("2.2.2.2").unwrap(),
            "sorted: fast first"
        );
    }
}
