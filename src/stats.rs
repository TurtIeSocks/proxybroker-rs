//! Aggregate statistics over a set of found proxies. `api.py:Broker.show_stats`.
//!
//! proxybroker2's `show_stats` also produced a per-proxy log breakdown from the `_log` vec
//! this port deliberately dropped (unbounded growth, design critique #23). What remains is
//! the useful aggregate: counts by protocol, anonymity level, and country, the error
//! histogram, and the mean response time.

use crate::proxy::Proxy;
use crate::types::{AnonLevel, Proto};
use std::collections::BTreeMap;
use std::fmt;

/// A summary over a batch of proxies.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stats {
    /// Total proxies seen.
    pub total: usize,
    /// How many are working (have at least one confirmed protocol).
    pub working: usize,
    /// Count of confirmed support, per protocol.
    pub by_protocol: BTreeMap<Proto, usize>,
    /// Count of HTTP proxies per anonymity level.
    pub by_anonymity: BTreeMap<AnonLevel, usize>,
    /// Count per ISO country code (`??` when geo is unknown).
    pub by_country: BTreeMap<String, usize>,
    /// Error histogram, keyed by the stats `errmsg` strings.
    pub errors: BTreeMap<&'static str, u32>,
    /// Mean response time over proxies that recorded one, seconds.
    pub avg_resp_time: f64,
}

impl Stats {
    /// Aggregate a slice of proxies.
    pub fn from_proxies(proxies: &[Proxy]) -> Stats {
        let mut s = Stats {
            total: proxies.len(),
            ..Default::default()
        };
        let mut rt_sum = 0.0;
        let mut rt_n = 0usize;

        for p in proxies {
            if p.is_working() {
                s.working += 1;
            }
            for (proto, level) in p.types() {
                *s.by_protocol.entry(*proto).or_default() += 1;
                if let Some(lvl) = level {
                    *s.by_anonymity.entry(*lvl).or_default() += 1;
                }
            }
            let country = p
                .geo
                .as_ref()
                .map(|c| c.code.clone())
                .unwrap_or_else(|| "??".into());
            *s.by_country.entry(country).or_default() += 1;
            for (err, n) in p.errors() {
                *s.errors.entry(err.as_str()).or_default() += n;
            }
            let art = p.avg_resp_time();
            if art > 0.0 {
                rt_sum += art;
                rt_n += 1;
            }
        }
        s.avg_resp_time = if rt_n > 0 {
            format!("{:.2}", rt_sum / rt_n as f64).parse().unwrap()
        } else {
            0.0
        };
        s
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Found {} proxies ({} working)", self.total, self.working)?;
        if !self.by_protocol.is_empty() {
            let by_proto = self
                .by_protocol
                .iter()
                .map(|(p, n)| format!("{p}: {n}"))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(f, "  By protocol: {by_proto}")?;
        }
        if !self.by_anonymity.is_empty() {
            // Best-to-worst reads more naturally than the enum's worst-to-best order.
            let order = [
                AnonLevel::High,
                AnonLevel::Anonymous,
                AnonLevel::Transparent,
            ];
            let by_anon = order
                .iter()
                .filter_map(|l| self.by_anonymity.get(l).map(|n| format!("{l}: {n}")))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(f, "  By anonymity (HTTP): {by_anon}")?;
        }
        if !self.by_country.is_empty() {
            // Top 10 countries by count.
            let mut countries: Vec<_> = self.by_country.iter().collect();
            countries.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
            let top = countries
                .iter()
                .take(10)
                .map(|(c, n)| format!("{c}: {n}"))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(f, "  By country: {top}")?;
        }
        writeln!(f, "  Avg response time: {:.2}s", self.avg_resp_time)?;
        if !self.errors.is_empty() {
            let errs = self
                .errors
                .iter()
                .map(|(e, n)| format!("{e}: {n}"))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(f, "  Errors: {errs}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ProxyError;
    use crate::proxy::Country;
    use std::collections::BTreeSet;

    fn proxy(ip: &str, protos: &[(Proto, Option<AnonLevel>)], country: Option<&str>) -> Proxy {
        let mut p = Proxy::new(ip.parse().unwrap(), 80, BTreeSet::new());
        for (proto, lvl) in protos {
            p.add_type(*proto, *lvl);
        }
        if let Some(c) = country {
            p.geo = Some(Country {
                code: c.into(),
                name: c.into(),
            });
        }
        p
    }

    #[test]
    fn aggregates_protocols_anonymity_and_country() {
        let proxies = vec![
            proxy(
                "1.1.1.1",
                &[(Proto::Http, Some(AnonLevel::High))],
                Some("US"),
            ),
            proxy(
                "2.2.2.2",
                &[(Proto::Http, Some(AnonLevel::High)), (Proto::Https, None)],
                Some("US"),
            ),
            proxy("3.3.3.3", &[(Proto::Socks5, None)], Some("DE")),
        ];
        let s = Stats::from_proxies(&proxies);
        assert_eq!(s.total, 3);
        assert_eq!(s.working, 3);
        assert_eq!(s.by_protocol[&Proto::Http], 2);
        assert_eq!(s.by_protocol[&Proto::Https], 1);
        assert_eq!(s.by_protocol[&Proto::Socks5], 1);
        assert_eq!(s.by_anonymity[&AnonLevel::High], 2);
        assert_eq!(s.by_country["US"], 2);
        assert_eq!(s.by_country["DE"], 1);
    }

    #[test]
    fn unknown_country_is_double_question_mark() {
        let s = Stats::from_proxies(&[proxy("1.1.1.1", &[(Proto::Http, None)], None)]);
        assert_eq!(s.by_country["??"], 1);
    }

    #[test]
    fn aggregates_errors_and_avg_time() {
        let mut p = proxy("1.1.1.1", &[(Proto::Http, None)], None);
        p.record_attempt(Some(0.4), None);
        p.record_attempt(None, Some(ProxyError::Timeout));
        let s = Stats::from_proxies(&[p]);
        assert_eq!(s.errors["connection_timeout"], 1);
        assert_eq!(s.avg_resp_time, 0.4);
    }

    #[test]
    fn empty_batch_is_all_zero() {
        let s = Stats::from_proxies(&[]);
        assert_eq!(s.total, 0);
        assert_eq!(s.working, 0);
        assert_eq!(s.avg_resp_time, 0.0);
        // Display must not panic on empty.
        let _ = s.to_string();
    }

    #[test]
    fn display_lists_the_sections() {
        let proxies = vec![proxy(
            "1.1.1.1",
            &[(Proto::Http, Some(AnonLevel::High))],
            Some("US"),
        )];
        let out = Stats::from_proxies(&proxies).to_string();
        assert!(out.contains("Found 1 proxies (1 working)"), "{out}");
        assert!(out.contains("HTTP: 1"), "{out}");
        assert!(out.contains("High: 1"), "{out}");
        assert!(out.contains("US: 1"), "{out}");
    }
}
