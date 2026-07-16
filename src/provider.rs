//! Proxy providers: where candidate proxies come from.
//!
//! Data-driven by design. Provider sites rot continuously — measurement on 2026-07-15 found
//! ~10 of proxybroker2's 38 registry entries already dead — so a provider is a
//! [`ProviderSpec`] (deserializable from YAML/JSON), not a hardcoded Rust type. A dead
//! provider is a config edit, not a recompile-and-republish. See `decisions.md`.
//!
//! Extraction defaults to the whole-text IP:port scanner ([`crate::parse::find_addrs_global`]),
//! which subsumes plain-text, `ip:port`-per-line, and HTML-table formats — so no per-format
//! parser zoo is needed (design critique #36). A provider that needs bespoke extraction
//! supplies a `pattern` (a 2-capture-group regex).

use crate::parse::find_addrs_global;
use crate::types::Proto;
use crate::utils::canonicalize_ip;
use regex::Regex;
use serde::Deserialize;
use std::collections::BTreeSet;

/// A provider definition. Serializable, so the bundled registry and user configs share one
/// shape. `kind` selects the fetch strategy; extraction is common to all.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderSpec {
    /// The page to fetch.
    pub url: String,
    /// Protocols proxies from this source may support. Empty = unknown (checked against all).
    #[serde(default)]
    pub protocols: Vec<Proto>,
    /// Optional bespoke extraction: a regex with two capture groups, `(host, port)`. When
    /// absent, the default whole-text IP:port scanner is used.
    #[serde(default)]
    pub pattern: Option<String>,
    /// Request timeout, seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_timeout() -> u64 {
    20
}

/// A candidate proxy scraped from a provider: a canonical IP, a port, and the protocols the
/// provider claims for it. Not yet a [`crate::Proxy`] — it has not been checked.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Candidate {
    pub host: String,
    pub port: u16,
    pub protocols: BTreeSet<Proto>,
}

impl ProviderSpec {
    /// A convenience constructor for the bundled registry.
    pub fn new(url: &str, protocols: &[Proto]) -> Self {
        ProviderSpec {
            url: url.to_owned(),
            protocols: protocols.to_vec(),
            pattern: None,
            timeout: default_timeout(),
        }
    }

    /// Extract candidate proxies from a fetched page body.
    ///
    /// Mirrors the Provider→`find_proxies`→`proxies.setter` path in `providers.py`: pairs are
    /// found, then filtered — `providers.py:78` keeps only pairs with a truthy port. IPs are
    /// canonicalized (`canonicalize_ip`), which drops the leading-zero and out-of-range matches
    /// the scanner permits, exactly as the Python pipeline does. Deduplicated.
    pub fn extract(&self, body: &str) -> Vec<Candidate> {
        let protocols: BTreeSet<Proto> = self.protocols.iter().copied().collect();
        let pairs = match &self.pattern {
            Some(pat) => extract_with_pattern(pat, body),
            None => find_addrs_global(body),
        };

        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for (raw_host, raw_port) in pairs {
            // providers.py:78 — `for host, port in new if port`: drop empty/zero ports.
            let Ok(port) = raw_port.parse::<u16>() else {
                continue;
            };
            if port == 0 {
                continue;
            }
            let Some(host) = canonicalize_ip(&raw_host) else {
                continue;
            };
            let cand = Candidate {
                host,
                port,
                protocols: protocols.clone(),
            };
            if seen.insert((cand.host.clone(), cand.port)) {
                out.push(cand);
            }
        }
        out
    }
}

/// The bundled default providers, parsed from the embedded `data/providers.yaml`. Only
/// sources confirmed live on 2026-07-15; proxybroker2's dead entries are not carried over.
pub fn bundled_registry() -> Vec<ProviderSpec> {
    const YAML: &str = include_str!("../data/providers.yaml");
    serde_yaml_ng::from_str(YAML).expect("bundled providers.yaml is valid")
}

/// Load provider configs from a directory: every `*.yaml` / `*.yml` / `*.json` file, one
/// provider per file, sorted by name, skipping files whose name starts with `_` (the rename
/// -to-disable convention). Mirrors `provider_utils.load_provider_configs_from_directory`.
///
/// A file that fails to parse is logged and skipped so one bad config does not sink the rest.
/// Unknown fields (e.g. Python's `name`, `format`, `max_connections`) are ignored, so
/// proxybroker2 config files load directly.
///
/// **Deviation:** proxybroker2 also has `load_python_providers_from_directory`, which
/// *executes* `.py` files. There is no safe Rust equivalent (we do not run arbitrary code),
/// so only data files are loaded — which is also the safer default.
pub fn load_provider_dir(dir: &std::path::Path) -> Vec<ProviderSpec> {
    if !dir.is_dir() {
        tracing::warn!(dir = %dir.display(), "provider directory does not exist");
        return Vec::new();
    }
    let mut paths: Vec<std::path::PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                let ext_ok = matches!(
                    p.extension().and_then(|e| e.to_str()),
                    Some("yaml" | "yml" | "json")
                );
                let named = p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| !n.starts_with('_'));
                ext_ok && named
            })
            .collect(),
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "cannot read provider directory");
            return Vec::new();
        }
    };
    paths.sort();

    paths
        .into_iter()
        .filter_map(|p| match std::fs::read_to_string(&p) {
            Ok(text) => match serde_yaml_ng::from_str::<ProviderSpec>(&text) {
                // serde_yaml_ng parses JSON too (JSON is a YAML subset), so one path covers both.
                Ok(spec) => Some(spec),
                Err(e) => {
                    tracing::warn!(path = %p.display(), error = %e, "skipping invalid provider config");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(path = %p.display(), error = %e, "cannot read provider config");
                None
            }
        })
        .collect()
}

/// A YAML config template for a provider, to help users write their own.
/// `provider_utils.create_provider_config_template`.
pub fn config_template() -> &'static str {
    "# proxybroker provider config — one provider per file.\n\
     # Drop this in a directory and pass it with --provider-dir.\n\
     # A filename starting with `_` is skipped (rename to disable).\n\
     url: \"http://example.com/proxy-list.txt\"\n\
     # Protocols proxies from this source may support (empty = check all):\n\
     protocols: [HTTP, HTTPS]\n\
     # Optional 2-group (host, port) regex; omit to use the default IP:port scanner:\n\
     # pattern: \"(\\\\d+\\\\.\\\\d+\\\\.\\\\d+\\\\.\\\\d+):(\\\\d+)\"\n\
     timeout: 20\n"
}

/// Fetch a provider's page and extract candidate proxies. Network I/O; the pure extraction
/// it wraps is [`ProviderSpec::extract`]. On any fetch error the provider yields nothing —
/// one dead source must never sink a grab (mirrors `providers.py`, which swallows request
/// errors per provider).
pub async fn fetch(spec: &ProviderSpec, client: &reqwest::Client) -> Vec<Candidate> {
    match fetch_body(spec, client).await {
        Ok(body) => spec.extract(&body),
        Err(e) => {
            tracing::debug!(url = %spec.url, error = %e, "provider fetch failed");
            Vec::new()
        }
    }
}

async fn fetch_body(spec: &ProviderSpec, client: &reqwest::Client) -> reqwest::Result<String> {
    client
        .get(&spec.url)
        .timeout(std::time::Duration::from_secs(spec.timeout))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
}

/// Apply a 2-capture-group `(host, port)` regex, mirroring `SimpleProvider`'s custom-pattern
/// path (`provider_utils.py`): a match with ≥2 groups yields `(g1, g2)`; a single string
/// match containing `:` is split on the last colon; anything else is dropped.
fn extract_with_pattern(pattern: &str, body: &str) -> Vec<(String, String)> {
    let Ok(re) = Regex::new(pattern) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for caps in re.captures_iter(body) {
        match (caps.get(1), caps.get(2)) {
            (Some(h), Some(p)) => out.push((h.as_str().to_owned(), p.as_str().to_owned())),
            _ => {
                let whole = caps.get(0).map(|m| m.as_str()).unwrap_or("");
                if let Some((h, p)) = whole.rsplit_once(':') {
                    out.push((h.to_owned(), p.to_owned()));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(protocols: &[Proto]) -> ProviderSpec {
        ProviderSpec::new("http://example/", protocols)
    }

    #[test]
    fn extracts_from_plain_text_list() {
        let body = "8.8.8.8:8080\n1.1.1.1:3128\n";
        let got = spec(&[Proto::Http]).extract(body);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].host, "8.8.8.8");
        assert_eq!(got[0].port, 8080);
        assert!(got[0].protocols.contains(&Proto::Http));
    }

    #[test]
    fn extracts_from_html_table() {
        let body = "<tr><td>66.55.44.33</td><td>8888</td></tr>\
                    <tr><td>22.33.44.55</td><td>9999</td></tr>";
        let got = spec(&[]).extract(body);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].host, "66.55.44.33");
        assert_eq!(got[1].port, 9999);
    }

    #[test]
    fn matches_python_pipeline_on_messy_input() {
        // Verified against proxybroker2's actual regex + canonicalize pipeline:
        //   raw pairs: [('99.1.1.1','80'), ('5.5.5.5',''), ('010.1.1.1','80'), ('7.7.7.7','1234')]
        //   kept:      [('99.1.1.1','80'), ('7.7.7.7','1234')]
        // - 999.1.1.1 → the scanner greedily extracts the valid substring 99.1.1.1 (same as
        //   300.1.2.3 → 00.1.2.3); it is a real, canonicalizable IP, so it is KEPT.
        // - 5.5.5.5 gets an empty port (the nearest following token is the IP 010.1.1.1), dropped.
        // - 010.1.1.1 canonicalizes to None (leading zero), dropped.
        let body = "999.1.1.1:80 5.5.5.5:0 010.1.1.1:80 7.7.7.7:1234";
        let got = spec(&[]).extract(body);
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got[0].host, "99.1.1.1");
        assert_eq!(got[0].port, 80);
        assert_eq!(got[1].host, "7.7.7.7");
        assert_eq!(got[1].port, 1234);
    }

    #[test]
    fn deduplicates_repeated_addresses() {
        let body = "9.9.9.9:53\n9.9.9.9:53\n9.9.9.9:53";
        assert_eq!(spec(&[]).extract(body).len(), 1);
    }

    #[test]
    fn custom_pattern_with_two_groups() {
        let body = "IP=1.2.3.4 PORT=8080; IP=5.6.7.8 PORT=3128";
        let mut s = spec(&[Proto::Socks5]);
        s.pattern = Some(r"IP=(\d+\.\d+\.\d+\.\d+) PORT=(\d+)".to_owned());
        let got = s.extract(body);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].host, "1.2.3.4");
        assert_eq!(got[1].port, 3128);
        assert!(got[1].protocols.contains(&Proto::Socks5));
    }

    #[test]
    fn spec_deserializes_from_yaml() {
        let yaml = "url: https://example.com/list\nprotocols: [HTTP, SOCKS5]\ntimeout: 15";
        let s: ProviderSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(s.url, "https://example.com/list");
        assert_eq!(s.protocols, vec![Proto::Http, Proto::Socks5]);
        assert_eq!(s.timeout, 15);
        assert!(s.pattern.is_none());
    }

    #[test]
    fn python_config_extra_fields_are_ignored() {
        // A proxybroker2 config with name/type/format/max_connections must still load.
        let yaml = "name: My List\ntype: simple\nformat: text\nurl: http://x/list\n\
                    protocols: [HTTP]\nmax_connections: 4\ntimeout: 30";
        let s: ProviderSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(s.url, "http://x/list");
        assert_eq!(s.timeout, 30);
    }

    #[test]
    fn config_template_is_a_valid_provider_spec() {
        let s: ProviderSpec = serde_yaml_ng::from_str(config_template()).unwrap();
        assert_eq!(s.protocols, vec![Proto::Http, Proto::Https]);
    }

    #[test]
    fn load_provider_dir_reads_sorted_skips_underscore_and_bad() {
        let dir = std::env::temp_dir().join(format!("pb_prov_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("b.yaml"), "url: http://b/\nprotocols: [SOCKS5]").unwrap();
        std::fs::write(
            dir.join("a.json"),
            r#"{"url":"http://a/","protocols":["HTTP"]}"#,
        )
        .unwrap();
        std::fs::write(dir.join("_disabled.yaml"), "url: http://skip/").unwrap();
        std::fs::write(dir.join("broken.yaml"), "url: [not a string").unwrap();
        std::fs::write(dir.join("notconfig.txt"), "ignored").unwrap();

        let specs = load_provider_dir(&dir);
        // sorted by filename: a.json then b.yaml; _disabled, broken, and .txt excluded.
        assert_eq!(specs.len(), 2, "{specs:?}");
        assert_eq!(specs[0].url, "http://a/");
        assert_eq!(specs[0].protocols, vec![Proto::Http]);
        assert_eq!(specs[1].url, "http://b/");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_provider_dir_missing_dir_is_empty() {
        let dir = std::env::temp_dir().join("pb_prov_definitely_missing_xyz");
        assert!(load_provider_dir(&dir).is_empty());
    }
}
