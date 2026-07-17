# Providers

A *provider* is a page that lists proxies. proxybroker-rs treats providers as **data, not code**:
provider sites rot continuously (measurement on 2026-07-15 found ~10 of proxybroker2's 38 registry
entries already dead), so a dead provider is a config edit, not a recompile-and-republish.

## The `ProviderSpec` model

Each provider is a `ProviderSpec`, deserializable from YAML or JSON so the
bundled registry and user configs share one shape:

```rust
pub struct ProviderSpec {
    pub url: String,            // the page to fetch
    pub protocols: Vec<Proto>,  // claimed protocols; empty = unknown (check all)
    pub pattern: Option<String>,// optional bespoke (host, port) regex
    pub timeout: u64,           // request timeout in seconds (default 20)
    pub kind: Option<String>,   // proxybroker2 `type`; only `simple` is supported
}
```

Extraction produces `Candidate { host, port, protocols }` values — canonical IP, port, and the
protocols the provider claims. A `Candidate` is not yet a [`Proxy`](../library/proxy.md): it has not
been checked.

## The whole-text IP:port scanner

By default a provider needs **no format-specific parser**. Extraction runs
`parse::find_addrs_global` over the entire page body — a whole-text scanner that finds every IPv4
and pairs it with the nearest following port. This one scanner subsumes the three formats a per-site
parser zoo would otherwise need:

| Format | Example row | Handled by |
| --- | --- | --- |
| Plain text | `8.8.8.8:8080` | whole-text scanner |
| One per line | `1.1.1.1 3128` | whole-text scanner |
| HTML table | `<td>66.55.44.33</td><td>8888</td>` | whole-text scanner |

Extraction then filters exactly as the Python pipeline does: ports that are empty or zero are
dropped, IPs are canonicalized (leading-zero and out-of-range matches removed), the unspecified
address (`0.0.0.0` / `::`) is rejected as a non-routable sentinel, and the result is deduplicated.

> The scanner uses proxybroker2's exact IPv4 octet sub-pattern, so its quirks are byte-identical —
> including that `999.1.1.1` yields the valid substring `99.1.1.1`. Rust's `regex` crate rejects the
> lookahead of the original global pattern, so `find_addrs_global` is a two-pass scanner (regex for
> IPs, code for pairing) verified against a characterization oracle. See the [checking](./checking.md)
> page for the resolver/negotiator pipeline the resulting candidates flow into.

## Custom `pattern` regexes

A provider whose page needs bespoke extraction supplies a `pattern`: a regex with two capture
groups, `(host, port)`. When present it replaces the whole-text scanner for that provider.

```yaml
url: "https://example.com/odd-format"
protocols: [SOCKS5]
pattern: "IP=(\\d+\\.\\d+\\.\\d+\\.\\d+) PORT=(\\d+)"
```

## Adding your own providers

Write one provider per file into a directory and point the CLI at it. `config_template()` (also
printed by the library) is a ready-to-edit starting point:

```yaml
# one provider per file; a filename starting with `_` is skipped (rename to disable).
url: "http://example.com/proxy-list.txt"
protocols: [HTTP, HTTPS]
# pattern: "(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)"   # omit to use the default scanner
timeout: 20
```

| Flag | Effect |
| --- | --- |
| `--provider-dir <DIR>` | Load every `*.yaml` / `*.yml` / `*.json` in `DIR`, appended to the bundled registry. May be repeated. |
| `--providers-only` | Use **only** the `--provider-dir` configs, ignoring the bundled registry. |

```sh
proxybroker --provider-dir ./my-providers find --types HTTP
proxybroker --provider-dir ./my-providers --providers-only grab
```

The loader (`load_provider_dir`) reads files sorted by name, skips names starting with `_`, and logs
and skips any file that fails to parse — one bad config never sinks the rest. Two deliberate
deviations from proxybroker2:

- **Only `type: simple` is supported.** A config declaring `paginated` or `api` is warned about and
  skipped, rather than loaded as a silently-broken plain GET. A type-less config is treated as
  `simple`. Harmless unknown fields (`name`, `format`, `max_connections`) are ignored, so an existing
  proxybroker2 `simple` config loads directly.
- **No Python execution.** proxybroker2 can *execute* `.py` provider files; there is no safe Rust
  equivalent, so only data files are loaded.

## The bundled registry

50 sources ship embedded (`data/providers.yaml`, read via `include_str!` and exposed as
`bundled_registry()`). Only sources confirmed live and yielding are carried over; proxybroker2's dead
entries are not. The set combines the surviving proxybroker2 sources (proxyscrape HTTP/SOCKS4/SOCKS5,
sslproxies, free-proxy-list, socks-proxy, proxydb, and others) with a curated 2026-07-17 expansion of
hourly-refreshed GitHub-hosted lists (TheSpeedX, monosans, proxifly, and more).

> The bundled proxyscrape SOCKS entries fix a live upstream bug: in proxybroker2 the SOCKS providers'
> `proto=("SOCKS4")` has no trailing comma, so it is a string, not a tuple, and the type filter
> silently drops both — despite them being among the highest-yield sources still alive. In Rust a
> `Vec<Proto>` cannot be a string, so the bug is unrepresentable.

## Scheduled liveness audit

Provider liveness is checked by a scheduled GitHub Actions workflow (`.github/workflows/
provider-audit.yml`), which runs **Mondays at 06:00 UTC** (plus manual `workflow_dispatch`). It
fetches every bundled provider and fails, listing the dead URLs, if any source yields zero proxies.

The audit is deliberately **not** run on pull requests or pushes: a source rotting upstream must never
block a merge. A red audit is a maintenance signal — "curate `data/providers.yaml`" — not a broken PR.
An offline test guards the registry's *shape*; the audit guards its *liveness*.
