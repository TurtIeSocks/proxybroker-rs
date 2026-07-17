# Output formats

The [grab](./grab.md), [find](./find.md), and [check](./check.md) subcommands share one output
layer. It offers a set of built-in formats (`--format`), a fully custom line template
(`--output-format`), and — for the `--show-stats` summary on `find`/`check` — a choice of text or
JSON (`--stats-format`).

Proxy output goes to `stdout` (or `--outfile <PATH>`). Summaries and progress always go to
`stderr`, so they never mix with the proxy stream on `stdout`.

## `--format`

| Value | Output |
| --- | --- |
| `default` | `host:port`, one per line. |
| `txt` | `host:port`, one per line (alias of `default`). |
| `url` | `scheme://host:port`, one per line. |
| `csv` | Comma-separated, with a header row (see below). |
| `json` | One JSON object per line (NDJSON). |
| `json-array` | A single `[ {...}, {...} ]` array document, streamed incrementally. |

The default is `default`.

```sh
proxybroker find --types HTTP --limit 5 --format url
# http://1.2.3.4:8080
# socks5://5.6.7.8:1080
```

### The `url` scheme

The scheme is how a *client dials the proxy*, which is what tools like `curl --proxy` need:

- SOCKS5 proxies → `socks5://`
- SOCKS4 proxies → `socks4://`
- The whole HTTP family (`HTTP`, `HTTPS`, `CONNECT:*`) → `http://` — these are all reached over
  plain HTTP. An `HTTPS`/`CONNECT` capability describes *target* traffic the proxy can tunnel, not
  a TLS endpoint on the proxy itself.

### CSV columns

`--format csv` emits a header row followed by one row per proxy:

```
host,port,protocols,anon,country,resp_time,error_rate
1.2.3.4,8080,HTTP|HTTPS,High,US,0.42,0
```

Every field is comma-free by construction: `protocols` are `|`-joined, `country` is the ISO code
only, and the rest are numeric — so no CSV quoting layer is needed. Unchecked (grabbed) proxies
have empty `protocols`/`anon`/`country` columns.

### `json` vs `json-array`

`json` is NDJSON — exactly one self-contained JSON object per line, ideal for streaming pipelines
(`jq -c`, log ingestion). `json-array` wraps the same objects in a single well-formed
`[...]` document (streamed, not buffered), for consumers that want one parseable array. An empty
stream under `json-array` yields `[]`.

## `--output-format` templates

`--output-format <TEMPLATE>` renders each proxy through a custom line template, overriding
`--format` (output is always plain lines — a template ignores JSON-array wrapping). Tokens are
substituted; **unknown `{{...}}` tokens are left literally**, so the template needs no escaping.

| Token | Expands to |
| --- | --- |
| `{{proxy}}` | `host:port` |
| `{{host}}` | Host (IP) |
| `{{port}}` | Port |
| `{{scheme}}` | `http` / `socks4` / `socks5` (as in `--format url`) |
| `{{protocols}}` | Confirmed protocols, `\|`-joined |
| `{{anon}}` | HTTP anonymity level, or empty |
| `{{country}}` | ISO country code, or empty |
| `{{asn}}` | ASN number, or empty (needs `--asn-db`) |
| `{{asn_org}}` | ASN owner organization, or empty (needs `--asn-db`) |
| `{{duration}}` | Average response time, seconds |
| `{{error_rate}}` | Rolling error rate |

```sh
proxybroker find --types HTTP --asn-db GeoLite2-ASN.mmdb \
  --output-format '{{proxy}} {{country}} AS{{asn}} {{asn_org}}'
# 1.2.3.4:8080 US AS15169 Google LLC
```

`{{asn}}` and `{{asn_org}}` only resolve when a global `--asn-db <PATH>` (a MaxMind-format ASN
database) is supplied; nothing ASN-shaped is bundled, so without the flag both render empty.

## `--stats-format`

[find](./find.md) and [check](./check.md) accept `--show-stats`, which prints an aggregate summary
(by protocol / anonymity / country) to `stderr` after the run. `--stats-format` picks its
rendering:

| Value | Summary |
| --- | --- |
| `text` | The human-readable summary (default). |
| `json` | A single JSON object. |

`--stats-format` is inert without `--show-stats`.

```sh
proxybroker find --types HTTP --limit 20 --show-stats --stats-format json 2> stats.json
```

## See also

- [find](./find.md) · [grab](./grab.md) · [check](./check.md) — the subcommands that emit these formats.
- [serve](./serve.md) — consumes the NDJSON produced by `find --save` / `check --save`.
