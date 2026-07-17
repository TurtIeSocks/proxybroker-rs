# find

Scrape candidates from the providers **and** check that they work, classifying HTTP anonymity
as it goes. `find` streams each proxy to output the moment it passes, so results appear
incrementally rather than all at once.

```sh
proxybroker find --types HTTP HTTPS --limit 10
```

`--types` is required. Everything else has a default.

## Selection & filtering

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--types` | protocols | *(required)* | Protocols to check. E.g. `--types HTTP HTTPS SOCKS5 CONNECT:80`. Names are case-insensitive: `HTTP`, `HTTPS`, `SOCKS4`, `SOCKS5`, `CONNECT:80`, `CONNECT:25`. |
| `--lvl` | levels | *(any)* | Anonymity levels to accept for HTTP: `Transparent`, `Anonymous`, `High` (case-insensitive). Applies to HTTP only; other protocols ignore it. |
| `--limit` | integer | `0` | Stop after this many working proxies. `0` means unlimited. |
| `--countries`, `--only-cc` | ISO codes | *(all)* | Keep only proxies in these ISO country codes. Space-separated or comma-separated. |
| `--strict` | flag | off | Require the anonymity level to match exactly (rather than "at least this anonymous"). |

## Judges, timing & concurrency

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--judges` | URLs | bundled | Judge URLs to use instead of the bundled defaults. |
| `--dnsbl` | zones | *(none)* | DNS blocklist zones; reject proxies listed in any (e.g. `zen.spamhaus.org`). |
| `--timeout` | seconds | `8` | Per-request timeout. |
| `--max-conn` | integer | `200` | Maximum concurrent checks. |
| `--post` | flag | off | Use `POST` instead of `GET` for the test request. |

## Retry policy

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--max-tries` | integer | `3` | Attempts per protocol before giving up. |
| `--retry-on` | `timeout`\|`transient`\|`all` | `timeout` | Which errors trigger a retry. `timeout` retries only timeouts; `transient` adds reset/conn-failed/empty-recv; `all` also retries bad-status. |
| `--backoff-ms` | milliseconds | `0` | Base backoff before a retry. `0` = no delay. |

## Capability filters

These probe and optionally require specific proxy behaviors. Filtering flags drop proxies
that fail the requirement; `--relaxed-validity` loosens what counts as a passing check.

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--liveness-url` | `URL` | *(none)* | Fallback endpoint to probe when no judge verifies. Proxies confirmed this way report anonymity `None`, so combining it with `--lvl` yields nothing. |
| `--relaxed-validity` | flag | off | Accept proxies that forward the request (marker + IP) even if they strip Referer/Cookie, recording what they pass through as capabilities. |
| `--require-cookie` | flag | off | Keep only proxies that pass our `Cookie` header through. |
| `--require-referer` | flag | off | Keep only proxies that pass our `Referer` header through. |
| `--require-connect25` | flag | off | Keep only proxies with a confirmed `CONNECT:25` (SMTP) tunnel. |
| `--trust-check` | flag | off | Run honeypot detection on each proxy and record the verdict. The injected-header scan needs a judge that echoes raw request headers, which the bundled judges do not — pair it with a raw-header-echo judge via `--judges` for real detection. |
| `--require-trusted` | flag | off | Keep only proxies whose trust verdict is clean (implies `--trust-check`). |

## Output & reporting

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--format` | `default`\|`txt`\|`json`\|`json-array`\|`url`\|`csv` | `default` | Output format. See [Output Formats](./output-formats.md). |
| `--output-format` | template | *(none)* | Per-proxy template, overriding `--format`. Tokens: `{{proxy}} {{host}} {{port}} {{scheme}} {{protocols}} {{anon}} {{country}} {{asn}} {{asn_org}} {{duration}} {{error_rate}}`. |
| `--outfile` | `PATH` | *(stdout)* | Write to this file instead of stdout. |
| `--save` | `PATH` | *(none)* | Also append every working proxy as NDJSON to this file (reloadable via [`check --load`](./check.md) / [`serve --load`](./serve.md)). Independent of `--format`/`--outfile`. |
| `--show-stats` | flag | off | Print an aggregate summary (by protocol/anonymity/country) to stderr when done. |
| `--stats-format` | `text`\|`json` | `text` | Format for the `--show-stats` summary. Inert without `--show-stats`. |
| `--progress` | flag | off | Show a live progress bar (checked / working / avg) on stderr. Renders only when built with the `progress` feature. |
| `--state` | `PATH_OR_URL` | *(none)* | Remember proxies across runs — each checked proxy is folded into its durable history. A file path uses SQLite (`store-sqlite`); a `redis://` URL uses Redis (`store-redis`). |

The `--show-stats` summary aggregates **every** checked proxy (working or not), not just the
ones written to output. `--save` writes only working proxies. `--progress`, `--state`, and
the store backends are gated behind [feature flags](../architecture/feature-flags.md); without
the backing feature, `--state` prints a notice and is otherwise inert.

## Examples

Find 20 high-anonymity HTTP/HTTPS proxies in the US, as JSON, with a summary:

```sh
proxybroker find --types HTTP HTTPS --lvl High --only-cc US \
  --limit 20 --format json --show-stats
```

Deep check with the transient retry set and a liveness fallback, saving a reloadable pool:

```sh
proxybroker find --types HTTP --retry-on transient --max-tries 4 \
  --liveness-url https://httpbin.org/ip --save pool.ndjson
```

The library equivalent of these queries is shown in
[Broker & Queries](../library/broker.md) and the [`checking_depth`](../library/examples.md)
example.
