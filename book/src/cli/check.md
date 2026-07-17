# check

Check a list of proxies you already have. `check` reads `host:port` addresses from stdin (or
a file), dials each through the same checking pipeline as [`find`](./find.md), and streams the
working ones to output. Unlike `find`, it does not scrape providers — you supply the
candidates.

```sh
cat proxies.txt | proxybroker check --types HTTP HTTPS
```

`--types` is required **unless** `--load` is used (which emits already-checked proxies without
re-checking).

## Input

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--infile` | `PATH` | *(stdin)* | Read `host:port` addresses from this file instead of stdin. |
| `--load` | `PATH` | *(none)* | Load already-checked proxies from an NDJSON file (from a prior `--save`) and emit them **without** re-checking. Stats restart from empty (a warm start, not a resumed history). Conflicts with `--infile` and `--types`. |

With `--load`, no network activity occurs and `--types` is ignored; the saved proxies are
streamed straight to output. Timing fields are not persisted, so under `--show-stats` the
`avg_resp_time`/error counters read zero while total/working and the
protocol/anonymity/country breakdowns remain meaningful.

## Selection & filtering

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--types` | protocols | *(required unless `--load`)* | Protocols to check. E.g. `--types HTTP HTTPS SOCKS5 CONNECT:80`. Case-insensitive. |
| `--lvl` | levels | *(any)* | Anonymity levels to accept for HTTP: `Transparent`, `Anonymous`, `High`. HTTP only. |
| `--limit` | integer | `0` | Stop after this many working proxies. `0` means unlimited. |
| `--countries`, `--only-cc` | ISO codes | *(all)* | Keep only proxies in these ISO country codes. Space- or comma-separated. |
| `--strict` | flag | off | Require the anonymity level to match exactly. |

## Judges, timing & concurrency

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--judges` | URLs | bundled | Judge URLs to use instead of the bundled defaults. |
| `--dnsbl` | zones | *(none)* | DNS blocklist zones; reject proxies listed in any (e.g. `zen.spamhaus.org`). |
| `--timeout` | seconds | `8` | Per-request timeout. |
| `--max-conn` | integer | `200` | Maximum concurrent checks. |
| `--max-tries` | integer | `3` | Attempts per protocol before giving up. |
| `--post` | flag | off | Use `POST` instead of `GET` for the test request. |

`check` uses a plain retry-on-timeout policy (`--max-tries` attempts); the richer
`--retry-on`/`--backoff-ms` and capability filters of [`find`](./find.md) are find-only.

## Output & reporting

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--format` | `default`\|`txt`\|`json`\|`json-array`\|`url`\|`csv` | `default` | Output format. See [Output Formats](./output-formats.md). |
| `--output-format` | template | *(none)* | Per-proxy template, overriding `--format`. Tokens: `{{proxy}} {{host}} {{port}} {{scheme}} {{protocols}} {{anon}} {{country}} {{asn}} {{asn_org}} {{duration}} {{error_rate}}`. |
| `--outfile` | `PATH` | *(stdout)* | Write to this file instead of stdout. |
| `--save` | `PATH` | *(none)* | Also append every working proxy as NDJSON to this file (reloadable via `--load`). Independent of `--format`/`--outfile`. |
| `--show-stats` | flag | off | Print an aggregate summary to stderr when done. |
| `--stats-format` | `text`\|`json` | `text` | Format for the `--show-stats` summary. Inert without `--show-stats`. |

Input parsing is lenient: addresses are extracted line-by-line, and if nothing parses,
`check` prints a notice to stderr and exits cleanly.

## Examples

Check a file of addresses for working HTTPS proxies, saving winners:

```sh
proxybroker check --infile candidates.txt --types HTTPS \
  --save working.ndjson --show-stats
```

Re-emit a previously saved pool without touching the network:

```sh
proxybroker check --load working.ndjson --format url
```

Pipe grabbed candidates directly into a check:

```sh
proxybroker grab --limit 500 | proxybroker check --types HTTP --limit 50
```

See [grab](./grab.md) for producing an unchecked candidate list and
[find](./find.md) for the scrape-and-check path.
