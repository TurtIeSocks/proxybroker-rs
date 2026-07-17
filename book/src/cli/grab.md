# grab

Gather proxy candidates from the providers **without** checking them. `grab` scrapes each
configured provider and emits every candidate address it finds — none are dialed, so there is
no guarantee any of them actually work. Use [`find`](./find.md) when you want only proxies
that pass a live check.

```sh
proxybroker grab --limit 50
```

## Options

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--limit` | integer | `0` | Stop after this many proxies. `0` means unlimited. |
| `--countries`, `--only-cc` | ISO codes | *(all)* | Keep only proxies located in these ISO country codes (e.g. `US GB DE`). Accepts space-separated values or a comma-separated list. |
| `--format` | `default`\|`txt`\|`json`\|`json-array`\|`url`\|`csv` | `default` | Output format. See [Output Formats](./output-formats.md). |
| `--output-format` | template | *(none)* | Render each proxy through a template, overriding `--format`. Tokens: `{{proxy}} {{host}} {{port}} {{scheme}} {{protocols}} {{anon}} {{country}} {{asn}} {{asn_org}} {{duration}} {{error_rate}}`; unknown tokens pass through literally. |
| `--outfile` | `PATH` | *(stdout)* | Write to this file instead of stdout. |

`--countries` filtering requires geo data, so a candidate's country is only known when the
binary is built with the `geo` feature (default) or a [`--geo-db`](./overview.md) is supplied.

Because grabbed proxies are unchecked, several template/CSV fields are empty or defaulted:
there are no confirmed `{{protocols}}`, no `{{anon}}` level, and `{{scheme}}` falls back to
`http`. Timing and error-rate fields read zero. Country and ASN are populated only if geo/ASN
databases resolved the address.

## Examples

Grab up to 100 US or German candidates as `scheme://host:port` URLs:

```sh
proxybroker grab --limit 100 --only-cc US,DE --format url
```

Grab everything into a file as NDJSON:

```sh
proxybroker grab --format json --outfile candidates.ndjson
```

Feed grabbed candidates straight into a check:

```sh
proxybroker grab --limit 500 | proxybroker check --types HTTP HTTPS
```

See [check](./check.md) for validating a candidate list and [find](./find.md) for the
combined scrape-and-check path.
