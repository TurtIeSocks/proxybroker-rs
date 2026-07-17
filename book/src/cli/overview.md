# CLI Overview & Global Options

The `proxybroker` binary is a thin shell over the [library](../library/broker.md). Every
invocation has the shape:

```sh
proxybroker [GLOBAL OPTIONS] <SUBCOMMAND> [SUBCOMMAND OPTIONS]
```

`--help` and `--version` work at every level. The version string also prints the DB-IP
attribution required by the bundled geo data's CC BY 4.0 license (see
[Data & Licensing](../data-and-licensing.md)).

```sh
proxybroker --help
proxybroker find --help
```

## Global options

These are declared `global = true`, so they may appear before *or* after the subcommand and
apply to whichever command runs. `proxybroker --log debug find …` and
`proxybroker find --log debug …` are equivalent.

| Option | Value | Default | Meaning |
| --- | --- | --- | --- |
| `--log` | `error`\|`warn`\|`info`\|`debug`\|`trace` | `warn` | Log level. The `RUST_LOG` env filter, if set, overrides this. |
| `--log-format` | `text`\|`json` | `text` | Log output format. `json` emits line-delimited JSON for a log pipeline. |
| `--geo-db` | `PATH` | bundled DB-IP | Path to a MaxMind-format country database, overriding the bundled one. |
| `--asn-db` | `PATH` | *(off)* | Path to a MaxMind-format ASN database (e.g. `GeoLite2-ASN.mmdb`) to attribute each proxy to its Autonomous System. No ASN data is bundled, so ASN fields are empty unless this is given. |
| `--provider-dir` | `DIR` | *(none)* | Load extra providers from YAML/JSON configs in this directory, appended to the bundled set. May be repeated. |
| `--providers-only` | flag | off | Use only the `--provider-dir` providers, ignoring the bundled registry. Errors if no valid configs are found. |

Notes:

- `--geo-db` and `--asn-db` are only honored when the binary is built with the `geo`
  feature (on by default). See [Feature Flags](../architecture/feature-flags.md).
- `--provider-dir` may be passed multiple times; each directory's configs are appended.
  Pair with `--providers-only` to replace the bundled registry entirely. See
  [Providers & Scraping](../architecture/providers.md).

## Subcommands

| Command | Purpose | Feature |
| --- | --- | --- |
| [`grab`](./grab.md) | Gather proxy candidates from providers **without** checking them. | always |
| [`find`](./find.md) | Gather **and** check proxies, classifying anonymity. | always |
| [`check`](./check.md) | Check a list of proxies you already have (stdin or `--infile`). | always |
| [`serve`](./serve.md) | Run a local proxy server that rotates through working proxies. | `server` |
| [`top`](./top.md) | Live terminal dashboard: sortable pool table + latency sparklines. | `tui` |
| [`mcp`](./mcp.md) | Serve the live pool over MCP (stdio): `get_proxy`, `pool_status`, `report_dead`. | `mcp` |

`serve`, `top`, and `mcp` are compiled in only when their feature is enabled. A stock build
may not list them in `--help`.

## Value syntax

Two argument types recur across the checking subcommands:

- **Protocols** (`--types`): case-insensitive wire names — `HTTP`, `HTTPS`, `SOCKS4`,
  `SOCKS5`, `CONNECT:80`, `CONNECT:25`. Pass several space-separated:
  `--types HTTP HTTPS SOCKS5`.
- **Anonymity levels** (`--lvl`): `Transparent`, `Anonymous`, or `High` (case-insensitive).
  Levels apply to HTTP only; other protocols ignore them.

Output formatting (`--format`, `--output-format`, `--outfile`, `--save`) is shared by
`grab`, `find`, and `check` — see [Output Formats](./output-formats.md).
