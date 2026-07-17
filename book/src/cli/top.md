# top

A live terminal dashboard (a `top`-style TUI) over a working proxy pool: a sortable table of
proxies plus a response-time sparkline for the selected row. Built with
[ratatui](https://ratatui.rs).

`top` is behind the `tui` feature, which is **off by default** — it pulls in `ratatui` and
`crossterm`. Build with it enabled:

```sh
cargo build --features tui
proxybroker top --types HTTP HTTPS
```

The command fills a pool by running [find](./find.md) in the background (optionally warm-started
from `--state`) and redraws the dashboard on a fixed interval.

> Keep `--log` at its default `warn`. Higher log levels write to stderr, over the dashboard.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--types <TYPE>...` | — | Protocols to find for the pool (required). E.g. `HTTP HTTPS`. |
| `--limit <N>` | `100` | Stop filling the pool after this many working proxies. |
| `--countries <CC>...` | — | Keep only proxies in these ISO country codes. Alias: `--only-cc` (comma-separated). |
| `--timeout <SECS>` | `8` | Per-request timeout, in seconds. |
| `--refresh <SECS>` | `2` | Dashboard redraw interval, in seconds (floored to 100 ms). |
| `--state <PATH_OR_URL>` | — | Warm-start the pool from stored history: a file path (SQLite) or a `redis://` URL (Redis). Requires a store backend feature. |

Protocol values are exactly as in [find](./find.md).

## Layout

The screen has three regions:

1. A one-line pool summary header:

   ```
   total <N> | http <N> | https <N> | avg err <x.xx> | avg resp <x.xx>s
   ```

2. A bordered **Proxies** table:

   | Column | Contents |
   | --- | --- |
   | `Addr` | `host:port` |
   | `Protos` | Confirmed protocols, comma-joined |
   | `Err%` | Rolling error rate (`0.00`–`1.00`) |
   | `Resp(s)` | Average response time, seconds |
   | `Country` | ISO country code (blank if geo absent) |

3. A bordered **Selected resp time (ms)** sparkline of the highlighted row's recent response-time
   history (up to 60 samples, one per refresh).

## Keybindings

| Key | Action |
| --- | --- |
| `q` / `Esc` | Quit |
| `a` | Sort by address |
| `e` | Sort by error rate (ascending) |
| `r` | Sort by response time (ascending) — the default sort |
| `c` | Sort by country |
| `Down` / `j` | Move selection down |
| `Up` / `k` | Move selection up |

Any other key is a no-op. Sorting re-orders in place without re-fetching the pool.

## See also

- [serve](./serve.md) — the rotating proxy server over the same pool machinery.
- [mcp](./mcp.md) — expose the same live pool to agents.
- [feature flags](../architecture/feature-flags.md) — enabling `tui` and store backends.
