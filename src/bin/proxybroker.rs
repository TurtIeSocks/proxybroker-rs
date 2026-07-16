//! The `proxybroker` CLI — a thin shell over the library.
//!
//! - `grab` — scrape providers, no checking.
//! - `find` — scrape, check, and classify anonymity.
//! - `serve` — run a local rotating proxy server (requires the `server` feature).

use clap::{Parser, Subcommand, ValueEnum};
use futures_util::StreamExt;
use proxybroker::broker::{Broker, FindQuery, GrabQuery};
use proxybroker::types::{AnonLevel, ParseProtoError, Proto, TypeSpec};
use proxybroker::Proxy;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// clap value parsers via the types' own `FromStr` (keeps clap out of the library's
/// always-compiled `types.rs`).
fn parse_proto(s: &str) -> Result<Proto, ParseProtoError> {
    s.parse()
}
fn parse_lvl(s: &str) -> Result<AnonLevel, ParseProtoError> {
    s.parse()
}

/// Shown in `--version`. The DB-IP attribution is required by CC BY 4.0 whenever the geo
/// data is bundled — see `LICENSE-DATA`.
const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    "\nIP Geolocation by DB-IP (https://db-ip.com), licensed CC BY 4.0"
);

#[derive(Parser)]
#[command(
    name = "proxybroker",
    version = VERSION,
    about = "Find, check, and serve public HTTP(S) and SOCKS4/5 proxies.",
    long_about = "Find, check, and serve public HTTP(S) and SOCKS4/5 proxies.\n\n\
        A Rust rewrite of proxybroker2. Geo data: DB-IP Country Lite (CC BY 4.0)."
)]
struct Cli {
    /// Log level (error, warn, info, debug, trace).
    #[arg(long, global = true, default_value = "warn")]
    log: String,

    /// Path to a MaxMind-format country database, overriding the bundled DB-IP one.
    #[arg(long, global = true, value_name = "PATH")]
    geo_db: Option<PathBuf>,

    /// Load extra providers from YAML/JSON configs in this directory (appended to the
    /// bundled set). May be repeated. Pass --providers-only to use ONLY these.
    #[arg(long, global = true, value_name = "DIR")]
    provider_dir: Vec<PathBuf>,

    /// Use only the --provider-dir providers, ignoring the bundled registry.
    #[arg(long, global = true)]
    providers_only: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Gather proxies from the providers without checking them.
    Grab(GrabArgs),
    /// Gather proxies and check that they work, classifying anonymity.
    Find(FindArgs),
    /// Check a list of proxies you already have (from stdin or --infile).
    Check(CheckArgs),
    /// Run a local proxy server that rotates through working proxies.
    #[cfg(feature = "server")]
    Serve(ServeArgs),
}

/// CLI mirror of [`proxybroker::server::Strategy`] (keeps clap's `ValueEnum` out of the library).
#[cfg(feature = "server")]
#[derive(Clone, Copy, Default, ValueEnum)]
enum SelectStrategy {
    /// Lowest error rate then fastest response.
    #[default]
    Best,
    /// Rotate through eligible proxies in order.
    RoundRobin,
    /// Uniform random pick.
    Random,
    /// Pin each client to one upstream (by IP, or --sticky-header).
    Sticky,
}

#[cfg(feature = "server")]
impl SelectStrategy {
    fn to_server(self) -> proxybroker::server::Strategy {
        use proxybroker::server::Strategy;
        match self {
            SelectStrategy::Best => Strategy::Best,
            SelectStrategy::RoundRobin => Strategy::RoundRobin,
            SelectStrategy::Random => Strategy::Random,
            SelectStrategy::Sticky => Strategy::Sticky,
        }
    }
}

#[cfg(feature = "server")]
#[derive(clap::Args, Default)]
struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8888")]
    host: String,

    /// Protocols to find for the pool. Required unless --load.
    #[arg(long, num_args = 1.., required_unless_present = "load", value_name = "TYPE", value_parser = parse_proto)]
    types: Vec<Proto>,

    /// Fill the pool from an NDJSON file of already-checked proxies (from a prior --save) instead
    /// of finding fresh ones. The pool serves these, then drains (no top-up).
    #[arg(long, value_name = "PATH", conflicts_with = "types")]
    load: Option<PathBuf>,

    /// Anonymity levels to accept for HTTP (e.g. High Anonymous). Default: any.
    #[arg(long, num_args = 1.., value_name = "LVL", value_parser = parse_lvl)]
    lvl: Vec<AnonLevel>,

    /// DNS blocklist zones; reject proxies listed in any (e.g. zen.spamhaus.org).
    #[arg(long, num_args = 1.., value_name = "ZONE")]
    dnsbl: Vec<String>,

    /// Use POST instead of GET for the pool-fill test request.
    #[arg(long)]
    post: bool,

    /// Require the anonymity level to match exactly.
    #[arg(long)]
    strict: bool,

    /// How to pick an upstream per request.
    #[arg(long, value_enum, default_value_t = SelectStrategy::Best)]
    strategy: SelectStrategy,

    /// With --strategy sticky, key the session on this request header instead of the client IP
    /// (HTTP requests only).
    #[arg(long, value_name = "HEADER")]
    sticky_header: Option<String>,

    /// Keep the pool topped up to this many working proxies.
    #[arg(long, default_value_t = 100)]
    limit: usize,

    /// Keep only proxies located in these ISO country codes.
    #[arg(long, visible_alias = "only-cc", num_args = 1.., value_delimiter = ',', value_name = "CC")]
    countries: Vec<String>,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 8)]
    timeout: u64,

    /// Drop a proxy once its error rate exceeds this (0.0–1.0).
    #[arg(long, default_value_t = 0.5)]
    max_error_rate: f64,

    /// Drop a proxy once its average response time (seconds) exceeds this.
    #[arg(long, default_value_t = 8.0)]
    max_resp_time: f64,

    /// Seconds a proxy is benched after a failure before it is re-probed.
    #[arg(long, default_value_t = 30)]
    fail_timeout: u64,

    /// Prefer proxies that support CONNECT:80 when otherwise equally ranked.
    #[arg(long)]
    prefer_connect: bool,

    /// Retry through another proxy when the upstream HTTP status is outside this set (e.g. 200 204
    /// 301 302), to dodge block pages. Empty = accept any status. HTTP requests only.
    #[arg(long, num_args = 1.., value_name = "CODE")]
    http_allowed_codes: Vec<u16>,

    /// Wait until the pool holds at least this many proxies before accepting clients.
    #[arg(long, default_value_t = 0)]
    min_queue: usize,

    /// TCP listen backlog (queued pending connections).
    #[arg(long, default_value_t = 1024)]
    backlog: u32,

    /// Require clients to authenticate with `Proxy-Authorization: Basic base64(user:pass)` (also
    /// gates the SOCKS5 front-end via RFC 1929). Absent = open server.
    #[arg(long, value_name = "USER:PASS")]
    auth: Option<String>,

    /// Attempts (with different proxies) per client request.
    #[arg(long, default_value_t = 3)]
    max_tries: usize,
}

#[derive(clap::Args)]
struct GrabArgs {
    /// Stop after this many proxies. 0 means unlimited.
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Keep only proxies located in these ISO country codes (e.g. US GB DE).
    #[arg(long, visible_alias = "only-cc", num_args = 1.., value_delimiter = ',', value_name = "CC")]
    countries: Vec<String>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Default)]
    format: Format,

    /// Render each proxy through this template instead — overrides --format. Tokens: {{proxy}}
    /// {{host}} {{port}} {{scheme}} {{protocols}} {{anon}} {{country}} {{duration}} {{error_rate}};
    /// unknown tokens pass through literally.
    #[arg(long, value_name = "TEMPLATE")]
    output_format: Option<String>,

    /// Write to this file instead of stdout.
    #[arg(long, value_name = "PATH")]
    outfile: Option<PathBuf>,
}

#[derive(clap::Args)]
struct FindArgs {
    /// Protocols to check (required). E.g. --types HTTP HTTPS SOCKS5 CONNECT:80.
    #[arg(long, num_args = 1.., required = true, value_name = "TYPE", value_parser = parse_proto)]
    types: Vec<Proto>,

    /// Anonymity levels to accept for HTTP (e.g. High Anonymous). Default: any.
    #[arg(long, num_args = 1.., value_name = "LVL", value_parser = parse_lvl)]
    lvl: Vec<AnonLevel>,

    /// Stop after this many working proxies. 0 means unlimited.
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Keep only proxies located in these ISO country codes.
    #[arg(long, visible_alias = "only-cc", num_args = 1.., value_delimiter = ',', value_name = "CC")]
    countries: Vec<String>,

    /// Judge URLs to use instead of the bundled defaults.
    #[arg(long, num_args = 1.., value_name = "URL")]
    judges: Vec<String>,

    /// DNS blocklist zones; reject proxies listed in any (e.g. zen.spamhaus.org).
    #[arg(long, num_args = 1.., value_name = "ZONE")]
    dnsbl: Vec<String>,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 8)]
    timeout: u64,

    /// Maximum concurrent checks.
    #[arg(long, default_value_t = 200)]
    max_conn: usize,

    /// Attempts per protocol before giving up.
    #[arg(long, default_value_t = 3)]
    max_tries: usize,

    /// Use POST instead of GET for the test request.
    #[arg(long)]
    post: bool,

    /// Require the anonymity level to match exactly.
    #[arg(long)]
    strict: bool,

    /// Print an aggregate summary (by protocol/anonymity/country) to stderr when done.
    #[arg(long)]
    show_stats: bool,

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Default)]
    format: Format,

    /// Render each proxy through this template instead — overrides --format. Tokens: {{proxy}}
    /// {{host}} {{port}} {{scheme}} {{protocols}} {{anon}} {{country}} {{duration}} {{error_rate}};
    /// unknown tokens pass through literally.
    #[arg(long, value_name = "TEMPLATE")]
    output_format: Option<String>,

    /// Write to this file instead of stdout.
    #[arg(long, value_name = "PATH")]
    outfile: Option<PathBuf>,

    /// Also append every working proxy as NDJSON to this file (for `check --load` / `serve
    /// --load`). Independent of --format/--outfile.
    #[arg(long, value_name = "PATH")]
    save: Option<PathBuf>,
}

#[derive(clap::Args)]
struct CheckArgs {
    /// Protocols to check. E.g. --types HTTP HTTPS SOCKS5 CONNECT:80. Required unless --load.
    #[arg(long, num_args = 1.., required_unless_present = "load", value_name = "TYPE", value_parser = parse_proto)]
    types: Vec<Proto>,

    /// Read `host:port` addresses from this file instead of stdin.
    #[arg(long, value_name = "PATH")]
    infile: Option<PathBuf>,

    /// Load already-checked proxies from an NDJSON file (from a prior --save) and emit them
    /// WITHOUT re-checking. Stats restart from empty (a warm start, not a resumed history).
    #[arg(long, value_name = "PATH", conflicts_with_all = ["infile", "types"])]
    load: Option<PathBuf>,

    /// Anonymity levels to accept for HTTP (e.g. High Anonymous). Default: any.
    #[arg(long, num_args = 1.., value_name = "LVL", value_parser = parse_lvl)]
    lvl: Vec<AnonLevel>,

    /// Stop after this many working proxies. 0 means unlimited.
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Keep only proxies located in these ISO country codes.
    #[arg(long, visible_alias = "only-cc", num_args = 1.., value_delimiter = ',', value_name = "CC")]
    countries: Vec<String>,

    /// Judge URLs to use instead of the bundled defaults.
    #[arg(long, num_args = 1.., value_name = "URL")]
    judges: Vec<String>,

    /// DNS blocklist zones; reject proxies listed in any (e.g. zen.spamhaus.org).
    #[arg(long, num_args = 1.., value_name = "ZONE")]
    dnsbl: Vec<String>,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 8)]
    timeout: u64,

    /// Maximum concurrent checks.
    #[arg(long, default_value_t = 200)]
    max_conn: usize,

    /// Attempts per protocol before giving up.
    #[arg(long, default_value_t = 3)]
    max_tries: usize,

    /// Use POST instead of GET for the test request.
    #[arg(long)]
    post: bool,

    /// Require the anonymity level to match exactly.
    #[arg(long)]
    strict: bool,

    /// Print an aggregate summary to stderr when done.
    #[arg(long)]
    show_stats: bool,

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Default)]
    format: Format,

    /// Render each proxy through this template instead — overrides --format. Tokens: {{proxy}}
    /// {{host}} {{port}} {{scheme}} {{protocols}} {{anon}} {{country}} {{duration}} {{error_rate}};
    /// unknown tokens pass through literally.
    #[arg(long, value_name = "TEMPLATE")]
    output_format: Option<String>,

    /// Write to this file instead of stdout.
    #[arg(long, value_name = "PATH")]
    outfile: Option<PathBuf>,

    /// Also append every working proxy as NDJSON to this file (reloadable via --load). Independent
    /// of --format/--outfile.
    #[arg(long, value_name = "PATH")]
    save: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    /// `host:port`, one per line.
    Default,
    /// `host:port`, one per line (alias of default for grabbed proxies).
    Txt,
    /// One JSON object per line (NDJSON).
    Json,
    /// A single `[ {...}, {...} ]` JSON array document, emitted incrementally.
    JsonArray,
    /// `scheme://host:port`, one per line.
    Url,
    /// `host,port,protocols,anon,country,resp_time,error_rate`, with a header row.
    Csv,
}

/// Stateful output formatter: a one-time `prefix` (CSV header, JSON-array `[`), a per-proxy `item`
/// (which owns its own newline / array separator), and a one-time `suffix` (JSON-array `]`). One
/// source of format truth shared by both `write_stream` sink loops. Later waves' formats plug in as
/// new `Format` variants + arms here.
struct Emitter<'a> {
    format: Format,
    /// C6: when `Some`, `--output-format` template overrides `format` (always line output).
    template: Option<&'a str>,
    /// JSON-array separator state: `true` once at least one element has been emitted.
    started: bool,
}

impl<'a> Emitter<'a> {
    fn new(format: Format, template: Option<&'a str>) -> Self {
        Emitter {
            format,
            template,
            started: false,
        }
    }

    /// Bytes emitted once before the first proxy. `None` for the streaming line formats, and for a
    /// template (which is always plain line output — it ignores `json-array` wrapping).
    fn prefix(&self) -> Option<String> {
        if self.template.is_some() {
            return None;
        }
        match self.format {
            Format::JsonArray => Some("[".into()),
            Format::Csv => Some("host,port,protocols,anon,country,resp_time,error_rate\n".into()),
            Format::Default | Format::Txt | Format::Json | Format::Url => None,
        }
    }

    /// One proxy rendered as a self-contained chunk (including its trailing newline for line
    /// formats). `&mut self` so structural formats (JSON array) can track separator state.
    fn item(&mut self, proxy: &Proxy) -> String {
        if let Some(tmpl) = self.template {
            return format!("{}\n", render_template(tmpl, proxy));
        }
        match self.format {
            Format::Default | Format::Txt => format!("{}\n", proxy.addr()),
            Format::Json => format!("{}\n", serde_json::to_string(proxy).unwrap()),
            Format::Url => format!("{}://{}\n", scheme_str(proxy), proxy.addr()),
            Format::Csv => format!("{}\n", csv_row(proxy)),
            Format::JsonArray => {
                // Same per-object bytes as `Json`, wrapped: a leading `,` for every element after
                // the first, so the array streams out well-formed without buffering.
                let sep = if self.started { "," } else { "" };
                self.started = true;
                format!("{sep}{}", serde_json::to_string(proxy).unwrap())
            }
        }
    }

    /// Bytes emitted once after the last proxy. `None` for line formats and templates.
    fn suffix(&self) -> Option<String> {
        if self.template.is_some() {
            return None;
        }
        match self.format {
            Format::JsonArray => Some("]\n".into()),
            Format::Default | Format::Txt | Format::Json | Format::Url | Format::Csv => None,
        }
    }
}

/// `https` if the proxy can tunnel TLS, else `http` — the URL scheme for `--format url`. A grabbed
/// (unchecked) proxy has no confirmed types, so it falls back to `http`. `Scheme` has no
/// `Display`, so the two-arm choice is inlined here rather than widened into the library.
fn scheme_str(p: &Proxy) -> &'static str {
    if p.schemes().contains(&proxybroker::Scheme::Https) {
        "https"
    } else {
        "http"
    }
}

/// Confirmed protocols as `|`-joined wire names (never contains a comma). Shared by CSV + template.
fn proto_list(p: &Proxy) -> String {
    p.types()
        .keys()
        .map(|proto| proto.as_str())
        .collect::<Vec<_>>()
        .join("|")
}

/// The HTTP anonymity level as a wire string, or `""` if unchecked / not HTTP. Shared by CSV +
/// template.
fn anon_str(p: &Proxy) -> &'static str {
    p.types()
        .get(&Proto::Http)
        .and_then(|l| *l)
        .map(AnonLevel::as_str)
        .unwrap_or("")
}

/// The ISO country code, or `""` when geo is absent. Shared by CSV + template.
fn country_str(p: &Proxy) -> &str {
    p.geo.as_ref().map(|c| c.code.as_str()).unwrap_or("")
}

/// Render a proxy through a `--output-format` template (C6). A **closed** token set replaced
/// sequentially — tokens are distinct non-overlapping literals, so `str::replace` per token is
/// correct without a parser. Unknown `{{...}}` tokens are left literally (predictable, config-free).
fn render_template(tmpl: &str, p: &Proxy) -> String {
    tmpl.replace("{{proxy}}", &p.addr())
        .replace("{{host}}", &p.host.to_string())
        .replace("{{port}}", &p.port.to_string())
        .replace("{{scheme}}", scheme_str(p))
        .replace("{{protocols}}", &proto_list(p))
        .replace("{{anon}}", anon_str(p))
        .replace("{{country}}", country_str(p))
        .replace("{{duration}}", &p.avg_resp_time().to_string())
        .replace("{{error_rate}}", &p.error_rate().to_string())
}

/// One CSV row for `--format csv`. Every field is comma-free by construction (protocols `|`-joined,
/// ISO country code only, numeric stats), so no quoting layer is needed — a deviation guarded by
/// the `csv_header_and_row` test (it fails if a field ever gains a comma).
fn csv_row(p: &Proxy) -> String {
    format!(
        "{},{},{},{},{},{},{}",
        p.host,
        p.port,
        proto_list(p),
        anon_str(p),
        country_str(p),
        p.avg_resp_time(),
        p.error_rate()
    )
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = Broker::builder();

    // Providers: bundled registry plus any --provider-dir configs, or ONLY the configs when
    // --providers-only is set (mirrors proxybroker2's providers=[] + provider_dirs pattern).
    if !cli.provider_dir.is_empty() || cli.providers_only {
        let mut providers = if cli.providers_only {
            Vec::new()
        } else {
            proxybroker::provider::bundled_registry()
        };
        for dir in &cli.provider_dir {
            providers.extend(proxybroker::load_provider_dir(dir));
        }
        if providers.is_empty() {
            return Err(
                "no providers: --providers-only with no valid --provider-dir configs".into(),
            );
        }
        builder = builder.providers(providers);
    }

    #[cfg(feature = "geo")]
    {
        use proxybroker::geo::GeoDb;
        let db = match &cli.geo_db {
            Some(path) => Some(GeoDb::open(path)?),
            None => GeoDb::bundled().ok(),
        };
        if let Some(db) = db {
            builder = builder.geo(db);
        }
    }

    let broker = builder.build();

    match cli.command {
        Command::Grab(args) => grab(broker, args).await,
        Command::Find(args) => find(broker, args).await,
        Command::Check(args) => check(broker, args).await,
        #[cfg(feature = "server")]
        Command::Serve(args) => serve_cmd(broker, args).await,
    }
}

#[cfg(feature = "server")]
async fn serve_cmd(broker: Broker, args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    use proxybroker::resolver::Resolver;
    use proxybroker::server::{serve, Pool, PoolConfig};
    use std::sync::Arc;

    let addr: std::net::SocketAddr = args.host.parse()?;
    let pool_config = PoolConfig {
        max_tries: args.max_tries,
        max_error_rate: args.max_error_rate,
        max_resp_time: args.max_resp_time,
        // Uppercased allow-list so the pool screens admissions (esp. the --load path, which never
        // ran find's country filter). None when no countries requested.
        countries: (!args.countries.is_empty())
            .then(|| args.countries.iter().map(|c| c.to_uppercase()).collect()),
        strategy: args.strategy.to_server(),
        sticky_header: args.sticky_header.clone(),
        fail_timeout: Duration::from_secs(args.fail_timeout),
        prefer_connect: args.prefer_connect,
        http_allowed_codes: (!args.http_allowed_codes.is_empty())
            .then(|| args.http_allowed_codes.clone()),
        ..Default::default()
    };

    // --load: fill the pool from a saved NDJSON file (already-checked proxies) and serve those,
    // no finding. The pool drains as it is used (no top-up); stats restart from empty.
    let pool = if let Some(path) = &args.load {
        let loaded = proxybroker::read_ndjson(std::io::BufReader::new(std::fs::File::open(path)?))?;
        eprintln!("loaded {} proxies from {}", loaded.len(), path.display());
        Pool::from_proxies(loaded, pool_config)
    } else {
        // Find proxies to fill the pool, filtered by the serve flags (types/lvl/strict/post/
        // dnsbl/countries). The flag→query mapping lives in the pure `serve_query`.
        let stream = broker.find(serve_query(&args)).await?;
        Pool::spawn(stream, pool_config)
    };
    let resolver = Arc::new(Resolver::new(Duration::from_secs(args.timeout))?);
    let handle = serve(
        addr,
        pool,
        resolver,
        Duration::from_secs(args.timeout),
        args.min_queue,
        args.backlog,
        args.auth,
    )
    .await?;
    eprintln!(
        "proxybroker serving on {} — Ctrl-C to stop",
        handle.local_addr()
    );

    tokio::signal::ctrl_c().await?;
    handle.shutdown();
    eprintln!("shutting down");
    Ok(())
}

/// Build the pool-fill `FindQuery` from the serve flags. Pure (no I/O, no broker) so the
/// flag→query mapping is unit-testable offline — the filtering itself runs upstream in `find`.
/// `serve` needs a positive limit (an unbounded pool would fill forever), so `0` maps to `1`,
/// matching api.py's `if limit <= 0: raise ValueError`.
#[cfg(feature = "server")]
fn serve_query(args: &ServeArgs) -> FindQuery {
    let mut b = FindQuery::builder()
        .types(types_from(args.types.clone(), args.lvl.clone()))
        .limit(args.limit.max(1))
        .dnsbl(args.dnsbl.clone())
        .timeout(Duration::from_secs(args.timeout))
        .post(args.post)
        .strict(args.strict);
    if !args.countries.is_empty() {
        b = b.countries(args.countries.clone());
    }
    b.build()
}

async fn grab(broker: Broker, args: GrabArgs) -> Result<(), Box<dyn std::error::Error>> {
    let query = GrabQuery {
        countries: (!args.countries.is_empty()).then_some(args.countries),
        // --limit 0 means unlimited. Mapped here, once, so the rest of the code never sees
        // 0-as-unlimited (which would otherwise make a `take(0)` yield nothing).
        limit: (args.limit > 0).then_some(args.limit),
    };
    let mut stream = broker.grab(query);
    write_stream(
        &mut stream,
        args.format,
        args.output_format.as_deref(),
        args.outfile.as_deref(),
        None,
    )
    .await
}

/// Attach the requested anonymity levels to every requested type. `--lvl` applies only to
/// HTTP; for other protocols the checker ignores levels.
fn types_from(protos: Vec<Proto>, lvl: Vec<AnonLevel>) -> Vec<TypeSpec> {
    let levels = (!lvl.is_empty()).then_some(lvl);
    protos
        .into_iter()
        .map(|proto| TypeSpec {
            proto,
            levels: levels.clone(),
        })
        .collect()
}

async fn find(broker: Broker, args: FindArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = FindQuery::builder()
        .types(types_from(args.types, args.lvl))
        .limit(args.limit)
        .judges(args.judges)
        .dnsbl(args.dnsbl)
        .timeout(Duration::from_secs(args.timeout))
        .max_conn(args.max_conn)
        .max_tries(args.max_tries)
        .post(args.post)
        .strict(args.strict);
    if !args.countries.is_empty() {
        builder = builder.countries(args.countries);
    }
    let query = builder.build();

    let mut stream = broker.find(query).await?;
    write_stream(
        &mut stream,
        args.format,
        args.output_format.as_deref(),
        args.outfile.as_deref(),
        args.save.as_deref(),
    )
    .await?;

    if args.show_stats {
        // Stats come from the stream itself, which aggregated EVERY checked proxy (working or
        // not) — not just the winners written above. Printed to stderr so it never mixes with
        // the proxy output on stdout. `stats()` is complete now: the stream is fully drained,
        // so all checks have finished and recorded.
        if let Some(s) = stream.stats() {
            eprint!("\n{s}");
        }
    }
    Ok(())
}

async fn check(broker: Broker, args: CheckArgs) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::AsyncReadExt;

    // --load: the proxies are already checked. Read them and stream straight to output, no broker,
    // no network. `--types` is optional here (enforced by clap), and unused.
    if let Some(path) = &args.load {
        let loaded = proxybroker::read_ndjson(std::io::BufReader::new(std::fs::File::open(path)?))?;
        let mut stream = futures_util::stream::iter(loaded);
        write_stream(
            &mut stream,
            args.format,
            args.output_format.as_deref(),
            args.outfile.as_deref(),
            args.save.as_deref(),
        )
        .await?;
        return Ok(());
    }

    // Input: a file, or stdin by default.
    let text = match &args.infile {
        Some(path) => tokio::fs::read_to_string(path).await?,
        None => {
            let mut buf = String::new();
            tokio::io::stdin().read_to_string(&mut buf).await?;
            buf
        }
    };
    let proxies = proxybroker::parse_proxy_lines(&text);
    if proxies.is_empty() {
        eprintln!("no proxy addresses parsed from input");
    }

    let query = FindQuery {
        types: types_from(args.types, args.lvl),
        countries: (!args.countries.is_empty()).then_some(args.countries),
        limit: (args.limit > 0).then_some(args.limit),
        judges: args.judges,
        dnsbl: args.dnsbl,
        timeout: Duration::from_secs(args.timeout),
        max_conn: args.max_conn,
        max_tries: args.max_tries,
        post: args.post,
        strict: args.strict,
    };

    let mut stream = broker
        .check(futures_util::stream::iter(proxies), query)
        .await?;
    write_stream(
        &mut stream,
        args.format,
        args.output_format.as_deref(),
        args.outfile.as_deref(),
        args.save.as_deref(),
    )
    .await?;

    if args.show_stats {
        if let Some(s) = stream.stats() {
            eprint!("\n{s}");
        }
    }
    Ok(())
}

/// Drain a proxy stream to a file or stdout in the chosen format. Takes `&mut` so the caller
/// keeps the stream afterwards (e.g. to read `stats()`). When `save` is set, each streamed proxy
/// is also appended to that file as NDJSON (the C2 warm-start artifact), independent of `format`.
async fn write_stream<S>(
    stream: &mut S,
    format: Format,
    template: Option<&str>,
    outfile: Option<&std::path::Path>,
    save: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: futures_util::Stream<Item = Proxy> + Unpin,
{
    // The save sink is the exact NDJSON bytes read_ndjson expects. Open once, before draining.
    // ponytail: blocking std::fs write inside the async loop — one small line per proxy at CLI
    // scale, not worth a spawn_blocking dance.
    let mut save_file = match save {
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };
    let mut save_line = |proxy: &Proxy| -> std::io::Result<()> {
        if let Some(f) = save_file.as_mut() {
            proxybroker::write_ndjson(f, std::slice::from_ref(proxy))?;
        }
        Ok(())
    };

    // Writing to a file is async I/O; stdout is a blocking lock. Keep them separate rather
    // than boxing a trait object over two very different sinks; the *format* logic lives only in
    // the shared `Emitter`, so the two branches differ only in their I/O mechanics.
    let mut emitter = Emitter::new(format, template);
    if let Some(path) = outfile {
        let mut file = tokio::fs::File::create(path).await?;
        if let Some(p) = emitter.prefix() {
            file.write_all(p.as_bytes()).await?;
        }
        let mut count = 0u64;
        while let Some(proxy) = stream.next().await {
            file.write_all(emitter.item(&proxy).as_bytes()).await?;
            save_line(&proxy)?;
            count += 1;
        }
        if let Some(s) = emitter.suffix() {
            file.write_all(s.as_bytes()).await?;
        }
        file.flush().await?;
        eprintln!("wrote {count} proxies to {}", path.display());
    } else {
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        if let Some(p) = emitter.prefix() {
            write!(lock, "{p}")?;
        }
        while let Some(proxy) = stream.next().await {
            write!(lock, "{}", emitter.item(&proxy))?;
            save_line(&proxy)?;
        }
        if let Some(s) = emitter.suffix() {
            write!(lock, "{s}")?;
        }
    }
    Ok(())
}

fn init_tracing(level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("proxybroker={level}")));
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(all(test, feature = "server"))]
mod tests {
    use super::*;

    #[test]
    fn serve_query_threads_lvl_and_strict() {
        // The four passthrough flags must reach the FindQuery that fills the pool; before B3 they
        // were dropped, so anonymity-filtered serving was impossible.
        let args = ServeArgs {
            types: vec![Proto::Http],
            lvl: vec![AnonLevel::High],
            strict: true,
            post: true,
            dnsbl: vec!["zen.spamhaus.org".into()],
            ..Default::default()
        };
        let q = serve_query(&args);
        assert_eq!(q.types[0].levels, Some(vec![AnonLevel::High]));
        assert!(q.strict);
        assert!(q.post);
        assert_eq!(q.dnsbl, vec!["zen.spamhaus.org".to_string()]);
    }

    fn serve_countries(argv: &[&str]) -> Vec<String> {
        match Cli::try_parse_from(argv).unwrap().command {
            Command::Serve(a) => a.countries,
            _ => panic!("expected serve"),
        }
    }

    #[test]
    fn only_cc_alias_splits_on_comma() {
        // --only-cc US,DE (comma) and --countries US DE (space) must be equivalent spellings.
        let comma = serve_countries(&[
            "proxybroker",
            "serve",
            "--types",
            "HTTP",
            "--only-cc",
            "US,DE",
        ]);
        let space = serve_countries(&[
            "proxybroker",
            "serve",
            "--types",
            "HTTP",
            "--countries",
            "US",
            "DE",
        ]);
        assert_eq!(comma, vec!["US".to_string(), "DE".to_string()]);
        assert_eq!(comma, space);
    }
}

#[cfg(test)]
mod format_tests {
    use super::*;
    use proxybroker::{AnonLevel, Country, Proto, Proxy};
    use std::collections::BTreeSet;

    /// A checked proxy with geo + one HTTP type at High + one recorded runtime.
    fn proxy_fixture() -> Proxy {
        let mut p = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
        p.geo = Some(Country {
            code: "US".into(),
            name: "United States".into(),
        });
        p.add_type(Proto::Http, Some(AnonLevel::High));
        p.record_attempt(Some(0.42), None);
        p
    }

    #[test]
    fn emitter_default_is_addr_per_line() {
        let mut e = Emitter::new(Format::Default, None);
        assert!(e.prefix().is_none());
        assert_eq!(e.item(&proxy_fixture()), "1.2.3.4:8080\n");
        assert!(e.suffix().is_none());
    }

    #[test]
    fn emitter_json_is_ndjson() {
        let mut e = Emitter::new(Format::Json, None);
        assert!(e.prefix().is_none() && e.suffix().is_none());
        let line = e.item(&proxy_fixture());
        assert!(line.ends_with('\n'), "{line:?}");
        assert_eq!(
            line.matches('\n').count(),
            1,
            "exactly one object per line: {line:?}"
        );
        assert!(line.trim_end().starts_with('{'), "{line:?}");
    }

    #[test]
    fn url_format_prefixes_scheme() {
        // HTTP-only proxy → http.
        assert_eq!(
            Emitter::new(Format::Url, None).item(&proxy_fixture()),
            "http://1.2.3.4:8080\n"
        );
        // A SOCKS5 proxy tunnels TLS → https.
        let mut p = Proxy::new("5.6.7.8".parse().unwrap(), 1080, BTreeSet::new());
        p.add_type(Proto::Socks5, None);
        assert_eq!(
            Emitter::new(Format::Url, None).item(&p),
            "https://5.6.7.8:1080\n"
        );
    }

    #[test]
    fn csv_header_and_row() {
        let mut e = Emitter::new(Format::Csv, None);
        assert_eq!(
            e.prefix().unwrap(),
            "host,port,protocols,anon,country,resp_time,error_rate\n"
        );
        let row = e.item(&proxy_fixture());
        let row = row.trim_end();
        let fields: Vec<&str> = row.split(',').collect();
        // Exactly 7 fields ⇒ no field contained a comma (the no-quoting guard).
        assert_eq!(fields.len(), 7, "{row}");
        assert_eq!(&fields[..5], ["1.2.3.4", "8080", "HTTP", "High", "US"]);
    }

    #[test]
    fn csv_unchecked_proxy_has_empty_type_columns() {
        // A grabbed proxy: no confirmed types, no geo → the type/geo columns are empty.
        let p = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
        let row = Emitter::new(Format::Csv, None).item(&p);
        assert_eq!(row.trim_end(), "1.2.3.4,8080,,,,0,0");
    }

    #[test]
    fn json_array_emits_bracketed_comma_separated() {
        let mut e = Emitter::new(Format::JsonArray, None);
        let mut out = e.prefix().unwrap();
        out.push_str(&e.item(&proxy_fixture()));
        out.push_str(&e.item(&proxy_fixture()));
        out.push_str(&e.suffix().unwrap());
        assert!(out.starts_with('['), "{out}");
        assert!(out.ends_with("]\n"), "{out}");
        let v: Vec<serde_json::Value> = serde_json::from_str(out.trim_end()).unwrap();
        assert_eq!(v.len(), 2, "parses as a 2-element array: {out}");
    }

    #[test]
    fn json_array_empty_stream_is_empty_array() {
        let e = Emitter::new(Format::JsonArray, None);
        let out = format!("{}{}", e.prefix().unwrap(), e.suffix().unwrap());
        assert_eq!(out, "[]\n");
        assert!(
            serde_json::from_str::<Vec<serde_json::Value>>(out.trim_end())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ndjson_still_one_object_per_line() {
        // C4 must not change the streaming NDJSON default: each element is a standalone object with
        // no array wrapping and no leading separator.
        let mut e = Emitter::new(Format::Json, None);
        let a = e.item(&proxy_fixture());
        let b = e.item(&proxy_fixture());
        assert!(
            a.trim_end().starts_with('{') && a.trim_end().ends_with('}'),
            "{a:?}"
        );
        assert!(
            b.starts_with('{'),
            "NDJSON element must not have a leading separator: {b:?}"
        );
    }

    #[test]
    fn template_renders_known_fields() {
        assert_eq!(
            render_template("{{proxy}}/{{country}}/{{duration}}", &proxy_fixture()),
            "1.2.3.4:8080/US/0.42"
        );
    }

    #[test]
    fn template_leaves_unknown_tokens_literal() {
        assert_eq!(render_template("{{nope}}", &proxy_fixture()), "{{nope}}");
    }

    #[test]
    fn output_format_overrides_format() {
        // A template forces line output even when the format is json-array.
        let mut e = Emitter::new(Format::JsonArray, Some("{{host}}"));
        assert!(e.prefix().is_none() && e.suffix().is_none());
        assert_eq!(e.item(&proxy_fixture()), "1.2.3.4\n");
    }
}
