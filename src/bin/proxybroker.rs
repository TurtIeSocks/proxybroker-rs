//! The `proxybroker` CLI — a thin shell over the library.
//!
//! Currently exposes `grab` (scrape providers, no checking). `find` and `serve` land with
//! the checker and server modules.

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

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Gather proxies from the providers without checking them.
    Grab(GrabArgs),
    /// Gather proxies and check that they work, classifying anonymity.
    Find(FindArgs),
    /// Run a local proxy server that rotates through working proxies.
    #[cfg(feature = "server")]
    Serve(ServeArgs),
}

#[cfg(feature = "server")]
#[derive(clap::Args)]
struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8888")]
    host: String,

    /// Protocols to find for the pool (required).
    #[arg(long, num_args = 1.., required = true, value_name = "TYPE", value_parser = parse_proto)]
    types: Vec<Proto>,

    /// Keep the pool topped up to this many working proxies.
    #[arg(long, default_value_t = 100)]
    limit: usize,

    /// Keep only proxies located in these ISO country codes.
    #[arg(long, num_args = 1.., value_name = "CC")]
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
    #[arg(long, num_args = 1.., value_name = "CC")]
    countries: Vec<String>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Default)]
    format: Format,

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
    #[arg(long, num_args = 1.., value_name = "CC")]
    countries: Vec<String>,

    /// Judge URLs to use instead of the bundled defaults.
    #[arg(long, num_args = 1.., value_name = "URL")]
    judges: Vec<String>,

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

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Default)]
    format: Format,

    /// Write to this file instead of stdout.
    #[arg(long, value_name = "PATH")]
    outfile: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    /// `host:port`, one per line.
    Default,
    /// `host:port`, one per line (alias of default for grabbed proxies).
    Txt,
    /// One JSON object per line.
    Json,
}

impl Format {
    fn render(self, proxy: &Proxy) -> String {
        match self {
            Format::Default | Format::Txt => proxy.addr(),
            Format::Json => serde_json::to_string(proxy).unwrap(),
        }
    }
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
    let types: Vec<TypeSpec> = args.types.into_iter().map(TypeSpec::any).collect();

    // Find proxies to fill the pool. `serve` requires a positive limit (an unbounded pool
    // would grab forever), matching api.py's `if limit <= 0: raise ValueError`.
    let stream = broker
        .find(FindQuery {
            types,
            countries: (!args.countries.is_empty()).then_some(args.countries),
            limit: Some(args.limit.max(1)),
            timeout: Duration::from_secs(args.timeout),
            ..Default::default()
        })
        .await?;

    let pool = Pool::spawn(
        stream,
        PoolConfig {
            max_tries: args.max_tries,
            max_error_rate: args.max_error_rate,
            max_resp_time: args.max_resp_time,
            ..Default::default()
        },
    );
    let resolver = Arc::new(Resolver::new(Duration::from_secs(args.timeout))?);
    let handle = serve(addr, pool, resolver, Duration::from_secs(args.timeout)).await?;
    eprintln!(
        "proxybroker serving on {} — Ctrl-C to stop",
        handle.local_addr()
    );

    tokio::signal::ctrl_c().await?;
    handle.shutdown();
    eprintln!("shutting down");
    Ok(())
}

async fn grab(broker: Broker, args: GrabArgs) -> Result<(), Box<dyn std::error::Error>> {
    let query = GrabQuery {
        countries: (!args.countries.is_empty()).then_some(args.countries),
        // --limit 0 means unlimited. Mapped here, once, so the rest of the code never sees
        // 0-as-unlimited (which would otherwise make a `take(0)` yield nothing).
        limit: (args.limit > 0).then_some(args.limit),
    };
    write_stream(broker.grab(query), args.format, args.outfile.as_deref()).await
}

async fn find(broker: Broker, args: FindArgs) -> Result<(), Box<dyn std::error::Error>> {
    // Attach the requested anonymity levels to every requested type. `--lvl` applies only to
    // HTTP; for other protocols the checker ignores levels.
    let levels = (!args.lvl.is_empty()).then_some(args.lvl);
    let types: Vec<TypeSpec> = args
        .types
        .into_iter()
        .map(|proto| TypeSpec {
            proto,
            levels: levels.clone(),
        })
        .collect();

    let query = FindQuery {
        types,
        countries: (!args.countries.is_empty()).then_some(args.countries),
        limit: (args.limit > 0).then_some(args.limit),
        judges: args.judges,
        timeout: Duration::from_secs(args.timeout),
        max_conn: args.max_conn,
        max_tries: args.max_tries,
        post: args.post,
        strict: args.strict,
    };

    let stream = broker.find(query).await?;
    write_stream(stream, args.format, args.outfile.as_deref()).await
}

/// Drain a proxy stream to a file or stdout in the chosen format.
async fn write_stream(
    mut stream: proxybroker::ProxyStream,
    format: Format,
    outfile: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Writing to a file is async I/O; stdout is a blocking lock. Keep them separate rather
    // than boxing a trait object over two very different sinks.
    if let Some(path) = outfile {
        let mut file = tokio::fs::File::create(path).await?;
        let mut count = 0u64;
        while let Some(proxy) = stream.next().await {
            file.write_all(format.render(&proxy).as_bytes()).await?;
            file.write_all(b"\n").await?;
            count += 1;
        }
        file.flush().await?;
        eprintln!("wrote {count} proxies to {}", path.display());
    } else {
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        while let Some(proxy) = stream.next().await {
            writeln!(lock, "{}", format.render(&proxy))?;
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
