//! The `proxybroker` CLI — a thin shell over the library.
//!
//! Currently exposes `grab` (scrape providers, no checking). `find` and `serve` land with
//! the checker and server modules.

use clap::{Parser, Subcommand, ValueEnum};
use futures_util::StreamExt;
use proxybroker::broker::{Broker, GrabQuery};
use proxybroker::Proxy;
use std::io::Write;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

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
    }
}

async fn grab(broker: Broker, args: GrabArgs) -> Result<(), Box<dyn std::error::Error>> {
    let query = GrabQuery {
        countries: (!args.countries.is_empty()).then_some(args.countries),
        // --limit 0 means unlimited. Mapped here, once, so the rest of the code never sees
        // 0-as-unlimited (which would otherwise make a `take(0)` yield nothing).
        limit: (args.limit > 0).then_some(args.limit),
    };

    let mut stream = broker.grab(query);
    let format = args.format;

    // Writing to a file is async I/O; stdout is a blocking lock. Keep them separate rather
    // than boxing a trait object over two very different sinks.
    if let Some(path) = &args.outfile {
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
