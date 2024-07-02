use std::{collections::HashMap, path::PathBuf, sync::Arc};

use clap::Parser;
use log::{error, info, trace, warn};
use tokio::{sync::Semaphore, task::JoinSet};

/// Pulls all NYSE symbols and logos and dumps them to the
/// given directory.
#[derive(Parser)]
struct Opts {
    /// Turns on verbose logging
    #[clap(short = 'v', long)]
    verbose: bool,
    /// Output directory
    #[clap(short = 'o', long, default_value = ".")]
    output: String,
    /// Force-fetch existing logos
    #[clap(short = 'f', long)]
    force: bool,
    /// Maximum number of concurrent logo fetches
    /// (note that setting this too high may result in
    /// rate limiting)
    #[clap(short = 'j', long, default_value = "8")]
    jobs: usize,
}

async fn pmain() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::parse();

    colog::basic_builder()
        .filter_level(if opts.verbose {
            log::LevelFilter::Trace
        } else {
            log::LevelFilter::Info
        })
        .init();

    info!("fetching latest stock symbol list from NYSE");

    let client = reqwest::Client::new();
    let res = client.get("https://www.nyse.com/publicdocs/nyse/markets/nyse/NYSE_and_NYSE_MKT_Trading_Units_Daily_File.xls").send().await?;

    trace!("response: {:?}", res.status());

    let nyse_content = res.text().await?;

    trace!("response size: {} bytes", nyse_content.as_bytes().len());
    trace!("parsing as TSV...");

    let tsv = Tsv::from_str(&nyse_content)?;

    trace!("parsed {} rows", tsv.rows.len());

    let toml_path = PathBuf::from(&opts.output).join("symbols.toml");
    info!("writing symbols to TOML file at '{}'", toml_path.display());
    let mut toml_data = HashMap::new();
    toml_data.insert("symbol".to_string(), &tsv.rows);
    let toml_str = toml::to_string_pretty(&toml_data)?;
    tokio::fs::write(&toml_path, toml_str).await?;
    drop(toml_data);
    trace!("wrote TOML file");

    let symbol = tsv
        .find_header_index_case_insensitive("symbol")
        .ok_or("NYSE data is missing 'symbol' column")?;

    info!("fetching logos...");

    let mut join_set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(opts.jobs));

    for row in tsv.rows {
        let symbol = row.get(&tsv.headers[symbol]).ok_or("missing symbol")?;
        let symbol = symbol.trim().to_uppercase();

        // is the symbol ENTIRELY alphanumeric?
        if !symbol.chars().all(|c| c.is_alphanumeric()) {
            warn!("skipping non-alphanumeric symbol '{}'", symbol);
            continue;
        }

        let logo_path = PathBuf::from(&opts.output).join(format!("{symbol}.svg"));

        if !opts.force && logo_path.exists() {
            trace!("skipping existing logo for '{symbol}'");
            continue;
        }

        let logo_url = format!(
            "https://logos.stockanalysis.com/{}.svg",
            symbol.to_lowercase()
        );

        let client = client.clone();
        let semaphore = semaphore.clone();

        join_set.spawn(async move {
            let _permit = semaphore.acquire().await;

            trace!("fetching {symbol} logo from '{logo_url}'");

            let res = client.get(&logo_url).send().await;
            let res = match res {
                Ok(res) => res,
                Err(e) => {
                    warn!("failed to fetch logo for '{symbol}' (from '{logo_url}'): {e:?}");
                    return;
                }
            };

            trace!("response: {:?}", res.status());
            if res.status().is_success() {
                let logo_content = match res.text().await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("failed to fetch logo for '{symbol}' (from '{logo_url}'): {e:?}");
                        return;
                    }
                };
                trace!("response size: {} bytes", logo_content.as_bytes().len());
                if let Err(e) = tokio::fs::write(&logo_path, logo_content).await {
                    warn!(
                        "failed to write logo for '{symbol}' to '{}': {e:?}",
                        logo_path.display()
                    );
                    return;
                }
                trace!("wrote logo to '{}'", logo_path.display());
            } else {
                warn!(
                    "failed to fetch logo for '{symbol}' (from '{logo_url}'): {}",
                    res.status(),
                );
            }
        });
    }

    info!(
        "fetching {} logos (jobs = {})...",
        join_set.len(),
        opts.jobs
    );

    while join_set.join_next().await.is_some() {}

    info!("done");

    Ok(())
}

#[derive(Debug)]
struct Tsv {
    headers: Vec<String>,
    rows: Vec<HashMap<String, String>>,
}

impl Tsv {
    fn from_str(s: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut lines = s.lines();
        let headers = lines
            .next()
            .ok_or("missing headers")?
            .split('\t')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for line in lines {
            let row = line
                .split('\t')
                .map(|s| s.trim().to_string())
                .enumerate()
                .map(|(i, v)| (headers[i].clone(), v))
                .collect();
            rows.push(row);
        }
        Ok(Self { headers, rows })
    }

    fn find_header_index_case_insensitive(&self, name: &str) -> Option<usize> {
        let name = name.to_lowercase();
        self.headers.iter().position(|h| h.to_lowercase() == name)
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = pmain().await {
        error!("fatal error: {}", e);
        std::process::exit(1);
    }
}
