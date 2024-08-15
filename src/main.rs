use std::path::PathBuf;

use clap::Parser;
use retl::{Config, Result};

#[derive(Parser)]
enum Cli {
    /// Run a given configuration.
    Run(RunArgs),
}

#[derive(Parser)]
struct RunArgs {
    config: PathBuf,
}
fn main() -> Result<()> {
    match Cli::parse() {
        Cli::Run(args) => {
            let file = std::fs::read_to_string(&args.config)?;
            let job: Config = toml::from_str(&file)?;
            if let Some(dir) = args.config.canonicalize()?.parent() {
                std::env::set_current_dir(dir)?;
            }
            job.run()
        }
    }
}
