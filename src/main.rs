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
        Cli::Run(args) => Config::from_path(&args.config)?.run(),
    }
}
