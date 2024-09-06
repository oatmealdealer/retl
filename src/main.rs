use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use retl::Config;
use schemars::schema_for;

#[derive(Parser)]
enum Cli {
    /// Load and run the configuration at the given path.
    Run(RunArgs),
    /// Dump the configuration JSON schema to the given path.
    DumpSchema {
        path: PathBuf
    },
}

#[derive(Parser)]
struct RunArgs {
    config: PathBuf,
}
fn main() -> Result<()> {
    match Cli::parse() {
        Cli::Run(args) => Config::from_path(&args.config)?.run(),
        Cli::DumpSchema { path } => {
            let schema = schema_for!(Config);
            let writer = std::fs::File::create(path)?;
            Ok(serde_json::to_writer_pretty(writer, &schema)?)
        }
    }
}
