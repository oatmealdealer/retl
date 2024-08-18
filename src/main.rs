use std::path::PathBuf;

use clap::Parser;
use retl::{Config, Result};
use schemars::schema_for;

#[derive(Parser)]
enum Cli {
    /// Run a given configuration.
    Run(RunArgs),
    DumpSchema,
}

#[derive(Parser)]
struct RunArgs {
    config: PathBuf,
}
fn main() -> Result<()> {
    match Cli::parse() {
        Cli::Run(args) => Config::from_path(&args.config)?.run(),
        Cli::DumpSchema => {
            let schema = schema_for!(Config);
            let writer = std::fs::File::create("schema.json")?;
            Ok(serde_json::to_writer_pretty(writer, &schema)?)
        }
    }
}
