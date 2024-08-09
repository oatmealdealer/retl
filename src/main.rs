use std::path::PathBuf;

use clap::Parser;
use retl::{EtlJob, Result};

#[derive(Parser)]
struct Args {
    config: PathBuf,
}
fn main() -> Result<()> {
    let args = Args::parse();
    let file = std::fs::read_to_string(args.config)?;
    let job: EtlJob = toml::from_str(&file)?;
    job.run()
}
