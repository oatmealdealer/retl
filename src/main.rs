//! CLI to run commands relating to `retl` configuration files.
//! The `dump-schema` subcommand can be used to dump a JSON schema to disk using [`schemars`].

use std::{io::Write, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use retl::{
    sources::{Schema, SourceItem},
    Config,
};
use schemars::schema_for;
use tracing::debug;

#[derive(Parser)]
enum Cli {
    /// Load and run the configuration at the given path.
    Run(RunArgs),
    /// Dump the configuration JSON schema to the given path.
    DumpSchema {
        /// Path to dump the JSON schema to.
        path: PathBuf,
    },
}

#[derive(Parser)]
struct RunArgs {
    /// Path to the configuration file to run.
    config: PathBuf,
    #[arg(long)]
    dump_schema: Option<PathBuf>,
}
fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    match Cli::parse() {
        Cli::Run(args) => Config::from_path(&args.config.canonicalize()?, |config| {
            debug!("Running parsed config: {:?}", config);
            if let Some(path) = &args.dump_schema {
                let schema = config.load()?.collect_schema()?.as_ref().clone();
                let mut writer = std::fs::File::create(path)?;
                let mut source = config.source.clone();
                match &mut source.source {
                    SourceItem::Csv(source) => {
                        source.schema = Some(Schema(schema));
                    }
                    SourceItem::Json(source) => {
                        source.schema = Some(Schema(schema));
                    }
                    SourceItem::JsonLine(source) => {
                        source.schema = Some(Schema(schema));
                    }
                    SourceItem::Parquet(source) => {
                        source.schema = Some(Schema(schema));
                    }
                    _ => {
                        writer.write(toml::to_string_pretty(&schema)?.as_bytes())?;
                        writer.flush()?;
                        return Ok(());
                    }
                }
                writer.write(
                    toml::to_string_pretty(&Config {
                        source,
                        exports: Default::default(),
                        transforms: Default::default(),
                    })?
                    .as_bytes(),
                )?;
                writer.flush()?;
                Ok(())
            } else {
                config.run()
            }
        }),
        Cli::DumpSchema { path } => {
            let schema = schema_for!(Config);
            let writer = std::fs::File::create(path)?;
            Ok(serde_json::to_writer_pretty(writer, &schema)?)
        }
    }
}
