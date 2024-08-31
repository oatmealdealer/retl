use anyhow::Result;
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, path::PathBuf};

pub(crate) trait Export: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    fn export(&self, lf: LazyFrame) -> Result<()>;
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ExportItem {
    Csv(CsvExport),
}

impl ExportItem {
    pub(crate) fn export(&self, lf: LazyFrame) -> Result<()> {
        match self {
            Self::Csv(export) => export.export(lf),
        }
    }
}

/// Export data to CSV
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct CsvExport {
    /// Folder in which to create files
    folder: PathBuf,
    /// Name of the output file (not including the file extension)
    name: String,
    /// Optional format string to append the current time to the filename -
    /// refer to https://docs.rs/chrono/latest/chrono/format/strftime/index.html for available format codes
    date_format: Option<String>,
}

impl Export for CsvExport {
    fn export(&self, lf: LazyFrame) -> Result<()> {
        std::fs::create_dir_all(&self.folder)?;
        let mut filename = self.folder.clone();
        filename.push(&self.name);
        if let Some(fstring) = &self.date_format {
            filename.push(
                chrono::Local::now()
                    .naive_local()
                    .format(&fstring)
                    .to_string(),
            )
        }
        filename.push(".csv");
        lf.sink_csv(
            &filename,
            CsvWriterOptions {
                ..Default::default()
            },
        )?;
        Ok(())
    }
}
