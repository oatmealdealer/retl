//! Available methods for exporting data.

use anyhow::Result;
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Write},
    path::PathBuf,
};

/// Trait for a data structure that represents a data export destination.
pub trait Export: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Export the supplied data to the specified destination.
    fn export(&self, lf: LazyFrame) -> Result<()>;
}

/// Available exports that can be used in configuration files.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExportItem {
    /// Export data to CSV.
    Csv(CsvExport),
}

impl ExportItem {
    pub(crate) fn export(&self, lf: LazyFrame) -> Result<()> {
        match self {
            Self::Csv(export) => export.export(lf),
        }
    }
}

/// Export data to CSV.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct CsvExport {
    /// Folder in which to create files.
    pub folder: PathBuf,
    /// Name of the output file, not including the file extension.
    pub name: String,
    /// Optional format string to append the current time to the filename -
    /// refer to <https://docs.rs/chrono/latest/chrono/format/strftime/index.html> for available format codes.
    pub date_format: Option<String>,
}

impl Export for CsvExport {
    fn export(&self, lf: LazyFrame) -> Result<()> {
        std::fs::create_dir_all(&self.folder)?;
        let mut filename = String::new();
        filename.write_str(&self.name)?;
        if let Some(fstring) = &self.date_format {
            filename.write_str(
                &chrono::Local::now()
                    .naive_local()
                    .format(&fstring)
                    .to_string(),
            )?
        }
        filename.write_str(".csv")?;
        lf.sink_csv(
            self.folder.join(filename),
            CsvWriterOptions {
                ..Default::default()
            },
        )?;
        Ok(())
    }
}
