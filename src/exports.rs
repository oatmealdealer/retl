//! Available methods for exporting data.

use anyhow::Result;
use polars::{io::SerWriter, lazy::prelude::*, prelude::CsvWriter};
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
    /// Export data to newline-delimited JSON.
    NdJson(NdJsonExport),
    /// Collect and serialize the dataframe itself to a single JSON object. You probably don't need this.
    Json(JsonExport),
}

impl ExportItem {
    pub(crate) fn export(&self, lf: LazyFrame) -> Result<()> {
        match self {
            Self::Csv(export) => export.export(lf),
            Self::NdJson(export) => export.export(lf),
            Self::Json(export) => export.export(lf),
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
    /// Whether to lazily sink data to the CSV. Defaults to true. Set to false if necessary to resolve errors.
    /// If set to false, all data will be loaded into memory as a [`polars::prelude::DataFrame`] before being
    /// written to disk.
    pub sink: Option<bool>,
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
        if self.sink.unwrap_or(true) {
            lf.sink_csv(
                self.folder.join(filename),
                CsvWriterOptions {
                    ..Default::default()
                },
                None,
            )?;
        } else {
            let mut file = std::fs::File::create(self.folder.join(filename))?;
            CsvWriter::new(&mut file)
                .include_header(true)
                .with_separator(b',')
                .finish(&mut lf.collect()?)?;
        }
        Ok(())
    }
}

/// Export data to newline-delimited JSON.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct NdJsonExport {
    /// Folder in which to create files.
    pub folder: PathBuf,
    /// Name of the output file, not including the file extension.
    pub name: String,
    /// Optional format string to append the current time to the filename -
    /// refer to <https://docs.rs/chrono/latest/chrono/format/strftime/index.html> for available format codes.
    pub date_format: Option<String>,
}

impl Export for NdJsonExport {
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
        filename.write_str(".jsonl")?;
        lf.sink_json(
            self.folder.join(filename),
            JsonWriterOptions::default(),
            None,
        )?;
        Ok(())
    }
}

/// Export data to CSV.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct JsonExport {
    /// Folder in which to create files.
    pub folder: PathBuf,
    /// Name of the output file, not including the file extension.
    pub name: String,
    /// Optional format string to append the current time to the filename -
    /// refer to <https://docs.rs/chrono/latest/chrono/format/strftime/index.html> for available format codes.
    pub date_format: Option<String>,
}

impl Export for JsonExport {
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
        filename.write_str(".json")?;
        let file = std::fs::File::create(self.folder.join(filename))?;
        let df = lf.collect()?;
        serde_json::to_writer(file, &df)?;
        Ok(())
    }
}
