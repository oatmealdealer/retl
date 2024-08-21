use polars::lazy::prelude::*;
use schemars::JsonSchema;
use std::{fmt::Debug, path::PathBuf};

use crate::Result;

pub trait Export: Debug {
    fn export(&self, lf: LazyFrame) -> anyhow::Result<()>;
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExportItem {
    Csv(CsvExport),
}

impl ExportItem {
    pub fn export(&self, lf: LazyFrame) -> Result<()> {
        match self {
            Self::Csv(export) => export.export(lf),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct CsvExport {
    folder: PathBuf,
    name: String,
}

impl Export for CsvExport {
    fn export(&self, lf: LazyFrame) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.folder)?;
        let mut filename = self.folder.clone();
        filename.push(format!(
            "{}_{}.csv",
            self.name,
            chrono::Local::now().naive_local().format("%Y-%m-%d_%H%M%S")
        ));
        lf.sink_csv(
            &filename,
            CsvWriterOptions {
                ..Default::default()
            },
        )?;
        Ok(())
    }
}
