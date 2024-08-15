use polars::lazy::prelude::*;
use std::fmt::Debug;
use std::path::PathBuf;

#[typetag::serde(tag = "type")]
pub trait Export: Debug {
    fn export(&self, lf: LazyFrame) -> anyhow::Result<()>;
}
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct CsvExport {
    folder: PathBuf,
    name: String,
}

#[typetag::serde(name = "csv")]
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
