pub use anyhow::Result;
use polars::{lazy::prelude::*, prelude::*};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("path {0:?} does not exist")]
    BadPath(PathBuf),
}

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(try_from = "PathBuf")]
pub struct RealPathBuf(PathBuf);

impl AsRef<std::path::Path> for RealPathBuf {
    fn as_ref(&self) -> &std::path::Path {
        self.0.as_ref()
    }
}

impl TryFrom<PathBuf> for RealPathBuf {
    type Error = anyhow::Error;

    fn try_from(value: PathBuf) -> std::result::Result<Self, Self::Error> {
        if value.try_exists()? {
            Ok(Self(value.canonicalize()?))
        } else {
            Err(Error::BadPath(value).into())
        }
    }
}

#[typetag::serde(tag = "type")]
pub trait DataSource: std::fmt::Debug {
    // fn name(&self) -> &str;
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame>;
}

impl TryFrom<&Box<dyn DataSource>> for LazyFrame {
    type Error = anyhow::Error;
    fn try_from(value: &Box<dyn DataSource>) -> std::result::Result<Self, Self::Error> {
        value.to_lazy_frame()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct CsvSource {
    path: RealPathBuf,
}

#[typetag::serde(name = "csv")]
impl DataSource for CsvSource {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        Ok(LazyCsvReader::new(&self.path)
            .with_has_header(true)
            .finish()?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Join {
    Inner,
    Left,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct JoinSource {
    left: Box<dyn DataSource>,
    left_on: String,
    right: Box<dyn DataSource>,
    right_on: String,
    how: Join,
}

#[typetag::serde(name = "join")]
impl DataSource for JoinSource {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        let lf1 = self.left.to_lazy_frame()?;
        let lf2 = self.right.to_lazy_frame()?;
        Ok(lf1.join(
            lf2,
            [col(&self.left_on)],
            [col(&self.right_on)],
            JoinArgs::new(match self.how {
                Join::Inner => JoinType::Inner,
                Join::Left => JoinType::Left,
            })
            .with_coalesce(JoinCoalesce::CoalesceColumns),
        ))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Exporter {
    folder: PathBuf,
    name: String,
}

impl Exporter {
    pub fn export(&self, lf: LazyFrame) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.folder)?;
        let mut filename = self.folder.clone();
        filename.push(format!(
            "{}_{}.csv",
            self.name,
            chrono::Local::now().naive_local().to_string()
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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct EtlJob {
    source: Box<dyn DataSource>,
    export: Exporter,
}

impl EtlJob {
    pub fn run(&self) -> Result<()> {
        let lf: LazyFrame = (&self.source).try_into()?;
        self.export.export(lf)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
