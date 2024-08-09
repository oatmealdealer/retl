pub use anyhow::Result;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("path {0:?} does not exist")]
    BadPath(PathBuf),
}

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(try_from = "PathBuf")]
pub struct RealPathBuf(PathBuf);

impl TryFrom<PathBuf> for RealPathBuf {
    type Error = anyhow::Error;

    fn try_from(value: PathBuf) -> std::result::Result<Self, Self::Error> {
        if value.try_exists()? {
            Ok(Self(value))
        } else {
            Err(Error::BadPath(value).into())
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct FileSource {
    pub path: RealPathBuf,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DataSourceType {
    Csv(FileSource),

    Excel(FileSource),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct DataSource {
    #[serde(flatten)]
    r#type: DataSourceType,
    name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct EtlJob {
    sources: Vec<DataSource>,
}

impl EtlJob {
    pub fn run(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
