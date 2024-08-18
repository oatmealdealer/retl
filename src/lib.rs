extern crate tuple_vec_map;
pub mod exports;
pub mod expressions;
pub mod ops;
pub mod sources;
pub mod transforms;
pub mod types;
pub mod prelude {
    pub use crate::{
        exports::Export,
        expressions::{Column, ToExpr},
        ops::Op,
        sources::Loader,
        transforms::Transform,
    };
    pub use polars::{lazy::prelude::*, prelude::*};
}
pub use anyhow::Result;
use exports::ExportItem;
use ops::OpItem;
pub use prelude::*;

use indexmap::IndexMap;
use schemars::JsonSchema;
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};
use transforms::TransformItem;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Other(String),
    #[error("path {0:?} does not exist")]
    BadPath(PathBuf),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct ColMap {
    #[serde(
        flatten,
        serialize_with = "tuple_vec_map::serialize",
        deserialize_with = "tuple_vec_map::deserialize"
    )]
    inner: Vec<(Column, Vec<OpItem>)>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct Config {
    source: Loader,
    #[serde(default)]
    transforms: Vec<TransformItem>,
    #[serde(default)]
    exports: Vec<ExportItem>,
}

impl Config {
    pub fn load(&self) -> Result<LazyFrame> {
        let mut lf: LazyFrame = self.source.load()?;
        for t in self.transforms.iter() {
            lf = t.transform(lf)?;
        }
        Ok(lf)
    }
    pub fn run(&self) -> Result<()> {
        if self.exports.is_empty() {
            return Err(Error::Other("no exports defined".into()).into());
        }
        let lf = self.load()?;
        for e in self.exports.iter() {
            e.export(lf.clone())?;
        }
        Ok(())
    }
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let canonical_path = path.as_ref().canonicalize()?;
        let file = std::fs::read_to_string(&canonical_path)?;
        with_current_dir(canonical_path.parent().unwrap(), move || {
            toml::from_str(&file).map_err(<_>::into)
        })
    }
}

pub fn with_current_dir<T, P, F>(path: P, func: F) -> Result<T>
where
    P: AsRef<Path>,
    F: Fn() -> Result<T>,
{
    let old_cd = std::env::current_dir()?;
    std::env::set_current_dir(path)?;
    match func() {
        Ok(t) => {
            std::env::set_current_dir(old_cd)?;
            Ok(t)
        }
        Err(e) => {
            std::env::set_current_dir(old_cd)?;
            Err(e)
        }
    }
}
#[cfg(test)]
mod tests;
