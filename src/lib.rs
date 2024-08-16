pub mod exports;
pub mod expressions;
pub mod ops;
pub mod sources;
pub mod transforms;
pub mod prelude {
    pub use crate::{
        exports::Export,
        expressions::{Column, ToExpr},
        ops::{Op, Ops},
        sources::Loader,
        transforms::Transform,
    };
    pub use polars::{lazy::prelude::*, prelude::*};
}
pub use anyhow::Result;
pub use prelude::*;

use indexmap::IndexMap;
use std::fmt::Debug;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Other(String),
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ColMap(IndexMap<Column, Ops>);

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Config {
    source: Loader,
    #[serde(default)]
    transforms: Vec<Box<dyn Transform>>,
    exports: Vec<Box<dyn Export>>,
}

impl Config {
    pub fn run(&self) -> Result<()> {
        let mut lf: LazyFrame = self.source.load()?;
        for t in self.transforms.iter() {
            lf = t.transform(lf)?;
        }
        for e in self.exports.iter() {
            e.export(lf.clone())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
