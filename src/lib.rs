pub mod conditions;
pub mod exports;
pub mod sources;
pub mod transforms;

pub use crate::{conditions::Condition, exports::Export, sources::Loader, transforms::Transform};
pub use anyhow::Result;

use polars::lazy::prelude::*;
use std::fmt::Debug;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Other(String),
}

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
