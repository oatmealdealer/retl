use crate::{
    exports::ExportItem,
    sources::Loader,
    transforms::{Transform, TransformItem},
    utils::{with_current_dir, Error},
};
use anyhow::Result;
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, path::Path};

/// Configuration to load data, apply transformations, and export to one or multiple destinations.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Config {
    /// The top-level source that data should be loaded from.
    pub source: Loader,
    /// Transformations to apply to the data loaded from the source.
    #[serde(default)]
    pub transforms: Vec<TransformItem>,
    /// Export destinations for the transformed data.
    #[serde(default)]
    pub exports: Vec<ExportItem>,
}

impl Config {
    /// Load the end result without exporting.
    pub fn load(&self) -> Result<LazyFrame> {
        let mut lf: LazyFrame = self.source.load()?;
        for t in self.transforms.iter() {
            lf = t.transform(lf)?;
        }
        Ok(lf)
    }
    /// Run the configuration, exporting the transformed data.
    pub fn run(&self) -> Result<()> {
        if self.exports.is_empty() {
            return Err(Error::NoExports.into());
        }
        let lf = self.load()?;
        for e in self.exports.iter() {
            e.export(lf.clone())?;
        }
        Ok(())
    }
    /// Load a configuration from the given path.
    pub fn from_path<P, F, R>(path: P, func: F) -> Result<R>
    where
        P: AsRef<Path>,
        F: Fn(Self) -> Result<R>,
    {
        let canonical_path = path.as_ref().canonicalize()?;
        let file = std::fs::read_to_string(&canonical_path)?;
        with_current_dir(
            canonical_path
                .parent()
                .expect("path cannot be filesystem root"),
            move || {
                let config: Self = toml::from_str(&file)?;
                func(config)
            },
        )
    }
}
