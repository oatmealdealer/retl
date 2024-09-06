use std::{ops::Deref, path::Path};

use crate::{expressions::Column, ops::OpItem};
use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct ColMap {
    #[serde(
        flatten,
        serialize_with = "tuple_vec_map::serialize",
        deserialize_with = "tuple_vec_map::deserialize"
    )]
    pub(crate) inner: Vec<(Column, Vec<OpItem>)>,
}

impl Deref for ColMap {
    type Target = Vec<(Column, Vec<OpItem>)>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("must define at least one export")]
    NoExports,
    #[error("{0}")]
    Other(String),
}

pub(crate) fn with_current_dir<T, P, F>(path: P, func: F) -> Result<T>
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
