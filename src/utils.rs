use anyhow::{Context as _, Result};
use glob::glob;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

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

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(try_from = "PathBuf")]
pub struct CanonicalPaths(Arc<Vec<PathBuf>>);

impl TryFrom<PathBuf> for CanonicalPaths {
    type Error = anyhow::Error;
    fn try_from(value: PathBuf) -> std::result::Result<Self, Self::Error> {
        Ok(Self(Arc::new(
            glob(value.to_str().context("paths must be valid unicode")?)?
                .into_iter()
                .map(|res| res.map(|p| p.canonicalize()))
                .collect::<Result<Result<Vec<_>, _>, _>>()??,
        )))
    }
}

impl Deref for CanonicalPaths {
    type Target = Arc<Vec<PathBuf>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
#[serde(try_from = "PathBuf")]
pub struct CanonicalPath(PathBuf);

impl TryFrom<PathBuf> for CanonicalPath {
    type Error = anyhow::Error;
    fn try_from(value: PathBuf) -> std::result::Result<Self, Self::Error> {
        Ok(Self(value.canonicalize()?))
    }
}

impl Deref for CanonicalPath {
    type Target = PathBuf;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for CanonicalPath {
    fn as_ref(&self) -> &Path {
        self.0.as_path()
    }
}
