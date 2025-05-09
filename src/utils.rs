//! General utility types and functions.
use anyhow::{Context as _, Result};
use glob::glob;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Errors that can be encountered during configuration parsing.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// Returned when attempting to run a configuration does not contain any exports.
    #[error("must define at least one export")]
    NoExports,
    /// Other unspecified error encountered during parsing.
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

/// One or more paths that are canonicalized (see [`std::fs::canonicalize`]) and guaranteed to exist.
#[derive(Clone, Serialize, Deserialize, Debug)]
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

impl JsonSchema for CanonicalPaths {
    fn schema_name() -> String {
        PathBuf::schema_name()
    }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        PathBuf::json_schema(gen)
    }
}

impl Deref for CanonicalPaths {
    type Target = Arc<Vec<PathBuf>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A single path that is canonicalized (see [`std::fs::canonicalize`]) and guaranteed to exist.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(try_from = "PathBuf")]
pub struct CanonicalPath(PathBuf);

impl JsonSchema for CanonicalPath {
    fn schema_name() -> String {
        PathBuf::schema_name()
    }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        PathBuf::json_schema(gen)
    }
}

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

/// Wraps [`polars::prelude::DataType`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DataType(polars::prelude::DataType);

impl JsonSchema for DataType {
    fn schema_name() -> String {
        "DataType".to_owned()
    }
    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::Schema::Bool(true)
    }
}

impl Deref for DataType {
    type Target = polars::prelude::DataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
