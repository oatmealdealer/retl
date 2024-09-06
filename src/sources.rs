//! Sources from which data can be loaded and passed to transformations.

use crate::{
    config::Config,
    transforms::{Transform, TransformItem},
};
use anyhow::Result;
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, path::PathBuf, sync::Arc};

/// Trait for a source of data that can be loaded into a [`LazyFrame`].
pub trait Source: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Load the [`LazyFrame`] based on the provided data.
    fn load(&self) -> Result<LazyFrame>;
}

/// Available sources that can be used in configuration files.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceItem {
    /// Load data from CSV.
    Csv(CsvSource),
    /// Load data from another `retl` configuration file.
    Config(ConfigSource),
}

impl Source for SourceItem {
    fn load(&self) -> Result<LazyFrame> {
        match self {
            Self::Csv(source) => source.load(),
            Self::Config(source) => source.load(),
        }
    }
}

/// Load data from a given source and apply optional transformations.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub struct Loader {
    /// The source to load data from.
    #[serde(flatten)]
    pub source: SourceItem,
    /// Which transformations, if any, to apply to the data before returning it.
    #[serde(default)]
    pub transforms: Vec<TransformItem>,
}

impl Loader {
    pub(crate) fn load(&self) -> Result<LazyFrame> {
        let mut lf = self.source.load()?;
        for transform in self.transforms.iter() {
            lf = transform.transform(lf)?;
        }
        Ok(lf)
    }
}

/// A valid ASCII CSV separator, represented internally as a [`u8`].
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(try_from = "char")]
pub struct Separator(u8);

impl TryFrom<char> for Separator {
    type Error = anyhow::Error;

    fn try_from(value: char) -> std::result::Result<Self, Self::Error> {
        Ok(Self(u8::try_from(value)?))
    }
}

/// A polars Schema mapping columns to data types -
/// currently too difficult to provide a schema for, so you're on your own here.
/// Refer to [`polars::prelude::DataType`] and good luck!
#[derive(Serialize, Deserialize, Debug)]
pub struct Schema(polars::prelude::Schema);

impl JsonSchema for Schema {
    fn schema_name() -> String {
        "Schema".to_owned()
    }
    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::Schema::Bool(true)
    }
}

/// Load data from CSV.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct CsvSource {
    /// The path to load files from.
    /// This path is passed directly to [`LazyCsvReader`], so paths with globs are permissible
    /// (e.g. `./files/*.csv`).
    pub path: PathBuf,
    /// Separator to use when parsing.
    pub separator: Option<Separator>,
    /// Whether or not files have headers.
    pub has_header: Option<bool>,
    /// Optional [`polars::prelude::Schema`] to enforce specific datatypes.
    pub schema: Option<Schema>,
}

impl Source for CsvSource {
    fn load(&self) -> Result<LazyFrame> {
        let mut reader = LazyCsvReader::new(&self.path);
        reader = reader.with_has_header(self.has_header.as_ref().unwrap_or(&true).to_owned());
        if self.separator.is_some() {
            reader = reader.with_separator(self.separator.as_ref().unwrap().0)
        }
        reader = reader
            .with_truncate_ragged_lines(true)
            .with_schema(self.schema.as_ref().map(|s| Arc::new(s.0.clone())));
        Ok(reader.finish()?)
    }
}

/// Import another configuration file to be used as a data source.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ConfigSource {
    /// Path to the configuration file.
    pub path: PathBuf,
}

impl Source for ConfigSource {
    fn load(&self) -> Result<LazyFrame> {
        Config::from_path(&self.path)?.load()
    }
}
