use crate::{
    config::Config,
    transforms::{Transform, TransformItem},
};
use anyhow::Result;
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, path::PathBuf, sync::Arc};

pub(crate) trait Source: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    fn load(&self) -> Result<LazyFrame>;
}

#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceItem {
    Csv(CsvSource),
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

/// Load data from a source and apply optional transformations
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub(crate) struct Loader {
    #[serde(flatten)]
    source: SourceItem,
    #[serde(default)]
    transforms: Vec<TransformItem>,
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

/// Any valid ASCII CSV separator
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(try_from = "char")]
pub(crate) struct Separator(u8);

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
pub(crate) struct Schema(polars::prelude::Schema);

impl JsonSchema for Schema {
    fn schema_name() -> String {
        "Schema".to_owned()
    }
    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::Schema::Bool(true)
    }
}

/// Load data from CSV
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct CsvSource {
    /// Path to load files from, globs permitted
    path: PathBuf,
    /// Separator to use when parsing
    separator: Option<Separator>,
    /// Whether or not files have headers
    has_header: Option<bool>,
    /// Optional [`polars::prelude::Schema`]
    schema: Option<Schema>,
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

/// Import another configuration as a data source
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct ConfigSource {
    path: PathBuf,
}

impl Source for ConfigSource {
    fn load(&self) -> Result<LazyFrame> {
        Config::from_path(&self.path)?.load()
    }
}
