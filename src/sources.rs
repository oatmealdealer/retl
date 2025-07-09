//! Sources from which data can be loaded and passed to transformations.

use crate::{
    config::Config,
    transforms::{Transform, TransformItem},
    utils::{CanonicalPath, CanonicalPaths},
};
use anyhow::Result;
use polars::{
    frame::DataFrame,
    io::SerReader,
    lazy::prelude::*,
    prelude::{JsonReader, PlPath},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

/// Trait for a source of data that can be loaded into a [`LazyFrame`].
pub trait Source: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Load the [`LazyFrame`] based on the provided data.
    fn load(&self) -> Result<LazyFrame>;
}

/// Available sources that can be used in configuration files.
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceItem {
    /// Load data from CSV.
    Csv(CsvSource),
    /// Load data from newline-delimited JSON files.
    JsonLine(JsonLineSource),
    /// Load data from a JSON file.
    Json(JsonSource),
    /// Load data from another `retl` configuration file.
    Config(ConfigSource),
    /// Load data from a .parquet file.
    Parquet(ParquetSource),
    /// Experimental source for inlining a dataframe, used for mapping columns from one set of values to another via joins.
    /// Example:
    /// ```toml
    ///     [source.inline]
    ///
    ///     [[source.inline.columns]]
    ///     name     = "title"
    ///     datatype = "String"
    ///     values   = ["Foo", "Bar", "Baz"]
    ///
    ///     [[source.inline.columns]]
    ///     name     = "number"
    ///     datatype = "UInt64"
    ///     values   = [1, 2, 3]
    /// ```
    Inline(InlineSource),
}

impl Source for SourceItem {
    fn load(&self) -> Result<LazyFrame> {
        match self {
            Self::Csv(source) => source.load(),
            Self::JsonLine(source) => source.load(),
            Self::Json(source) => source.load(),
            Self::Config(source) => source.load(),
            Self::Parquet(source) => source.load(),
            Self::Inline(source) => source.load(),
        }
    }
}

/// Load data from a given source and apply optional transformations.
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema)]
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
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema)]
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
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Schema(pub polars::prelude::Schema);

impl JsonSchema for Schema {
    fn schema_name() -> String {
        "Schema".to_owned()
    }
    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::Schema::Bool(true)
    }
}

/// Load data from CSV.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct CsvSource {
    /// The path to load files from.
    /// This path is passed directly to [`LazyCsvReader`], so paths with globs are permissible
    /// (e.g. `./files/*.csv`).
    pub path: CanonicalPaths,
    /// Separator to use when parsing.
    pub separator: Option<Separator>,
    /// Whether or not files have headers.
    pub has_header: Option<bool>,
    /// Optional [`polars::prelude::Schema`] to enforce specific datatypes.
    pub schema: Option<Schema>,
}

impl Source for CsvSource {
    fn load(&self) -> Result<LazyFrame> {
        let paths: Arc<[PlPath]> = self
            .path
            .iter()
            .map(|path_buf| PlPath::Local(path_buf.clone().into()))
            .collect::<Vec<PlPath>>()
            .into();
        let mut reader = LazyCsvReader::new_paths(paths);
        reader = reader.with_has_header(self.has_header.as_ref().unwrap_or(&true).to_owned());
        if self.separator.is_some() {
            reader = reader.with_separator(self.separator.as_ref().unwrap().0)
        }
        reader = reader
            .with_truncate_ragged_lines(true)
            .with_dtype_overwrite(self.schema.as_ref().map(|s| Arc::new(s.0.clone())));
        Ok(reader.finish()?)
    }
}

/// Load data from newline-delimited JSON files.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct JsonLineSource {
    /// The path to load files from.
    /// This path is passed directly to [`LazyJsonLineReader`], so paths with globs are permissible
    /// (e.g. `./files/*.csv`).
    pub path: CanonicalPaths,
    /// Optional [`polars::prelude::Schema`] to enforce specific datatypes.
    pub schema: Option<Schema>,
}

impl Source for JsonLineSource {
    fn load(&self) -> Result<LazyFrame> {
        let paths: Arc<[PlPath]> = self
            .path
            .iter()
            .map(|path_buf| PlPath::Local(path_buf.clone().into()))
            .collect::<Vec<PlPath>>()
            .into();
        let mut reader = LazyJsonLineReader::new_paths(paths);
        reader = reader.with_schema_overwrite(self.schema.as_ref().map(|s| Arc::new(s.0.clone())));
        Ok(reader.finish()?)
    }
}

/// Load data from a JSON file.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct JsonSource {
    /// The path to load files from.
    /// This path is passed directly to [`LazyJsonLineReader`], so paths with globs are permissible
    /// (e.g. `./files/*.csv`).
    pub path: CanonicalPath,
    /// Optional [`polars::prelude::Schema`] to enforce specific datatypes.
    pub schema: Option<Schema>,
}

impl Source for JsonSource {
    fn load(&self) -> Result<LazyFrame> {
        let file = std::fs::File::open(&self.path)?;
        let mut df = JsonReader::new(file);
        if let Some(schema) = self.schema.as_ref().map(|s| Arc::new(s.0.clone())) {
            df = df.with_schema(schema);
        }
        Ok(df.finish()?.lazy())
    }
}

/// Import another configuration file to be used as a data source.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct ConfigSource {
    /// Path to the configuration file.
    pub path: CanonicalPath,
}

impl Source for ConfigSource {
    fn load(&self) -> Result<LazyFrame> {
        Config::from_path(&self.path, |config| config.load())
    }
}

/// Import another configuration file to be used as a data source.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct ParquetSource {
    /// Path to the configuration file.
    pub paths: Arc<[PlPath]>,
    /// Optional [`polars::prelude::Schema`] to enforce specific datatypes.
    pub schema: Option<Schema>,
}

impl Source for ParquetSource {
    fn load(&self) -> Result<LazyFrame> {
        Ok(LazyFrame::scan_parquet_files(
            self.paths.clone(),
            ScanArgsParquet {
                schema: self
                    .schema
                    .as_ref()
                    .map(|schema| Arc::new(schema.0.clone())),

                ..Default::default()
            },
        )?)
    }
}

/// Experimental source for inlining a dataframe, used for mapping columns from one set of values to another via joins.
/// Example:
/// ```toml
///     [source.inline]
///
///     [[source.inline.columns]]
///     name     = "title"
///     datatype = "String"
///     values   = ["Foo", "Bar", "Baz"]
///
///     [[source.inline.columns]]
///     name     = "number"
///     datatype = "UInt64"
///     values   = [1, 2, 3]
/// ```
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct InlineSource(DataFrame);

impl Source for InlineSource {
    fn load(&self) -> Result<LazyFrame> {
        Ok(self.0.clone().lazy())
    }
}

impl JsonSchema for InlineSource {
    fn schema_name() -> String {
        "InlineSource".to_owned()
    }
    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::Schema::Bool(true)
    }
}
