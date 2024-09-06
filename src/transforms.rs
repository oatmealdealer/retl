//! Transformations that modify a [`LazyFrame`] and pass it on to other transformations, or to be exported.

use crate::{
    expressions::{Expression, Match},
    sources::Loader,
    utils::ColMap,
};
use anyhow::Result;
use polars::{lazy::prelude::*, prelude::*};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};

/// Trait for transformations that take a [`LazyFrame`] as input and modify it.
pub trait Transform: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Transform a [`LazyFrame`] according to the provided data.
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame>;
}

/// Available transformations that can be used in configuration files.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransformItem {
    /// Select columns (equivalent to [`LazyFrame::select`])
    Select(Select),
    /// Rename columns (equivalent to [`LazyFrame::rename`])
    Rename(Rename),
    /// Filter columns (equivalent to [`LazyFrame::filter`])
    Filter(Filter),
    /// Extract capture groups of a regex into separate columns.
    Extract(Extract),
    /// Apply [`LazyFrame::unnest`] to the given struct columns.
    Unnest(Unnest),
    /// Sort the data by one or more columns.
    SortBy(SortBy),
    /// Drop duplicate rows from the dataset.
    DropDuplicates(DropDuplicates),
    /// Join the dataset with another dataset.
    Join(Join),
}

impl Transform for TransformItem {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        match self {
            Self::Select(transform) => transform.transform(lf),
            Self::Rename(transform) => transform.transform(lf),
            Self::Filter(transform) => transform.transform(lf),
            Self::Extract(transform) => transform.transform(lf),
            Self::Unnest(transform) => transform.transform(lf),
            Self::SortBy(transform) => transform.transform(lf),
            Self::DropDuplicates(transform) => transform.transform(lf),
            Self::Join(transform) => transform.transform(lf),
        }
    }
}

/// Select columns with the applied operations. Wraps [`polars::lazy::prelude::LazyFrame::select`].
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Select(ColMap);

impl Transform for Select {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        Ok(lf.select(
            self.0
                .iter()
                .map(|(k, v)| v.iter().try_fold(k.to_expr()?, |expr, op| op.expr(expr)))
                .collect::<Result<Vec<Expr>, _>>()?
                .as_slice(),
        ))
    }
}

/// Rename columns.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Rename {
    /// Rename using a direct mapping of old names to new.
    Map(BTreeMap<String, String>),
    // /// Rename all columns using a prefix.
    // Prefix(String),
}

impl Transform for Rename {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        match self {
            Self::Map(columns) => Ok(lf.rename(columns.keys(), columns.values())),
            // TODO: Fix successive uses of this not stacking properly
            // Self::Prefix(_prefix) => {
            //     let prefix = _prefix.clone();
            //     Ok(lf.select([all()
            //         .name()
            //         .map(move |col| Ok(format!("{}{}", prefix, col)))]))
            // }
        }
    }
}

/// Filter rows using a mapping of columns to operations to apply, which must yield boolean values.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Filter(ColMap);

impl Transform for Filter {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        Ok(self
            .0
            .iter()
            .try_fold(lf, |lf, (k, v)| -> Result<LazyFrame> {
                Ok(lf.filter(v.iter().try_fold(k.to_expr()?, |expr, op| op.expr(expr))?))
            })?)
    }
}

/// Extract capture groups from a regex into separate columns.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Extract {
    #[serde(flatten)]
    matcher: Match,
    #[serde(default)]
    filter: bool,
}

impl Transform for Extract {
    fn transform(&self, mut lf: LazyFrame) -> Result<LazyFrame> {
        if self.filter {
            lf = lf.filter(self.matcher.to_expr()?);
        }

        // TODO: See if this can be done without an intermediate alias
        let alias = format!("_{}_groups", &self.matcher.column);
        lf = lf
            .select([
                col("*"),
                col(&self.matcher.column)
                    .str()
                    .extract_groups(self.matcher.pattern.as_str())?
                    .alias(alias.as_str()),
            ])
            .unnest(vec![alias]);

        Ok(lf)
    }
}

/// Apply [`polars::lazy::prelude::LazyFrame::unnest`] to the given struct columns.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub struct Unnest(Vec<String>);

impl Transform for Unnest {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        Ok(lf.unnest(&self.0))
    }
}

/// Sort a column ascending or descending.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub struct Sort {
    column: String,
    #[serde(default)]
    descending: bool,
}

/// Sort the data by one or more columns.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub struct SortBy(Vec<Sort>);

impl Transform for SortBy {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        Ok(lf.sort_by_exprs(
            self.0
                .iter()
                .map(|s| col(s.column.as_str()))
                .collect::<Vec<Expr>>(),
            SortMultipleOptions::default()
                .with_order_descending_multi(self.0.iter().map(|s| s.descending)),
        ))
    }
}

/// Which duplicate rows to keep to keep when dropping duplicates from data.
#[derive(Deserialize, Serialize, Debug, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateKeep {
    /// Keep the first duplicate record.
    First,
    /// Keep the last duplicate record.
    Last,
    /// Keep any duplicate row. This allows for more optimization but makes no guarantees about which row will be kept.
    #[default]
    Any,
    /// Do not keep any duplicate rows.
    None,
}

impl From<&DuplicateKeep> for UniqueKeepStrategy {
    fn from(value: &DuplicateKeep) -> Self {
        match value {
            DuplicateKeep::Any => Self::Any,
            DuplicateKeep::First => Self::First,
            DuplicateKeep::Last => Self::Last,
            DuplicateKeep::None => Self::None,
        }
    }
}

/// Filter out duplicate rows.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub struct DropDuplicates {
    /// Columns to check for duplicate values (defaults to all columns).
    pub subset: Option<Vec<String>>,
    /// Which duplicate record (if any) to keep.
    #[serde(default)]
    pub keep: DuplicateKeep,
}

impl Transform for DropDuplicates {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        Ok(lf.unique(self.subset.clone(), UniqueKeepStrategy::from(&self.keep)))
    }
}

/// The method by which to join datasets. Maps to [`polars::prelude::JoinType`].
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    /// Inner join - keep only rows that match on both sides.
    Inner,
    /// Left join - keep all rows from the left dataset.
    Left,
    /// Right join - keep all rows from the right dataset.
    Right,
    /// Full join - keep all rows from both datasets.
    Full,
}

/// Transform data by joining it with data from another source.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub struct Join {
    /// The right-hand dataset to join the input with.
    pub right: Box<Loader>,
    /// The column in the left-hand dataset to join on.
    pub left_on: String,
    /// The column in the right-hand dataset to join on.
    pub right_on: String,
    /// Join method to use.
    pub how: JoinType,
}

impl Transform for Join {
    fn transform(&self, lf1: LazyFrame) -> Result<LazyFrame> {
        let lf2 = self.right.load()?;
        Ok(lf1.join(
            lf2,
            [col(&self.left_on)],
            [col(&self.right_on)],
            JoinArgs::new(match self.how {
                JoinType::Inner => polars::prelude::JoinType::Inner,
                JoinType::Left => polars::prelude::JoinType::Left,
                JoinType::Right => polars::prelude::JoinType::Right,
                JoinType::Full => polars::prelude::JoinType::Full,
            })
            .with_coalesce(JoinCoalesce::CoalesceColumns),
        ))
    }
}
