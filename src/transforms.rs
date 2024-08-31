use crate::{
    expressions::{Match, ToExpr},
    sources::Loader,
    utils::ColMap,
};
use anyhow::Result;
use polars::{lazy::prelude::*, prelude::*};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};

pub(crate) trait Transform:
    Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug
{
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame>;
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TransformItem {
    Select(Select),
    Rename(Rename),
    Filter(Filter),
    Extract(Extract),
    Unnest(Unnest),
    SortBy(SortBy),
    DropDuplicates(DropDuplicates),
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

/// Select columns with the applied operations. Wraps [`polars::lazy::prelude::LazyFrame::select`]
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct Select(ColMap);

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

/// Rename columns
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Rename {
    /// Rename using a direct mapping of old names to new
    Map(BTreeMap<String, String>),
    // /// Rename all columns using a prefix
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

/// Filter rows using a mapping of columns to operations to apply, which must yield boolean values
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct Filter(ColMap);

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

/// Extract capture groups from a regex into separate columns
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct Extract {
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

/// Apply [`polars::lazy::prelude::LazyFrame::unnest`] to the given struct columns
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub(crate) struct Unnest(Vec<String>);

impl Transform for Unnest {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        Ok(lf.unnest(&self.0))
    }
}

/// Sort a column ascending or descending
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub(crate) struct Sort {
    column: String,
    #[serde(default)]
    descending: bool,
}

/// Sort the data by one or more columns
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub(crate) struct SortBy(Vec<Sort>);

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

/// Which duplicate rows to keep to keep when dropping duplicates from data
#[derive(Deserialize, Serialize, Debug, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DuplicateKeep {
    First,
    Last,
    #[default]
    Any,
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

/// Filter out duplicate rows
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub(crate) struct DropDuplicates {
    /// Columns to check for duplicate values (defaults to all columns)
    subset: Option<Vec<String>>,
    #[serde(default)]
    keep: DuplicateKeep,
}

impl Transform for DropDuplicates {
    fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        Ok(lf.unique(self.subset.clone(), UniqueKeepStrategy::from(&self.keep)))
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Transform data by joining it with data from another source
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
pub(crate) struct Join {
    right: Box<Loader>,
    left_on: String,
    right_on: String,
    how: JoinType,
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
