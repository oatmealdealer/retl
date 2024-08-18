use schemars::JsonSchema;

use crate::{expressions::Match, prelude::*, ColMap, Result, ToExpr};
use std::{collections::BTreeMap, fmt::Debug};

pub trait Transform: Debug {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame>;
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransformItem {
    Select(Select),
    Rename(Rename),
    Filter(Filter),
    Extract(Extract),
    Unnest(Unnest),
    SortBy(SortBy),
    DropDuplicates(DropDuplicates),
}

impl TransformItem {
    pub fn transform(&self, lf: LazyFrame) -> Result<LazyFrame> {
        match self {
            Self::Select(transform) => transform.transform(lf),
            Self::Rename(transform) => transform.transform(lf),
            Self::Filter(transform) => transform.transform(lf),
            Self::Extract(transform) => transform.transform(lf),
            Self::Unnest(transform) => transform.transform(lf),
            Self::SortBy(transform) => transform.transform(lf),
            Self::DropDuplicates(transform) => transform.transform(lf),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct Select(ColMap);

impl Transform for Select {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.select(
            self.0
                .inner
                .iter()
                .map(|(k, v)| v.iter().try_fold(k.expr()?, |expr, op| op.expr(expr)))
                .collect::<Result<Vec<Expr>, _>>()?
                .as_slice(),
        ))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Rename {
    Map(BTreeMap<String, String>),
    Prefix(String),
}

impl Transform for Rename {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        match self {
            Self::Map(columns) => Ok(lf.rename(columns.keys(), columns.values())),
            Self::Prefix(_prefix) => {
                let prefix = _prefix.clone();
                Ok(lf.select([all()
                    .name()
                    .map(move |col| Ok(format!("{}{}", prefix, col)))]))
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
struct Filter(ColMap);

impl Transform for Filter {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(self
            .0
            .inner
            .iter()
            .try_fold(lf, |lf, (k, v)| -> anyhow::Result<LazyFrame> {
                anyhow::Result::Ok(
                    lf.filter(v.iter().try_fold(k.expr()?, |expr, op| op.expr(expr))?),
                )
            })?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
struct Extract {
    #[serde(flatten)]
    matcher: Match,
    #[serde(default)]
    filter: bool,
}

impl Transform for Extract {
    fn transform(&self, mut lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        if self.filter {
            lf = lf.filter(self.matcher.expr()?);
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

#[derive(serde::Deserialize, serde::Serialize, Debug, JsonSchema)]
struct Unnest(Vec<String>);

impl Transform for Unnest {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.unnest(&self.0))
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, JsonSchema)]
pub struct Sort {
    column: String,
    #[serde(default)]
    descending: bool,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, JsonSchema)]
pub struct SortBy(Vec<Sort>);

impl Transform for SortBy {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
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

#[derive(serde::Deserialize, serde::Serialize, Debug, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateKeep {
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

#[derive(serde::Deserialize, serde::Serialize, Debug, JsonSchema)]
pub struct DropDuplicates {
    subset: Option<Vec<String>>,
    #[serde(default)]
    keep: DuplicateKeep,
}

impl Transform for DropDuplicates {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.unique(self.subset.clone(), UniqueKeepStrategy::from(&self.keep)))
    }
}
