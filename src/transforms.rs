
use polars::{lazy::prelude::*, prelude::*};
use std::{collections::BTreeMap, fmt::Debug};

use crate::{conditions::Match, Condition};

#[typetag::serde]
pub trait Transform: Debug {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame>;
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Select {
    columns: Vec<String>,
}

#[typetag::serde(name = "select")]
impl Transform for Select {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.select([cols(self.columns.as_slice())]))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Rename {
    Map(BTreeMap<String, String>),
    Prefix(String),
}

#[typetag::serde(name = "rename")]
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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Filter(Box<dyn Condition>);

#[typetag::serde(name = "filter")]
impl Transform for Filter {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.filter(self.0.expr()?))
    }
}


#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Extract {
    #[serde(flatten)]
    matcher: Match,
    #[serde(default)]
    filter: bool,
}

#[typetag::serde(name = "extract")]
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

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct Sort {
    column: String,
    #[serde(default)]
    descending: bool,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct SortBy(Vec<Sort>);

#[typetag::serde(name = "sort_by")]
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

#[derive(serde::Deserialize, serde::Serialize, Debug, Default)]
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

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct DropDuplicates {
    subset: Option<Vec<String>>,
    #[serde(default)]
    keep: DuplicateKeep,
}

#[typetag::serde(name = "drop_duplicates")]
impl Transform for DropDuplicates {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.unique(self.subset.clone(), UniqueKeepStrategy::from(&self.keep)))
    }
}
