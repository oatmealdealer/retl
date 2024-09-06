use crate::{expressions::ToExpr, utils::ColMap};
use anyhow::Result;
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub(crate) trait Op: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    fn apply(&self, expr: Expr) -> Result<Expr>;
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OpItem {
    ExtractGroups(ExtractGroups),
    Alias(Alias),
    Contains(Contains),
    IsNull(IsNull),
    Or(Or),
    And(And),
}

impl OpItem {
    pub(crate) fn expr(&self, expr: Expr) -> Result<Expr> {
        match self {
            Self::ExtractGroups(op) => op.apply(expr),
            Self::Alias(op) => op.apply(expr),
            Self::Contains(op) => op.apply(expr),
            Self::IsNull(op) => op.apply(expr),
            Self::Or(op) => op.apply(expr),
            Self::And(op) => op.apply(expr),
        }
    }
}

/// Extract all capture groups from a regex into a struct column.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct ExtractGroups(String);

impl Op for ExtractGroups {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().extract_groups(&self.0)?)
    }
}

/// Name a column using the given alias.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct Alias(String);

impl Op for Alias {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.alias(&self.0))
    }
}

/// Check if values contain the given regex.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct Contains(String);

impl Op for Contains {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().contains(lit(self.0.as_str()), true))
    }
}

/// Check if values are null.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct IsNull(bool);

impl Op for IsNull {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(if self.0 {
            expr.is_null()
        } else {
            expr.is_not_null()
        })
    }
}

/// Chain an expression into a logical OR with conditions on one or more columns.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct Or(ColMap);

impl Op for Or {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
            .iter()
            .try_fold(expr, |chain, (name, ops)| -> Result<Expr> {
                Ok(chain.or(ops
                    .iter()
                    .try_fold(name.to_expr()?, |ex, op| -> Result<Expr> {
                        Ok(op.expr(ex)?)
                    })?))
            })?)
    }
}

/// Chain an expression into a logical AND with conditions on one or more columns.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct And(ColMap);

impl Op for And {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
            .iter()
            .try_fold(expr, |chain, (name, ops)| -> Result<Expr> {
                Ok(chain.and(
                    ops.iter()
                        .try_fold(name.to_expr()?, |ex, op| -> Result<Expr> {
                            Ok(op.expr(ex)?)
                        })?,
                ))
            })?)
    }
}
