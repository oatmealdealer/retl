//! Operations that can be used to modify/compose [`Expr`]s.
use crate::expressions::{Expression, ExpressionChain};
use anyhow::Result;
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait for an operation that modifies an expression supplied as input.
pub trait Op: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Modify the given expression based on the provided data.
    fn apply(&self, expr: Expr) -> Result<Expr>;
}

/// Possible operations that can be applied to an expression (i.e. [`polars::prelude::Expr`]).
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OpItem {
    /// Extract the capture groups of a regex from the given column.
    ExtractGroups(ExtractGroups),
    /// Name a column using the given alias.
    Alias(Alias),
    /// Check if values contain the given regex.
    Contains(Contains),
    /// Check if values are null.
    IsNull(IsNull),
    /// Chain an expression into a logical OR with conditions on one or more columns.
    Or(Or),
    /// Chain an expression into a logical AND with conditions on one or more columns.
    And(And),
    /// Fill in null values in a column with an expression.
    FillNull(FillNull),
}

impl OpItem {
    pub(crate) fn apply(&self, expr: Expr) -> Result<Expr> {
        match self {
            Self::ExtractGroups(op) => op.apply(expr),
            Self::Alias(op) => op.apply(expr),
            Self::Contains(op) => op.apply(expr),
            Self::IsNull(op) => op.apply(expr),
            Self::Or(op) => op.apply(expr),
            Self::And(op) => op.apply(expr),
            Self::FillNull(op) => op.apply(expr),
        }
    }
}

/// Extract the capture groups of a regex from the given column.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ExtractGroups(String);

impl Op for ExtractGroups {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().extract_groups(&self.0)?)
    }
}

/// Name a column using the given alias.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Alias(String);

impl Op for Alias {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.alias(&self.0))
    }
}

/// Check if values contain the given regex.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Contains(String);

impl Op for Contains {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().contains(lit(self.0.as_str()), true))
    }
}

/// Check if values are null.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct IsNull(bool);

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
pub struct Or(Vec<ExpressionChain>);

impl Op for Or {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
            .iter()
            .try_fold(expr, |chain, next_expr| -> Result<Expr> {
                Ok(chain.and(next_expr.expr()?))
            })?)
    }
}

/// Chain an expression into a logical AND with conditions on one or more columns.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct And(Vec<ExpressionChain>);

impl Op for And {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
            .iter()
            .try_fold(expr, |chain, next_expr| -> Result<Expr> {
                Ok(chain.and(next_expr.expr()?))
            })?)
    }
}

/// Fill in null values with a given expression.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct FillNull(ExpressionChain);

impl Op for FillNull {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.fill_null(self.0.expr()?))
    }
}
