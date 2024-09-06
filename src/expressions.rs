//! Expressions that evaluate to an [`Expr`]
use crate::utils::Error;
use anyhow::{Context, Result};
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait for a data structure that evaluates to a [`Expr`].
pub trait Expression: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Produce the expression based on the provided data.
    fn to_expr(&self) -> Result<Expr>;
}

/// Available expressions that can be used in configuration files.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionItem {
    /// Specify a column by name (equivalent to [`col`]).
    Column(Column),
    /// Match a regex against a column (equivalent to `col(...).str().contains(...)`).
    Match(Match),
    /// Group 2+ items together in a logical AND statement.
    And(And),
    /// Group 2+ items together in a logical OR statement.
    Or(Or),
}

impl ExpressionItem {
    pub(crate) fn expr(&self) -> Result<Expr> {
        match self {
            Self::Column(expr) => expr.to_expr(),
            Self::Match(expr) => expr.to_expr(),
            Self::And(expr) => expr.to_expr(),
            Self::Or(expr) => expr.to_expr(),
        }
    }
}

/// Specify a column by name (equivalent to [`polars::prelude::col`]).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, JsonSchema)]
pub struct Column(String);

impl Expression for Column {
    fn to_expr(&self) -> Result<Expr> {
        Ok(col(self.0.as_str()))
    }
}

/// Match a column against a regex.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Match {
    /// Column to apply pattern to.
    pub column: String,
    /// Pattern to match against the column.
    pub pattern: String,
}

impl Expression for Match {
    fn to_expr(&self) -> Result<Expr> {
        Ok(col(&self.column)
            .str()
            .contains(lit(self.pattern.as_str()), true))
    }
}

type Conditions = Vec<ExpressionItem>;

/// Logical AND against two or more conditions.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(try_from = "Conditions")]
pub struct And {
    /// Conditions to combine.
    pub conditions: Conditions,
}

impl TryFrom<Conditions> for And {
    type Error = Error;

    fn try_from(value: Conditions) -> std::result::Result<Self, Self::Error> {
        if value.len() < 2 {
            Err(Error::Other(
                "and statement must have at least 2 conditions".to_owned(),
            ))
        } else {
            Ok(Self { conditions: value })
        }
    }
}

impl Expression for And {
    fn to_expr(&self) -> Result<Expr> {
        let mut expr: Option<Expr> = None;
        for cond in self.conditions.iter() {
            expr = match expr {
                None => Some(cond.expr()?),
                Some(ex) => Some(ex.and(cond.expr()?)),
            }
        }
        expr.context("and statement must have at least 2 conditions")
    }
}

/// Logical OR against two or more conditions.
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(try_from = "Conditions")]
pub struct Or {
    /// Conditions to combine.
    pub conditions: Conditions,
}

impl TryFrom<Conditions> for Or {
    type Error = Error;

    fn try_from(value: Conditions) -> std::result::Result<Self, Self::Error> {
        if value.len() < 2 {
            Err(Error::Other(
                "or statement must have at least 2 conditions".to_owned(),
            ))
        } else {
            Ok(Self { conditions: value })
        }
    }
}

impl Expression for Or {
    fn to_expr(&self) -> Result<Expr> {
        let mut expr: Option<Expr> = None;
        for cond in self.conditions.iter() {
            expr = match expr {
                None => Some(cond.expr()?),
                Some(ex) => Some(ex.or(cond.expr()?)),
            }
        }
        expr.context("or statement must have at least 2 conditions")
    }
}
