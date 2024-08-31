use crate::utils::Error;
use anyhow::{Context, Result};
use polars::lazy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub(crate) trait ToExpr: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    fn to_expr(&self) -> Result<Expr>;
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToExprItem {
    Column(Column),
    Match(Match),
    And(And),
    Or(Or),
}

impl ToExprItem {
    pub(crate) fn expr(&self) -> Result<Expr> {
        match self {
            Self::Column(expr) => expr.to_expr(),
            Self::Match(expr) => expr.to_expr(),
            Self::And(expr) => expr.to_expr(),
            Self::Or(expr) => expr.to_expr(),
        }
    }
}

/// Specify a column by name (equivalent to [`polars::prelude::col`])
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, JsonSchema)]
pub(crate) struct Column(String);

impl ToExpr for Column {
    fn to_expr(&self) -> Result<Expr> {
        Ok(col(self.0.as_str()))
    }
}

/// Match a column against a regex
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct Match {
    pub(crate) column: String,
    pub(crate) pattern: String,
}

impl ToExpr for Match {
    fn to_expr(&self) -> Result<Expr> {
        Ok(col(&self.column)
            .str()
            .contains(lit(self.pattern.as_str()), true))
    }
}

type Conditions = Vec<ToExprItem>;

/// Logical AND against two or more conditions
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(try_from = "Conditions")]
pub(crate) struct And {
    conditions: Conditions,
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

impl ToExpr for And {
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

/// Logical OR against two or more conditions
#[derive(Deserialize, Serialize, Debug, JsonSchema)]
#[serde(try_from = "Conditions")]
pub(crate) struct Or {
    conditions: Conditions,
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

impl ToExpr for Or {
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
