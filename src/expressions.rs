use crate::{Error, Result};
use anyhow::Context;
use polars::{lazy::prelude::*, prelude::*};
use schemars::JsonSchema;
use std::fmt::Debug;

pub trait ToExpr: Debug {
    fn expr(&self) -> anyhow::Result<Expr>;
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToExprItem {
    Column(Column),
    Match(Match),
    And(And),
    Or(Or),
}

impl ToExprItem {
    pub fn expr(&self) -> Result<Expr> {
        match self {
            Self::Column(expr) => expr.expr(),
            Self::Match(expr) => expr.expr(),
            Self::And(expr) => expr.expr(),
            Self::Or(expr) => expr.expr(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Hash, JsonSchema)]
pub struct Column(String);

impl ToExpr for Column {
    fn expr(&self) -> anyhow::Result<Expr> {
        Ok(col(self.0.as_str()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct Match {
    pub column: String,
    pub pattern: String,
}

impl ToExpr for Match {
    fn expr(&self) -> anyhow::Result<Expr> {
        Ok(col(&self.column)
            .str()
            .contains(lit(self.pattern.as_str()), true))
    }
}

type Conditions = Vec<ToExprItem>;

#[derive(serde::Deserialize, serde::Serialize, Debug, JsonSchema)]
#[serde(try_from = "Conditions")]
pub struct And {
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
    fn expr(&self) -> anyhow::Result<Expr> {
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

#[derive(serde::Deserialize, serde::Serialize, Debug, JsonSchema)]
#[serde(try_from = "Conditions")]
pub struct Or {
    conditions: Vec<ToExprItem>,
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
    fn expr(&self) -> anyhow::Result<Expr> {
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
