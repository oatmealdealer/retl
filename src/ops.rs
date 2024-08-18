use std::fmt::Debug;

use schemars::JsonSchema;

use crate::{prelude::*, ColMap, Result, ToExpr};

pub trait Op: Debug {
    fn expr(&self, expr: Expr) -> Result<Expr>;
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OpItem {
    ExtractGroups(ExtractGroups),
    Alias(Alias),
    Contains(Contains),
    IsNull(IsNull),
    Or(Or),
    And(And),
}

impl OpItem {
    pub fn expr(&self, expr: Expr) -> Result<Expr> {
        match self {
            Self::ExtractGroups(op) => op.expr(expr),
            Self::Alias(op) => op.expr(expr),
            Self::Contains(op) => op.expr(expr),
            Self::IsNull(op) => op.expr(expr),
            Self::Or(op) => op.expr(expr),
            Self::And(op) => op.expr(expr),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct ExtractGroups(String);

impl Op for ExtractGroups {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().extract_groups(&self.0)?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct Alias(String);

impl Op for Alias {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.alias(&self.0))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct Contains(String);

impl Op for Contains {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().contains(lit(self.0.as_str()), true))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct IsNull(bool);

impl Op for IsNull {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(if self.0 {
            expr.is_null()
        } else {
            expr.is_not_null()
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct Or(ColMap);

impl Op for Or {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
            .inner
            .iter()
            .try_fold(expr, |chain, (name, ops)| -> Result<Expr> {
                Ok(chain.or(ops
                    .iter()
                    .try_fold(name.expr()?, |ex, op| -> Result<Expr> { Ok(op.expr(ex)?) })?))
            })?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, JsonSchema)]
pub struct And(ColMap);

impl Op for And {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
            .inner
            .iter()
            .try_fold(expr, |chain, (name, ops)| -> Result<Expr> {
                Ok(chain.and(
                    ops.iter()
                        .try_fold(name.expr()?, |ex, op| -> Result<Expr> { Ok(op.expr(ex)?) })?,
                ))
            })?)
    }
}
