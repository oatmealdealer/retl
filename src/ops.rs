use std::fmt::Debug;

use crate::{prelude::*, ColMap, Result, ToExpr};

#[typetag::serde]
pub trait Op: Debug {
    fn expr(&self, expr: Expr) -> Result<Expr>;
}

pub type Ops = Vec<Box<dyn Op>>;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ExtractGroups(String);

#[typetag::serde(name = "extract_groups")]
impl Op for ExtractGroups {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().extract_groups(&self.0)?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Alias(String);

#[typetag::serde(name = "alias")]
impl Op for Alias {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.alias(&self.0))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Contains(String);

#[typetag::serde(name = "contains")]
impl Op for Contains {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().contains(lit(self.0.as_str()), true))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Or(ColMap);

#[typetag::serde(name = "or")]
impl Op for Or {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
             .0
            .iter()
            .try_fold(expr, |chain, (name, ops)| -> Result<Expr> {
                Ok(chain.or(ops
                    .iter()
                    .try_fold(name.expr()?, |ex, op| -> Result<Expr> { Ok(op.expr(ex)?) })?))
            })?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct And(ColMap);

#[typetag::serde(name = "and")]
impl Op for And {
    fn expr(&self, expr: Expr) -> Result<Expr> {
        Ok(self
            .0
             .0
            .iter()
            .try_fold(expr, |chain, (name, ops)| -> Result<Expr> {
                Ok(chain.and(
                    ops.iter()
                        .try_fold(name.expr()?, |ex, op| -> Result<Expr> { Ok(op.expr(ex)?) })?,
                ))
            })?)
    }
}
