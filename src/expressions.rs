use crate::Error;
use anyhow::Context;
use polars::{lazy::prelude::*, prelude::*};
use std::fmt::Debug;

#[typetag::serde]
pub trait ToExpr: Debug {
    fn expr(&self) -> anyhow::Result<Expr>;
}


#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Column(SmartString);

#[typetag::serde(name = "column")]
impl ToExpr for Column {
    fn expr(&self) -> anyhow::Result<Expr> {
        Ok(col(self.0.as_str()))
    }
}


#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Match {
    pub column: String,
    pub pattern: String,
}

#[typetag::serde(name = "match")]
impl ToExpr for Match {
    fn expr(&self) -> anyhow::Result<Expr> {
        Ok(col(&self.column)
            .str()
            .contains(lit(self.pattern.as_str()), true))
    }
}

type Conditions = Vec<Box<dyn ToExpr>>;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
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

#[typetag::serde(name = "and")]
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

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(try_from = "Conditions")]
pub struct Or {
    conditions: Vec<Box<dyn ToExpr>>,
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

#[typetag::serde(name = "or")]
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
