//! Expressions that evaluate to an [`Expr`]
use crate::{ops::OpItem, utils::Error};
use anyhow::{Context, Result};
use polars::{lazy::prelude::*, prelude::Literal as _};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait for a data structure that evaluates to a [`Expr`].
pub trait Expression: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Produce the expression based on the provided data.
    fn expr(&self) -> Result<Expr>;
}

/// Available expressions that can be used in configuration files.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionItem {
    /// Specify a column by name (equivalent to [`col`]).
    Col(Column),
    /// Match a regex against a column (equivalent to `col(...).str().contains(...)`).
    Match(Match),
    /// Group 2+ items together in a logical AND statement.
    And(And),
    /// Group 2+ items together in a logical OR statement.
    Or(Or),
    /// Specify a literal string value (equivalent to [`lit`]).
    Lit(Literal),
    /// Literal null value.
    Null,
    /// Combine one or more expressions into a struct column as fields.
    AsStruct(AsStruct),
}

impl Expression for ExpressionItem {
    fn expr(&self) -> Result<Expr> {
        match self {
            Self::Col(expr) => expr.expr(),
            Self::Match(expr) => expr.expr(),
            Self::And(expr) => expr.expr(),
            Self::Or(expr) => expr.expr(),
            Self::Lit(expr) => expr.expr(),
            Self::Null => Ok(NULL.lit()),
            Self::AsStruct(expr) => expr.expr(),
        }
    }
}

/// An expression grouped together with chained operations.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ExpressionChain {
    expr: ExpressionItem,
    #[serde(default)]
    ops: Vec<OpItem>,
}

impl Expression for ExpressionChain {
    fn expr(&self) -> Result<Expr> {
        self.ops
            .iter()
            .try_fold(self.expr.expr()?, |expr, op| op.apply(expr))
    }
}

/// Specify a column by name (equivalent to [`polars::prelude::col`]).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, JsonSchema)]
pub struct Column(String);

impl Expression for Column {
    fn expr(&self) -> Result<Expr> {
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
    fn expr(&self) -> Result<Expr> {
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
    fn expr(&self) -> Result<Expr> {
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
    fn expr(&self) -> Result<Expr> {
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

/// Specify a literal value (equivalent to [`polars::prelude::lit`]).
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, JsonSchema)]
pub struct Literal(String);

impl Expression for Literal {
    fn expr(&self) -> Result<Expr> {
        Ok(lit(self.0.as_str()))
    }
}

/// Combine one or more expressions into a struct column as fields.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct AsStruct(Vec<ExpressionChain>);

impl Expression for AsStruct {
    fn expr(&self) -> Result<Expr> {
        Ok(as_struct(
            self.0
                .iter()
                .map(|chain| chain.expr())
                .collect::<Result<Vec<Expr>>>()?,
        ))
    }
}
