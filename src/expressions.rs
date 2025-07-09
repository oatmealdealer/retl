//! Expressions that evaluate to an [`Expr`]
use crate::{
    ops::OpItem,
    utils::{DataType, Error},
};
use anyhow::{Context, Result};
use polars::{lazy::prelude::*, prelude::Literal as _};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, ops::Deref};

/// Trait for a data structure that evaluates to a [`Expr`].
pub trait Expression: Serialize + for<'a> Deserialize<'a> + JsonSchema + Debug {
    /// Produce the expression based on the provided data.
    fn expr(&self) -> Result<Expr>;
}

/// Available expressions that can be used in configuration files.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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
    /// Logically invert the given expression.
    Not(Box<ExpressionChain>),
    /// Specify a literal string value (equivalent to [`lit`]).
    Lit(Literal),
    /// Literal null value.
    Null,
    /// Evaluates to the number of rows.
    Len,
    /// Combine one or more expressions into a struct column as fields.
    AsStruct(AsStruct),
    /// Generate a range of integers.
    IntRange(IntRange),
    /// Concatenate string expressions horizontally.
    ConcatStr(ConcatStr),
    /// Use to reference the current element in a list eval expression. Equivalent to `col("")`.
    Element,
    /// Create a when/then/otherwise expression.
    Condition(Condition),
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
            Self::Len => Ok(len()),
            Self::Not(expr) => Ok(expr.expr()?.not()),
            Self::AsStruct(expr) => expr.expr(),
            Self::IntRange(expr) => expr.expr(),
            Self::ConcatStr(expr) => expr.expr(),
            Self::Element => Ok(col("")),
            Self::Condition(expr) => expr.expr(),
        }
    }
}

/// An expression grouped together with chained operations.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, JsonSchema)]
pub struct Column(String);

impl Expression for Column {
    fn expr(&self) -> Result<Expr> {
        Ok(col(self.0.as_str()))
    }
}

/// Match a column against a regex.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema)]
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
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema)]
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
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, JsonSchema)]
pub struct Literal(String);

impl Expression for Literal {
    fn expr(&self) -> Result<Expr> {
        Ok(lit(self.0.as_str()))
    }
}

/// Combine one or more expressions into a struct column as fields.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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

/// Generate a range of integers.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct IntRange {
    start: i64,
    // end:
    step: i64,
    dtype: DataType,
}

impl Expression for IntRange {
    fn expr(&self) -> Result<Expr> {
        Ok(int_range(
            lit(self.start),
            len(),
            self.step,
            self.dtype.deref().clone(),
        ))
    }
}

/// Concatenate string expressions horizontally.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct ConcatStr {
    columns: Vec<ExpressionChain>,
    separator: String,
    ignore_nulls: bool,
}

impl Expression for ConcatStr {
    fn expr(&self) -> Result<Expr> {
        Ok(concat_str(
            self.columns
                .iter()
                .map(|chain| chain.expr())
                .collect::<Result<Vec<Expr>>>()?
                .as_slice(),
            self.separator.as_str(),
            self.ignore_nulls,
        ))
    }
}

/// Create a when/then/otherwise expression.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Condition {
    when: Box<ExpressionChain>,
    then: Box<ExpressionChain>,
    otherwise: Box<ExpressionChain>,
}

impl Expression for Condition {
    fn expr(&self) -> Result<Expr> {
        Ok(when(self.when.expr()?)
            .then(self.then.expr()?)
            .otherwise(self.otherwise.expr()?))
    }
}
