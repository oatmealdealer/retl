//! Operations that can be used to modify/compose [`Expr`]s.
use crate::{
    expressions::{Expression, ExpressionChain},
    utils::DataType,
};
use anyhow::Result;
use polars::{lazy::prelude::*, prelude::*};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, ops::Deref};

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
    /// Apply a `str`-namespaced operation.
    Str(Str),
    /// Filter rows that are equal to the given expression.
    Eq(Eq),
    /// Filter rows that are greater than or equal to the given expression.
    GtEq(GtEq),
    /// Filter rows that are less than or equal to the given expression.
    LtEq(LtEq),
    /// Apply a `list`-namespaced operation.
    List(List),
    Div(Div),
    Mul(Mul),
    Add(Add),
    Sub(Sub),
    Cast(Cast),
    Struct(Struct),
    // Dt(Dt),
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
            Self::Str(op) => op.apply(expr),
            Self::Eq(op) => op.apply(expr),
            Self::GtEq(op) => op.apply(expr),
            Self::LtEq(op) => op.apply(expr),
            Self::List(op) => op.apply(expr),
            Self::Div(op) => op.apply(expr),
            Self::Mul(op) => op.apply(expr),
            Self::Add(op) => op.apply(expr),
            Self::Sub(op) => op.apply(expr),
            Self::Cast(op) => op.apply(expr),
            Self::Struct(op) => op.apply(expr),
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

/// Apply a `str`-namespaced operation.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Str {
    /// Convert the string column to lowercase.
    ToLowercase,
    ToDate(StrptimeOptions),
    ToDateTime {
        time_unit: Option<TimeUnit>,
        time_zone: Option<TimeZone>,
        options: StrptimeOptions,
        ambiguous: Ambiguous,
    },
    ReplaceAll {
        pat: String,
        value: ExpressionChain,
        literal: bool,
    },
    JsonDecode {
        dtype: Option<DataType>,
        infer_schema_len: Option<usize>,
    },
    Zfill(u16),
}

#[derive(Serialize, Deserialize, Debug, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Ambiguous {
    #[default]
    Raise,
    Earliest,
    Latest,
    Null,
}

impl Expression for Ambiguous {
    fn expr(&self) -> Result<Expr> {
        Ok(match self {
            Self::Raise => lit("raise"),
            Self::Earliest => lit("earliest"),
            Self::Latest => lit("latest"),
            Self::Null => lit("null"),
        })
    }
}

impl Op for Str {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        let ns = expr.str();
        Ok(match self {
            Self::ToLowercase => ns.to_lowercase(),
            Self::ToDate(options) => ns.to_date(options.clone()),
            Self::ToDateTime {
                time_unit,
                time_zone,
                options,
                ambiguous,
            } => ns.to_datetime(
                time_unit.clone(),
                time_zone.clone(),
                options.clone(),
                ambiguous.expr()?,
            ),
            Self::ReplaceAll {
                pat,
                value,
                literal,
            } => ns.replace_all(lit(pat.as_str()), value.expr()?, *literal),
            Self::JsonDecode {
                dtype,
                infer_schema_len,
            } => ns.json_decode(
                dtype.as_ref().map(|pldt| pldt.deref().clone()),
                *infer_schema_len,
            ),
            Self::Zfill(len) => ns.zfill(lit(*len)),
        })
    }
}

/// Filter rows that are equal to the given expression.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Eq(ExpressionChain);

impl Op for Eq {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.eq(self.0.expr()?))
    }
}

/// Filter rows that are greater than or equal to the given expression.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GtEq(ExpressionChain);

impl Op for GtEq {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.gt_eq(self.0.expr()?))
    }
}

/// Filter rows that are less than or equal to the given expression.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct LtEq(ExpressionChain);

impl Op for LtEq {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.lt_eq(self.0.expr()?))
    }
}

/// Apply a `list`-namespaced operation.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum List {
    /// Join a list column with a string separator.
    Join(ExpressionChain),
}

impl Op for List {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        let ns = expr.list();
        Ok(match self {
            Self::Join(chain) => ns.join(chain.expr()?, true),
        })
    }
}

/// Divide the expression by another.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Div(ExpressionChain);

impl Op for Div {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr / self.0.expr()?)
    }
}

/// Multiply the expression by another.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Mul(ExpressionChain);

impl Op for Mul {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr * self.0.expr()?)
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Add(ExpressionChain);

impl Op for Add {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr + self.0.expr()?)
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Sub(ExpressionChain);

impl Op for Sub {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr - self.0.expr()?)
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Cast(DataType);

impl Op for Cast {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.cast(self.0.deref().clone()))
    }
}

/// Apply a `struct_`-namespaced operation.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Struct {
    JsonEncode,
}

impl Op for Struct {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        let ns = expr.struct_();
        Ok(match self {
            Self::JsonEncode => ns.json_encode(),
        })
    }
}
