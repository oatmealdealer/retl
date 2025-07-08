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
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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
    /// Remove null values in a column.
    DropNull(DropNull),
    /// Apply a `str`-namespaced operation.
    Str(Str),
    /// Filter rows that are equal to the given expression.
    Eq(Eq),
    /// Filter rows that are not equal to the given expression.
    Neq(ExpressionChain),
    /// Filter rows that are greater than the given expression.
    Gt(ExpressionChain),
    /// Filter rows that are less than the given expression.
    Lt(ExpressionChain),
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
            Self::DropNull(op) => op.apply(expr),
            Self::Str(op) => op.apply(expr),
            Self::Eq(op) => op.apply(expr),
            Self::Neq(neq) => Ok(expr.neq(neq.expr()?)),
            Self::Gt(gt) => Ok(expr.gt(gt.expr()?)),
            Self::Lt(lt) => Ok(expr.lt(lt.expr()?)),
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
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct ExtractGroups(String);

impl Op for ExtractGroups {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().extract_groups(&self.0)?)
    }
}

/// Name a column using the given alias.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Alias(String);

impl Op for Alias {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.alias(&self.0))
    }
}

/// Check if values contain the given regex.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Contains(String);

impl Op for Contains {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.str().contains(lit(self.0.as_str()), true))
    }
}

/// Check if values are null.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
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
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct FillNull(ExpressionChain);

impl Op for FillNull {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.fill_null(self.0.expr()?))
    }
}

/// Drop null values.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct DropNull {}

impl Op for DropNull {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.drop_nulls())
    }
}

/// Apply a `str`-namespaced operation.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Str {
    Len,
    StripChars(ExpressionChain),
    /// Convert the string column to lowercase.
    ToLowercase,
    /// Parse the string column as a date.
    ToDate(StrptimeOptions),
    /// Parse the string column as a datetime.
    ToDateTime {
        time_unit: Option<TimeUnit>,
        time_zone: Option<TimeZone>,
        options: StrptimeOptions,
        ambiguous: Ambiguous,
    },
    /// Replace all occurrences of the pattern within the string column with the value of a provided expression.
    ReplaceAll {
        pat: String,
        value: ExpressionChain,
        literal: bool,
    },
    /// Parse a JSON string column into a struct column using the provided schema.
    JsonDecode {
        dtype: Option<DataType>,
        infer_schema_len: Option<usize>,
    },
    /// Pad a string column with leading zeroes.
    Zfill(u16),
}

#[derive(Clone, Serialize, Deserialize, Debug, Default, JsonSchema)]
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
            Self::Len => ns.len_chars(),
            Self::StripChars(expr) => ns.strip_chars(expr.expr()?),
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
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Eq(ExpressionChain);

impl Op for Eq {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.eq(self.0.expr()?))
    }
}

/// Filter rows that are greater than or equal to the given expression.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct GtEq(ExpressionChain);

impl Op for GtEq {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.gt_eq(self.0.expr()?))
    }
}

/// Filter rows that are less than or equal to the given expression.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct LtEq(ExpressionChain);

impl Op for LtEq {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.lt_eq(self.0.expr()?))
    }
}

/// Apply a `list`-namespaced operation.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum List {
    /// Join a list column with a string separator.
    Join(ExpressionChain),
    /// Filter elements of a list column based on the given expression predicate.
    Filter(ExpressionChain),
    /// Return the first element of the list.
    First,
}

impl Op for List {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        let ns = expr.list();
        Ok(match self {
            Self::Join(chain) => ns.join(chain.expr()?, true),
            Self::Filter(chain) => ns.eval(Expr::Column(PlSmallStr::EMPTY).filter(chain.expr()?)),
            Self::First => ns.first(),
        })
    }
}

/// Divide the expression by another.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Div(ExpressionChain);

impl Op for Div {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr / self.0.expr()?)
    }
}

/// Multiply the expression by another.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Mul(ExpressionChain);

impl Op for Mul {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr * self.0.expr()?)
    }
}

/// Add the expression and another.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Add(ExpressionChain);

impl Op for Add {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr + self.0.expr()?)
    }
}

/// Subtract an expression.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Sub(ExpressionChain);

impl Op for Sub {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr - self.0.expr()?)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Cast(DataType);

impl Op for Cast {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        Ok(expr.cast(self.0.deref().clone()))
    }
}

/// Apply a `struct_`-namespaced operation.
#[derive(Clone, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Struct {
    /// Encode a struct column to JSON.
    JsonEncode,
    /// Extract a single field by name from a struct column.
    Field(String),
}

impl Op for Struct {
    fn apply(&self, expr: Expr) -> Result<Expr> {
        let ns = expr.struct_();
        Ok(match self {
            Self::JsonEncode => ns.json_encode(),
            Self::Field(name) => ns.field_by_name(name),
        })
    }
}
