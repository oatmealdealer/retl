use crate::Error;
use polars::lazy::prelude::*;
use std::fmt::Debug;

#[typetag::serde]
pub trait Condition: Debug {
    fn expr(&self) -> anyhow::Result<Expr>;
}


#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Match {
    pub column: String,
    pub pattern: String,
}

#[typetag::serde(name = "match")]
impl Condition for Match {
    fn expr(&self) -> anyhow::Result<Expr> {
        Ok(col(&self.column)
            .str()
            .contains(lit(self.pattern.as_str()), true))
    }
}

type Conditions = Vec<Box<dyn Condition>>;

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
impl Condition for And {
    fn expr(&self) -> anyhow::Result<Expr> {
        let mut expr: Option<Expr> = None;
        for cond in self.conditions.iter() {
            expr = match expr {
                None => Some(cond.expr()?),
                Some(ex) => Some(ex.and(cond.expr()?)),
            }
        }
        match expr {
            None => {
                Err(Error::Other("and statement must have at least 2 conditions".to_owned()).into())
            }
            Some(ex) => Ok(ex),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(try_from = "Conditions")]
pub struct Or {
    conditions: Vec<Box<dyn Condition>>,
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
impl Condition for Or {
    fn expr(&self) -> anyhow::Result<Expr> {
        let mut expr: Option<Expr> = None;
        for cond in self.conditions.iter() {
            expr = match expr {
                None => Some(cond.expr()?),
                Some(ex) => Some(ex.or(cond.expr()?)),
            }
        }
        match expr {
            None => {
                Err(Error::Other("and statement must have at least 2 conditions".to_owned()).into())
            }
            Some(ex) => Ok(ex),
        }
    }
}
