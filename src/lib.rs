pub use anyhow::Result;
use polars::{lazy::prelude::*, prelude::*};
use std::{collections::BTreeMap, fmt::Debug, path::PathBuf};

pub trait ToLazyFrame: Debug {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame>;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Other(String),
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct DataLoader {
    #[serde(flatten)]
    source: Source,
    #[serde(default)]
    transforms: Vec<Box<dyn Transform>>,
}

impl ToLazyFrame for DataLoader {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        let mut lf = self.source.to_lazy_frame()?;
        for transform in self.transforms.iter() {
            lf = transform.transform(lf)?;
        }
        Ok(lf)
    }
}

impl TryFrom<&Box<dyn ToLazyFrame>> for LazyFrame {
    type Error = anyhow::Error;
    fn try_from(value: &Box<dyn ToLazyFrame>) -> std::result::Result<Self, Self::Error> {
        value.to_lazy_frame()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Source {
    Csv(CsvSource),
    Join(JoinSource),
}

impl ToLazyFrame for Source {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        match self {
            Self::Csv(source) => source.to_lazy_frame(),
            Self::Join(source) => source.to_lazy_frame(),
        }
    }
}

impl TryFrom<Source> for LazyFrame {
    type Error = anyhow::Error;
    fn try_from(value: Source) -> std::result::Result<Self, Self::Error> {
        value.to_lazy_frame()
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(try_from = "char")]
pub struct Separator(u8);

impl TryFrom<char> for Separator {
    type Error = anyhow::Error;

    fn try_from(value: char) -> std::result::Result<Self, Self::Error> {
        Ok(Self(u8::try_from(value)?))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct CsvSource {
    path: PathBuf,
    separator: Option<Separator>,
    has_header: Option<bool>,
}

impl ToLazyFrame for CsvSource {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        let mut reader = LazyCsvReader::new(&self.path);
        reader = reader.with_has_header(self.has_header.as_ref().unwrap_or(&true).to_owned());
        if self.separator.is_some() {
            reader = reader.with_separator(self.separator.as_ref().unwrap().0)
        }
        Ok(reader.finish()?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct JoinSource {
    left: Box<DataLoader>,
    left_on: String,
    right: Box<DataLoader>,
    right_on: String,
    how: JoinType,
}

impl ToLazyFrame for JoinSource {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        let lf1 = self.left.to_lazy_frame()?;
        let lf2 = self.right.to_lazy_frame()?;
        Ok(lf1.join(
            lf2,
            [col(&self.left_on)],
            [col(&self.right_on)],
            JoinArgs::new(match self.how {
                JoinType::Inner => polars::prelude::JoinType::Inner,
                JoinType::Left => polars::prelude::JoinType::Left,
                JoinType::Right => polars::prelude::JoinType::Right,
            })
            .with_coalesce(JoinCoalesce::CoalesceColumns),
        ))
    }
}

#[typetag::serde(tag = "type")]
pub trait Transform: Debug {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame>;
}

#[typetag::serde]
pub trait Condition: Debug {
    fn expr(&self) -> anyhow::Result<Expr>;
}
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Select {
    columns: Vec<String>,
}

#[typetag::serde(name = "select")]
impl Transform for Select {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.select([cols(self.columns.as_slice())]))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Rename {
    columns: BTreeMap<String, String>,
}

#[typetag::serde(name = "rename")]
impl Transform for Rename {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.rename(self.columns.keys(), self.columns.values()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Filter(Box<dyn Condition>);

#[typetag::serde(name = "filter")]
impl Transform for Filter {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.filter(self.0.expr()?))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Match {
    column: String,
    pattern: String,
}

#[typetag::serde(name = "match")]
impl Condition for Match {
    fn expr(&self) -> anyhow::Result<Expr> {
        Ok(col(&self.column)
            .str()
            .contains(lit(self.pattern.as_str()), true))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Extract {
    #[serde(flatten)]
    matcher: Match,
    #[serde(default)]
    filter: bool,
}

#[typetag::serde(name = "extract")]
impl Transform for Extract {
    fn transform(&self, mut lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        if self.filter {
            lf = lf.filter(self.matcher.expr()?);
        }

        // TODO: See if this can be done without an intermediate alias
        let alias = format!("_{}_groups", &self.matcher.column);
        lf = lf
            .select([
                col("*"),
                col(&self.matcher.column)
                    .str()
                    .extract_groups(self.matcher.pattern.as_str())?
                    .alias(alias.as_str()),
            ])
            .unnest(vec![alias]);

        Ok(lf)
    }
}

// TODO: Implement these as transforms
pub struct Sort;
pub struct DropDuplicates;
pub struct PrefixColumns;

// TODO: Deduplicate the logic of combining these operators in the same way
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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Export {
    folder: PathBuf,
    name: String,
}

impl Export {
    pub fn export(&self, lf: LazyFrame) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.folder)?;
        let mut filename = self.folder.clone();
        filename.push(format!(
            "{}_{}.csv",
            self.name,
            chrono::Local::now().naive_local().format("%Y-%m-%d_%H%M%S")
        ));
        lf.sink_csv(
            &filename,
            CsvWriterOptions {
                ..Default::default()
            },
        )?;
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Config {
    source: DataLoader,
    #[serde(default)]
    transforms: Vec<Box<dyn Transform>>,
    export: Export,
}

impl Config {
    pub fn run(&self) -> Result<()> {
        let mut lf: LazyFrame = self.source.to_lazy_frame()?;
        for t in self.transforms.iter() {
            lf = t.transform(lf)?;
        }
        self.export.export(lf)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
