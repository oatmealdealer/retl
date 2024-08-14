pub use anyhow::Result;
use polars::{lazy::prelude::*, prelude::*};
use std::{collections::BTreeMap, fmt::Debug, path::PathBuf};

pub trait ToLazyFrame: Debug {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame>;
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
    left: Box<Source>,
    left_on: String,
    right: Box<Source>,
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

#[typetag::serde(tag = "type")]

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
struct SetHeaders {
    columns: BTreeMap<String, String>,
}

#[typetag::serde(name = "set_headers")]
impl Transform for SetHeaders {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.rename(self.columns.keys(), self.columns.values()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Match {
    column: String,
    pattern: String,
}

#[typetag::serde(name = "match")]
impl Transform for Match {
    fn transform(&self, lf: LazyFrame) -> anyhow::Result<LazyFrame> {
        Ok(lf.filter(self.expr()?))
    }
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
            lf = self.matcher.transform(lf)?;
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
    source: Source,
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
