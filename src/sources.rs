use crate::{types::CanonicalPathBuf, Config, Transform};
use polars::{lazy::prelude::*, prelude::*};
use std::fmt::Debug;

#[typetag::serde(tag = "type")]
pub trait Source: Debug {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame>;
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct Loader {
    #[serde(flatten)]
    source: Box<dyn Source>,
    #[serde(default)]
    transforms: Vec<Box<dyn Transform>>,
}

impl Loader {
    pub fn load(&self) -> anyhow::Result<LazyFrame> {
        let mut lf = self.source.to_lazy_frame()?;
        for transform in self.transforms.iter() {
            lf = transform.transform(lf)?;
        }
        Ok(lf)
    }
}

impl TryFrom<&Box<dyn Source>> for LazyFrame {
    type Error = anyhow::Error;
    fn try_from(value: &Box<dyn Source>) -> std::result::Result<Self, Self::Error> {
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
    path: CanonicalPathBuf,
    separator: Option<Separator>,
    has_header: Option<bool>,
    schema: Option<Schema>,
}

#[typetag::serde(name = "csv")]
impl Source for CsvSource {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        let mut reader = LazyCsvReader::new(&self.path);
        reader = reader.with_has_header(self.has_header.as_ref().unwrap_or(&true).to_owned());
        if self.separator.is_some() {
            reader = reader.with_separator(self.separator.as_ref().unwrap().0)
        }
        reader = reader
            .with_truncate_ragged_lines(true)
            .with_schema(self.schema.as_ref().map(|s| Arc::new(s.clone())));
        Ok(reader.finish()?)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct JoinSource {
    left: Box<Loader>,
    left_on: String,
    right: Box<Loader>,
    right_on: String,
    how: JoinType,
}

#[typetag::serde(name = "join")]
impl Source for JoinSource {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        let lf1 = self.left.load()?;
        let lf2 = self.right.load()?;
        Ok(lf1.join(
            lf2,
            [col(&self.left_on)],
            [col(&self.right_on)],
            JoinArgs::new(match self.how {
                JoinType::Inner => polars::prelude::JoinType::Inner,
                JoinType::Left => polars::prelude::JoinType::Left,
                JoinType::Right => polars::prelude::JoinType::Right,
                JoinType::Full => polars::prelude::JoinType::Full,
            })
            .with_coalesce(JoinCoalesce::CoalesceColumns),
        ))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ConfigSource {
    path: CanonicalPathBuf,
}

#[typetag::serde(name = "config")]
impl Source for ConfigSource {
    fn to_lazy_frame(&self) -> anyhow::Result<LazyFrame> {
        Config::from_path(&self.path)?.load()
    }
}
