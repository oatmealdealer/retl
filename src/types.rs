use std::path::PathBuf;

use schemars::JsonSchema;

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Clone, JsonSchema)]
#[serde(try_from = "PathBuf")]
pub struct CanonicalPathBuf(pub PathBuf);

impl AsRef<std::path::Path> for CanonicalPathBuf {
    fn as_ref(&self) -> &std::path::Path {
        self.0.as_ref()
    }
}

impl TryFrom<PathBuf> for CanonicalPathBuf {
    type Error = anyhow::Error;

    fn try_from(value: PathBuf) -> std::result::Result<Self, Self::Error> {
        Ok(Self(value.canonicalize()?))
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Clone, JsonSchema)]
#[serde(try_from = "PathBuf")]
pub struct CanonicalDirectory(pub PathBuf);

impl AsRef<std::path::Path> for CanonicalDirectory {
    fn as_ref(&self) -> &std::path::Path {
        self.0.as_ref()
    }
}

impl TryFrom<PathBuf> for CanonicalDirectory {
    type Error = anyhow::Error;

    fn try_from(value: PathBuf) -> std::result::Result<Self, Self::Error> {
        std::fs::create_dir_all(&value)?;
        Ok(Self(value.canonicalize()?))
    }
}
