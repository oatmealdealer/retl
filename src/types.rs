use std::path::PathBuf;

use crate::Error;


#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Clone)]
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
        if value.try_exists()? {
            Ok(Self(value.canonicalize()?))
        } else {
            Err(Error::BadPath(value).into())
        }
    }
}
