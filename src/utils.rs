use anyhow::Result;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("must define at least one export")]
    NoExports,
    #[error("{0}")]
    Other(String),
}

pub(crate) fn with_current_dir<T, P, F>(path: P, func: F) -> Result<T>
where
    P: AsRef<Path>,
    F: Fn() -> Result<T>,
{
    let old_cd = std::env::current_dir()?;
    std::env::set_current_dir(path)?;
    match func() {
        Ok(t) => {
            std::env::set_current_dir(old_cd)?;
            Ok(t)
        }
        Err(e) => {
            std::env::set_current_dir(old_cd)?;
            Err(e)
        }
    }
}
