use std::num::TryFromIntError;

use tempfile::PersistError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    IntConversion(#[from] TryFromIntError),
    #[error(transparent)]
    PersistingTemporaryFile(#[from] PersistError),
}