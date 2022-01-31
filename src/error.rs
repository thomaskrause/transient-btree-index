use std::{array::TryFromSliceError, num::TryFromIntError};

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
    SliceConversion(#[from] TryFromSliceError),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    
}
