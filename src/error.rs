use std::{array::TryFromSliceError, num::TryFromIntError};

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Size of existing block (ID {block_id}) is too small to write new block. It needs {needed}.")]
    ExistingBlockTooSmall { block_id: usize, needed: u64 },
    #[error("When trying to insert a non-existing key, the found node block was internal and not a leaf node")]
    InsertFoundInternalNode,    
    #[error("Splitting a node resulted in an empty child node.")]
    EmptyChildNodeInSplit,
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    IntConversion(#[from] TryFromIntError),
    #[error(transparent)]
    SliceConversion(#[from] TryFromSliceError),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
}
