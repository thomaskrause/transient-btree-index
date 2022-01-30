use crate::{error::Result, file::MemoryMappedFile};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, path::Path};

/// Map backed by a single file on disk implemented using a B-tree.
pub struct SingleFileBtreeMap<K, V> {
    file: MemoryMappedFile,
    phantom: PhantomData<(K, V)>,
}

impl<K, V> SingleFileBtreeMap<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd,
    V: Serialize + DeserializeOwned + PartialOrd,
{
    pub fn create(path: &Path) -> Result<SingleFileBtreeMap<K, V>> {
        let file = MemoryMappedFile::open(path)?;
        Ok(SingleFileBtreeMap {
            file,
            phantom: PhantomData,
        })
    }
}
