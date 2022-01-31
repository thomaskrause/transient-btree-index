use crate::{error::Result, file::MemoryMappedFile};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, path::Path};

/// Map backed by a single file on disk implemented using a B-tree.
pub struct BtreeIndex<K, V> {
    file: MemoryMappedFile,
    phantom: PhantomData<(K, V)>,
}

impl<K, V> BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd,
    V: Serialize + DeserializeOwned + PartialOrd,
{
    pub fn create(path: &Path) -> Result<BtreeIndex<K, V>> {
        let file = MemoryMappedFile::with_capacity(0)?;
        Ok(BtreeIndex {
            file,
            phantom: PhantomData,
        })
    }
}
