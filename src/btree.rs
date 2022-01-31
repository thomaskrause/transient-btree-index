use crate::{error::Result, file::TemporaryBlockFile};
use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Serialize, Deserialize)]
struct Block {}

/// Map backed by a single file on disk implemented using a B-tree.
pub struct BtreeIndex<K, V> {
    file: TemporaryBlockFile<Block>,
    phantom: PhantomData<(K, V)>,
}

impl<K, V> BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd,
    V: Serialize + DeserializeOwned + PartialOrd,
{
    /// Create a new instance with the given capacity in bytes.
    pub fn with_capacity(capacity: usize) -> Result<BtreeIndex<K, V>> {
        let file = TemporaryBlockFile::with_capacity(capacity)?;
        Ok(BtreeIndex {
            file,
            phantom: PhantomData,
        })
    }
}
