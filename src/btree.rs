use std::{marker::PhantomData, path::Path};

use serde::{de::DeserializeOwned, Serialize};

use crate::errors::Result;
/// Map backed by a single file on disk implemented using a B-tree.
pub struct SingleFileBtreeMap<K, V>
{
    phantom: PhantomData<(K, V)>,
}

impl<K,V> SingleFileBtreeMap<K,V> where
K: Serialize + DeserializeOwned + PartialOrd,
V: Serialize + DeserializeOwned + PartialOrd, {

    pub fn new_file(_path : &Path) -> Result<SingleFileBtreeMap<K,V>> {
        todo!()
    }

    pub fn new_temporary() -> Result<SingleFileBtreeMap<K,V>> {
        todo!()
    }

}
