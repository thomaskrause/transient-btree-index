use crate::{
    error::Result,
    file::{BlockHeader, TemporaryBlockFile},
};
use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;

const KB: usize = 1 << 10;
const PAGE_SIZE: usize = 4 * KB;
const BLOCK_SIZE: usize = PAGE_SIZE - BlockHeader::size();

#[derive(Serialize, Deserialize)]
struct NodeBlock<K, V> {
    number_of_keys: usize,
    child_nodes: Vec<usize>,
    keys: Vec<K>,
    payload: Vec<V>,
}

/// Map backed by a single file on disk implemented using a B-tree.
pub struct BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd,
    V: Serialize + DeserializeOwned,
{
    file: TemporaryBlockFile<NodeBlock<K, V>>,
    root_id: usize,
    phantom: PhantomData<(K, V)>,
}

fn find_key_in_node<K: PartialOrd, V>(key: &K, node: &NodeBlock<K, V>) -> Option<usize> {
    for i in 0..node.number_of_keys {
        if &node.keys[i] == key {
            return Some(i);
        } else if &node.keys[i] >= key {
            return Some(i - 1);
        }
    }
    None
}

impl<K, V> BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Create a new instance with the given capacity in bytes.
    pub fn with_capacity(capacity: usize) -> Result<BtreeIndex<K, V>> {
        let mut file = TemporaryBlockFile::with_capacity(capacity)?;

        // Always add an empty root node
        let root_node = NodeBlock {
            number_of_keys: 0,
            child_nodes: Vec::default(),
            keys: Vec::default(),
            payload: Vec::default(),
        };
        let root_id = file.allocate_block(BLOCK_SIZE)?;
        file.put(root_id, &root_node)?;

        Ok(BtreeIndex {
            root_id,
            file,
            phantom: PhantomData,
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let mut finished = false;
        let mut result = None;
        let mut node = self.file.get(self.root_id)?;

        while !finished {
            // Search for the key in the current node
            if let Some(i) = find_key_in_node(key, &node) {
                if &node.keys[i] == key {
                    // Key found
                    result = Some(node.payload[i].clone());
                    finished = true;
                } else {
                    // Follow reference to the next child node
                    node = self.file.get(node.child_nodes[i])?;
                }
            } else {
                finished = true;
            }
        }

        Ok(result)
    }
    pub fn insert(&mut self, _key: K, _value: V) -> Result<()> {
        todo!()
    }
}
