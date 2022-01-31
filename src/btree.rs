use crate::{
    error::Result,
    file::{BlockHeader, TemporaryBlockFile},
};
use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};

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
    order: usize,
}

fn find_key_in_node<K: PartialOrd, V>(key: &K, node: &NodeBlock<K, V>) -> usize {
    for i in 0..node.number_of_keys {
        if &node.keys[i] == key {
            return i;
        } else if &node.keys[i] >= key {
            return i - 1;
        }
    }
    node.number_of_keys + 1
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
            order: 128,
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let (node, _, i) = self.lookup_block(key)?;
        if i < node.number_of_keys && &node.keys[i] == key {
            Ok(Some(node.payload[i].clone()))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        // Get the node block this key should be inserted into
        let (mut node, node_id, i) = self.lookup_block(&key)?;
        if i < node.number_of_keys && node.keys[i] == key {
            // Key already exists, replace the value and return the previous one
            let old_value = std::mem::replace(&mut node.payload[i], value);
            self.save_node(node_id, node)?;
            Ok(Some(old_value))
        } else {
            // Insert key into this node and attempt to save or split it
            node.keys.insert(i, key);
            node.payload.insert(i, value);
            node.number_of_keys += 1;
            self.save_node(node_id, node)?;
            Ok(None)
        }
    }

    fn save_node(&mut self, node_id: usize, node: NodeBlock<K, V>) -> Result<()> {
        // Do not split if this node can handle both the number of keys and would not overflow
        let (update_fits, _needed_size) = self.file.can_update(node_id, &node)?;
        if node.number_of_keys < 2 * self.order && update_fits {
            self.file.put(node_id, &node)?;
            Ok(())
        } else {
            todo!()
        }
    }

    fn lookup_block(&self, key: &K) -> Result<(NodeBlock<K, V>, usize, usize)> {
        let mut finished = false;
        let mut node = self.file.get(self.root_id)?;
        let mut node_id = self.root_id;

        while !finished {
            // Search for the key in the current node
            let i = find_key_in_node(key, &node);
            if i < node.number_of_keys {
                if &node.keys[i] == key {
                    // Key found, return the node block and its index in the block
                    return Ok((node, node_id, i));
                } else {
                    // Follow reference to the next child node
                    node_id = node.child_nodes[i];
                    node = self.file.get(node_id)?;
                }
            } else {
                finished = true;
            }
        }

        // Key not found, return the last searched node (which is deepest in the tree)
        // i indicates where the key would have been inserted, in this case it should be the end
        // of the sorted list
        let i = node.number_of_keys;
        Ok((node, node_id, i))
    }
}

#[cfg(test)]
mod tests {
    use crate::BtreeIndex;

    use super::BLOCK_SIZE;

    #[test]
    fn insert_get_static_size() {
        // TODO: use more entries so more than one node is needed
        let nr_entries = 10;

        let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(BLOCK_SIZE).unwrap();

        t.insert(0, 42).unwrap();

        for i in 1..nr_entries {
            t.insert(i, i).unwrap();
        }

        assert_eq!(Some(42), t.get(&0).unwrap());
        for i in 1..nr_entries {
            let v = t.get(&i).unwrap();
            assert_eq!(Some(i), v);
        }
    }
}
