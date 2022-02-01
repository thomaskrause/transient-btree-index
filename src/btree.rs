use crate::{
    error::Result,
    file::{BlockHeader, TemporaryBlockFile, page_aligned_capacity}, Error, PAGE_SIZE,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};

const MIN_BLOCK_SIZE: usize = PAGE_SIZE - BlockHeader::size();

#[derive(Serialize, Deserialize)]
struct Key<K, V> {
    key: K,
    payload: V,
}

#[derive(Serialize, Deserialize)]
struct NodeBlock<K, V> {
    keys: Vec<Key<K, V>>,
    child_nodes: Vec<usize>,
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
    for i in 1..node.keys.len() {
        if &node.keys[i].key == key {
            return i;
        } else if &node.keys[i].key >= key {
            return i - 1;
        }
    }
    node.keys.len() + 1
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
            child_nodes: Vec::default(),
            keys: Vec::default(),
        };
        let root_id = file.allocate_block(MIN_BLOCK_SIZE)?;
        file.put(root_id, &root_node)?;

        Ok(BtreeIndex {
            root_id,
            file,
            order: 128,
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let (node, _, i) = self.lookup_block(key)?;
        if i < node.keys.len() && &node.keys[i].key == key {
            Ok(Some(node.keys[i].payload.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        // Get the leaf node block this key should be inserted into
        let (mut node, node_id, i) = self.lookup_block(&key)?;
        if i < node.keys.len() && node.keys[i].key == key {
            // Key already exists, replace the value and return the previous one
            let old_value = std::mem::replace(&mut node.keys[i].payload, value);
            // TODO: handle overflow because the new block is larger than the old one
            self.file.put(node_id, &node)?;
            Ok(Some(old_value))
        } else {            
            // Insert key into this leaf node and attempt to save or split it
            node.keys.insert(
                i,
                Key {
                    key,
                    payload: value,
                },
            );
            // If the key does not exist yet, the found node must be a leaf node
            self.save_leaf_node(node_id, node)?;
            Ok(None)
        }
    }

    fn save_leaf_node(&mut self, node_id: usize, mut node: NodeBlock<K, V>) -> Result<()> {
        // Sanity check
        if !node.child_nodes.is_empty() {
            return Err(Error::InsertFoundInternalNode);
        }
        let (update_fits, _needed_size) = self.file.can_update(node_id, &node)?;
        if node.keys.len() < 2 * self.order && update_fits {
            // Do not split if this node can handle both the number of keys and would not overflow
            self.file.put(node_id, &node)?;
            Ok(())
        } else {
            // Split into two nodes and attempt to save these
            let split_position = if node.keys.len() < 2 * self.order {
                self.order
            } else {
                node.keys.len() / 2
            };

            // Split all elements belonging to the new right node
            let right_entries = node.keys.split_off(split_position);
        

            let right_node = NodeBlock {
                keys: right_entries,
                child_nodes: Vec::new(),
            };

            // TODO: middle element needs to be pulled up and the new child nodes need to referenced by the parent node

            // Allocate a block for the new right node
            let right_block_size = self.file.serialized_size(&right_node)?;
            let right_aligned_block_size = page_aligned_capacity(right_block_size.try_into()?);
            let right_node_id = self.file.allocate_block(right_aligned_block_size)?;

            // Recursivly save both the left and right node, splitting again if necessary
            self.save_leaf_node(node_id, node)?;
            self.save_leaf_node(right_node_id, right_node)?;
            Ok(())
        }
    }

    fn lookup_block(
        &self,
        key: &K,
    ) -> Result<(NodeBlock<K, V>, usize, usize)> {
        let mut finished = false;
        let mut node = self.file.get(self.root_id)?;
        let mut node_id = self.root_id;

        while !finished {
            // Search for the key in the current node
            let i = find_key_in_node(key, &node);
            if i < node.keys.len() {
                if &node.keys[i].key == key {
                    // Key found, return the node block and its index in the block
                    return Ok((node, node_id, i));
                } else if i < node.child_nodes.len() {
                    // Follow reference to the next child node
                    node_id = node.child_nodes[i];
                    node = self.file.get(node_id)?;
                } else if node.child_nodes.is_empty() {
                    // reached a leaf node
                    finished = true;
                }
            } else {
                finished = true;
            }
        }

        // Key not found, return the last searched node (which is deepest in the tree)
        // i indicates where the key would have been inserted, in this case it should be the end
        // of the sorted list
        let i = node.keys.len();
        Ok((node, node_id, i))
    }
}

#[cfg(test)]
mod tests {
    use crate::BtreeIndex;

    use super::MIN_BLOCK_SIZE;

    #[test]
    fn insert_get_static_size() {
        let nr_entries = 2000;

        let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(MIN_BLOCK_SIZE).unwrap();

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
