use std::{
    ops::{Bound, RangeBounds},
    rc::Rc,
};

use crate::{
    error::Result,
    file::{page_aligned_capacity, BlockHeader, TemporaryBlockFile},
    Error,
};
use serde::{de::DeserializeOwned, Serialize};

#[derive(serde_derive::Deserialize, serde_derive::Serialize, Clone)]
struct Key<K, V> {
    key: K,
    payload: V,
}

impl<K, V> Into<(K, V)> for Key<K, V> {
    fn into(self) -> (K, V) {
        (self.key, self.payload)
    }
}

#[derive(serde_derive::Deserialize, serde_derive::Serialize, Clone)]
struct NodeBlock<K, V> {
    id: usize,
    keys: Vec<Key<K, V>>,
    child_nodes: Vec<usize>,
}

impl<K, V> NodeBlock<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    fn number_of_keys(&self) -> usize {
        self.keys.len()
    }

    fn is_leaf(&self) -> bool {
        self.child_nodes.is_empty()
    }

    /// Finds all indexes in the key list that are part of the given range.
    fn find_range<R>(&self, range: R) -> Vec<usize>
    where
        R: RangeBounds<K>,
    {
        // Get first matching index
        let start_offset = match range.start_bound() {
            Bound::Included(key) => self.keys.binary_search_by(|e| e.key.cmp(key)),
            Bound::Excluded(key) => match self.keys.binary_search_by(|e| e.key.cmp(&key)) {
                // Key was found, but should be excluded, so
                Ok(i) => Ok(i + 1),
                Err(i) => Ok(i),
            },
            Bound::Unbounded => Ok(0),
        };

        if let Ok(start_offset) = start_offset {
            let mut result = Vec::with_capacity(self.keys.len() - start_offset);
            for i in start_offset..self.keys.len() {
                let included = match range.end_bound() {
                    Bound::Included(end) => &self.keys[i].key <= end,
                    Bound::Excluded(end) => &self.keys[i].key < end,
                    Bound::Unbounded => true,
                };
                if included {
                    result.push(i)
                } else {
                    break;
                }
            }
            result
        } else {
            vec![]
        }
    }
}

/// Map backed by a single file on disk implemented using a B-tree.
pub struct BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    file: TemporaryBlockFile<NodeBlock<K, V>>,
    root_id: usize,
    order: usize,
    empty: bool,
}

pub struct BtreeConfig {
    order: usize,
    est_max_elem_size: usize,
}

impl Default for BtreeConfig {
    fn default() -> Self {
        Self {
            order: 128,
            est_max_elem_size: 32,
        }
    }
}

impl BtreeConfig {
    /// Set the estimated maximum size in bytes for each element (key + value size).
    ///
    /// Elements can be larger than this, but if this happens too often inside a BTree node
    /// the block might need to be re-allocated, which causes memory fragmentation on the disk
    /// and some main memory overhead for remembering the re-allocated block IDs.
    pub fn with_max_element_size(mut self, est_max_elem_size: usize) -> Self {
        self.est_max_elem_size = est_max_elem_size;
        self
    }

    /// Sets the order of the tree, which determines how many elements a single node can store.
    pub fn with_order(mut self, order: usize) -> Self {
        self.order = order;
        self
    }
}

impl<K, V> BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd + Clone + Ord,
    V: Serialize + DeserializeOwned + Clone,
{
    /// Create a new instance with the given configuration and capacity in number of elements.
    pub fn with_capacity(config: BtreeConfig, capacity: usize) -> Result<BtreeIndex<K, V>> {
        // Estimate the needed block size for the root node
        let empty_struct_size = std::mem::size_of::<NodeBlock<K, V>>();
        let keys_vec_size = config.order * config.est_max_elem_size;
        let child_nodes_size = (config.order + 1) * std::mem::size_of::<usize>();
        let block_size = empty_struct_size + keys_vec_size + child_nodes_size;

        let mut file =
            TemporaryBlockFile::with_capacity(capacity * (block_size + BlockHeader::size()))?;

        // Always add an empty root node
        let root_id = file.allocate_block(page_aligned_capacity(block_size))?;
        let root_node = NodeBlock {
            child_nodes: Vec::default(),
            keys: Vec::default(),
            id: root_id,
        };
        file.put(root_id, &root_node)?;

        Ok(BtreeIndex {
            root_id,
            file,
            order: config.order,
            empty: true,
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let root_node = self.file.get(self.root_id)?;
        if let Some((node, i)) = self.search(root_node, key)? {
            Ok(Some(node.keys[i].payload.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.empty = false;

        let mut root_node = self.file.get(self.root_id)?;
        if root_node.number_of_keys() == (2 * self.order) - 1 {
            // Create a new root node, because the current one is full
            let current_root_size = self.file.serialized_size(&root_node)?;
            let new_root_id = self
                .file
                .allocate_block(page_aligned_capacity(current_root_size.try_into()?))?;

            let mut new_root: NodeBlock<K, V> = NodeBlock {
                id: new_root_id,
                keys: vec![],
                child_nodes: vec![root_node.id],
            };
            self.split_child(&mut new_root, 0)?;
            self.insert_nonfull(&mut new_root, &key, value)?;
            self.root_id = new_root_id;
        } else {
            self.insert_nonfull(&mut root_node, &key, value)?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.empty
    }

    pub fn range<R>(&self, range: R) -> Result<Range<K, V>>
    where
        R: RangeBounds<K>,
    {
        // Start to search at the root node
        let root = Rc::new(self.file.get(self.root_id)?);
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        let idx_range = root.find_range(range);
        let stack = idx_range
            .into_iter()
            .rev()
            .map(|i| (root.clone(), i))
            .collect();
        let result = Range {
            stack,
            start,
            end,
            file: &self.file,
        };
        Ok(result)
    }

    fn search(&self, node: NodeBlock<K, V>, key: &K) -> Result<Option<(NodeBlock<K, V>, usize)>> {
        let mut i = 0;
        while i < node.number_of_keys() && key > &node.keys[i].key {
            i += 1;
        }
        if i < node.number_of_keys() && key == &node.keys[i].key {
            Ok(Some((node, i)))
        } else if node.is_leaf() {
            Ok(None)
        } else {
            // search in the matching child node
            let child_block_id = node.child_nodes[i];
            let child_node = self.file.get(child_block_id)?;
            self.search(child_node, key)
        }
    }

    fn insert_nonfull(&mut self, node: &mut NodeBlock<K, V>, key: &K, value: V) -> Result<()> {
        match node.keys.binary_search_by(|e| e.key.cmp(key)) {
            Ok(i) => {
                // Key already exists, replace the payload
                node.keys[i].payload = value;
                self.file.put(node.id, node)?;
            }
            Err(i) => {
                if node.is_leaf() {
                    // Insert new key with payload at the given position
                    node.keys.insert(
                        i,
                        Key {
                            key: key.clone(),
                            payload: value,
                        },
                    );
                    self.file.put(node.id, node)?;
                } else {
                    // Insert key into correct child
                    // Default to left child
                    let mut c = self.file.get(node.child_nodes[i])?;
                    // If the child is full, we need to split it
                    if c.number_of_keys() == (2 * self.order) - 1 {
                        self.split_child(node, i)?;
                        if key > &node.keys[i].key {
                            // Key is now larger, use the newly created right child
                            c = self.file.get(node.child_nodes[i + 1])?;
                        }
                    }
                    self.insert_nonfull(&mut c, key, value)?;
                }
            }
        }
        Ok(())
    }

    fn split_child(&mut self, parent: &mut NodeBlock<K, V>, i: usize) -> Result<()> {
        // Allocate a new block and use the original child block capacity as hint for the needed capacity
        let mut existing_node = self.file.get(parent.child_nodes[i])?;
        let existing_node_size = self.file.serialized_size(&existing_node)?;
        let new_node_id = self
            .file
            .allocate_block(page_aligned_capacity(existing_node_size.try_into()?))?;

        let new_node_keys = existing_node.keys.split_off(self.order + 1);
        let new_node_children = if existing_node.is_leaf() {
            vec![]
        } else {
            existing_node.child_nodes.split_off(self.order)
        };
        let new_node = NodeBlock {
            child_nodes: new_node_children,
            keys: new_node_keys,
            id: new_node_id,
        };

        // Insert the new child entry and the key
        let split_key = existing_node
            .keys
            .pop()
            .ok_or_else(|| Error::EmptyChildNodeInSplit)?;
        parent.keys.insert(i, split_key);
        parent.child_nodes.insert(i + 1, new_node_id);

        // Save all changed files
        self.file.put(new_node.id, &new_node)?;
        self.file.put(parent.id, parent)?;
        self.file.put(existing_node.id, &existing_node)?;

        Ok(())
    }
}

pub struct Range<'a, K, V> {
    start: Bound<K>,
    end: Bound<K>,
    file: &'a TemporaryBlockFile<NodeBlock<K, V>>,
    stack: Vec<(Rc<NodeBlock<K, V>>, usize)>,
}

impl<'a, K, V> Iterator for Range<'a, K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord,
    V: Clone + Serialize + DeserializeOwned,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((node, i)) = self.stack.pop() {
            let result = node.keys[i].clone().into();
            // Get child node large than this value and add it to the stack
            if let Some(c_id) = node.child_nodes.get(i + 1) {
                match self.file.get(*c_id) {
                    Ok(c) => {
                        let c = Rc::from(c);
                        let matching_child_idx =
                            c.find_range((self.start.clone(), self.end.clone()));
                        self.stack
                            .extend(matching_child_idx.into_iter().rev().map(|i| (c.clone(), i)));
                    }
                    Err(e) => return Some(Err(e)),
                }
            }
            Some(Ok(result))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::BtreeIndex;

    use super::*;

    #[test]
    fn insert_get_static_size() {
        let nr_entries = 2000;

        let config = BtreeConfig::default().with_max_element_size(16);

        let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 2000).unwrap();

        assert_eq!(true, t.is_empty());

        t.insert(0, 42).unwrap();

        assert_eq!(false, t.is_empty());
        for i in 1..nr_entries {
            t.insert(i, i).unwrap();
        }

        assert_eq!(Some(42), t.get(&0).unwrap());
        for i in 1..nr_entries {
            let v = t.get(&i).unwrap();
            assert_eq!(Some(i), v);
        }
    }

    #[test]
    fn range_query() {
        let nr_entries = 2000;

        let config = BtreeConfig::default().with_max_element_size(16);

        let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 2000).unwrap();

        for i in 0..nr_entries {
            t.insert(i, i).unwrap();
        }

        let result : Result<Vec<_>> = t.range(40..1024).unwrap().collect();
        let result = result.unwrap();
        assert_eq!(1024-40, result.len());
        assert_eq!(40, result[0].0);
        assert_eq!(40, result[0].1);
    }
}
