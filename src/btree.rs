use std::{
    marker::PhantomData,
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
struct Key<K> {
    key: K,
    payload_id: usize,
}

#[derive(serde_derive::Deserialize, serde_derive::Serialize, Clone)]
struct NodeBlock<K> {
    id: usize,
    keys: Vec<Key<K>>,
    child_nodes: Vec<usize>,
}

fn find_first_candidate<K>(node: Rc<NodeBlock<K>>, start_bound: Bound<&K>) -> StackEntry<K>
where
    K: Ord + Clone,
{
    match start_bound {
        Bound::Included(key) => {
            let key_pos = node.keys.binary_search_by(|e| e.key.cmp(key));
            match &key_pos {
                // Key was found, start at this position
                Ok(i) => StackEntry::Key {
                    node: node.clone(),
                    idx: *i,
                },
                // Key not found, but key would be inserted at i, so the next child or key could contain the key
                Err(i) => {
                    if node.is_leaf() {
                        // Whenn searching for for example for 5 in a leaf [2,4,8], i would be 3 and we need to
                        // start our search at the third key.
                        // If the search range is after the largest key (e.g. 10 for the previous example),
                        // the binary search will return the length of the vector as insertion position,
                        // effectivly generating an invalid candidate which needs to be filterred later
                        StackEntry::Key {
                            node: node.clone(),
                            idx: *i,
                        }
                    } else {
                        StackEntry::Child {
                            parent: node.clone(),
                            idx: *i,
                        }
                    }
                }
            }
        }
        Bound::Excluded(key) => {
            let key_pos = node.keys.binary_search_by(|e| e.key.cmp(key));
            match &key_pos {
                // Key was found, start at child or key after the key
                Ok(i) => {
                    if node.is_leaf() {
                        StackEntry::Key {
                            node: node.clone(),
                            idx: *i + 1,
                        }
                    } else {
                        StackEntry::Child {
                            parent: node.clone(),
                            idx: *i + 1,
                        }
                    }
                }
                // Key not found, but key would be inserted at i, so the previous child could contain the key
                // E.g. when searching for 5 in [c0, k0=2, c1, k1=4, c2, k3=8, c3 ], i would be 3 and we need to
                // start our search a c2 which is before this key.
                Err(i) => StackEntry::Child {
                    parent: node.clone(),
                    idx: *i - 1,
                },
            }
        }
        Bound::Unbounded => {
            if node.is_leaf() {
                // Return the first key
                StackEntry::Key {
                    node: node.clone(),
                    idx: 0,
                }
            } else {
                // Return the first child
                StackEntry::Child {
                    parent: node.clone(),
                    idx: 0,
                }
            }
        }
    }
}

impl<K> NodeBlock<K>
where
    K: Clone + Ord,
{
    fn number_of_keys(&self) -> usize {
        self.keys.len()
    }

    fn is_leaf(&self) -> bool {
        self.child_nodes.is_empty()
    }

    /// Finds all children and keys that are inside the range
    fn find_range<R>(self, range: R) -> Vec<StackEntry<K>>
    where
        R: RangeBounds<K>,
    {
        let mut result = Vec::with_capacity(self.number_of_keys() + self.child_nodes.len());
        let node = Rc::new(self);

        // Get first matching item for both the key and children list
        let first = find_first_candidate(node, range.start_bound());
        let mut candidate = Some(first);

        // Iterate over all remaining children and keys but stop when end range is reached
        while let Some(item) = &candidate {
            let included = match &item {
                // Always search in child nodes as long as it exists
                StackEntry::Child { parent, idx } => *idx < parent.child_nodes.len(),
                // Check if the key is still in range
                StackEntry::Key { node, idx } => match range.end_bound() {
                    Bound::Included(end) => *idx < node.keys.len() && &node.keys[*idx].key <= end,
                    Bound::Excluded(end) => *idx < node.keys.len() && &node.keys[*idx].key < end,
                    Bound::Unbounded => *idx < node.keys.len(),
                },
            };
            if included {
                result.push(item.clone());

                // get the next candidate
                let next_candidate = match item {
                    StackEntry::Child { parent, idx } => StackEntry::Key {
                        node: parent.clone(),
                        idx: *idx,
                    },
                    StackEntry::Key { node, idx } => {
                        if node.is_leaf() {
                            StackEntry::Key {
                                node: node.clone(),
                                idx: *idx + 1,
                            }
                        } else {
                            StackEntry::Child {
                                parent: node.clone(),
                                idx: *idx + 1,
                            }
                        }
                    }
                };
                candidate = Some(next_candidate);
            } else {
                candidate = None;
            }
        }

        result
    }
}

/// Map backed by a single file on disk implemented using a B-tree.
pub struct BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    keys: TemporaryBlockFile<NodeBlock<K>>,
    values: TemporaryBlockFile<V>,
    root_id: usize,
    order: usize,
    nr_elements: usize,
}

pub struct BtreeConfig {
    order: usize,
    est_max_key_size: usize,
    est_max_value_size: usize,
    block_cache_size: usize,
}

impl Default for BtreeConfig {
    fn default() -> Self {
        Self {
            order: 128,
            est_max_key_size: 32,
            est_max_value_size: 32,
            block_cache_size: 16,
        }
    }
}

impl BtreeConfig {
    /// Set the estimated maximum size in bytes for each key.
    ///
    /// Keys can be larger than this, but if this happens too often inside a BTree node
    /// the block might need to be re-allocated, which causes memory fragmentation on the disk
    /// and some main memory overhead for remembering the re-allocated block IDs.
    pub fn max_key_size(mut self, est_max_key_size: usize) -> Self {
        self.est_max_key_size = est_max_key_size;
        self
    }

    /// Set the estimated maximum size in bytes for each values.
    pub fn max_value_size(mut self, est_max_value_size: usize) -> Self {
        self.est_max_value_size = est_max_value_size;
        self
    }

    /// Sets the order of the tree, which determines how many elements a single node can store.
    pub fn order(mut self, order: u8) -> Self {
        self.order = order as usize;
        self
    }

    /// Sets the number of blocks/pages hold in an internal cache.
    pub fn block_cache_size(mut self, block_cache_size: usize) -> Self {
        self.block_cache_size = block_cache_size;
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
        if config.order < 2 {
            return Err(Error::OrderTooSmall(config.order));
        }

        // Estimate the needed block size for the root node
        let empty_struct_size = std::mem::size_of::<NodeBlock<K>>();
        let keys_vec_size = config.order * config.est_max_key_size;
        let child_nodes_size = (config.order + 1) * std::mem::size_of::<usize>();
        let block_size = empty_struct_size + keys_vec_size + child_nodes_size;

        let mut keys = TemporaryBlockFile::with_capacity(
            capacity * (block_size + BlockHeader::size()),
            config.block_cache_size,
        )?;
        let values = TemporaryBlockFile::with_capacity(
            (capacity * config.est_max_value_size) + BlockHeader::size(),
            config.block_cache_size,
        )?;

        // Always add an empty root node
        let root_id = keys.allocate_block(page_aligned_capacity(block_size))?;
        let root_node = NodeBlock {
            child_nodes: Vec::default(),
            keys: Vec::default(),
            id: root_id,
        };
        keys.put(root_id, &root_node)?;

        Ok(BtreeIndex {
            root_id,
            keys,
            values,
            order: config.order,
            nr_elements: 0,
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let root_node = self.keys.get(self.root_id)?;
        if let Some((node, i)) = self.search(root_node, key)? {
            let v = self.values.get(node.keys[i].payload_id)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub fn contains_key(&self, key: &K) -> Result<bool> {
        let root_node = self.keys.get(self.root_id)?;
        if let Some(_) = self.search(root_node, key)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        let mut root_node = self.keys.get(self.root_id)?;
        if root_node.number_of_keys() == (2 * self.order) - 1 {
            // Create a new root node, because the current will become full
            let current_root_size = self.keys.serialized_size(&root_node)?;
            let new_root_id = self
                .keys
                .allocate_block(page_aligned_capacity(current_root_size.try_into()?))?;

            let mut new_root: NodeBlock<K> = NodeBlock {
                id: new_root_id,
                keys: vec![],
                child_nodes: vec![root_node.id],
            };
            self.split_child(&mut new_root, 0)?;
            let existing = self.insert_nonfull(&mut new_root, &key, value)?;
            self.root_id = new_root_id;
            Ok(existing)
        } else {
            let existing = self.insert_nonfull(&mut root_node, &key, value)?;
            Ok(existing)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nr_elements == 0
    }

    pub fn len(&self) -> usize {
        self.nr_elements
    }

    pub fn range<R>(&self, range: R) -> Result<Range<K, V>>
    where
        R: RangeBounds<K>,
    {
        // Start to search at the root node
        let root = self.keys.get(self.root_id)?;
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        let mut stack = root.find_range(range);
        // The range is sorted by smallest first, but poping values from the end of the
        // stack is more effective
        stack.reverse();

        let result = Range {
            stack,
            start,
            end,
            keys: &self.keys,
            values: &self.values,
            phantom: PhantomData,
        };
        Ok(result)
    }

    fn search(&self, node: NodeBlock<K>, key: &K) -> Result<Option<(NodeBlock<K>, usize)>> {
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
            let child_node = self.keys.get(child_block_id)?;
            self.search(child_node, key)
        }
    }

    fn insert_nonfull(&mut self, node: &mut NodeBlock<K>, key: &K, value: V) -> Result<Option<V>> {
        match node.keys.binary_search_by(|e| e.key.cmp(key)) {
            Ok(i) => {
                // Key already exists, replace the payload
                let payload_id = node.keys[i].payload_id;
                let previous_payload = self.values.get(payload_id)?;
                self.values.put(payload_id, &value)?;
                Ok(Some(previous_payload))
            }
            Err(i) => {
                if node.is_leaf() {
                    // Insert new key with payload at the given position
                    let value_size: usize = self.values.serialized_size(&value)?.try_into()?;
                    let payload_id = self
                        .values
                        .allocate_block(value_size + BlockHeader::size())?;
                    self.values.put(payload_id, &value)?;
                    node.keys.insert(
                        i,
                        Key {
                            key: key.clone(),
                            payload_id,
                        },
                    );
                    self.keys.put(node.id, node)?;
                    self.nr_elements += 1;
                    Ok(None)
                } else {
                    // Insert key into correct child
                    // Default to left child
                    let mut c = self.keys.get(node.child_nodes[i])?;
                    // If the child is full, we need to split it
                    if c.number_of_keys() == (2 * self.order) - 1 {
                        let (mut left, mut right) = self.split_child(node, i)?;
                        if key == &node.keys[i].key {
                            // Key already exists and was added to the parent node, replace the payload
                            let payload_id = node.keys[i].payload_id;
                            let previous_payload = self.values.get(payload_id)?;
                            self.values.put(payload_id, &value)?;
                            Ok(Some(previous_payload))
                        } else if key > &node.keys[i].key {
                            // Key is now larger, use the newly created right child
                            let existing = self.insert_nonfull(&mut right, key, value)?;
                            Ok(existing)
                        } else {
                            // Use the updated left child (which has a new key vector)
                            let existing = self.insert_nonfull(&mut left, key, value)?;
                            Ok(existing)
                        }
                    } else {
                        let existing = self.insert_nonfull(&mut c, key, value)?;
                        Ok(existing)
                    }
                }
            }
        }
    }

    fn split_child(
        &mut self,
        parent: &mut NodeBlock<K>,
        i: usize,
    ) -> Result<(NodeBlock<K>, NodeBlock<K>)> {
        // Allocate a new block and use the original child block capacity as hint for the needed capacity
        let mut existing_node = self.keys.get(parent.child_nodes[i])?;
        let existing_node_size = self.keys.serialized_size(&existing_node)?;
        let new_node_id = self
            .keys
            .allocate_block(page_aligned_capacity(existing_node_size.try_into()?))?;

        let new_node_keys = existing_node.keys.split_off(self.order);
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
        self.keys.put(new_node.id, &new_node)?;
        self.keys.put(parent.id, parent)?;
        self.keys.put(existing_node.id, &existing_node)?;

        Ok((existing_node, new_node))
    }
}

#[derive(Clone)]
enum StackEntry<K> {
    Child {
        parent: Rc<NodeBlock<K>>,
        idx: usize,
    },
    Key {
        node: Rc<NodeBlock<K>>,
        idx: usize,
    },
}

pub struct Range<'a, K, V> {
    start: Bound<K>,
    end: Bound<K>,
    keys: &'a TemporaryBlockFile<NodeBlock<K>>,
    values: &'a TemporaryBlockFile<V>,
    stack: Vec<StackEntry<K>>,
    phantom: PhantomData<V>,
}

impl<'a, K, V> Iterator for Range<'a, K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord,
    V: Clone + Serialize + DeserializeOwned,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(e) = self.stack.pop() {
            match e {
                StackEntry::Child { parent, idx } => {
                    match self.keys.get(parent.child_nodes[idx]) {
                        Ok(c) => {
                            // Add all entries for this child node on the stack
                            let mut new_elements =
                                c.find_range((self.start.clone(), self.end.clone()));
                            new_elements.reverse();
                            self.stack.extend(new_elements.into_iter());
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                StackEntry::Key { node, idx } => {
                    let payload_id = node.keys[idx].payload_id;
                    match self.values.get(payload_id) {
                        Ok(v) => {
                            return Some(Ok((node.keys[idx].key.clone(), v)));
                        }
                        Err(e) => {
                            return Some(Err(e));
                        }
                    }
                }
            }
        }

        return None;
    }
}

#[cfg(test)]
mod tests;
