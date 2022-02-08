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
}

impl Default for BtreeConfig {
    fn default() -> Self {
        Self {
            order: 128,
            est_max_key_size: 32,
            est_max_value_size: 32,
        }
    }
}

impl BtreeConfig {
    /// Set the estimated maximum size in bytes for each key.
    ///
    /// Keys can be larger than this, but if this happens too often inside a BTree node
    /// the block might need to be re-allocated, which causes memory fragmentation on the disk
    /// and some main memory overhead for remembering the re-allocated block IDs.
    pub fn with_max_key_size(mut self, est_max_key_size: usize) -> Self {
        self.est_max_key_size = est_max_key_size;
        self
    }

    /// Set the estimated maximum size in bytes for each values.
    pub fn with_max_value_size(mut self, est_max_value_size: usize) -> Self {
        self.est_max_value_size = est_max_value_size;
        self
    }

    /// Sets the order of the tree, which determines how many elements a single node can store.
    pub fn with_order(mut self, order: u8) -> Self {
        self.order = order as usize;
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

        let mut keys =
            TemporaryBlockFile::with_capacity(capacity * (block_size + BlockHeader::size()))?;
        let values = TemporaryBlockFile::with_capacity(
            (capacity * config.est_max_value_size) + BlockHeader::size(),
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

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
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
            self.insert_nonfull(&mut new_root, &key, value)?;
            self.root_id = new_root_id;
        } else {
            self.insert_nonfull(&mut root_node, &key, value)?;
        }
        Ok(())
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

    fn insert_nonfull(&mut self, node: &mut NodeBlock<K>, key: &K, value: V) -> Result<()> {
        match node.keys.binary_search_by(|e| e.key.cmp(key)) {
            Ok(i) => {
                // Key already exists, replace the payload
                self.values.put(node.keys[i].payload_id, &value)?;
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
                } else {
                    // Insert key into correct child
                    // Default to left child
                    let mut c = self.keys.get(node.child_nodes[i])?;
                    // If the child is full, we need to split it
                    if c.number_of_keys() == (2 * self.order) - 1 {
                        let (left, right) = self.split_child(node, i)?;
                        if key > &node.keys[i].key {
                            // Key is now larger, use the newly created right child
                            c = right;
                        } else {
                            // Use the updated left child (which has a new key vector)
                            c = left;
                        }
                    }
                    self.insert_nonfull(&mut c, key, value)?;
                }
            }
        }
        Ok(())
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
mod tests {
    use std::{cmp::Ordering, collections::BTreeMap, fmt::Debug};

    use crate::BtreeIndex;

    use super::*;

    fn check_order<K, V, R>(t: &BtreeIndex<K, V>, range: R)
    where
        K: Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Debug,
        V: Serialize + DeserializeOwned + Clone,
        R: RangeBounds<K>,
    {
        let mut previous: Option<K> = None;
        for e in t.range(range).unwrap() {
            let (k, _v) = e.unwrap();

            if let Some(previous) = previous {
                if &previous >= &k {
                    dbg!(&previous, &k);
                }
                assert_eq!(Ordering::Less, previous.cmp(&k));
            }

            previous = Some(k);
        }
    }

    #[test]
    fn insert_get_static_size() {
        let nr_entries = 2000;

        let config = BtreeConfig::default()
            .with_max_key_size(8)
            .with_max_value_size(8);

        let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 2000).unwrap();

        assert_eq!(true, t.is_empty());

        t.insert(0, 42).unwrap();

        assert_eq!(false, t.is_empty());
        assert_eq!(1, t.len());

        for i in 1..nr_entries {
            t.insert(i, i).unwrap();
        }

        assert_eq!(false, t.is_empty());
        assert_eq!(nr_entries as usize, t.len());

        assert_eq!(true, t.contains_key(&0).unwrap());
        assert_eq!(Some(42), t.get(&0).unwrap());
        for i in 1..nr_entries {
            assert_eq!(true, t.contains_key(&i).unwrap());

            let v = t.get(&i).unwrap();
            assert_eq!(Some(i), v);
        }
        assert_eq!(false, t.contains_key(&nr_entries).unwrap());
        assert_eq!(None, t.get(&nr_entries).unwrap());
        assert_eq!(false, t.contains_key(&5000).unwrap());
        assert_eq!(None, t.get(&5000).unwrap());
    }

    #[test]
    fn range_query_dense() {
        let nr_entries = 2000;

        let config = BtreeConfig::default()
            .with_max_key_size(8)
            .with_max_value_size(8);

        let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 2000).unwrap();

        for i in 0..nr_entries {
            t.insert(i, i).unwrap();
        }

        // Get sub-range
        let result: Result<Vec<_>> = t.range(40..1024).unwrap().collect();
        let result = result.unwrap();
        assert_eq!(984, result.len());
        assert_eq!((40, 40), result[0]);
        assert_eq!((1023, 1023), result[983]);
        check_order(&t, 40..1024);

        // Get complete range
        let result: Result<Vec<_>> = t.range(..).unwrap().collect();
        let result = result.unwrap();
        assert_eq!(2000, result.len());
        assert_eq!((0, 0), result[0]);
        assert_eq!((1999, 1999), result[1999]);
        check_order(&t, ..);
    }

    #[test]
    fn range_query_sparse() {
        let config = BtreeConfig::default()
            .with_max_key_size(8)
            .with_max_value_size(8);

        let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 200).unwrap();

        for i in (0..2000).step_by(10) {
            t.insert(i, i).unwrap();
        }

        assert_eq!(200, t.len());

        // Get sub-range
        let result: Result<Vec<_>> = t.range(40..1200).unwrap().collect();
        let result = result.unwrap();
        assert_eq!(116, result.len());
        assert_eq!((40, 40), result[0]);
        check_order(&t, 40..1200);

        // Get complete range
        let result: Result<Vec<_>> = t.range(..).unwrap().collect();
        let result = result.unwrap();
        assert_eq!(200, result.len());
        assert_eq!((0, 0), result[0]);
        assert_eq!((1990, 1990), result[199]);
        check_order(&t, ..);

        // Check different variants of range queries
        check_order(&t, 40..=1200);
        check_order(&t, 40..);
        check_order(&t, ..1024);
        check_order(&t, ..=1024);
    }

    #[test]
    fn minimal_order() {
        let nr_entries = 2000u64;

        // Too small orders should create an error
        assert_eq!(
            true,
            BtreeIndex::<u64, u64>::with_capacity(
                BtreeConfig::default().with_order(0),
                nr_entries as usize
            )
            .is_err()
        );
        assert_eq!(
            true,
            BtreeIndex::<u64, u64>::with_capacity(
                BtreeConfig::default().with_order(1),
                nr_entries as usize
            )
            .is_err()
        );

        // Test with the minimal order 2
        let config = BtreeConfig::default()
            .with_max_key_size(8)
            .with_max_value_size(8)
            .with_order(2);

        let mut t: BtreeIndex<u64, u64> =
            BtreeIndex::with_capacity(config, nr_entries as usize).unwrap();

        for i in 0..nr_entries {
            t.insert(i, i).unwrap();
        }

        // Get sub-range
        let result: Result<Vec<_>> = t.range(40..1024).unwrap().collect();
        let result = result.unwrap();
        assert_eq!(984, result.len());
        assert_eq!((40, 40), result[0]);
        assert_eq!((1023, 1023), result[983]);
        check_order(&t, 40..1024);

        // Get complete range
        let result: Result<Vec<_>> = t.range(..).unwrap().collect();
        let result = result.unwrap();
        assert_eq!(2000, result.len());
        assert_eq!((0, 0), result[0]);
        assert_eq!((1999, 1999), result[1999]);
        check_order(&t, ..);
    }

    #[test]
    fn sorted_iterator() {
        let config = BtreeConfig::default()
            .with_max_key_size(64)
            .with_max_value_size(64);

        let mut t: BtreeIndex<Vec<u8>, bool> = BtreeIndex::with_capacity(config, 128).unwrap();

        for a in 0..=255 {
            t.insert(vec![1, a], true).unwrap();
        }
        for a in 0..=255 {
            t.insert(vec![0, a], true).unwrap();
        }
        assert_eq!(512, t.len());
        check_order(&t, ..);
    }

    #[test]
    fn control_characters() {
        let input = vec![
            (
                "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\t\u{0}\u{0}\u{0}\u{1f}",
                "",
            ),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("<", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("", ""),
            ("/", ""),
            ("", ""),
            ("\u{12}\u{12}", "\u{12}\u{12}\u{12}\u{12}\u{12}\u{12}"),
            ("", ""),
            ("/", ""),
            ("", ""),
            ("", ""),
        ];

        let mut m = BTreeMap::default();
        let mut t = BtreeIndex::with_capacity(BtreeConfig::default().with_order(2), 1024).unwrap();

        for (key, value) in input {
            m.insert(key.to_string(), value.to_string());
            t.insert(key.to_string(), value.to_string()).unwrap();
        }

        let m: Vec<_> = m.into_iter().collect();
        let t: Result<Vec<_>> = t.range(..).unwrap().collect();
        let t = t.unwrap();

        assert_eq!(m, t);
    }
}
