use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{
    error::Result,
    file::{BlockHeader, TemporaryBlockFile},
    Error,
};
use serde::{de::DeserializeOwned, Serialize};

use self::node::{NodeFile, SearchResult, StackEntry, MAX_NUMBER_KEYS};

mod node;

/// B-tree index backed by temporary memory mapped files.
///
/// Operations similar to the interface of [`std::collections::BTreeMap`] are implemented.
/// But since the index works with files, most of them return a `Result` to allow error-handling.
/// Deleting an entry is explicitly not implemented and when memory blocks need to grow fragmentation of the on-disk memory might occur.
///
/// Since serde is used to serialize the keys and values, the types need to implement the [`Serialize`] and [`DeserializeOwned`] traits.
/// Also, only keys and values that implement [`Clone`] can be used.
pub struct BtreeIndex<K, V>
where
    K: Serialize + DeserializeOwned + PartialOrd + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    nodes: node::NodeFile<K>,
    values: TemporaryBlockFile<V>,
    root_id: u64,
    last_inserted_node_id: u64,
    order: usize,
    nr_elements: usize,
}

/// Configuration for a B-tree index.
pub struct BtreeConfig {
    order: usize,
    est_max_key_size: usize,
    est_max_value_size: usize,
    block_cache_size: usize,
}

impl Default for BtreeConfig {
    fn default() -> Self {
        Self {
            order: 84,
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
    ///
    /// A B-tree is balanced so the number of keys of a node is between the order and the order times two.
    /// The order must be at least 2 for this implementation.
    pub fn order(mut self, order: u8) -> Self {
        self.order = order as usize;
        self
    }

    /// Sets the number of blocks/pages to hold in an internal cache.
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
        } else if config.order > MAX_NUMBER_KEYS / 2 {
            return Err(Error::OrderTooLarge(config.order));
        }
        let capacity_in_blocks = capacity / config.order;

        let mut nodes = NodeFile::with_capacity(capacity / config.order, &config)?;

        let values = TemporaryBlockFile::with_capacity(
            (capacity_in_blocks * config.est_max_value_size) + BlockHeader::size(),
            config.block_cache_size,
        )?;

        // Always add an empty root node
        let root_id = nodes.allocate_new_node()?;

        Ok(BtreeIndex {
            root_id,
            nodes,
            values,
            order: config.order,
            nr_elements: 0,
            last_inserted_node_id: root_id,
        })
    }

    /// Searches for a key in the index and returns the value if found.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        if let Some((node, i)) = self.search(self.root_id, key)? {
            let payload_id = self.nodes.get_payload(node, i)?;
            let v = self.values.get_owned(payload_id.try_into()?)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    /// Returns whether the index contains the given key.
    pub fn contains_key(&self, key: &K) -> Result<bool> {
        Ok(self.search(self.root_id, key)?.is_some())
    }

    /// Insert a new element into the index.
    ///
    /// Existing values will be overwritten and returned.
    /// If the operation fails, you should assume that the whole index is corrupted.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        // On sorted insert, the last inserted block might the one we need to insert the key into
        let last_inserted_number_keys = self
            .nodes
            .number_of_keys(self.last_inserted_node_id)
            .unwrap_or(0);
        if last_inserted_number_keys > 0 {
            let start = self.nodes.get_key(self.last_inserted_node_id, 0)?;
            let end = self
                .nodes
                .get_key(self.last_inserted_node_id, last_inserted_number_keys - 1)?;

            if &key >= start.as_ref()
                && &key <= end.as_ref()
                && last_inserted_number_keys < (2 * self.order) - 1
            {
                let expected = self.insert_nonfull(self.last_inserted_node_id, &key, value)?;
                return Ok(expected);
            }
        }

        let root_number_of_keys = self.nodes.number_of_keys(self.root_id).unwrap_or(0);
        if root_number_of_keys == (2 * self.order) - 1 {
            // Create a new root node, because the current will become full
            let new_root_id = self.nodes.split_root_node(self.root_id, self.order)?;

            let existing = self.insert_nonfull(new_root_id, &key, value)?;
            self.root_id = new_root_id;
            Ok(existing)
        } else {
            let existing = self.insert_nonfull(self.root_id, &key, value)?;
            Ok(existing)
        }
    }

    /// Returns true if the index does not contain any elements.
    pub fn is_empty(&self) -> bool {
        self.nr_elements == 0
    }

    /// Returns the length of the index.
    pub fn len(&self) -> usize {
        self.nr_elements
    }

    /// Return an iterator over a range of keys.
    ///
    /// If you want to iterate over all entries of the index, use the unbounded `..` iterator.
    ///
    /// # Example
    ///
    /// ```rust
    /// use transient_btree_index::{BtreeConfig, BtreeIndex, Error};
    ///
    /// fn main() -> std::result::Result<(), Error> {
    ///     let mut b = BtreeIndex::<u16,u16>::with_capacity(BtreeConfig::default(), 10)?;
    ///     b.insert(1,2)?;
    ///     b.insert(200, 4)?;
    ///     b.insert(20, 3)?;
    ///
    ///     for e in b.range(..)? {
    ///         let (k, v) = e?;
    ///         dbg!(k, v);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn range<R>(&self, range: R) -> Result<Range<K, V>>
    where
        R: RangeBounds<K>,
    {
        // Start to search at the root node
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        let mut stack = self.nodes.find_range(self.root_id, range);
        // The range is sorted by smallest first, but poping values from the end of the
        // stack is more effective
        stack.reverse();

        let result = Range {
            stack,
            start,
            end,
            nodes: &self.nodes,
            values: &self.values,
            phantom: PhantomData,
        };
        Ok(result)
    }

    fn search(&self, node_id: u64, key: &K) -> Result<Option<(u64, usize)>> {
        match self.nodes.binary_search(node_id, key)? {
            SearchResult::Found(i) => Ok(Some((node_id, i))),
            SearchResult::NotFound(i) => {
                if self.nodes.is_leaf(node_id)? {
                    Ok(None)
                } else {
                    // search in the matching child node
                    let child_node_id = self.nodes.get_child_node(node_id, i)?;
                    self.search(child_node_id, key)
                }
            }
        }
    }

    fn insert_nonfull(&mut self, node_id: u64, key: &K, value: V) -> Result<Option<V>> {
        match self.nodes.binary_search(node_id, key)? {
            SearchResult::Found(i) => {
                // Key already exists, replace the payload
                let payload_id = self.nodes.get_payload(node_id, i)?.try_into()?;
                let previous_payload = self.values.get_owned(payload_id)?;
                self.values.put(payload_id, &value)?;
                self.last_inserted_node_id = node_id;
                Ok(Some(previous_payload))
            }
            SearchResult::NotFound(i) => {
                if self.nodes.is_leaf(node_id)? {
                    let value_size: usize = self.values.serialized_size(&value)?.try_into()?;
                    let payload_id = self
                        .values
                        .allocate_block(value_size + BlockHeader::size())?;
                    self.values.put(payload_id, &value)?;

                    // Make space for the new key by moving the other items to the right
                    let number_of_node_keys = self.nodes.number_of_keys(node_id)?;
                    for i in ((i + 1)..=number_of_node_keys).rev() {
                        self.nodes.set_key(
                            node_id,
                            i,
                            self.nodes.get_key(node_id, i - 1)?.as_ref(),
                        )?;
                        self.nodes.set_payload(
                            node_id,
                            i,
                            self.nodes.get_payload(node_id, i - 1)?,
                        )?;
                    }
                    // Insert new key with payload at the given position
                    self.nodes.set_key(node_id, i, key)?;
                    self.nodes.set_payload(node_id, i, payload_id.try_into()?)?;
                    self.nr_elements += 1;
                    self.last_inserted_node_id = node_id;
                    Ok(None)
                } else {
                    // Insert key into correct child
                    // Default to left child
                    let child_id = self.nodes.get_child_node(node_id, i)?;
                    // If the child is full, we need to split it
                    if self.nodes.number_of_keys(child_id)? == (2 * self.order) - 1 {
                        let (left, right) = self.nodes.split_child(node_id, i, self.order)?;
                        let node_key = self.nodes.get_key(node_id, i)?;
                        if key == node_key.as_ref() {
                            // Key already exists and was added to the parent node, replace the payload
                            let payload_id: usize =
                                self.nodes.get_payload(node_id, i)?.try_into()?;
                            let previous_payload = self.values.get_owned(payload_id)?;
                            self.values.put(payload_id, &value)?;
                            self.last_inserted_node_id = node_id;
                            Ok(Some(previous_payload))
                        } else if key > node_key.as_ref() {
                            // Key is now larger, use the newly created right child
                            let existing = self.insert_nonfull(right, key, value)?;
                            Ok(existing)
                        } else {
                            // Use the updated left child (which has a new key vector)
                            let existing = self.insert_nonfull(left, key, value)?;
                            Ok(existing)
                        }
                    } else {
                        let existing = self.insert_nonfull(child_id, key, value)?;
                        Ok(existing)
                    }
                }
            }
        }
    }
}

pub struct Range<'a, K, V> {
    start: Bound<K>,
    end: Bound<K>,
    nodes: &'a NodeFile<K>,
    values: &'a TemporaryBlockFile<V>,
    stack: Vec<node::StackEntry>,
    phantom: PhantomData<V>,
}

impl<'a, K, V> Range<'a, K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord,
    V: Clone + Serialize + DeserializeOwned,
{
    fn get_key_value_tuple(&self, node: u64, idx: usize) -> Result<(K, V)> {
        let payload_id = self.nodes.get_payload(node, idx)?;
        let value = self.values.get_owned(payload_id.try_into()?)?;
        let key = self.nodes.get_key_owned(node, idx)?;
        Ok((key, value))
    }
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
                    match self.nodes.get_child_node(parent, idx) {
                        Ok(c) => {
                            // Add all entries for this child node on the stack
                            let mut new_elements = self
                                .nodes
                                .find_range(c, (self.start.clone(), self.end.clone()));
                            new_elements.reverse();
                            self.stack.extend(new_elements.into_iter());
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                StackEntry::Key { node, idx } => match self.get_key_value_tuple(node, idx) {
                    Ok(result) => {
                        return Some(Ok(result));
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                },
            }
        }

        None
    }
}

#[cfg(test)]
mod tests;
