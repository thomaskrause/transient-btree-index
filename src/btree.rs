use std::{
    cell::RefCell,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{
    error::Result,
    file::{BlockHeader, FixedSizeTupleFile, TupleFile, VariableSizeTupleFile},
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
    V: Serialize + DeserializeOwned + Clone + Sync,
{
    nodes: node::NodeFile<K>,
    values: Box<dyn TupleFile<V>>,
    root_id: u64,
    last_inserted_node_id: u64,
    order: usize,
    nr_elements: usize,
}

#[derive(Clone)]
pub enum TypeSize {
    Estimated(usize),
    Fixed(usize),
}

/// Configuration for a B-tree index.
#[derive(Clone)]
pub struct BtreeConfig {
    order: usize,
    key_size: TypeSize,
    value_size: TypeSize,
    block_cache_size: usize,
}

impl Default for BtreeConfig {
    fn default() -> Self {
        Self {
            order: 84,
            key_size: TypeSize::Estimated(32),
            value_size: TypeSize::Estimated(32),
            block_cache_size: 16,
        }
    }
}

impl BtreeConfig {
    /// Set the estimated maximum size in bytes for each key.
    ///
    /// Keys can be larger than this, but if this happens too often the block for the key
    /// might need to be re-allocated, which causes memory fragmentation on the disk
    /// and some main memory overhead for remembering the re-allocated block IDs.
    pub fn max_key_size(mut self, est_max_key_size: usize) -> Self {
        self.key_size = TypeSize::Estimated(est_max_key_size);
        self
    }

    /// Set the fixed size in bytes for each key.
    ///
    /// If serializing the key needs a fixed number of bytes
    /// (assuming [bincode](https://crates.io/crates/bincode) is used with a fixed integer encoding),
    /// a more efficient internal implementation will be used.
    pub fn fixed_key_size(mut self, key_size: usize) -> Self {
        self.key_size = TypeSize::Fixed(key_size);
        self
    }

    /// Set the estimated maximum size in bytes for each values.
    ///
    /// Values can be larger than this, but if this happens too often the block for the value
    /// might need to be re-allocated, which causes memory fragmentation on the disk
    /// and some main memory overhead for remembering the re-allocated block IDs.
    pub fn max_value_size(mut self, est_max_value_size: usize) -> Self {
        self.value_size = TypeSize::Estimated(est_max_value_size);
        self
    }

    /// Set the fixed size in bytes for each value.
    ///
    /// If serializing the value needs a fixed number of bytes
    /// (assuming [bincode](https://crates.io/crates/bincode) is used with a fixed integer encoding),  
    /// a more efficient internal implementation will be used.
    pub fn fixed_value_size(mut self, value_size: usize) -> Self {
        self.value_size = TypeSize::Fixed(value_size);
        self
    }

    /// Sets the order of the tree, which determines how many elements a single node can store.
    ///
    /// A B-tree is balanced, so the number of keys of a node is between the order and the order times two.
    /// The order must be at least 2 and at most 84 for this implementation, and
    /// it is guaranteed that the internal structure for a node always fits inside a memory page.
    /// The default is to use the maximum number of keys, so the memory page is utilized as much as possible.
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

impl<'a, K, V> BtreeIndex<K, V>
where
    K: 'a + Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Send + Sync,
    V: 'a + Serialize + DeserializeOwned + Clone + Send + Sync,
{
}

impl<K, V> BtreeIndex<K, V>
where
    K: 'static + Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Send + Sync,
    V: 'static + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    /// Create a new instance with the given configuration and capacity in number of elements.
    pub fn with_capacity(config: BtreeConfig, capacity: usize) -> Result<BtreeIndex<K, V>> {
        if config.order < 2 {
            return Err(Error::OrderTooSmall(config.order));
        } else if config.order > MAX_NUMBER_KEYS / 2 {
            return Err(Error::OrderTooLarge(config.order));
        }

        let mut nodes = NodeFile::with_capacity(capacity, &config)?;

        let values: Box<dyn TupleFile<V>> = match config.value_size {
            TypeSize::Estimated(est_max_value_size) => {
                let f = VariableSizeTupleFile::with_capacity(
                    capacity * (est_max_value_size + BlockHeader::size()),
                    config.block_cache_size,
                )?;
                Box::new(f)
            }
            TypeSize::Fixed(fixed_value_size) => {
                let f = FixedSizeTupleFile::with_capacity(
                    capacity * fixed_value_size,
                    fixed_value_size,
                )?;
                Box::new(f)
            }
        };

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
        thread_local! {
            // Since this cell is per thread and can't be accessed outside of
            // this function, is always possible to get a mutable reference to this cell.
            static LAST_READ_NODE_ID: RefCell<u64>  = RefCell::new(0);
        }

        let mut search_root_node_id = self.root_id;

        // Try the node that was read last first in case the the read operation
        // is executed on the next ID. If we can't get the lock, just ignore the
        // hint and search from the top root node.
        LAST_READ_NODE_ID.with(|n| {
            if let Ok(last_read_node_id) = n.try_borrow() {
                let last_read_number_keys =
                    self.nodes.number_of_keys(*last_read_node_id).unwrap_or(0);
                if last_read_number_keys > 0 {
                    if let (Ok(start), Ok(end)) = (
                        self.nodes.get_key(*last_read_node_id, 0),
                        self.nodes
                            .get_key(*last_read_node_id, last_read_number_keys - 1),
                    ) {
                        if key >= start.as_ref() && key <= end.as_ref() {
                            search_root_node_id = *last_read_node_id;
                        }
                    }
                }
            }
        });

        if let Some((node, i)) = self.search(search_root_node_id, key)? {
            let payload_id = self.nodes.get_payload(node, i)?;
            let v = self.values.get_owned(payload_id.try_into()?)?;
            LAST_READ_NODE_ID.with(|last_read_node_id| {
                if let Ok(mut last_read_node_id) = last_read_node_id.try_borrow_mut() {
                    *last_read_node_id = node;
                }
            });
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
        // The range is sorted by smallest first, but popping values from the end of the
        // stack is more effective
        stack.reverse();

        let result = Range {
            stack,
            start,
            end,
            nodes: &self.nodes,
            values: self.values.as_ref(),
            phantom: PhantomData,
        };
        Ok(result)
    }

    /// Return an iterator over all entries and consumes the B-tree index.
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
    ///     for e in b.into_iter()? {
    ///         let (k, v) = e?;
    ///         dbg!(k, v);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn into_iter(self) -> Result<BtreeIntoIter<K, V>> {
        let mut stack = self.nodes.find_range(self.root_id, ..);
        // The range is sorted by smallest first, but popping values from the end of the
        // stack is more effective
        stack.reverse();

        let result = BtreeIntoIter {
            stack,
            nodes: self.nodes,
            values: self.values,
            phantom: PhantomData,
        };
        Ok(result)
    }

    /// Swaps the values for the given keys.
    pub fn swap(&mut self, a: &K, b: &K) -> Result<()> {
        // Get the node ids and position in the node for both keys,
        // fail when they do not exist
        let (a_node, a_pos) = self.search(self.root_id, a)?.ok_or(Error::NonExistingKey)?;
        let (b_node, b_pos) = self.search(self.root_id, b)?.ok_or(Error::NonExistingKey)?;

        // Get the payload IDs for the node positions
        let a_payload = self.nodes.get_payload(a_node, a_pos)?;
        let b_payload = self.nodes.get_payload(b_node, b_pos)?;

        // Swap the payload IDs at these positions
        self.nodes.set_payload(a_node, a_pos, b_payload)?;
        self.nodes.set_payload(b_node, b_pos, a_payload)?;

        Ok(())
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
                    let payload_id = self.values.allocate_block(value_size)?;
                    self.values.put(payload_id, &value)?;

                    // Make space for the new key by moving the other items to the right
                    let number_of_node_keys = self.nodes.number_of_keys(node_id)?;
                    for i in ((i + 1)..=number_of_node_keys).rev() {
                        self.nodes.set_key_id(
                            node_id,
                            i,
                            self.nodes.get_key_id(node_id, i - 1)?,
                        )?;
                        self.nodes.set_payload(
                            node_id,
                            i,
                            self.nodes.get_payload(node_id, i - 1)?,
                        )?;
                    }
                    // Insert new key with payload at the given position
                    self.nodes.set_key_value(node_id, i, key)?;
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

pub struct Range<'a, K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Sync,
{
    start: Bound<K>,
    end: Bound<K>,
    nodes: &'a NodeFile<K>,
    values: &'a dyn TupleFile<V>,
    stack: Vec<node::StackEntry>,
    phantom: PhantomData<V>,
}

impl<'a, K, V> Range<'a, K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
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
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
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

pub struct BtreeIntoIter<K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Sync,
{
    nodes: NodeFile<K>,
    values: Box<dyn TupleFile<V>>,
    stack: Vec<node::StackEntry>,
    phantom: PhantomData<V>,
}

impl<K, V> BtreeIntoIter<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
{
    fn get_key_value_tuple(&self, node: u64, idx: usize) -> Result<(K, V)> {
        let payload_id = self.nodes.get_payload(node, idx)?;
        let value = self.values.get_owned(payload_id.try_into()?)?;
        let key = self.nodes.get_key_owned(node, idx)?;
        Ok((key, value))
    }
}

impl<K, V> Iterator for BtreeIntoIter<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(e) = self.stack.pop() {
            match e {
                StackEntry::Child { parent, idx } => {
                    match self.nodes.get_child_node(parent, idx) {
                        Ok(c) => {
                            // Add all entries for this child node on the stack
                            let mut new_elements = self.nodes.find_range(c, ..);
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
