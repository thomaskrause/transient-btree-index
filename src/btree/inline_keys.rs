use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::{
    error::Result,
    file::{BlockHeader, FixedSizeTupleFile, TupleFile, VariableSizeTupleFile},
    BtreeConfig, Error,
};
use serde::{de::DeserializeOwned, Serialize};

use node::{NodeFile, SearchResult, StackEntry, MAX_NUMBER_KEYS};

use super::TypeSize;

mod node;

pub trait KeyType: Sized + Ord + Eq + Copy {
    fn into_bytes(self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

macro_rules! impl_key_type {
    ( $type:ident ) => {
        impl KeyType for $type {
            fn into_bytes(self) -> Vec<u8> {
                self.to_le_bytes().into()
            }

            fn from_bytes(bytes: &[u8]) -> Result<Self> {
                Ok(Self::from_le_bytes(bytes.try_into()?))
            }
        }
    };
}

impl_key_type!(u8);
impl_key_type!(u16);
impl_key_type!(u32);
impl_key_type!(u64);
impl_key_type!(i8);
impl_key_type!(i16);
impl_key_type!(i32);
impl_key_type!(i64);

/// B-tree index backed by temporary memory mapped files.
///
/// Operations similar to the interface of [`std::collections::BTreeMap`] are implemented.
/// But since the index works with files, most of them return a `Result` to allow error-handling.
/// Deleting an entry is explicitly not implemented and when memory blocks need to grow fragmentation of the on-disk memory might occur.
///
/// Since serde is used to serialize the keys and values, the types need to implement the [`Serialize`] and [`DeserializeOwned`] traits.
/// Also, only keys and values that implement [`Clone`] can be used.
pub struct InlineKeyBtreeIndex<K, V>
where
    K: KeyType,
    V: Serialize + DeserializeOwned + Clone + Sync,
{
    nodes: node::NodeFile<K>,
    values: Box<dyn TupleFile<V>>,
    root_id: u64,
    last_inserted_node_id: u64,
    order: usize,
    nr_elements: usize,
}

impl<'a, K, V> InlineKeyBtreeIndex<K, V>
where
    K: 'a + KeyType,
    V: 'a + Serialize + DeserializeOwned + Clone + Send + Sync,
{
}

impl<K, V> InlineKeyBtreeIndex<K, V>
where
    K: 'static + KeyType,
    V: 'static + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    /// Create a new instance with the given configuration and capacity in number of elements.
    pub fn with_capacity(
        config: BtreeConfig,
        capacity: usize,
    ) -> Result<InlineKeyBtreeIndex<K, V>> {
        if config.order < 2 {
            return Err(Error::OrderTooSmall(config.order));
        } else if config.order > MAX_NUMBER_KEYS / 2 {
            return Err(Error::OrderTooLarge(config.order));
        }

        let mut nodes = NodeFile::with_capacity(capacity)?;

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

        Ok(InlineKeyBtreeIndex {
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

            if key >= start && key <= end && last_inserted_number_keys < (2 * self.order) - 1 {
                let expected = self.insert_nonfull(self.last_inserted_node_id, key, value)?;
                return Ok(expected);
            }
        }

        let root_number_of_keys = self.nodes.number_of_keys(self.root_id).unwrap_or(0);
        if root_number_of_keys == (2 * self.order) - 1 {
            // Create a new root node, because the current will become full
            let new_root_id = self.nodes.split_root_node(self.root_id, self.order)?;

            let existing = self.insert_nonfull(new_root_id, key, value)?;
            self.root_id = new_root_id;
            Ok(existing)
        } else {
            let existing = self.insert_nonfull(self.root_id, key, value)?;
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

    fn insert_nonfull(&mut self, node_id: u64, key: K, value: V) -> Result<Option<V>> {
        match self.nodes.binary_search(node_id, &key)? {
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
                        self.nodes
                            .set_key(node_id, i, self.nodes.get_key(node_id, i - 1)?)?;
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
                        if key == node_key {
                            // Key already exists and was added to the parent node, replace the payload
                            let payload_id: usize =
                                self.nodes.get_payload(node_id, i)?.try_into()?;
                            let previous_payload = self.values.get_owned(payload_id)?;
                            self.values.put(payload_id, &value)?;
                            self.last_inserted_node_id = node_id;
                            Ok(Some(previous_payload))
                        } else if key > node_key {
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
    K: KeyType,
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
    K: KeyType,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
{
    fn get_key_value_tuple(&self, node: u64, idx: usize) -> Result<(K, V)> {
        let payload_id = self.nodes.get_payload(node, idx)?;
        let value = self.values.get_owned(payload_id.try_into()?)?;
        let key = self.nodes.get_key(node, idx)?;
        Ok((key, value))
    }
}

impl<'a, K, V> Iterator for Range<'a, K, V>
where
    K: KeyType,
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
    K: KeyType,
    V: Sync,
{
    nodes: NodeFile<K>,
    values: Box<dyn TupleFile<V>>,
    stack: Vec<node::StackEntry>,
    phantom: PhantomData<V>,
}

impl<K, V> BtreeIntoIter<K, V>
where
    K: KeyType,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
{
    fn get_key_value_tuple(&self, node: u64, idx: usize) -> Result<(K, V)> {
        let payload_id = self.nodes.get_payload(node, idx)?;
        let value = self.values.get_owned(payload_id.try_into()?)?;
        let key = self.nodes.get_key(node, idx)?;
        Ok((key, value))
    }
}

impl<K, V> Iterator for BtreeIntoIter<K, V>
where
    K: KeyType,
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
