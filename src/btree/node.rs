use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::error::Result;
use crate::file::{BlockHeader, TemporaryBlockFile};
use crate::{BtreeConfig, Error};
use binary_layout::prelude::*;
use memmap2::{MmapMut, MmapOptions};
use serde::de::DeserializeOwned;
use serde::Serialize;

const NODE_BLOCK_SIZE: usize = 4081;
const NODE_BLOCK_ALIGNED_SIZE: usize = 4096;

pub const MAX_NUMBER_KEYS: usize = 169;
const MAX_NUMBER_CHILD_NODES: usize = MAX_NUMBER_KEYS + 1;

// Defines a single BTree node with references to the actual values in a tuple file
// This node can up to 1361 keys and 1362 child node references.
define_layout!(node, LittleEndian, {
    id: u64,
    num_keys: u64,
    is_leaf: u8,
    keys: [u8; MAX_NUMBER_KEYS*8],
    payloads: [u8; MAX_NUMBER_KEYS*8],
    child_nodes: [u8; MAX_NUMBER_CHILD_NODES*8],
});

pub struct NodeFile<K> {
    free_space_offset: usize,
    mmap: MmapMut,
    keys: TemporaryBlockFile<K>,
}

pub enum SearchResult {
    Found(usize),
    NotFound(usize),
}

#[derive(Clone)]
pub enum StackEntry {
    Child { parent: u64, idx: usize },
    Key { node: u64, idx: usize },
}

impl<K> NodeFile<K>
where
    K: Serialize + DeserializeOwned + Clone + Ord,
{
    pub fn with_capacity(capacity: usize, config: &BtreeConfig) -> Result<NodeFile<K>> {
        // Create an anonymous memory mapped file with the capacity as size
        let capacity = capacity.max(1);
        let mmap = MmapOptions::new()
            .stack()
            .len(capacity * NODE_BLOCK_ALIGNED_SIZE)
            .map_anon()?;

        // Each node can hold 1361 keys, so we need the space for them as well
        let number_of_keys = capacity * 1361;
        let keys = TemporaryBlockFile::with_capacity(
            (number_of_keys * config.est_max_value_size) + BlockHeader::size(),
            config.block_cache_size,
        )?;

        Ok(NodeFile {
            mmap,
            keys,
            free_space_offset: 0,
        })
    }

    /// Allocate a new node.
    ///
    /// Returns the ID of the new node.
    pub fn allocate_new_node(&mut self) -> Result<u64> {
        // Make sure we still have enough space left
        let new_offset = self.free_space_offset + NODE_BLOCK_ALIGNED_SIZE;
        self.grow(new_offset)?;

        // Return the old start of free space as block index
        let result: u64 = (self.free_space_offset / NODE_BLOCK_ALIGNED_SIZE).try_into()?;

        // Initialize some of the values
        self.get_mut(result)?.id_mut().write(result);
        self.get_mut(result)?.num_keys_mut().write(0);
        self.get_mut(result)?.is_leaf_mut().write(1);

        // The next free block can be added after this block
        self.free_space_offset = new_offset;
        Ok(result)
    }

    pub fn number_of_keys(&self, node_id: u64) -> Result<usize> {
        let view = self.get(node_id)?;
        Ok(view.num_keys().read() as usize)
    }

    pub fn number_of_children(&self, node_id: u64) -> Result<usize> {
        if !self.is_leaf(node_id)? {
            Ok(self.number_of_keys(node_id)? + 1)
        } else {
            Ok(0)
        }
    }

    pub fn is_leaf(&self, node_id: u64) -> Result<bool> {
        let view = self.get(node_id)?;
        Ok(view.is_leaf().read() != 0)
    }

    /// Finds all children and keys that are inside the range
    pub fn find_range<R>(&self, node_id: u64, range: R) -> Vec<StackEntry>
    where
        R: RangeBounds<K>,
    {
        let mut result: Vec<StackEntry> =
            Vec::with_capacity(2 * (self.number_of_keys(node_id).unwrap_or(1024) + 1));

        // Get first matching item for both the key and children list
        let mut candidate = self.find_first_candidate(node_id, range.start_bound()).ok();

        // Iterate over all remaining children and keys but stop when end range is reached
        while let Some(item) = candidate {
            let included = match &item {
                // Always search in child nodes as long as it exists
                StackEntry::Child { parent, idx } => {
                    *idx < self.number_of_children(*parent).unwrap_or(0)
                }
                // Check if the key is still in range
                StackEntry::Key { node, idx } => match range.end_bound() {
                    Bound::Included(end) => {
                        if let Ok(key) = self.get_key(*node, *idx) {
                            key.as_ref() <= end
                        } else {
                            false
                        }
                    }
                    Bound::Excluded(end) => {
                        if let Ok(key) = self.get_key(*node, *idx) {
                            key.as_ref() < end
                        } else {
                            false
                        }
                    }
                    Bound::Unbounded => *idx < self.number_of_keys(*node).unwrap_or(0),
                },
            };
            if included {
                result.push(item.clone());

                // get the next candidate
                let next_candidate = match item {
                    StackEntry::Child { parent, idx } => StackEntry::Key { node: parent, idx },
                    StackEntry::Key { node, idx } => {
                        if self.is_leaf(node).unwrap_or(false) {
                            StackEntry::Key { node, idx: idx + 1 }
                        } else {
                            StackEntry::Child {
                                parent: node,
                                idx: idx + 1,
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

    fn find_first_candidate(&self, node_id: u64, start_bound: Bound<&K>) -> Result<StackEntry> {
        let result = match start_bound {
            Bound::Included(key) => {
                let key_pos = self.binary_search(node_id, key)?;
                match &key_pos {
                    // Key was found, start at this position
                    SearchResult::Found(i) => StackEntry::Key {
                        node: node_id,
                        idx: *i,
                    },
                    // Key not found, but key would be inserted at i, so the next child or key could contain the key
                    SearchResult::NotFound(i) => {
                        if self.is_leaf(node_id)? {
                            // When searching for for example for 5 in a leaf [2,4,8], i would be 3 and we need to
                            // start our search at the third key.
                            // If the search range is after the largest key (e.g. 10 for the previous example),
                            // the binary search will return the length of the vector as insertion position,
                            // effectivly generating an invalid candidate which needs to be filterred later
                            StackEntry::Key {
                                node: node_id,
                                idx: *i,
                            }
                        } else {
                            StackEntry::Child {
                                parent: node_id,
                                idx: *i,
                            }
                        }
                    }
                }
            }
            Bound::Excluded(key) => {
                let key_pos = self.binary_search(node_id, key)?;
                match &key_pos {
                    // Key was found, start at child or key after the key
                    SearchResult::Found(i) => {
                        if self.is_leaf(node_id)? {
                            StackEntry::Key {
                                node: node_id,
                                idx: *i + 1,
                            }
                        } else {
                            StackEntry::Child {
                                parent: node_id,
                                idx: *i + 1,
                            }
                        }
                    }
                    // Key not found, but key would be inserted at i, so the previous child could contain the key
                    // E.g. when searching for 5 in [c0, k0=2, c1, k1=4, c2, k2=8, c3 ], i would be 2 and we need to
                    // start our search a c2 which is before this key.
                    SearchResult::NotFound(i) => {
                        if self.is_leaf(node_id)? {
                            StackEntry::Key {
                                node: node_id,
                                idx: *i,
                            }
                        } else {
                            StackEntry::Child {
                                parent: node_id,
                                idx: *i,
                            }
                        }
                    }
                }
            }
            Bound::Unbounded => {
                if self.is_leaf(node_id)? {
                    // Return the first key
                    StackEntry::Key {
                        node: node_id,
                        idx: 0,
                    }
                } else {
                    // Return the first child
                    StackEntry::Child {
                        parent: node_id,
                        idx: 0,
                    }
                }
            }
        };
        Ok(result)
    }

    /// Get a block with the given id give ownership of the result to the caller.
    pub fn get_key_owned(&self, node_id: u64, i: usize) -> Result<K> {
        let view = self.get(node_id)?;
        let n: usize = view.num_keys().read() as usize;
        if i < n && i < MAX_NUMBER_KEYS {
            let offset = i * 8;
            let key_id: u64 =
                u64::from_le_bytes(view.keys().data()[offset..(offset + 8)].try_into()?);
            let result = self.keys.get_owned(key_id.try_into()?)?;
            Ok(result)
        } else {
            Err(Error::KeyIndexOutOfBounds { idx: i, len: n })
        }
    }

    pub fn get_key(&self, node_id: u64, i: usize) -> Result<Arc<K>> {
        let view = self.get(node_id)?;
        let n: usize = view.num_keys().read() as usize;
        if i < n && i < MAX_NUMBER_KEYS {
            let offset = i * 8;
            let key_id: u64 =
                u64::from_le_bytes(view.keys().data()[offset..(offset + 8)].try_into()?);
            let result = self.keys.get(key_id.try_into()?)?;
            Ok(result)
        } else {
            Err(Error::KeyIndexOutOfBounds { idx: i, len: n })
        }
    }

    pub fn set_key(&mut self, node_id: u64, i: usize, key: &K) -> Result<()> {
        let n: usize = self.get(node_id)?.num_keys().read() as usize;
        if i <= n && i < MAX_NUMBER_KEYS {
            let offset = i * 8;
            let key_size: usize = self.keys.serialized_size(key)?.try_into()?;
            let key_id = self.keys.allocate_block(key_size + BlockHeader::size())?;
            self.keys.put(key_id, key)?;

            let key_id: u64 = key_id.try_into()?;
            let key_id = key_id.to_le_bytes();
            let mut view = self.get_mut(node_id)?;

            view.keys_mut().data_mut()[offset..(offset + 8)].copy_from_slice(&key_id);

            if i == n {
                // The key was inserted at the end of the list
                let mut view = self.get_mut(node_id)?;
                let n: u64 = (n + 1).try_into()?;
                view.num_keys_mut().write(n);
            }
            Ok(())
        } else {
            Err(Error::KeyIndexOutOfBounds { idx: i, len: n })
        }
    }

    pub fn get_payload(&self, node_id: u64, i: usize) -> Result<u64> {
        let view = self.get(node_id)?;
        let n: usize = view.num_keys().read() as usize;
        if i < n && i < MAX_NUMBER_KEYS {
            let offset = i * 8;
            let result: u64 =
                u64::from_le_bytes(view.payloads().data()[offset..(offset + 8)].try_into()?);
            Ok(result)
        } else {
            Err(Error::KeyIndexOutOfBounds { idx: i, len: n })
        }
    }

    pub fn set_payload(&mut self, node_id: u64, i: usize, value: u64) -> Result<()> {
        let mut view = self.get_mut(node_id)?;
        let n: usize = view.num_keys().read() as usize;
        if i < n && i < MAX_NUMBER_KEYS {
            let offset = i * 8;
            let value = value.to_le_bytes();
            view.payloads_mut().data_mut()[offset..(offset + 8)].copy_from_slice(&value);
            Ok(())
        } else {
            Err(Error::KeyIndexOutOfBounds { idx: i, len: n })
        }
    }

    pub fn get_child_node(&self, node_id: u64, i: usize) -> Result<u64> {
        let view = self.get(node_id)?;
        let n: usize = view.num_keys().read() as usize;
        let has_children: bool = view.is_leaf().read() == 0;
        if has_children && i < (n + 1) && i < MAX_NUMBER_CHILD_NODES {
            let offset = i * 8;
            let result: u64 =
                u64::from_le_bytes(view.child_nodes().data()[offset..(offset + 8)].try_into()?);
            Ok(result)
        } else {
            Err(Error::KeyIndexOutOfBounds { idx: i, len: n })
        }
    }

    pub fn set_child_node(&mut self, node_id: u64, i: usize, value: u64) -> Result<()> {
        let mut view = self.get_mut(node_id)?;
        let has_children: bool = view.is_leaf().read() == 0;
        let n: usize = if has_children {
            (view.num_keys().read() as usize) + 1
        } else {
            0
        };

        if i <= n && i < MAX_NUMBER_CHILD_NODES {
            let offset = i * 8;
            let value = value.to_le_bytes();
            view.child_nodes_mut().data_mut()[offset..(offset + 8)].copy_from_slice(&value);
            view.is_leaf_mut().write(0);
            Ok(())
        } else {
            Err(Error::KeyIndexOutOfBounds { idx: i, len: n })
        }
    }

    pub fn binary_search(&self, node_id: u64, key: &K) -> Result<SearchResult> {
        let mut size = self.number_of_keys(node_id).unwrap_or(0);
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;

            let mid_key = self.get_key(node_id, mid).unwrap();
            let cmp = mid_key.as_ref().cmp(key);

            if cmp == Ordering::Less {
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                right = mid;
            } else {
                return Ok(SearchResult::Found(mid));
            }

            size = right - left;
        }
        Ok(SearchResult::NotFound(left))
    }

    pub fn split_child(
        &mut self,
        parent_node_id: u64,
        child_idx: usize,
        split_at: usize,
    ) -> Result<(u64, u64)> {
        let existing_node = self.get_child_node(parent_node_id, child_idx)?;
        // Allocate a new block for the new child node
        let new_node_id = self.split_off(existing_node, split_at)?;

        // The last element of the existing node is dangling without a child node,
        // use it as the key for the parent node
        let split_key = self.get_key(existing_node, split_at - 1)?;
        let split_payload = self.get_payload(existing_node, split_at - 1)?;
        let mut existing_node_view = self.get_mut(existing_node)?;
        existing_node_view
            .num_keys_mut()
            .write((split_at - 1).try_into()?);

        // Make space for the new entry in the parent node
        for i in ((child_idx + 1)..=self.number_of_keys(parent_node_id)?).rev() {
            self.set_key(
                parent_node_id,
                i,
                self.get_key(parent_node_id, i - 1)?.as_ref(),
            )?;
            self.set_payload(parent_node_id, i, self.get_payload(parent_node_id, i - 1)?)?;
        }
        for i in ((child_idx + 1)..=self.number_of_children(parent_node_id)?).rev() {
            self.set_child_node(
                parent_node_id,
                i,
                self.get_child_node(parent_node_id, i - 1)?,
            )?;
        }

        // Insert the new child entry, the key and the payload into the parent node
        self.set_key(parent_node_id, child_idx, &split_key)?;
        self.set_payload(parent_node_id, child_idx, split_payload)?;
        self.set_child_node(parent_node_id, child_idx + 1, new_node_id)?;

        Ok((existing_node, new_node_id))
    }

    pub fn split_root_node(&mut self, old_root_id: u64, split_at: usize) -> Result<u64> {
        let new_root_id = self.allocate_new_node()?;

        let new_node_id = self.split_off(old_root_id, split_at)?;

        // The last element of the previous root node is dangling without a child node,
        // use it as the key for the parent node
        let split_key = self.get_key(old_root_id, split_at - 1)?;
        let split_payload = self.get_payload(old_root_id, split_at - 1)?;
        let mut existing_node_view = self.get_mut(old_root_id)?;
        existing_node_view
            .num_keys_mut()
            .write((split_at - 1).try_into()?);

        // Insert the new child entry, the key and the payload into the parent node
        self.set_key(new_root_id, 0, &split_key)?;
        self.set_payload(new_root_id, 0, split_payload)?;
        self.set_child_node(new_root_id, 0, old_root_id)?;
        self.set_child_node(new_root_id, 1, new_node_id)?;

        Ok(new_root_id)
    }

    fn split_off(&mut self, source_node_id: u64, split_at: usize) -> Result<u64> {
        let n = self.number_of_keys(source_node_id)?;
        if split_at < n {
            // Allocate a new node
            let target_node_id = self.allocate_new_node()?;

            // Copy the right half of the keys, payload and child nodes to the new node
            for i in split_at..n {
                self.set_key(
                    target_node_id,
                    i - split_at,
                    self.get_key(source_node_id, i)?.as_ref(),
                )?;
                self.set_payload(
                    target_node_id,
                    i - split_at,
                    self.get_payload(source_node_id, i)?,
                )?;
            }
            if !self.is_leaf(source_node_id)? {
                for i in split_at..self.number_of_children(source_node_id)? {
                    self.set_child_node(
                        target_node_id,
                        i - split_at,
                        self.get_child_node(source_node_id, i)?,
                    )?;
                }
            }

            // Clip the size of keys in the source node
            let mut source_node_view = self.get_mut(source_node_id)?;
            source_node_view.num_keys_mut().write(split_at.try_into()?);
            Ok(target_node_id)
        } else {
            Err(Error::KeyIndexOutOfBounds {
                idx: split_at,
                len: n,
            })
        }
    }

    fn get(&self, node_id: u64) -> Result<node::View<&[u8]>> {
        let node_id: usize = node_id.try_into()?;
        let offset: usize = NODE_BLOCK_ALIGNED_SIZE * node_id;
        let view = node::View::new(&self.mmap[offset..(offset + NODE_BLOCK_SIZE)]);
        Ok(view)
    }

    fn get_mut(&mut self, node_id: u64) -> Result<node::View<&mut [u8]>> {
        let node_id: usize = node_id.try_into()?;
        let offset: usize = NODE_BLOCK_ALIGNED_SIZE * node_id;
        let view = node::View::new(&mut self.mmap[offset..(offset + NODE_BLOCK_SIZE)]);
        Ok(view)
    }

    /// Grows the file to contain at least the requested number of bytes.
    /// This needs to copy all content into a new temporary file.
    /// To avoid this costly operation, the file size is at least doubled.
    fn grow(&mut self, requested_size: usize) -> Result<()> {
        if requested_size <= self.mmap.len() {
            // Still enough space, no action required
            return Ok(());
        }

        // Create a new anonymous memory mapped the content is copied to.
        // Allocate at least twice the old file size so we don't need to grow too often
        let new_size = requested_size.max(self.mmap.len() * 2);
        let mut new_mmap = MmapOptions::new().stack().len(new_size).map_anon()?;

        // Copy all content from the old file into the new file
        new_mmap[0..self.mmap.len()].copy_from_slice(&self.mmap);

        self.mmap = new_mmap;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
