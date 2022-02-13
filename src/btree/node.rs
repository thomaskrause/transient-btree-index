use std::marker::PhantomData;

use crate::BtreeConfig;
use crate::error::Result;
use crate::file::{TemporaryBlockFile, BlockHeader};
use binary_layout::{prelude::*, FieldView};
use memmap2::{MmapMut, MmapOptions};
use serde::de::DeserializeOwned;
use serde::Serialize;

// Defines a single BTree node with references to the actual values in a tuple file
// This node can up to 1361 keys and 1362 child node references.
define_layout!(node, LittleEndian, {
    id: u64,
    num_keys: u16,
    is_leaf: u8,
    keys: [u8; 10888],
    payloads: [u8; 10888],
    child_nodes: [u8; 10896],
});

const NODE_BLOCK_SIZE: usize = 4095;
const NODE_BLOCK_ALIGNED_SIZE: usize = 4096;

pub fn get_key(field: &FieldView<&[u8], node::keys>, i: usize) -> Result<u64> {
    let offset = i * 8;
    let result: u64 = u64::from_le_bytes(field.data()[offset..(offset + 8)].try_into()?);
    Ok(result)
}

pub fn set_key(field: &mut FieldView<&mut [u8], node::keys>, i: usize, value: usize) {
    let offset = i * 8;
    let value = value.to_le_bytes();
    field.data_mut()[offset..].copy_from_slice(&value);
}

pub fn get_payload(field: &FieldView<&[u8], node::payloads>, i: usize) -> Result<u64> {
    let offset = i * 8;
    let result: u64 = u64::from_le_bytes(field.data()[offset..(offset + 8)].try_into()?);
    Ok(result)
}

pub fn set_payload(field: &mut FieldView<&mut [u8], node::payloads>, i: usize, value: usize) {
    let offset = i * 8;
    let value = value.to_le_bytes();
    field.data_mut()[offset..].copy_from_slice(&value);
}

pub fn get_child_node(field: &FieldView<&[u8], node::child_nodes>, i: usize) -> Result<u64> {
    let offset = i * 8;
    let result: u64 = u64::from_le_bytes(field.data()[offset..(offset + 8)].try_into()?);
    Ok(result)
}

pub fn set_child_node(field: &mut FieldView<&mut [u8], node::child_nodes>, i: usize, value: usize) {
    let offset = i * 8;
    let value = value.to_le_bytes();
    field.data_mut()[offset..].copy_from_slice(&value);
}


pub struct NodeFile<K> {
    free_space_offset: usize,
    mmap: MmapMut,
    keys: TemporaryBlockFile<K>,
}

impl<K> NodeFile<K>
where K: Serialize + DeserializeOwned + Clone {
    pub fn with_capacity(capacity: usize, config : &BtreeConfig) -> Result<NodeFile<K>> {
        // Create an anonymous memory mapped file with the capacity as size
        let capacity = capacity.max(1);
        let mmap = MmapOptions::new()
            .stack()
            .len(capacity * NODE_BLOCK_ALIGNED_SIZE)
            .map_anon()?;

        // Each node can hold 1361 keys, so we need the space for them as well
        let number_of_keys = capacity * 1361;
        let keys = TemporaryBlockFile::with_capacity((number_of_keys * config.est_max_value_size) + BlockHeader::size(),
            config.block_cache_size,
        )?;

        Ok(NodeFile {
            mmap,
            keys,
            free_space_offset: 0,
        })
    }
    fn number_of_keys(&self, node_id : u64) -> Result<usize> {
        let view = self.get(node_id)?;
        Ok(view.num_keys().read() as usize)
    }

    fn is_leaf(&self, node_id : u64) -> Result<bool> {
        let view = self.get(node_id)?;
        Ok(view.is_leaf().read() != 0)
    }

    fn get<'a>(&'a self, node_id: u64) -> Result<node::View<&[u8]>> {
        let node_id: usize = node_id.try_into()?;
        let offset: usize = NODE_BLOCK_ALIGNED_SIZE * node_id;
        let view = node::View::new(&self.mmap[offset..(offset + NODE_BLOCK_SIZE)]);
        Ok(view)
    }

    fn get_mut<'a>(&'a mut self, node_id: u64) -> Result<node::View<&mut [u8]>> {
        let node_id: usize = node_id.try_into()?;
        let offset: usize = NODE_BLOCK_ALIGNED_SIZE * node_id;
        let view = node::View::new(&mut self.mmap[offset..(offset + NODE_BLOCK_SIZE)]);
        Ok(view)
    }

    


    /// Allocate a new node.
    ///
    /// Returns the ID of the new node.
    pub fn allocate_block(&mut self) -> Result<u64> {
        // Make sure we still have enough space left
        let new_offset = self.free_space_offset + NODE_BLOCK_ALIGNED_SIZE;
        self.grow(new_offset)?;

        // Return the old start of free space as block index
        let result: u64 = (self.free_space_offset / NODE_BLOCK_ALIGNED_SIZE).try_into()?;

        // Initialize some of the values
        self.get_mut(result)?.num_keys_mut().write(0);
        self.get_mut(result)?.is_leaf_mut().write(1);

        // The next free block can be added after this block
        self.free_space_offset = new_offset;
        Ok(result)
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
