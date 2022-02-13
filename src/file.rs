use std::{
    collections::HashMap,
    io::Write,
    mem::size_of,
    sync::{Arc, Mutex},
};

use crate::{error::Result, PAGE_SIZE};
use binary_layout::{prelude::*, FieldView};
use bincode::Options;
use linked_hash_map::LinkedHashMap;
use memmap2::{MmapMut, MmapOptions};
use serde::{de::DeserializeOwned, Serialize};

/// Return a value that is at least the given capacity, but ensures the block ends at a memory page
pub fn page_aligned_capacity(capacity: usize) -> usize {
    let mut num_full_pages = capacity / PAGE_SIZE;
    if capacity % PAGE_SIZE != 0 {
        num_full_pages += 1;
    }
    // Make sure there is enough space for the block header
    (num_full_pages * PAGE_SIZE) - BlockHeader::size()
}

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
const NODE_BLOCK_ALIGNED_SIZE: usize = PAGE_SIZE;

pub fn get_key(field: &FieldView<&[u8], node::keys>, i: u8) -> Result<u64> {
    let offset: usize = (i as usize) * 8;
    let result: u64 = u64::from_le_bytes(field.data()[offset..(offset + 8)].try_into()?);
    Ok(result)
}

pub fn set_key(field: &mut FieldView<&mut [u8], node::keys>, i: u8, value: usize) {
    let offset: usize = (i as usize) * 8;
    let value = value.to_le_bytes();
    field.data_mut()[offset..].copy_from_slice(&value);
}

pub fn get_payload(field: &FieldView<&[u8], node::payloads>, i: u8) -> Result<u64> {
    let offset: usize = (i as usize) * 8;
    let result: u64 = u64::from_le_bytes(field.data()[offset..(offset + 8)].try_into()?);
    Ok(result)
}

pub fn set_payload(field: &mut FieldView<&mut [u8], node::payloads>, i: u8, value: usize) {
    let offset: usize = (i as usize) * 8;
    let value = value.to_le_bytes();
    field.data_mut()[offset..].copy_from_slice(&value);
}

pub fn get_child_node(field: &FieldView<&[u8], node::child_nodes>, i: u8) -> Result<u64> {
    let offset: usize = (i as usize) * 8;
    let result: u64 = u64::from_le_bytes(field.data()[offset..(offset + 8)].try_into()?);
    Ok(result)
}

pub fn set_child_node(field: &mut FieldView<&mut [u8], node::child_nodes>, i: u8, value: usize) {
    let offset: usize = (i as usize) * 8;
    let value = value.to_le_bytes();
    field.data_mut()[offset..].copy_from_slice(&value);
}

pub struct NodeFile {
    free_space_offset: usize,
    mmap: MmapMut,
}

impl NodeFile {
    pub fn with_capacity(capacity: usize) -> Result<NodeFile> {
        // Create an anonymous memory mapped file with the capacity as size
        let capacity = capacity.max(1);
        let mmap = MmapOptions::new()
            .stack()
            .len(capacity * NODE_BLOCK_ALIGNED_SIZE)
            .map_anon()?;

        Ok(NodeFile {
            mmap,
            free_space_offset: 0,
        })
    }

    pub fn get(&self, node_id: u64) -> Result<node::View<&[u8]>> {
        let node_id: usize = node_id.try_into()?;
        let offset: usize = NODE_BLOCK_ALIGNED_SIZE * node_id;
        let view = node::View::new(&self.mmap[offset..(offset + NODE_BLOCK_SIZE)]);
        Ok(view)
    }

    pub fn get_mut(&mut self, node_id: u64) -> Result<node::View<&mut [u8]>> {
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

/// Representation of a header at the start of each block.
///
/// When allocating new blocks, the size of this header is not included.
pub struct BlockHeader {
    capacity: u64,
    used: u64,
}

impl BlockHeader {
    /// Create a new block header by reading it from an array.
    fn read(buffer: &[u8; 16]) -> Result<BlockHeader> {
        let block_size = u64::from_le_bytes(buffer[0..8].try_into()?);
        let used_size = u64::from_le_bytes(buffer[8..16].try_into()?);
        Ok(BlockHeader {
            capacity: block_size,
            used: used_size,
        })
    }

    /// Write the block header to a buffer.
    fn write<W>(&self, mut buffer: W) -> Result<()>
    where
        W: Write,
    {
        buffer.write_all(&self.capacity.to_le_bytes())?;
        buffer.write_all(&self.used.to_le_bytes())?;
        Ok(())
    }

    /// The number of bytes needed to serialize the block header.
    ///
    /// Should be used as an offset. Also, when you want to allocate
    /// blocks aligned to the page size, you should subtract the size.
    pub const fn size() -> usize {
        2 * size_of::<u64>()
    }
}

/// Represents a temporary memory mapped file that can store and retrieve blocks of type `B`.
///
/// Blocks will be (de-) serializable with the Serde crate.
pub struct TemporaryBlockFile<B> {
    free_space_offset: usize,
    mmap: MmapMut,
    relocated_blocks: HashMap<usize, usize>,
    serializer: bincode::DefaultOptions,
    cache: Arc<Mutex<LinkedHashMap<usize, Arc<B>>>>,
    block_cache_size: usize,
}

impl<B> TemporaryBlockFile<B>
where
    B: Serialize + DeserializeOwned + Clone,
{
    /// Create a new file with the given capacity.
    ///
    /// New blocks can be allocated with [`Self::allocate_block()`].
    /// While the file will automatically grow when block are allocated and the capacity is reached,
    /// you cannot change the capacity of a single block after allocating it.
    pub fn with_capacity(
        capacity: usize,
        block_cache_size: usize,
    ) -> Result<TemporaryBlockFile<B>> {
        // Create an anonymous memory mapped file with the capacity as size
        let capacity = capacity.max(1);
        let mmap = MmapOptions::new().stack().len(capacity).map_anon()?;

        Ok(TemporaryBlockFile {
            mmap,
            free_space_offset: 0,
            relocated_blocks: HashMap::default(),
            serializer: bincode::DefaultOptions::new(),
            cache: Arc::new(Mutex::new(LinkedHashMap::with_capacity(block_cache_size))),
            block_cache_size,
        })
    }

    fn read_block(&self, block_id: usize) -> Result<B> {
        // Read the size of the stored block
        let header = self.block_header(block_id)?;
        let used_size: usize = header.used.try_into()?;
        // Deserialize and return
        let block_start = block_id + BlockHeader::size();
        let block_end = block_start + used_size;
        let result: B = self
            .serializer
            .deserialize(&self.mmap[block_start..block_end])?;
        Ok(result)
    }

    fn get_cached_entry(&self, block_id: usize) -> Option<Arc<B>> {
        if let Ok(mut cache) = self.cache.try_lock() {
            if let Some(b) = cache.get(&block_id).cloned() {
                // Mark the block as recently used by re-inserting it
                cache.insert(block_id, b.clone());
                return Some(b);
            }
        }
        None
    }

    /// Get a block with the given id give ownership of the result to the caller.
    pub fn get_owned(&self, block_id: usize) -> Result<B> {
        let block_id = *self.relocated_blocks.get(&block_id).unwrap_or(&block_id);

        if let Some(b) = self.get_cached_entry(block_id) {
            Ok(b.as_ref().clone())
        } else {
            let result = self.read_block(block_id)?;
            Ok(result)
        }
    }

    pub fn get(&self, block_id: usize) -> Result<Arc<B>> {
        let block_id = *self.relocated_blocks.get(&block_id).unwrap_or(&block_id);

        if let Some(b) = self.get_cached_entry(block_id) {
            Ok(b)
        } else {
            let result = self.read_block(block_id)?;
            Ok(Arc::new(result))
        }
    }

    /// Determines wether a given block would still fit in the originally allocated space.
    ///
    /// Returns a tuple with the first value beeing true when the update fits.
    /// The second value is the needed size for this block.
    pub fn can_update(&self, block_id: usize, block: &B) -> Result<(bool, u64)> {
        let block_id = *self.relocated_blocks.get(&block_id).unwrap_or(&block_id);
        // Get the allocated size of this block
        let header = self.block_header(block_id)?;

        // Get its new size and check it still fits
        let new_size = self.serialized_size(block)?;
        let result = if new_size <= header.capacity {
            (true, new_size)
        } else {
            (false, new_size)
        };
        Ok(result)
    }

    /// Get the number of bytes necessary to store the given block.
    ///
    pub fn serialized_size(&self, block: &B) -> Result<u64> {
        let new_size = self.serializer.serialized_size(&block)?;
        Ok(new_size)
    }

    /// Set the content of a block with the given id.
    ///
    /// If the block needs more space than was originally allocated, a new block is allocated
    /// and the redirection is saved in an in-memory hash map.
    /// The old block will remain empty. So try to avoid writing any
    /// blocks with a larger size than originally allocated.
    pub fn put(&mut self, block_id: usize, block: &B) -> Result<()> {
        let relocated_block_id = *self.relocated_blocks.get(&block_id).unwrap_or(&block_id);

        // Check there is still enough space in the block
        let (update_fits, new_used_size) = self.can_update(relocated_block_id, block)?;
        let block_id = if update_fits {
            relocated_block_id
        } else {
            // Relocate (possible again) to a new block with double the size
            let new_used_size: usize = new_used_size.try_into()?;
            let new_block_id = self.allocate_block(page_aligned_capacity(new_used_size * 2))?;
            self.relocated_blocks.insert(block_id, new_block_id);
            new_block_id
        };

        // Update the header with the new size
        let mut header = self.block_header(block_id)?;
        header.used = new_used_size;
        header.write(&mut self.mmap[block_id..(block_id + BlockHeader::size())])?;

        // Serialize the block and write it at the proper location in the file
        let block_size: usize = header.capacity.try_into()?;
        let block_start = block_id + BlockHeader::size();
        let block_end = block_start + block_size;
        self.serializer
            .serialize_into(&mut self.mmap[block_start..block_end], &block)?;

        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(block_id, Arc::new(block.clone()));
            // Remove the oldest entry when capacity is reached
            if cache.len() > self.block_cache_size {
                cache.pop_front();
            }
        }

        Ok(())
    }

    /// Allocate a new block with the given capacity.
    ///
    /// Returns the ID of the new block.
    pub fn allocate_block(&mut self, capacity: usize) -> Result<usize> {
        // Make sure we still have enough space left
        let new_offset = self.free_space_offset + BlockHeader::size() + capacity;
        self.grow(new_offset)?;

        // Return the old start of free space as block index
        let result = self.free_space_offset;

        // Write the block header to the file
        let header = BlockHeader {
            capacity: capacity.try_into()?,
            used: 0,
        };
        header.write(&mut self.mmap[result..(result + BlockHeader::size())])?;

        // The next free block can be added after this block
        self.free_space_offset = new_offset;
        Ok(result)
    }

    /// Parses the header of the block.
    fn block_header(&self, block_id: usize) -> Result<BlockHeader> {
        let header =
            BlockHeader::read(self.mmap[block_id..(block_id + BlockHeader::size())].try_into()?)?;
        Ok(header)
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
