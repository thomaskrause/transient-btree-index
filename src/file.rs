use std::{
    collections::HashMap,
    io::Write,
    marker::PhantomData,
    mem::size_of,
    sync::{Arc, Mutex},
};

use crate::{create_mmap, error::Result, Error, PAGE_SIZE};
use bincode::Options;
use generic_array::{ArrayLength, GenericArray};
use linked_hash_map::LinkedHashMap;
use memmap2::MmapMut;
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

pub trait TupleFile<B>: Sync
where
    B: Sync,
{
    /// Allocate a new block with the given capacity.
    ///
    /// Returns the ID of the new block.
    fn allocate_block(&mut self, capacity: usize) -> Result<usize>;

    /// Get a block with the given id give ownership of the result to the caller.
    fn get_owned(&self, block_id: usize) -> Result<B>;

    fn get(&self, block_id: usize) -> Result<Arc<B>>;

    /// Set the content of a block with the given id.
    ///
    /// If the block needs more space than was originally allocated, a new block is allocated
    /// and the redirection is saved in an in-memory hash map.
    /// The old block will remain empty. So try to avoid writing any
    /// blocks with a larger size than originally allocated.
    fn put(&mut self, block_id: usize, block: &B) -> Result<()>;

    /// Get the number of bytes necessary to store the given block.
    fn serialized_size(&self, block: &B) -> Result<u64>;
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
pub struct VariableSizeTupleFile<B>
where
    B: Sync,
{
    free_space_offset: usize,
    mmap: MmapMut,
    relocated_blocks: HashMap<usize, usize>,
    serializer: bincode::DefaultOptions,
    cache: Arc<Mutex<LinkedHashMap<usize, Arc<B>>>>,
    block_cache_size: usize,
}

impl<B> TupleFile<B> for VariableSizeTupleFile<B>
where
    B: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    fn allocate_block(&mut self, capacity: usize) -> Result<usize> {
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

    fn get_owned(&self, block_id: usize) -> Result<B> {
        let block_id = *self.relocated_blocks.get(&block_id).unwrap_or(&block_id);

        if let Some(b) = self.get_cached_entry(block_id) {
            Ok(b.as_ref().clone())
        } else {
            let result = self.read_block(block_id)?;
            Ok(result)
        }
    }

    fn get(&self, block_id: usize) -> Result<Arc<B>> {
        let block_id = *self.relocated_blocks.get(&block_id).unwrap_or(&block_id);

        if let Some(b) = self.get_cached_entry(block_id) {
            Ok(b)
        } else {
            let result = self.read_block(block_id)?;
            Ok(Arc::new(result))
        }
    }

    fn put(&mut self, block_id: usize, block: &B) -> Result<()> {
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

    fn serialized_size(&self, block: &B) -> Result<u64> {
        let new_size = self.serializer.serialized_size(&block)?;
        Ok(new_size)
    }
}

impl<B> VariableSizeTupleFile<B>
where
    B: Serialize + DeserializeOwned + Clone + Sync + Send + Sync,
{
    /// Create a new file with the given capacity.
    ///
    /// New blocks can be allocated with [`Self::allocate_block()`].
    /// While the file will automatically grow when block are allocated and the capacity is reached,
    /// you cannot change the capacity of a single block after allocating it.
    pub fn with_capacity(
        capacity: usize,
        block_cache_size: usize,
    ) -> Result<VariableSizeTupleFile<B>> {
        // Create an anonymous memory mapped file with the capacity as size
        let capacity = capacity.max(1);
        let mmap = create_mmap(capacity)?;

        Ok(VariableSizeTupleFile {
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
            if let Some(b) = cache.remove(&block_id) {
                // Mark the block as recently used by re-inserting it
                cache.insert(block_id, b.clone());
                return Some(b);
            }
        }
        None
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
        let mut new_mmap = create_mmap(new_size)?;

        // Copy all content from the old file into the new file
        new_mmap[0..self.mmap.len()].copy_from_slice(&self.mmap);

        self.mmap = new_mmap;
        Ok(())
    }
}

pub struct FixedSizeTupleFile<B, N>
where
    N: ArrayLength<u8>,
    B: Sync,
{
    free_space_offset: usize,
    mmap: MmapMut,
    phantom: PhantomData<(B, N)>,
}

impl<B, N> TupleFile<B> for FixedSizeTupleFile<B, N>
where
    B: Into<GenericArray<u8, N>> + From<GenericArray<u8, N>> + Clone + Sync,
    N: ArrayLength<u8> + Sync,
{
    fn allocate_block(&mut self, capacity: usize) -> Result<usize> {
        if capacity != N::to_usize() {
            return Err(Error::InvalidCapacity { capacity });
        }

        // Make sure we still have enough space left in the file
        let new_offset = self.free_space_offset + N::to_usize();
        self.grow(new_offset)?;

        // Return the old start of free space as block index
        let result = self.free_space_offset;

        // The next free block can be added after this block
        self.free_space_offset = new_offset;
        Ok(result)
    }

    fn get_owned(&self, block_id: usize) -> Result<B> {
        let result = self.read_block(block_id)?;
        Ok(result)
    }

    fn get(&self, block_id: usize) -> Result<Arc<B>> {
        let result = self.read_block(block_id)?;
        Ok(Arc::new(result))
    }

    fn put(&mut self, block_id: usize, block: &B) -> Result<()> {
        // Serialize the block and write it at the proper location in the file
        let block_size: usize = N::to_usize();
        let block_start = block_id;
        let block_end = block_start + block_size;

        let block_as_bytes: GenericArray<u8, N> = block.clone().into();

        self.mmap[block_start..block_end].copy_from_slice(&block_as_bytes);
        Ok(())
    }

    fn serialized_size(&self, _block: &B) -> Result<u64> {
        Ok(N::to_u64())
    }
}

impl<B, N> FixedSizeTupleFile<B, N>
where
    B: Into<GenericArray<u8, N>> + From<GenericArray<u8, N>> + Sync,
    N: ArrayLength<u8>,
{
    /// Create a new file with the given capacity.
    ///
    /// New blocks can be allocated with [`Self::allocate_block()`].
    /// The file will automatically grow when block are allocated and the capacity is reached
    pub fn with_capacity(capacity: usize) -> Result<FixedSizeTupleFile<B, N>> {
        // Create an anonymous memory mapped file with the capacity as size
        let capacity = capacity.max(1);
        let mmap = create_mmap(capacity)?;
        Ok(FixedSizeTupleFile {
            mmap,
            free_space_offset: 0,
            phantom: PhantomData,
        })
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
        let mut new_mmap = create_mmap(new_size)?;

        // Copy all content from the old file into the new file
        new_mmap[0..self.mmap.len()].copy_from_slice(&self.mmap);

        self.mmap = new_mmap;
        Ok(())
    }

    fn read_block(&self, block_id: usize) -> Result<B> {
        // Deserialize and return
        let block_start = block_id;
        let block_end = block_start + N::to_usize();

        let data: GenericArray<u8, N> =
            GenericArray::clone_from_slice(&self.mmap[block_start..block_end]);

        let block: B = data.into();

        Ok(block)
    }
}

#[cfg(test)]
mod tests;
