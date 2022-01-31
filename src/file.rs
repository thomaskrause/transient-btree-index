use std::marker::PhantomData;

use crate::error::Result;
use bincode::Options;
use memmap2::MmapMut;
use serde::{de::DeserializeOwned, Serialize};

pub struct TemporaryBlockFile<B> {
    free_space_offset: usize,
    mmap: MmapMut,
    serializer: bincode::DefaultOptions,
    phantom: PhantomData<B>,
}

impl<B> TemporaryBlockFile<B>
where
    B: Serialize + DeserializeOwned,
{
    pub fn with_capacity(capacity: usize) -> Result<TemporaryBlockFile<B>> {
        // Create an anonymous memory mapped file with the capacity as size
        let capacity = capacity.max(1);
        let mmap = MmapMut::map_anon(capacity)?;

        Ok(TemporaryBlockFile {
            mmap,
            free_space_offset: 0,
            serializer: bincode::DefaultOptions::new(),
            phantom: PhantomData,
        })
    }

    pub fn get(&self, block_index: usize) -> Result<B> {
        // Read the size of the stored block
        let block_start = block_index + 8;
        let block_size = u64::from_le_bytes(self.mmap[block_index..block_start].try_into()?);
        let block_size: usize = block_size.try_into()?;
        // Deserialize and return
        let block_end = block_start + block_size;
        let result: B = bincode::deserialize(&self.mmap[block_start..block_end])?;
        Ok(result)
    }

    pub fn can_update(&self, block: B, block_index: usize) -> Result<bool> {
        // TODO: Get the allocated size of this block

        // Get its new size and check it still fits
        let new_size = self.serializer.serialized_size(&block)?;
        todo!()
    }

    pub fn update(&mut self, block: B, block_index: usize) -> Result<()> {
        todo!()
    }

    pub fn allocate_block(&mut self, block_size: usize) -> Result<usize> {
        // Make sure we still have enough space left
        let new_offset = self.free_space_offset + block_size + 8;
        self.grow(new_offset)?;

        // Return the old start of free space as block index
        let result = self.free_space_offset;

        // Write the block size to the file
        let block_size: u64 = block_size.try_into()?;
        self.mmap[result..(result + 8)].copy_from_slice(&block_size.to_le_bytes());

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

        // Create a new anonymous memory mapped the content is copied to
        // Allocate at least twice the old file size so we don't need to grow too often
        let new_size = requested_size.max(self.mmap.len() * 2);
        let mut new_mmap = MmapMut::map_anon(new_size)?;
        
        // Copy all content from the old file into the new file
        new_mmap[0..self.mmap.len()].copy_from_slice(&self.mmap);
     
        self.mmap = new_mmap;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::TemporaryBlockFile;

    #[test]
    fn grow_mmap_from_zero_capacity() {
        // Create file with empty capacity
        let mut m = TemporaryBlockFile::<u64>::with_capacity(0).unwrap();
        // The capacity must be at least one
        assert_eq!(1, m.mmap.len());

        // Needs to grow
        m.grow(128).unwrap();
        assert_eq!(128, m.mmap.len());
        m.grow(4096).unwrap();
        assert_eq!(4096, m.mmap.len());

        // No growing necessar
        m.grow(1024).unwrap();
        assert_eq!(4096, m.mmap.len());

        // Grow with double size
        m.grow(8192).unwrap();
        assert_eq!(8192, m.mmap.len());

        // Grow with less than the double size still creates the double size
        m.grow(9000).unwrap();
        assert_eq!(16384, m.mmap.len());
    }

    #[test]
    fn grow_mmap_with_capacity() {
        let mut m = TemporaryBlockFile::<u64>::with_capacity(4096).unwrap();
        assert_eq!(4096, m.mmap.len());

        // Don't grow if not necessary
        m.grow(128).unwrap();
        assert_eq!(4096, m.mmap.len());
        m.grow(4096).unwrap();
        assert_eq!(4096, m.mmap.len());

        // Grow with double size
        m.grow(8192).unwrap();
        assert_eq!(8192, m.mmap.len());

        // Grow with less than the double size still creates the double size
        m.grow(9000).unwrap();
        assert_eq!(16384, m.mmap.len());
    }
}
