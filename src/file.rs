use std::{io::Write, marker::PhantomData, mem::size_of};

use crate::{error::Result, Error};
use bincode::Options;
use memmap2::MmapMut;
use serde::{de::DeserializeOwned, Serialize};

pub struct TemporaryBlockFile<B> {
    free_space_offset: usize,
    mmap: MmapMut,
    serializer: bincode::DefaultOptions,
    phantom: PhantomData<B>,
}

struct BlockHeader {
    block_size: u64,
    used_size: u64,
}

impl BlockHeader {
    fn read(buffer: &[u8; 16]) -> Result<BlockHeader> {
        let block_size = u64::from_le_bytes(buffer[0..8].try_into()?);
        let used_size = u64::from_le_bytes(buffer[8..16].try_into()?);
        Ok(BlockHeader {
            block_size,
            used_size,
        })
    }

    fn write<W>(&self, mut buffer: W) -> Result<()>
    where
        W: Write,
    {
        buffer.write(&self.block_size.to_le_bytes())?;
        buffer.write(&self.used_size.to_le_bytes())?;
        Ok(())
    }

    fn size() -> usize {
        2 * size_of::<u64>()
    }
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
        let header = self.block_header(block_index)?;
        let used_size: usize = header.used_size.try_into()?;
        // Deserialize and return
        let block_start = block_index + BlockHeader::size();
        let block_end = block_start + used_size;
        let result: B = self.serializer.deserialize(&self.mmap[block_start..block_end])?;
        Ok(result)
    }

    pub fn can_update(&self, block_index: usize, block: &B) -> Result<u64> {
        // Get the allocated size of this block
        let header = self.block_header(block_index)?;

        // Get its new size and check it still fits
        let new_size = self.serializer.serialized_size(&block)?;
        if new_size <= header.block_size {
            Ok(new_size)
        } else {
            Err(Error::ExistingBlockTooSmall { block_index })
        }
    }

    pub fn update(&mut self, block_index: usize, block: &B) -> Result<()> {
        // Check there is still enough space in the block
        let new_used_size = self.can_update(block_index, &block)?;

        // Update the header with the new size
        let mut header = self.block_header(block_index)?;
        header.used_size = new_used_size;
        header.write(&mut self.mmap[block_index..(block_index + BlockHeader::size())])?;

        // Serialize the block and write it at the proper location in the file
        let block_size: usize = header.block_size.try_into()?;
        let block_start = block_index + BlockHeader::size();
        let block_end = block_start + block_size;
        self.serializer
            .serialize_into(&mut self.mmap[block_start..block_end], &block)?;

        Ok(())
    }

    pub fn allocate_block(&mut self, block_size: usize) -> Result<usize> {
        // Make sure we still have enough space left
        let new_offset = self.free_space_offset + BlockHeader::size() + block_size;
        self.grow(new_offset)?;

        // Return the old start of free space as block index
        let result = self.free_space_offset;

        // Write the block header to the file
        let header = BlockHeader {
            block_size: block_size.try_into()?,
            used_size: 0,
        };
        header.write(&mut self.mmap[result..(result + BlockHeader::size())])?;

        // The next free block can be added after this block
        self.free_space_offset = new_offset;
        Ok(result)
    }

    fn block_header(&self, block_index: usize) -> Result<BlockHeader> {
        let header = BlockHeader::read(
            self.mmap[block_index..(block_index + BlockHeader::size())].try_into()?,
        )?;
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
    use crate::file::BlockHeader;

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

    #[test]
    fn block_insert_get_update() {
        let mut m = TemporaryBlockFile::<Vec<u64>>::with_capacity(128).unwrap();
        assert_eq!(128, m.mmap.len());

        let mut b: Vec<u64> = std::iter::repeat(42).take(10).collect();
        let idx = m.allocate_block(256 - BlockHeader::size()).unwrap();
        // The block needs space for the data, but also for the header
        assert_eq!(256, m.mmap.len());

        // Insert the block as it is
        assert_eq!(true, m.can_update(idx, &b).is_ok());
        m.update(idx, &b).unwrap();

        // Get and check it is still equal
        let retrieved_block = m.get(idx).unwrap();
        assert_eq!(b, retrieved_block);

        // The block should be able to hold a little bit more vector elements
        for i in 1..20 {
            b.push(i);
        }
        assert_eq!(true, m.can_update(idx, &b).is_ok());
        m.update(idx, &b).unwrap();
        let retrieved_block = m.get(idx).unwrap();
        assert_eq!(b, retrieved_block);

        // We can't grow the block beyond the allocated limit
        let mut large_block = b.clone();
        for i in 1..300 {
            large_block.push(i);
        }
        assert_eq!(false, m.can_update(idx, &large_block).is_ok());
        assert_eq!(false, m.update(idx, &large_block).is_ok());

        // Allocate and put the new large block, but check the old block was not changed
        let large_block_idx = m.allocate_block(1024).unwrap();
        m.update(large_block_idx, &large_block).unwrap();
        let retrieved_block = m.get(idx).unwrap();
        assert_eq!(b, retrieved_block);
    }
}
