use std::marker::PhantomData;

use crate::error::Result;
use memmap2::MmapMut;
use serde::{de::DeserializeOwned, Serialize};
use tempfile::tempfile;

pub struct TemporaryBlockStorage<B> {
    free_space_offset: u64,
    mmap: MmapMut,
    phantom: PhantomData<B>,
}

impl<B> TemporaryBlockStorage<B>
where
    B: Serialize + DeserializeOwned,
{
    pub fn with_capacity(capacity: usize) -> Result<TemporaryBlockStorage<B>> {
        // Create a temporary file with the capacity as size
        let file = tempfile::tempfile()?;
        if capacity > 0 {
            file.set_len(capacity.try_into()?)?;
        }

        // Load this file as memory mapped file
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(TemporaryBlockStorage {
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

        // Create a new temporary file to which the content is copied to
        let mut new_file = tempfile()?;

        // Set the file size so it can hold all requested content.
        // Allocate at least twice the old file size so we don't need to grow too often
        let new_size = requested_size.max(self.mmap.len() * 2);
        new_file.set_len(new_size.try_into()?)?;

        // Copy all content from the old file into the new file
        let mut reader = &self.mmap[..];
        std::io::copy(&mut reader, &mut new_file)?;

        // Re-open mmap
        self.mmap = unsafe { MmapMut::map_mut(&new_file)? };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::TemporaryBlockStorage;

    #[test]
    fn grow_mmap_from_zero_capacity() {
        // Create file with empty capacity
        let mut m = TemporaryBlockStorage::<u64>::with_capacity(0).unwrap();
        assert_eq!(0, m.mmap.len());

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
        let mut m = TemporaryBlockStorage::<u64>::with_capacity(4096).unwrap();
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
