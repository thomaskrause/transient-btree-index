use crate::error::Result;
use byte_pool::Poolable;
use memmap2::MmapMut;
use packed_struct::prelude::*;
use std::{fs::OpenOptions, io::Write, path::PathBuf, mem::size_of};
use tempfile::NamedTempFile;

const METADATA_BLOCK_OFFSET : usize = 4096;

#[derive(PackedStruct)]
#[packed_struct(endian = "lsb")]
pub struct Metadata {
    free_space_offset: u64,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            free_space_offset: METADATA_BLOCK_OFFSET as u64,
        }
    }
}

pub struct MemoryMappedFile {
    mmap: MmapMut,
    path: PathBuf,
}

impl MemoryMappedFile {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<MemoryMappedFile> {
        let path = path.into();
        let file_exists = path.is_file();

        if !file_exists {
            // Create the file and write the default metadata header at the first position
            let mut f = OpenOptions::new().write(true).create(true).open(&path)?;
            let md = Metadata::default().pack()?;
            let written = f.write(&md)?;
            // Fill remaining metadata block with ones
            let one : [u8; 1] = [1];
            for _ in written..METADATA_BLOCK_OFFSET {
                // TODO: use less calls to write an larger arrays
                f.write(&one)?;
            }
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // TOOD: lock this file, eg. using the fd-lock crate to avoid two processes accessing it
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(MemoryMappedFile { mmap, path })
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    fn metadata(&self) -> Result<Metadata> {
        let md = Metadata::unpack(&self.mmap[0..size_of::<Metadata>()].try_into()?)?;
        Ok(md)
    }

    /// Grows the file to contain at least the requested number of bytes.
    /// This needs to copy all content into a new temporary file.
    /// To avoid this costly operation, the file size is at least doubled.
    fn grow(&mut self, requested_size: usize) -> Result<()> {
        if requested_size <= self.mmap.len() {
            // Still enough space, no action required
            return Ok(());
        }

        // Create a temporary file on the same file system as the target path
        let mut new_file = self
            .path
            .parent()
            .map_or_else(|| NamedTempFile::new(), |dir| NamedTempFile::new_in(dir))?;

        // Set the file size so it can hold all requested content.
        // Allocate at least twice the old file size so we don't need to grow too often
        let new_size = requested_size.max(self.mmap.len() * 2);
        new_file.as_file_mut().set_len(new_size.try_into()?)?;

        // Copy all content from the old file into the new file
        let mut reader = &self.mmap[..];
        std::io::copy(&mut reader, &mut new_file.as_file())?;

        // Overwrite the file and re-open mmap
        new_file.persist(&self.path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&self.path)?;
        // TOOD: lock this file, eg. using the fd-lock crate to avoid two processes accessing it
        self.mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(())
    }
}

impl Poolable for MemoryMappedFile {
    fn capacity(&self) -> usize {
        todo!()
    }

    fn alloc(size: usize) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::MemoryMappedFile;

    #[test]
    fn grow_mmap() {
        let path: PathBuf = "testfile.mmap".into();
        if path.is_file() {
            std::fs::remove_file(&path).unwrap();
        }

        let mut m = MemoryMappedFile::open(&path).unwrap();
        assert_eq!(4096, m.len());

        // Don't grow if not necessary
        m.grow(128).unwrap();
        assert_eq!(4096, m.len());
        m.grow(4096).unwrap();
        assert_eq!(4096, m.len());

        // Grow with double size
        m.grow(8192).unwrap();
        assert_eq!(8192, m.len());

        // Grow with less than the double size still creates the double size
        m.grow(9000).unwrap();
        assert_eq!(16384, m.len());

        std::fs::remove_file(&path).unwrap();
    }
}
