use std::{path::{PathBuf}, fs::OpenOptions};
use memmap2::MmapMut;
use tempfile::{tempfile, tempfile_in, NamedTempFile};
use crate::error::Result;

pub struct MemoryMappedFile {
    mmap: MmapMut,
    path: PathBuf,
}

impl MemoryMappedFile {
    pub fn open<P: Into<PathBuf>>(path : P) -> Result<MemoryMappedFile> {
        let path = path.into();

        let file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
        // TOOD: lock this file, eg. using the fd-lock crate to avoid two processes accessing it
        let mmap = unsafe { MmapMut::map_mut(&file)?};

        Ok(MemoryMappedFile {
            mmap,
            path
        })
        
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Grows the file to contain at least the requested number of bytes.
    /// This needs to copy all content into a new temporary file.
    /// To avoid this costly operation, the file size is at least doubled.
    fn grow(&mut self, requested_size: usize) -> Result<()> {

        if requested_size < self.mmap.len() {
            // Still enough space, no action required
            return Ok(());
        }

        // Create a temporary file on the same file system as the target path
        let mut new_file = self.path.parent().map_or_else(|| NamedTempFile::new(), |dir | NamedTempFile::new_in(dir))?;

        // Set the file size so it can hold all requested content.
        // Allocate at least twice the old file size so we don't need to grow too often
        let new_size = requested_size.max(self.mmap.len()*2);
        new_file.as_file_mut().set_len(new_size.try_into()?)?;

        // Copy all content from the old file into the new file
        let mut reader = &self.mmap[..];
        std::io::copy(&mut reader, &mut new_file.as_file())?;

        // Overwrite the file and re-open mmap
        new_file.persist(&self.path)?;
        let file = OpenOptions::new().read(true).write(true).create(true).open(&self.path)?;
        // TOOD: lock this file, eg. using the fd-lock crate to avoid two processes accessing it
        self.mmap = unsafe { MmapMut::map_mut(&file)?};
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::MemoryMappedFile;

    #[test]
    fn grow_mmap() {
        let path : PathBuf = "testfile.mmap".into();
        if path.is_file() {
            std::fs::remove_file(&path).unwrap();
        }

        let mut m = MemoryMappedFile::open(&path).unwrap();
        assert_eq!(0, m.len());

        m.grow(128).unwrap();
            assert_eq!(128, m.len());

        m.grow(512).unwrap();
        assert_eq!(512, m.len());

        m.grow(513).unwrap();
        assert_eq!(1024, m.len());

        m.grow(600).unwrap();
        assert_eq!(1024, m.len());

        std::fs::remove_file(&path).unwrap();
    }
}