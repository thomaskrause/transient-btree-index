use std::{path::{PathBuf}, fs::OpenOptions};
use memmap2::MmapMut;
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
}