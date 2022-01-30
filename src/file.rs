use std::path::{PathBuf};

use crate::error::Result;

pub struct MemoryMappedFile {
    path: PathBuf,

}

impl MemoryMappedFile {
    pub fn open<P: Into<PathBuf>>(path : P) -> Result<MemoryMappedFile> {
        let path = path.into();

        Ok(MemoryMappedFile {
            path
        })
        
    }
}