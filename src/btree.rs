
mod node;
pub mod outside_keys;



#[derive(Clone)]
pub enum TypeSize {
    Estimated(usize),
    Fixed(usize),
}

/// Configuration for a B-tree index.
#[derive(Clone)]
pub struct BtreeConfig {
    order: usize,
    key_size: TypeSize,
    value_size: TypeSize,
    block_cache_size: usize,
}

impl Default for BtreeConfig {
    fn default() -> Self {
        Self {
            order: 84,
            key_size: TypeSize::Estimated(32),
            value_size: TypeSize::Estimated(32),
            block_cache_size: 16,
        }
    }
}

impl BtreeConfig {
    /// Set the estimated maximum size in bytes for each key.
    ///
    /// Keys can be larger than this, but if this happens too often the block for the key
    /// might need to be re-allocated, which causes memory fragmentation on the disk
    /// and some main memory overhead for remembering the re-allocated block IDs.
    pub fn max_key_size(mut self, est_max_key_size: usize) -> Self {
        self.key_size = TypeSize::Estimated(est_max_key_size);
        self
    }

    /// Set the fixed size in bytes for each key.
    ///
    /// If serializing the key needs a fixed number of bytes
    /// (assuming [bincode](https://crates.io/crates/bincode) is used with a fixed integer encoding),
    /// a more efficient internal implementation will be used.
    pub fn fixed_key_size(mut self, key_size: usize) -> Self {
        self.key_size = TypeSize::Fixed(key_size);
        self
    }

    /// Set the estimated maximum size in bytes for each values.
    ///
    /// Values can be larger than this, but if this happens too often the block for the value
    /// might need to be re-allocated, which causes memory fragmentation on the disk
    /// and some main memory overhead for remembering the re-allocated block IDs.
    pub fn max_value_size(mut self, est_max_value_size: usize) -> Self {
        self.value_size = TypeSize::Estimated(est_max_value_size);
        self
    }

    /// Set the fixed size in bytes for each value.
    ///
    /// If serializing the value needs a fixed number of bytes
    /// (assuming [bincode](https://crates.io/crates/bincode) is used with a fixed integer encoding),  
    /// a more efficient internal implementation will be used.
    pub fn fixed_value_size(mut self, value_size: usize) -> Self {
        self.value_size = TypeSize::Fixed(value_size);
        self
    }

    /// Sets the order of the tree, which determines how many elements a single node can store.
    ///
    /// A B-tree is balanced, so the number of keys of a node is between the order and the order times two.
    /// The order must be at least 2 and at most 84 for this implementation, and
    /// it is guaranteed that the internal structure for a node always fits inside a memory page.
    /// The default is to use the maximum number of keys, so the memory page is utilized as much as possible.
    pub fn order(mut self, order: u8) -> Self {
        self.order = order as usize;
        self
    }

    /// Sets the number of blocks/pages to hold in an internal cache.
    pub fn block_cache_size(mut self, block_cache_size: usize) -> Self {
        self.block_cache_size = block_cache_size;
        self
    }
}

