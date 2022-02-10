//! # Transient Index using B-Trees
//!
//! `transient-btree-index` allows you to create a BTree index backed by temporary files.
//! This is helpful if you
//!
//! - need to index large datasets (thus only working on disk) by inserting entries in unsorted order,
//! - want to query entries (get and range queries) while the index is still constructed, e.g. to check existence of a previous entry, and
//! - need support for all serde-serializable key and value types with varying key-size.
//!
//! Because of its intended use case, it is therefore **not possible to**
//!
//! - delete entries once they are inserted (you can use [`Option`] values and set them to [`Option::None`], but this will not reclaim any used space),
//! - persist the index to a file (you can use other crates like [sstable](https://crates.io/crates/sstable) to create immutable maps), or
//! - load an existing index file (you might want to use an immutable map file and this index can act as an "overlay" for all changed entries).
//!
//! # Example
//!
//! ```rust
//! use transient_btree_index::{BtreeConfig, BtreeIndex, Error};
//!
//! fn main() -> std::result::Result<(), Error> {
//!     let mut b = BtreeIndex::<u16,u16>::with_capacity(BtreeConfig::default(), 10)?;
//!     b.insert(1,2)?;
//!     b.insert(200, 4)?;
//!     b.insert(20, 3)?;
//!
//!     assert_eq!(true, b.contains_key(&200)?);
//!     assert_eq!(false, b.contains_key(&2)?);  
//!
//!     assert_eq!(3, b.get(&20)?.unwrap());  
//!
//!     for e in b.range(1..30)? {
//!         let (k, v) = e?;
//!         dbg!(k, v);
//!     }
//!     Ok(())
//! }
//! ```
mod btree;
mod error;
mod file;

pub use btree::{BtreeConfig, BtreeIndex};
pub use error::Error;

const KB: usize = 1 << 10;
const PAGE_SIZE: usize = 4 * KB;
