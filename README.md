# Transient Index using B-Trees

`transient-btree-index` allows you to create a BTree index backed by temporary files.
This is helpful if you
 
- need to index large datasets (thus only working on disk) by inserting entries in unsorted order,
- want to query entries (get and range queries) while the index is still constructed, e.g. to check existence of a previous entry, and
- need support for all serde-serializable key and value types with varying key-size.
 
Because of its intended use case, it is therefore **not possible to**
 
- delete entries once they are inserted (you can use `Option` values and set them to `Option::None`, but this will not reclaim any used space),
- persist the index to a file (you can use other crates like [sstable](https://crates.io/crates/sstable) to create immutable maps), or
- load an existing index file (you might want to use an immutable map file and this index can act as an "overlay" for all changed entries).

