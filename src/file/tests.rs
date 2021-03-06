use super::VariableSizeTupleFile;
use crate::file::{FixedSizeTupleFile, TupleFile};

#[test]
fn grow_mmap_from_zero_capacity() {
    // Create file with empty capacity
    let mut m = VariableSizeTupleFile::<u64>::with_capacity(0, 0).unwrap();
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
    let mut m = VariableSizeTupleFile::<u64>::with_capacity(4096, 0).unwrap();
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
    let mut m = VariableSizeTupleFile::<Vec<u64>>::with_capacity(128, 0).unwrap();
    assert_eq!(128, m.mmap.len());

    let mut b: Vec<u64> = std::iter::repeat(42).take(10).collect();
    let idx = m
        .allocate_block(256 - crate::file::BlockHeader::size())
        .unwrap();
    // The block needs space for the data, but also for the header
    assert_eq!(256, m.mmap.len());

    // Insert the block as it is
    assert_eq!(true, m.can_update(idx, &b).is_ok());
    m.put(idx, &b).unwrap();

    // Get and check it is still equal
    let retrieved_block = m.get_owned(idx).unwrap();
    assert_eq!(b, retrieved_block);

    // The block should be able to hold a little bit more vector elements
    for i in 1..20 {
        b.push(i);
    }
    assert_eq!(true, m.can_update(idx, &b).is_ok());
    m.put(idx, &b).unwrap();
    let retrieved_block = m.get_owned(idx).unwrap();
    assert_eq!(b, retrieved_block);

    // We can't grow the block beyond the allocated limit
    let mut large_block = b.clone();
    for i in 1..300 {
        large_block.push(i);
    }
    assert_eq!(false, m.can_update(idx, &large_block).unwrap().0);
    // Check that we can still insert the block, but that the relocation table is updated
    m.put(idx, &large_block).unwrap();
    assert_eq!(1, m.relocated_blocks.len());
    assert_eq!(true, m.relocated_blocks.contains_key(&idx));
    // Get the block and check the new value is returned
    assert_eq!(large_block, m.get_owned(idx).unwrap());
}

#[test]
fn block_insert_get_update_fixed_size() {
    let mut m = FixedSizeTupleFile::<u64>::with_capacity(128, 8).unwrap();
    assert_eq!(128, m.mmap.len());

    // Check that we can't allocate block with a size different to 8
    assert!(m.allocate_block(4).is_err());
    assert!(m.allocate_block(16).is_err());

    let b = 42;
    let idx = m.allocate_block(8).unwrap();

    // Insert the block as it is
    m.put(idx, &b).unwrap();

    // Get the block and check the new value is returned
    assert_eq!(b, m.get_owned(idx).unwrap());
}
