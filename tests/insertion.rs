use transient_btree_index::BtreeIndex;

#[test]
fn create_map() {
    let m: BtreeIndex<u64, String> = BtreeIndex::with_capacity(0).unwrap();
}
