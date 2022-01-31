use transient_btree_index::BtreeIndex;

#[test]
fn create_map() {
    let mut _m: BtreeIndex<u64, String> = BtreeIndex::with_capacity(0).unwrap();

    //m.insert(42, "TestValue".to_string()).unwrap();
}
