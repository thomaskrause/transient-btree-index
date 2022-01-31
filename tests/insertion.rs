use transient_btree_index::BtreeIndex;
use tempfile::NamedTempFile;

#[test]
fn create_map() {
    let tmp_file = NamedTempFile::new().unwrap();
    let m: BtreeIndex<u64, String> = BtreeIndex::create(tmp_file.path()).unwrap();
}
