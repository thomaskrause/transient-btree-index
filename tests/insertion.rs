use single_file_btree::SingleFileBtreeMap;
use tempfile::NamedTempFile;

#[test]
fn create_map() {
    let tmp_file = NamedTempFile::new().unwrap();
    let m  : SingleFileBtreeMap<u64, String>= SingleFileBtreeMap::create(tmp_file.path()) .unwrap();
}