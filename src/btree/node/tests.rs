use super::*;

#[test]
fn allocate_nodes() {
    let mut f : NodeFile<u64> = NodeFile::with_capacity(0, &BtreeConfig::default()).unwrap();
    let n1 = f.allocate_block().unwrap();
    let n2 = f.allocate_block().unwrap();
    let n3 = f.allocate_block().unwrap();

    assert_eq!(0, n1);
    assert_eq!(1, n2);
    assert_eq!(2, n3);

    assert_eq!(0, f.get_mut(n1).unwrap().num_keys().read());
    assert_eq!(1, f.get_mut(n1).unwrap().is_leaf().read());
}
