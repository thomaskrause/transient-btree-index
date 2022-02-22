use super::*;

#[test]
fn allocate_nodes() {
    let mut f: NodeFile<u64, _> = NodeFile::with_capacity(0, &BtreeConfig::default()).unwrap();
    let n1 = f.allocate_new_node().unwrap();
    let n2 = f.allocate_new_node().unwrap();
    let n3 = f.allocate_new_node().unwrap();

    assert_eq!(0, n1);
    assert_eq!(1, n2);
    assert_eq!(2, n3);

    assert_eq!(0, f.number_of_keys(n1).unwrap());
    assert_eq!(true, f.is_leaf(n1).unwrap());
}
