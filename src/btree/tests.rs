use crate::BtreeIndex;
use debug_tree::TreeBuilder;
use std::{cmp::Ordering, collections::BTreeMap, fmt::Debug};

use super::*;

fn print_tree<K, V>(t: &BtreeIndex<K, V>) -> Result<()>
where
    K: Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Debug,
    V: Serialize + DeserializeOwned + Clone,
{
    let mut b = TreeBuilder::new();

    print_tree_node(&mut b, t, t.root_id)?;

    b.print();
    Ok(())
}

fn print_tree_node<K, V>(builder: &mut TreeBuilder, t: &BtreeIndex<K, V>, node: usize) -> Result<()>
where
    K: Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Debug,
    V: Serialize + DeserializeOwned + Clone,
{
    let nb = t.keys.get(node)?;
    let mut branch = builder.add_branch(&format!(
        "{} (node with {} keys and {} children)",
        nb.id,
        nb.keys.len(),
        nb.child_nodes.len()
    ));
    if nb.is_leaf() {
        // Only print the keys
        for k in nb.keys.iter() {
            builder.add_leaf(&format!("{:?} (key)", k.key));
        }
    } else {
        // Print both the keys and the child nodes
        let max_index = nb.child_nodes.len().max(nb.keys.len());
        for i in 0..max_index {
            if i < nb.child_nodes.len() {
                print_tree_node(builder, t, nb.child_nodes[i])?;
            } else {
                builder.add_leaf(&format!("ERROR: no child at index {}", i));
            }
            if i < nb.keys.len() {
                builder.add_leaf(&format!("{:?} (key)", nb.keys[i].key));
            } else if i < nb.child_nodes.len() - 1 {
                builder.add_leaf(&format!("ERROR: no key at index {}", i));
            }
        }
    }
    branch.release();

    Ok(())
}

fn check_order<K, V, R>(t: &BtreeIndex<K, V>, range: R)
where
    K: Serialize + DeserializeOwned + PartialOrd + Clone + Ord + Debug,
    V: Serialize + DeserializeOwned + Clone,
    R: RangeBounds<K>,
{
    let mut previous: Option<K> = None;
    for e in t.range(range).unwrap() {
        let (k, _v) = e.unwrap();

        if let Some(previous) = previous {
            if &previous >= &k {
                dbg!(&previous, &k);
            }
            assert_eq!(Ordering::Less, previous.cmp(&k));
        }

        previous = Some(k);
    }
}

#[test]
fn insert_get_static_size() {
    let nr_entries = 2000;

    let config = BtreeConfig::default()
        .with_max_key_size(8)
        .with_max_value_size(8);

    let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 2000).unwrap();

    assert_eq!(true, t.is_empty());

    t.insert(0, 42).unwrap();

    assert_eq!(false, t.is_empty());
    assert_eq!(1, t.len());

    for i in 1..nr_entries {
        t.insert(i, i).unwrap();
    }

    assert_eq!(false, t.is_empty());
    assert_eq!(nr_entries as usize, t.len());

    assert_eq!(true, t.contains_key(&0).unwrap());
    assert_eq!(Some(42), t.get(&0).unwrap());
    for i in 1..nr_entries {
        assert_eq!(true, t.contains_key(&i).unwrap());

        let v = t.get(&i).unwrap();
        assert_eq!(Some(i), v);
    }
    assert_eq!(false, t.contains_key(&nr_entries).unwrap());
    assert_eq!(None, t.get(&nr_entries).unwrap());
    assert_eq!(false, t.contains_key(&5000).unwrap());
    assert_eq!(None, t.get(&5000).unwrap());
}

#[test]
fn range_query_dense() {
    let nr_entries = 2000;

    let config = BtreeConfig::default()
        .with_max_key_size(8)
        .with_max_value_size(8);

    let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 2000).unwrap();

    for i in 0..nr_entries {
        t.insert(i, i).unwrap();
    }

    // Get sub-range
    let result: Result<Vec<_>> = t.range(40..1024).unwrap().collect();
    let result = result.unwrap();
    assert_eq!(984, result.len());
    assert_eq!((40, 40), result[0]);
    assert_eq!((1023, 1023), result[983]);
    check_order(&t, 40..1024);

    // Get complete range
    let result: Result<Vec<_>> = t.range(..).unwrap().collect();
    let result = result.unwrap();
    assert_eq!(2000, result.len());
    assert_eq!((0, 0), result[0]);
    assert_eq!((1999, 1999), result[1999]);
    check_order(&t, ..);
}

#[test]
fn range_query_sparse() {
    let config = BtreeConfig::default()
        .with_max_key_size(8)
        .with_max_value_size(8);

    let mut t: BtreeIndex<u64, u64> = BtreeIndex::with_capacity(config, 200).unwrap();

    for i in (0..2000).step_by(10) {
        t.insert(i, i).unwrap();
    }

    assert_eq!(200, t.len());

    // Get sub-range
    let result: Result<Vec<_>> = t.range(40..1200).unwrap().collect();
    let result = result.unwrap();
    assert_eq!(116, result.len());
    assert_eq!((40, 40), result[0]);
    check_order(&t, 40..1200);

    // Get complete range
    let result: Result<Vec<_>> = t.range(..).unwrap().collect();
    let result = result.unwrap();
    assert_eq!(200, result.len());
    assert_eq!((0, 0), result[0]);
    assert_eq!((1990, 1990), result[199]);
    check_order(&t, ..);

    // Check different variants of range queries
    check_order(&t, 40..=1200);
    check_order(&t, 40..);
    check_order(&t, ..1024);
    check_order(&t, ..=1024);
}

#[test]
fn minimal_order() {
    let nr_entries = 2000u64;

    // Too small orders should create an error
    assert_eq!(
        true,
        BtreeIndex::<u64, u64>::with_capacity(
            BtreeConfig::default().with_order(0),
            nr_entries as usize
        )
        .is_err()
    );
    assert_eq!(
        true,
        BtreeIndex::<u64, u64>::with_capacity(
            BtreeConfig::default().with_order(1),
            nr_entries as usize
        )
        .is_err()
    );

    // Test with the minimal order 2
    let config = BtreeConfig::default()
        .with_max_key_size(8)
        .with_max_value_size(8)
        .with_order(2);

    let mut t: BtreeIndex<u64, u64> =
        BtreeIndex::with_capacity(config, nr_entries as usize).unwrap();

    for i in 0..nr_entries {
        t.insert(i, i).unwrap();
    }

    // Get sub-range
    let result: Result<Vec<_>> = t.range(40..1024).unwrap().collect();
    let result = result.unwrap();
    assert_eq!(984, result.len());
    assert_eq!((40, 40), result[0]);
    assert_eq!((1023, 1023), result[983]);
    check_order(&t, 40..1024);

    // Get complete range
    let result: Result<Vec<_>> = t.range(..).unwrap().collect();
    let result = result.unwrap();
    assert_eq!(2000, result.len());
    assert_eq!((0, 0), result[0]);
    assert_eq!((1999, 1999), result[1999]);
    check_order(&t, ..);
}

#[test]
fn sorted_iterator() {
    let config = BtreeConfig::default()
        .with_max_key_size(64)
        .with_max_value_size(64);

    let mut t: BtreeIndex<Vec<u8>, bool> = BtreeIndex::with_capacity(config, 128).unwrap();

    for a in 0..=255 {
        t.insert(vec![1, a], true).unwrap();
    }
    for a in 0..=255 {
        t.insert(vec![0, a], true).unwrap();
    }
    assert_eq!(512, t.len());
    check_order(&t, ..);
}

#[test]
fn fuzz1() {
    let input = vec![
        (
            "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\t\u{0}\u{0}\u{0}\u{1f}",
            "",
        ),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("<", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("", ""),
        ("/", ""),
        ("", ""),
        ("\u{12}\u{12}", "\u{12}\u{12}\u{12}\u{12}\u{12}\u{12}"),
        ("", ""),
        ("/", ""),
        ("", ""),
        ("", ""),
    ];

    let mut m = BTreeMap::default();
    let mut t = BtreeIndex::with_capacity(BtreeConfig::default().with_order(2), 1024).unwrap();

    for (key, value) in input {
        m.insert(key.to_string(), value.to_string());
        t.insert(key.to_string(), value.to_string()).unwrap();
    }

    let m: Vec<_> = m.into_iter().collect();
    let t: Result<Vec<_>> = t.range(..).unwrap().collect();
    let t = t.unwrap();

    assert_eq!(m, t);
}

#[test]
fn fuzz2() {
    let input: Vec<(u32, u32)> = vec![
        (67109157, 2610666395),
        (77306779, 2610666395),
        (2604374939, 2610666395),
        (4294967295, 3587506687),
        (3587560917, 3583770369),
        (3587560917, 0),
    ];

    let mut m = BTreeMap::default();
    let mut t = BtreeIndex::with_capacity(BtreeConfig::default().with_order(2), 1024).unwrap();

    for (key, value) in input {
        m.insert(key.to_string(), value.to_string());
        t.insert(key.to_string(), value.to_string()).unwrap();
    }

    print_tree(&t).unwrap();

    let m: Vec<_> = m.into_iter().collect();
    let t: Result<Vec<_>> = t.range(..).unwrap().collect();
    let t = t.unwrap();

    assert_eq!(m, t);
}