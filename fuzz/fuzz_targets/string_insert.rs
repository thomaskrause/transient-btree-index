#![no_main]
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use transient_btree_index::{BtreeConfig, BtreeIndex, Error};

fuzz_target!(|data: (Vec<(String, String)>, u8)| {
    let order = data.1.max(2);
    let mut m: BTreeMap<String, String> = BTreeMap::default();
    let mut t: BtreeIndex<String, String> =
        BtreeIndex::with_capacity(BtreeConfig::default().with_order(order), 1024).unwrap();

    for (key, value) in data.0 {
        m.insert(key.clone(), value.clone());
        t.insert(key, value).unwrap();
    }

    // Check that the maps are equal
    let m: Vec<_> = m.into_iter().collect();
    let t: Result<Vec<_>, Error> = t.range(..).unwrap().collect();
    let t = t.unwrap();

    assert_eq!(m, t);
});
