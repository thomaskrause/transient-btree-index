#![no_main]
use libfuzzer_sys::fuzz_target;

use std::collections::BTreeMap;
use transient_btree_index::{BtreeConfig, BtreeIndex, Error};

fuzz_target!(|data: (Vec<(u32, u32)>, u8)| {
    let order = data.1.max(2);
    let mut m = BTreeMap::default();
    let mut fixture =
        BtreeIndex::with_capacity(BtreeConfig::default().with_order(order), 1024).unwrap();

    for (key, value) in data.0 {
        m.insert(key, value);
        fixture.insert(key, value).unwrap();
    }

    // Check len() function
    assert_eq!(m.len(), fixture.len());

    // get query for each entry
    for (k, v1) in m.iter() {
        assert!(fixture.contains_key(k).unwrap());
        let v2 = fixture.get(k).unwrap();
        assert_eq!(Some(*v1), v2);
    }

    // Check that the maps are equal with differnt range queries
    let m: Vec<_> = m.into_iter().collect();
    let fixture_result: Result<Vec<_>, Error> = fixture.range(..).unwrap().collect();
    let fixture_result = fixture_result.unwrap();

    assert_eq!(m, fixture_result);
});
