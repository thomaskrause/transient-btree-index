#![no_main]
use fake::{Fake, StringFaker};
use libfuzzer_sys::fuzz_target;
use rand::SeedableRng;
use transient_btree_index::{BtreeConfig, BtreeIndex};

fuzz_target!(|seed: u64| {
    // Create an index with random entries
    let n_entries = 2000;
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let id_faker = StringFaker::with(Vec::from(ASCII), 8..16);
    let name_faker = fake::faker::name::en::Name();

    let config = BtreeConfig::default().max_key_size(16).max_value_size(64);

    let mut btree: BtreeIndex<String, String> =
        BtreeIndex::with_capacity(config, n_entries).unwrap();

    // Insert the strings
    for _ in 0..n_entries {
        btree
            .insert(
                id_faker.fake_with_rng(&mut rng),
                name_faker.fake_with_rng(&mut rng),
            )
            .unwrap();
    }
    // Generate and insert a known key/value
    let search_key: String = id_faker.fake_with_rng(&mut rng);
    let search_value: String = name_faker.fake_with_rng(&mut rng);

    btree
        .insert(search_key.clone(), search_value.clone())
        .unwrap();

    let found = btree.get(&search_key).unwrap().unwrap();
    assert_eq!(&search_value, &found);
});
