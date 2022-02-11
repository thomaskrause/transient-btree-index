use criterion::{criterion_group, criterion_main, Criterion};
use fake::{Fake, StringFaker};
use transient_btree_index::{BtreeConfig, BtreeIndex};

fn benchmark(c: &mut Criterion) {
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    c.bench_function("insert 1 string", |b| {
        // Create an index with 10.000 random entries
        let n_entries = 10_000;
        let id_faker = StringFaker::with(Vec::from(ASCII), 8..16);
        let name_faker = fake::faker::name::en::Name();

        let config = BtreeConfig::default().max_key_size(16).max_value_size(64);

        let mut btree: BtreeIndex<String, String> =
            BtreeIndex::with_capacity(config, n_entries).unwrap();

        // Insert the strings
        for _ in 0..n_entries {
            btree.insert(id_faker.fake(), name_faker.fake()).unwrap();
        }

        // Generate and insert a known key/value
        let search_key: String = id_faker.fake();
        let search_value: String = name_faker.fake();

        b.iter(|| {
            btree
                .insert(search_key.clone(), search_value.clone())
                .unwrap();
        })
    });

    c.bench_function("insert byte vector", |b| {
        // Create an index with 10.000 random entries
        let n_entries = 10_000;

        let config = BtreeConfig::default().max_key_size(16).max_value_size(64);

        let mut btree: BtreeIndex<Vec<u8>, Vec<u8>> =
            BtreeIndex::with_capacity(config, n_entries).unwrap();

        // Insert the strings
        for _ in 0..n_entries {
            btree
                .insert(fake::vec![u8; 4..16], fake::vec![u8; 32..64])
                .unwrap();
        }

        // Generate and insert a known key/value
        let search_key = fake::vec![u8; 4..16];
        let search_value = fake::vec![u8; 32..64];

        b.iter(|| {
            btree
                .insert(search_key.clone(), search_value.clone())
                .unwrap();
        })
    });

    c.bench_function("search existing string", |b| {
        // Create an index with 10.000 random entries
        let n_entries = 10_000;
        let id_faker = StringFaker::with(Vec::from(ASCII), 8..16);
        let name_faker = fake::faker::name::en::Name();

        let config = BtreeConfig::default().max_key_size(16).max_value_size(64);

        let mut btree: BtreeIndex<String, String> =
            BtreeIndex::with_capacity(config, n_entries).unwrap();

        // Insert the strings
        for _ in 0..n_entries {
            btree.insert(id_faker.fake(), name_faker.fake()).unwrap();
        }
        // Generate and insert a known key/value
        let search_key: String = id_faker.fake();
        let search_value: String = name_faker.fake();

        btree
            .insert(search_key.clone(), search_value.clone())
            .unwrap();

        b.iter(|| {
            let found = btree.get(&search_key).unwrap().unwrap();
            assert_eq!(&search_value, &found);
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
