use criterion::{criterion_group, criterion_main, Criterion};
use fake::{Fake, Faker, StringFaker};
use transient_btree_index::{BtreeConfig, BtreeIndex};

const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn fixed_vs_variable(c: &mut Criterion) {
    let mut g = c.benchmark_group("variable vs. fixed tuple size");

    let n_entries = 10_000;
    let name_faker = fake::faker::name::en::Name();

    g.bench_function("insert fixed size key", |b| {
        let mut btree: BtreeIndex<u64, String> =
            BtreeIndex::fixed_key_size_with_capacity::<generic_array::typenum::U8>(
                BtreeConfig::default().max_key_size(8).max_value_size(64),
                n_entries,
            )
            .unwrap();

        // Insert the initial strings
        for _ in 0..n_entries {
            btree.insert(Faker.fake(), name_faker.fake()).unwrap();
        }

        let additional_key: u64 = Faker.fake();
        let additional_value: String = name_faker.fake();

        b.iter(|| {
            btree
                .insert(additional_key.clone(), additional_value.clone())
                .unwrap();
        })
    });

    g.bench_function("insert variable size key", |b| {
        let mut btree: BtreeIndex<u64, String> = BtreeIndex::with_capacity(
            BtreeConfig::default().max_key_size(8).max_value_size(64),
            n_entries,
        )
        .unwrap();

        // Insert the initial strings
        for _ in 0..n_entries {
            btree.insert(Faker.fake(), name_faker.fake()).unwrap();
        }

        let additional_key: u64 = Faker.fake();
        let additional_value: String = name_faker.fake();

        b.iter(|| {
            btree
                .insert(additional_key.clone(), additional_value.clone())
                .unwrap();
        })
    });

    g.finish()
}

fn insertion(c: &mut Criterion) {
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
}

fn search(c: &mut Criterion) {
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

criterion_group!(benches, insertion, fixed_vs_variable, search);
criterion_main!(benches);
