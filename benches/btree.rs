use criterion::{criterion_group, criterion_main, Criterion};
use fake::{Fake, StringFaker};
use transient_btree_index::{BtreeConfig, BtreeIndex};

fn benchmark(c: &mut Criterion) {
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let id_faker = StringFaker::with(Vec::from(ASCII), 8..16);

    // Create an index with 10.000 random entries
    let n_entries = 10_000;
    let name_faker = fake::faker::name::en::Name();

    let config = BtreeConfig::default()
        .with_max_key_size(16)
        .with_max_value_size(64);

    let mut btree: BtreeIndex<String, String> =
        BtreeIndex::with_capacity(config, n_entries).unwrap();

    // Insert the strings
    for _ in 0..n_entries {
        btree
            .insert(id_faker.fake(), name_faker.fake())
            .unwrap();
    }

    c.bench_function("insert 1 string", |b| {
        // Generate and insert a known key/value
        let search_key : String = id_faker.fake();
        let search_value: String = name_faker.fake();

        b.iter(|| {
            btree
                .insert(search_key.clone(), search_value.clone())
                .unwrap();
        })
    });

    c.bench_function("search existing string", |b| {
        // Generate and insert a known key/value
        let search_key : String = id_faker.fake();
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
