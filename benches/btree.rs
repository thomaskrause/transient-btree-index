use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use fake::{Fake, Faker, StringFaker};
use transient_btree_index::{BtreeConfig, BtreeIndex};

fn insertion_benchmark(c: &mut Criterion) {
    let mut g_insertion = c.benchmark_group("insertion");
    g_insertion.sample_size(20);
    g_insertion.measurement_time(Duration::from_secs(60));
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let id_faker = StringFaker::with(Vec::from(ASCII), 8..16);

    g_insertion.bench_function("insert 10.000 strings", |b| {
        let n_entries = 10_000;
        let name_faker = fake::faker::name::en::Name();
        // Create some random strings to insert
        let mut entries: Vec<(String, String)> = Vec::with_capacity(n_entries);
        for _ in 0..n_entries {
            entries.push((id_faker.fake(), name_faker.fake()))
        }

        let config = BtreeConfig::default()
            .with_max_key_size(16)
            .with_max_value_size(64);

        let mut btree: BtreeIndex<String, String> =
            BtreeIndex::with_capacity(config, n_entries).unwrap();
        b.iter(|| {
            for e in &entries {
                btree.insert(e.0.to_string(), e.1.to_string()).unwrap();
            }
        })
    });
}
fn search_benchmark(c: &mut Criterion) {
    let mut g_search = c.benchmark_group("search");

    g_search.bench_function("search existing string", |b| {
        let n_entries = 10_000;
        let name_faker = fake::faker::name::en::Name();

        let search_key = Faker.fake::<String>();
        let search_value: String = name_faker.fake();

        let config = BtreeConfig::default()
            .with_max_key_size(64)
            .with_max_value_size(64);

        let mut btree: BtreeIndex<String, String> =
            BtreeIndex::with_capacity(config, n_entries).unwrap();
        btree
            .insert(search_key.clone(), search_value.clone())
            .unwrap();

        // Create some more random strings
        for _ in 1..n_entries {
            btree
                .insert(Faker.fake::<String>(), name_faker.fake())
                .unwrap();
        }
        b.iter(|| {
            let found = btree.get(&search_key).unwrap().unwrap();
            assert_eq!(&search_value, &found);
        })
    });
}

criterion_group!(benches, insertion_benchmark, search_benchmark);
criterion_main!(benches);
