use criterion::{criterion_group, criterion_main, Criterion};
use fake::{Fake, Faker, StringFaker};
use serde_derive::{Deserialize, Serialize};
use transient_btree_index::{AsByteVec, BtreeConfig, BtreeIndex, FromByteSlice};

const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

#[derive(Serialize, Deserialize, Clone, PartialEq, PartialOrd, Eq, Ord)]
struct FixedKey(u64);

impl AsByteVec for FixedKey {
    fn as_byte_vec(&self) -> Vec<u8> {
        self.0.to_le_bytes().into()
    }
}

impl FromByteSlice for FixedKey {
    fn from_byte_slice<T: AsRef<[u8]> + ?Sized>(
        slice: &T,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized,
    {
        let slice: &[u8] = slice.as_ref();
        let bytes: [u8; 8] = slice.try_into()?;
        Ok(FixedKey(u64::from_le_bytes(bytes)))
    }
}

fn fixed_vs_variable(c: &mut Criterion) {
    let mut g = c.benchmark_group("variable vs. fixed tuple size");

    let n_entries = 10_000;
    let name_faker = fake::faker::name::en::Name();

    g.bench_function("insert fixed size key", |b| {
        let mut btree: BtreeIndex<FixedKey, String> =
            BtreeIndex::fixed_key_size_with_capacity::<generic_array::typenum::U8>(
                BtreeConfig::default().max_key_size(8).max_value_size(64),
                n_entries,
            )
            .unwrap();

        // Insert the initial strings
        for _ in 0..n_entries {
            btree
                .insert(FixedKey(Faker.fake()), name_faker.fake())
                .unwrap();
        }

        let additional_key = FixedKey(Faker.fake());
        let additional_value: String = name_faker.fake();

        b.iter(|| {
            btree
                .insert(additional_key.clone(), additional_value.clone())
                .unwrap();
        })
    });

    g.bench_function("insert variable size key", |b| {
        let mut btree: BtreeIndex<FixedKey, String> = BtreeIndex::with_capacity(
            BtreeConfig::default().max_key_size(8).max_value_size(64),
            n_entries,
        )
        .unwrap();

        // Insert the initial strings
        for _ in 0..n_entries {
            btree
                .insert(FixedKey(Faker.fake()), name_faker.fake())
                .unwrap();
        }

        let additional_key = FixedKey(Faker.fake());
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
