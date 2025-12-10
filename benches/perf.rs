// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arrow_digest::RecordDigest;
use std::path::Path;
use std::sync::Arc;

use arrow::array;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng, SeedableRng};

///////////////////////////////////////////////////////////////////////////////

fn setup_batch(params: &Params) -> RecordBatch {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(123_456);

    let mut columns: Vec<Arc<dyn array::Array>> = Vec::new();

    for _ in 0..params.num_columns {
        let mut b = array::PrimitiveBuilder::<Int64Type>::with_capacity(params.num_records);

        let mut buf = vec![0; params.num_records];
        rng.fill(&mut buf[..]);
        b.append_slice(&buf[..]);

        columns.push(Arc::new(b.finish()));
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(
            (0..params.num_columns)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        )),
        columns,
    )
    .unwrap()
}

fn setup_batch_nullable(params: &Params) -> RecordBatch {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(123_456);

    let mut columns: Vec<Arc<dyn array::Array>> = Vec::new();

    for _ in 0..params.num_columns {
        let mut b = array::PrimitiveBuilder::<Int64Type>::with_capacity(params.num_records);

        for _ in 0..params.num_records {
            if rng.random_bool(0.5) {
                b.append_null()
            } else {
                b.append_value(rng.random())
            }
        }

        columns.push(Arc::new(b.finish()));
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(
            (0..params.num_columns)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        )),
        columns,
    )
    .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

fn setup_flat_data(params: &Params) -> Vec<u8> {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(123_456);
    let mut buf = vec![0; params.total_bytes_to_hash()];
    rng.fill(&mut buf[..]);
    buf
}

///////////////////////////////////////////////////////////////////////////////

fn bench_read_parquet(path: &Path) {
    let reader = parquet::arrow::arrow_reader::ArrowReaderBuilder::try_new(
        std::fs::File::open(path).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();

    let _batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
}

///////////////////////////////////////////////////////////////////////////////

fn bench_write_parquet(batch: &RecordBatch, out_path: &Path) {
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(out_path).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

///////////////////////////////////////////////////////////////////////////////

// sha2-256 is used as default by IPFS
// See: https://github.com/ipfs/kubo/blob/ba3f7f39bdac3f0ebc2ce2741608af4036abdb3f/core/commands/add.go#L163
// See: https://github.com/ipfs/kubo/issues/6893
fn bench_sha2_256(data: &[u8]) {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    hasher.finalize();
}

fn bench_sha3_256(data: &[u8]) {
    use sha3::Digest;
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(data);
    hasher.finalize();
}

fn bench_blake2_512(data: &[u8]) {
    use sha3::Digest;
    let mut hasher = blake2::Blake2b512::new();
    hasher.update(data);
    hasher.finalize();
}

fn bench_xxh3_128(data: &[u8]) {
    xxhash_rust::xxh3::xxh3_128(data);
}

///////////////////////////////////////////////////////////////////////////////

struct Params {
    num_columns: usize,
    num_records: usize,
}

impl Params {
    fn total_bytes_to_hash(&self) -> usize {
        self.num_records * self.num_columns * std::mem::size_of::<i64>()
            + (self.num_records * self.num_columns) / 8
            + if (self.num_records * self.num_columns) % 8 != 0 {
                1
            } else {
                0
            }
    }
}

fn bench(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().join("data.parquet");

    let params = Params {
        num_columns: 8,
        num_records: 1_000_000,
    };

    println!(
        "Benchmarking using ({} x {}) dataset (~{} bytes)",
        params.num_columns,
        params.num_records,
        params.total_bytes_to_hash()
    );

    let batch = setup_batch(&params);
    let batch_null = setup_batch_nullable(&params);
    let flat_data = setup_flat_data(&params);

    {
        let mut group = c.benchmark_group("baseline");

        group.sample_size(10);
        group.bench_function("write_parquet", |b| {
            b.iter(|| bench_write_parquet(&batch, &data_path));
        });

        bench_write_parquet(&batch, &data_path);
        group.bench_function("read_parquet", |b| {
            b.iter(|| bench_read_parquet(&data_path));
        });

        group.bench_function("sha2_256", |b| {
            b.iter(|| bench_sha2_256(&flat_data));
        });

        group.bench_function("sha3_256", |b| {
            b.iter(|| bench_sha3_256(&flat_data));
        });

        group.bench_function("blake2_512", |b| {
            b.iter(|| bench_blake2_512(&flat_data));
        });

        group.bench_function("xxh3_128", |b| {
            b.iter(|| bench_xxh3_128(&flat_data));
        });
    }

    {
        let mut group = c.benchmark_group("arrow-digest");

        group.bench_function("sha3_256_i64_no_nulls", |b| {
            b.iter(|| arrow_digest::RecordDigestV0::<sha3::Sha3_256>::digest(&batch));
        });

        group.bench_function("sha3_256_i64_with_nulls", |b| {
            b.iter(|| arrow_digest::RecordDigestV0::<sha3::Sha3_256>::digest(&batch_null));
        });
    }
}

///////////////////////////////////////////////////////////////////////////////

criterion_group!(benches, bench);
criterion_main!(benches);
