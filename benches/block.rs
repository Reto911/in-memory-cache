use itertools::Itertools;
use std::sync::Arc;
use std::thread::spawn;
use std::time::Duration;
use std::vec;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use in_memory_cache::mock::BlackHole;
use in_memory_cache::policy::LruPolicy;
use in_memory_cache::BlockCoordinate;
use in_memory_cache::InMemoryCache;
use in_memory_cache::Storage;
use in_memory_cache::{Block, IoBlock};

const KB_SIZE: usize = 1024;
const MB_SIZE: usize = 1024 * KB_SIZE;

const MEMORY_SIZE: usize = 1024 * MB_SIZE;
const JOBS: usize = 2;

const BLOCK_SIZES_IN_KB: [usize; 5] = [256, 512, 1024, 2048, 4096];

/// Create an `IoBlock`
macro_rules! create_block {
    ($content:expr, $start:expr, $end:expr) => {{
        let mut block = Block::new(($content).len());
        block.make_mut().copy_from_slice(($content).as_slice());
        let io_block = IoBlock::new(block, $start, $end);
        io_block
    }};
    ($content:expr) => {
        create_block!($content, 0, ($content).len())
    };
}

fn generate_data(block_size: usize) -> Vec<IoBlock> {
    let content = vec![1u8; block_size];
    let mut res = vec![];
    let block_num = MEMORY_SIZE / block_size;

    res.resize_with(block_num, || create_block!(content));

    res
}

fn test_single_thread_write(data: Vec<IoBlock>, block_size: usize) -> impl Storage {
    let cache = InMemoryCache::new(LruPolicy::<BlockCoordinate>::new(4), BlackHole, block_size);

    for (block_id, block) in data.into_iter().enumerate() {
        cache.store(0, block_id, block);
    }

    cache
}

fn test_multiple_thread_write(data: Vec<IoBlock>, block_size: usize) -> Arc<impl Storage> {
    let cache = Arc::new(InMemoryCache::new(
        LruPolicy::<BlockCoordinate>::new(4),
        BlackHole,
        block_size,
    ));

    for chunk in data.into_iter().enumerate().chunks(JOBS).into_iter() {
        let mut threads = Vec::with_capacity(JOBS);
        for (block_id, block) in chunk {
            let c = Arc::clone(&cache);
            let thread = spawn(move || {
                c.store(0, block_id, block);
            });
            threads.push(thread);
        }

        threads
            .into_iter()
            .for_each(|thread| thread.join().unwrap());
    }

    cache
}

fn single(c: &mut Criterion) {
    let mut group = c.benchmark_group("Single Thread Write");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.throughput(criterion::Throughput::Bytes(MEMORY_SIZE as u64));

    for ref block_size in BLOCK_SIZES_IN_KB.iter().map(|size| size * KB_SIZE) {
        group.bench_with_input(
            BenchmarkId::from_parameter(block_size / KB_SIZE),
            block_size,
            |b, &block_size| {
                b.iter_batched(
                    || generate_data(block_size),
                    |data| test_single_thread_write(data, block_size),
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn multiple(c: &mut Criterion) {
    let mut group = c.benchmark_group("Multiple Thread Write");

    for block_size in BLOCK_SIZES_IN_KB.iter().map(|s| s * KB_SIZE) {
        group.throughput(criterion::Throughput::Bytes(MEMORY_SIZE as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(block_size / KB_SIZE),
            &block_size,
            |b, &block_size| {
                b.iter_batched(
                    || generate_data(block_size),
                    |data| test_multiple_thread_write(data, block_size),
                    criterion::BatchSize::PerIteration,
                )
            },
        );
    }

    group.finish();
}

fn configure() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_secs(90))
        .sample_size(10)
}

criterion_group!(
    name = benches;
    config = configure();
    targets = single, multiple
);
criterion_main!(benches);
