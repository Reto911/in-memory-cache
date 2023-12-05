// Append
// Overwrite

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, SystemTime},
};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use in_memory_cache::{
    mock::BlackHole,
    policy::{Infinite, LruPolicy},
    Block, InMemoryCache, IoBlock, Storage, StorageManager,
};

const KB_SIZE: usize = 1024;
const MB_SIZE: usize = 1024 * KB_SIZE;

/// The data size of a single request
const REQUEST_SIZE: usize = 128 * KB_SIZE;
const MEMORY_SIZE: usize = 512 * MB_SIZE;

const BLOCK_SIZES_IN_KB: [usize; 4] = [128, 256, 512, 1024];

/// Create an `IoBlock`
macro_rules! create_block {
    ($content:expr, $start:expr, $end:expr) => {{
        let mut block = Block::new(($content).len());
        block.make_mut().copy_from_slice($content);
        let io_block = IoBlock::new(block, $start, $end);
        io_block
    }};
    ($content:expr) => {
        create_block!($content, 0, ($content).len())
    };
}

fn generate_content() -> Vec<u8> {
    vec![0u8; MEMORY_SIZE]
}

fn create_storage_for_append(
    block_size: usize,
) -> StorageManager<impl Storage + Send + Sync + 'static> {
    let cache = InMemoryCache::new(LruPolicy::new(4), BlackHole, block_size);
    StorageManager::new(cache, block_size)
}

fn create_storage_for_overwrite(
    block_size: usize,
) -> StorageManager<impl Storage + Send + Sync + 'static> {
    let cache = InMemoryCache::new(Infinite, BlackHole, block_size);
    let content = vec![0u8; MEMORY_SIZE];
    for (request, block_id) in content.chunks(REQUEST_SIZE).zip(0..) {
        let block = create_block!(request);
        cache.store(0, block_id, block);
    }
    StorageManager::new(cache, block_size)
}

fn test_write<S>(storage: StorageManager<S>, content: Vec<u8>, offset: usize) -> StorageManager<S>
where
    S: Storage + Send + Sync + 'static,
{
    let ino = 0;
    let mut offset = offset;
    let mut mtime = SystemTime::now();

    for request in content.chunks(REQUEST_SIZE) {
        mtime = storage.store(ino, offset, request, mtime);
        offset += request.len();
    }

    storage
}

fn test_read_after_write<S>(storage: StorageManager<S>, content_size: usize)
where
    S: Storage + Send + Sync + 'static,
{
    const LIMIT: usize = 5;
    let segment_size = content_size / LIMIT;

    let write_pointer = AtomicUsize::new(0);
    let ino = 0;

    thread::scope(|s| {
        // Writer
        s.spawn(|| {
            for point in 0..=LIMIT {
                let offset = point * segment_size;
                let content = vec![0u8; segment_size];
                storage.store(ino, offset, &content, SystemTime::now());
                write_pointer.store(point, Ordering::Release);
                // println!("Point: {point}, seg_size: {segment_size}, offset: {offset}");
            }
        });
        // Reader
        s.spawn(|| {
            for i in 0..LIMIT {
                let offset = i * segment_size;

                loop {
                    if write_pointer.load(Ordering::Acquire) > i {
                        break;
                    } else {
                        thread::yield_now();
                    }
                }

                let loaded = storage.load(ino, offset, segment_size, SystemTime::now());
                assert!(!loaded.is_empty(), "Segment {i} is empty.");
            }
        });
    });
}

pub fn append(c: &mut Criterion) {
    let mut group = c.benchmark_group("Append");

    group.throughput(criterion::Throughput::Bytes(MEMORY_SIZE as u64));

    for block_size in BLOCK_SIZES_IN_KB.iter().map(|s| s * KB_SIZE) {
        group.bench_with_input(
            BenchmarkId::from_parameter(block_size / KB_SIZE),
            &block_size,
            |b, &block_size| {
                b.iter_batched(
                    || (create_storage_for_append(block_size), generate_content()),
                    |(s, content)| test_write(s, content, 0),
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

pub fn overwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("Overwrite");

    group.throughput(criterion::Throughput::Bytes(MEMORY_SIZE as u64));
    group.sampling_mode(criterion::SamplingMode::Flat);
    for block_size in BLOCK_SIZES_IN_KB.iter().map(|s| s * KB_SIZE) {
        group.bench_with_input(
            BenchmarkId::from_parameter(block_size / KB_SIZE),
            &block_size,
            |b, &block_size| {
                b.iter_batched(
                    || (create_storage_for_overwrite(block_size), generate_content()),
                    |(s, content)| test_write(s, content, 0),
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

pub fn read_after_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read after Write");
    let content_size = 64 * MB_SIZE;
    group.throughput(criterion::Throughput::Bytes(content_size as u64));
    for block_size in BLOCK_SIZES_IN_KB.iter().map(|s| s * KB_SIZE) {
        group.bench_with_input(
            BenchmarkId::from_parameter(block_size / KB_SIZE),
            &block_size,
            |b, &block_size| {
                b.iter_batched(
                    || {
                        let cache = InMemoryCache::new(Infinite, BlackHole, block_size);
                        StorageManager::new(cache, block_size)
                    },
                    |s| test_read_after_write(s, content_size),
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
}

fn configure() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_secs(90))
        .sample_size(10)
}

criterion_group!(
    name = append_group;
    config = configure();
    targets = append
);

criterion_group!(
    name = overwrite_group;
    config = configure();
    targets = overwrite
);

criterion_group!(
    name = read_after_write_group;
    config = configure();
    targets = read_after_write
);

criterion_main!(append_group, overwrite_group, read_after_write_group);
