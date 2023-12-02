//! The storage manager.

use clippy_utilities::OverflowArithmetic;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use std::{sync::Arc, time::SystemTime};

use super::{Block, IoBlock, Storage};
use crate::INum;

/// The storage manager, which exposes the interfaces to `FileSystem` for interacting with the storage layers.
pub struct StorageManager<S> {
    /// The top-level storage, `InMemoryCache` for example
    storage: Arc<S>,
    /// Block size in bytes
    block_size: usize,
    /// Last modified times of the cache (on file level)
    mtimes: HashMap<INum, SystemTime>,
}

impl<S> StorageManager<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Create a `StorageManager` with the top-level `Storage`.
    pub fn new(storage: S, block_size: usize) -> Self {
        StorageManager {
            storage: Arc::new(storage),
            block_size,
            mtimes: HashMap::new(),
        }
    }

    /// Convert offset in byte to block id via the equation:
    ///
    /// `block_id = offset / block_size`
    fn offset_to_block_id(&self, offset: usize) -> usize {
        offset.overflow_div(self.block_size)
    }

    /// Load blocks from the storage concurrently.
    fn load_blocks(&self, ino: INum, start_block: usize, end_block: usize) -> Vec<Block> {
        let mut maybe_blocks = vec![];

        for block_id in start_block..end_block {
            let block = self.storage.load(ino, block_id);
            maybe_blocks.push(block);
        }

        maybe_blocks
            .into_iter()
            .take_while(|b| b.is_some())
            .filter_map(|b| b)
            .collect()
    }

    /// Store blocks into the storage concurrently.
    fn store_blocks(
        &self,
        ino: INum,
        start_block: usize,
        io_blocks: impl Iterator<Item = IoBlock>,
    ) {
        for (io_block, block_id) in io_blocks.zip(start_block..) {
            self.storage.store(ino, block_id, io_block);
        }
    }

    /// Convert `IoBlock`s from slice.
    fn make_io_blocks_from_slice(
        &self,
        offset: usize,
        data: &[u8],
    ) -> impl Iterator<Item = IoBlock> + '_ {
        let data_len = data.len();

        let mut blocks = vec![];

        // Handle the first block
        let mut first_block = Block::new(self.block_size);
        let first_block_offset = offset.overflow_rem(self.block_size);
        let first_block_end = first_block_offset
            .overflow_add(data_len)
            .min(self.block_size);
        let first_block_length = first_block_end.overflow_sub(first_block_offset);
        let first_block_data = data
            .get(..first_block_length)
            .unwrap_or_else(|| unreachable!("`data` is checked that it has enough bytes."));
        first_block
            .make_mut()
            .get_mut(first_block_offset..first_block_end)
            .unwrap_or_else(|| {
                unreachable!("Both `blk_offset` and `blk_end` must not be greater than `blk_size`")
            })
            .copy_from_slice(first_block_data);
        blocks.push(first_block);

        // Handle the rest blocks
        if data_len > first_block_length {
            let chunks = data
                .get(first_block_length..)
                .unwrap_or_else(|| unreachable!("`data` is checked that it has enough bytes."))
                .chunks(self.block_size);
            for chunk in chunks {
                let chunk_len = chunk.len();
                let mut block = Block::new(self.block_size);
                block
                    .make_mut()
                    .get_mut(..chunk_len)
                    .unwrap_or_else(|| {
                        unreachable!("The `capacity` must be greater than `chunk_len`")
                    })
                    .copy_from_slice(chunk);
                blocks.push(block);
            }
        }

        let block_num = blocks.len();

        // Convert blocks to IoBlock
        blocks.into_iter().enumerate().map(move |(i, block)| {
            let offset = if i == 0 { first_block_offset } else { 0 };

            let end = if i == 0 {
                first_block_end
            } else if i == block_num.overflow_sub(1) {
                first_block_offset
                    .overflow_add(data_len)
                    .overflow_sub(1)
                    .overflow_rem(self.block_size)
                    .overflow_add(1)
            } else {
                self.block_size
            };

            IoBlock::new(block, offset, end)
        })
    }

    /// Load data from storage.
    pub fn load(&self, ino: INum, offset: usize, len: usize, mtime: SystemTime) -> Vec<IoBlock> {
        // Check if the cache is valid.
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage.invalidate(ino);
        }

        if len == 0 {
            return vec![];
        }

        // Calculate the `[start_block, end_block)` range.
        let start_block = self.offset_to_block_id(offset);
        let end_block = self
            .offset_to_block_id(offset.overflow_add(len).overflow_sub(1))
            .overflow_add(1);

        let blocks = self.load_blocks(ino, start_block, end_block);

        // If the cache is invalidated, it must be re-fetched from backend.
        // So the mtime of the cache should be updated to the passed-in one.
        if invalid {
            self.mtimes.insert(ino, mtime);
        }

        let block_num = blocks.len();
        // Convert `Block`s to `IoBlock`s
        blocks
            .into_iter()
            .enumerate()
            .map(|(i, block)| {
                // Calculate the start_offset and end_offset inside each block.
                // For example, for block_size = 4, offset = 1, len = 9:
                //
                // | 0 1 2 3 |  4 5 6 7 |  8 9 A B |
                //   0 1 2 3 4  0 1 2 3 4  0 1 2 3 4
                //     ^     ^  ^       ^  ^   ^
                //     s     e  s       e  s   e
                let start_offset = if i == 0 {
                    offset.overflow_rem(self.block_size)
                } else {
                    0
                };

                let end_offset = if i == block_num.overflow_sub(1) {
                    offset
                        .overflow_add(len)
                        .overflow_sub(1)
                        .overflow_rem(self.block_size)
                        .overflow_add(1)
                } else {
                    self.block_size
                };

                IoBlock::new(block, start_offset, end_offset)
            })
            .collect()
    }

    /// Store data into storage.
    pub fn store(&self, ino: INum, offset: usize, data: &[u8], mtime: SystemTime) -> SystemTime {
        // Check if the cache is valid.
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage.invalidate(ino);
        }

        if data.is_empty() {
            // The cache is invalid, but no new blocks will be loaded into the cache
            // thus the mtime of this file is removed.
            if invalid {
                self.mtimes.remove(&ino);
            }
            // No data will be written, so the passed-in mtime will be passed-out changelessly.
            return mtime;
        }

        let start_block = self.offset_to_block_id(offset);

        let io_blocks = self.make_io_blocks_from_slice(offset, data);

        self.store_blocks(ino, start_block, io_blocks);

        // As the cache is overwritten, the cache mtime should be set to now.
        let new_mtime = SystemTime::now();
        self.mtimes.insert(ino, new_mtime);

        new_mtime
    }

    /// Remove a file from the storage.
    pub fn remove(&self, ino: INum) {
        self.mtimes.remove(&ino);
        self.storage.remove(ino);
    }

    /// Flush the cache to the persistent layer.
    pub fn flush(&self, ino: INum) {
        self.storage.flush(ino);
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, SystemTime},
    };

    use clippy_utilities::OverflowArithmetic;

    use crate::{
        mock::MemoryStorage, policy::LruPolicy, Block, BlockCoordinate, InMemoryCache, Storage,
        StorageManager,
    };

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
    const CACHE_CAPACITY_IN_BLOCKS: usize = 4;

    #[test]
    fn test_read_write_single_block() {
        let ino = 0;
        let offset = 0;
        let mtime = SystemTime::now();

        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        let new_mtime = storage.store(ino, offset, BLOCK_CONTENT, mtime);

        let loaded = storage.load(ino, 4, 4, new_mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"bar ");

        let loaded = storage.load(ino, 0, BLOCK_SIZE_IN_BYTES, new_mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), BLOCK_CONTENT);
    }

    #[test]
    fn test_read_write_miltiple_blocks() {
        let ino = 0;
        let offset = 0;
        let mtime = SystemTime::now();

        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        // b"foo bar foo bar "
        let content: Vec<_> = BLOCK_CONTENT
            .iter()
            .chain(BLOCK_CONTENT.iter())
            .chain(BLOCK_CONTENT.iter())
            .copied()
            .collect();

        let new_mtime = storage.store(ino, offset, content.as_slice(), mtime);

        // Loaded: "foo bar foo bar "
        let loaded = storage.load(ino, 0, BLOCK_SIZE_IN_BYTES.overflow_mul(3), new_mtime);
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");
        assert_eq!(loaded[1].as_slice(), b"foo bar ");
        assert_eq!(loaded[2].as_slice(), b"foo bar ");

        // ori: b"foo bar foo bar foo bar "
        //           "foo bar "
        // res: b"foo foo bar bar foo bar "
        let new_mtime = storage.store(ino, 4, BLOCK_CONTENT, mtime);

        let loaded = storage.load(ino, 0, BLOCK_SIZE_IN_BYTES.overflow_mul(2), new_mtime);
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].as_slice(), b"foo foo ");
        assert_eq!(loaded[1].as_slice(), b"bar bar ");

        let loaded = storage.load(ino, 4, BLOCK_SIZE_IN_BYTES, new_mtime);
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].as_slice(), b"foo ");
        assert_eq!(loaded[1].as_slice(), b"bar ");
    }

    #[test]
    fn test_zero_size_read_write() {
        let ino = 0;
        let offset = 0;
        let mtime = SystemTime::UNIX_EPOCH;

        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        let new_mtime = storage.store(ino, offset, BLOCK_CONTENT, mtime);
        assert_ne!(mtime, new_mtime);

        let loaded = storage.load(ino, offset, 0, new_mtime);
        assert!(loaded.is_empty());

        let just_now = SystemTime::now();
        let mtime_from_store = storage.store(ino, offset, b"", just_now);
        assert_eq!(just_now, mtime_from_store);
    }

    #[test]
    fn test_flush() {
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        storage.flush(0);
        assert!(backend.flushed(0));
    }

    #[test]
    fn test_invalid_cache_on_read() {
        let ino = 0;
        let offset = 0;

        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        let mtime = storage.store(ino, offset, BLOCK_CONTENT, SystemTime::now());
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");

        let mut block = Block::new(BLOCK_SIZE_IN_BYTES);
        block.make_mut().copy_from_slice(b"bar foo ");
        let io_block = block.into();

        // Simulating a modify on another node
        backend.store(ino, 0, io_block);
        // If we use the old mtime for loading, this node won't load the newest data
        let loaded = storage.load(ino, 0, BLOCK_SIZE_IN_BYTES, mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");
        // Then we can use a new mtime to invalidate the cache
        let loaded = storage.load(ino, 0, BLOCK_SIZE_IN_BYTES, mtime + Duration::from_secs(10));
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"bar foo ");
    }

    #[test]
    fn test_invalid_cache_on_write() {
        let ino = 0;
        let offset = 0;

        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        let mtime = storage.store(ino, offset, BLOCK_CONTENT, SystemTime::now());
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");

        let mut block = Block::new(BLOCK_SIZE_IN_BYTES);
        block.make_mut().copy_from_slice(b"bar foo ");
        let io_block = block.into();

        // Simulating a modify on another node
        backend.store(ino, 0, io_block);
        // Use a new mtime to invalidate the cache
        let mtime = storage.store(ino, 0, b"foo ", SystemTime::now());
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo foo ");
    }

    #[test]
    fn test_too_large() {
        let ino = 0;
        let offset = 0;

        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        let mtime = storage.store(ino, offset, BLOCK_CONTENT, SystemTime::now());
        // The size of this file is 1 block, so it will only returns 1 block though we requested 2 blocks.
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES.overflow_mul(2), mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");
    }

    #[test]
    fn test_remove() {
        let ino = 0;
        let offset = 0;

        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = InMemoryCache::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        let mtime = storage.store(ino, offset, BLOCK_CONTENT, SystemTime::now());
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");

        storage.remove(ino);
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime);
        assert!(loaded.is_empty());
    }
}
