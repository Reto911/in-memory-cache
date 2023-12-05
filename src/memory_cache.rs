//! The in-memory cache.

use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::collections::HashMap as StdHashMap;

use super::policy::EvictPolicy;
use super::{Block, BlockCoordinate, IoBlock, Storage};
use crate::INum;

/// The in-memory cache, implemented with lockfree hashmaps.
pub struct InMemoryCache<P, S> {
    /// The inner map where the cached blocks stored
    map: HashMap<INum, RwLock<StdHashMap<usize, Block>>>,
    /// The evict policy
    policy: P,
    /// The backend storage
    backend: S,
    /// The block size
    block_size: usize,
}

impl<P, S> InMemoryCache<P, S> {
    /// Create a new `InMemoryCache` with specified `policy`, `backend` and `block_size`.
    pub fn new(policy: P, backend: S, block_size: usize) -> Self {
        InMemoryCache {
            map: HashMap::new(),
            policy,
            backend,
            block_size,
        }
    }

    /// Get a block from the in-memory cache without fetch it from backend.
    fn get_block_from_cache(&self, ino: INum, block_id: usize) -> Option<Block> {
        let guard = pin();
        let block = self
            .map
            .get(&ino, &guard)
            .and_then(|file_cache| file_cache.read().get(&block_id).cloned());
        block
    }

    fn merge_two_block(dst: &mut Block, src: &IoBlock) {
        let start_offset = src.offset();
        let end_offset = src.end();
        let len_dst = dst.len();

        dst.make_mut()
            .get_mut(start_offset..end_offset)
            .unwrap_or_else(|| {
                unreachable!(
                    "Slice {}..{} in block is out of range of {}.",
                    start_offset, end_offset, len_dst
                )
            })
            .copy_from_slice(src.as_slice());
    }

    /// Return true if success
    fn merge_block_in_place(&self, ino: INum, block_id: usize, to_merge: &IoBlock) -> bool {
        let guard = pin();

        let res = if let Some(mut lock) = self
            .map
            .get(&ino, &guard)
            .map(|file_cache| file_cache.write())
        {
            if let Some(block) = lock.get_mut(&block_id) {
                Self::merge_two_block(block, to_merge);
                true
            } else {
                false
            }
        } else {
            false
        };
        res
    }

    /// Write a block into the cache without writing through.
    ///
    /// If an old block is to be evicted, returns it with its coordinate.
    fn write_block_into_cache(
        &self,
        ino: INum,
        block_id: usize,
        block: Block,
    ) -> Option<(BlockCoordinate, Block)>
    where
        P: EvictPolicy<BlockCoordinate>,
    {
        let to_be_evicted = self.policy.put(BlockCoordinate(ino, block_id));
        let guard = pin();
        // Try to get the evicted block
        let evicted = to_be_evicted.and_then(|coord| {
            self.map
                .get(&coord.0, &guard)
                .and_then(|file_cache| file_cache.write().remove(&coord.1))
                .map(|evicted| (coord, evicted))
        });
        // Insert the written block into cache
        self.map
            .get_or_insert(ino, RwLock::new(StdHashMap::new()), &guard)
            .write()
            .insert(block_id, block);
        evicted
    }

    /// Write a block into the cache and evict a block to backend (if needed).
    fn write_and_evict(&self, ino: INum, block_id: usize, block: Block)
    where
        P: EvictPolicy<BlockCoordinate>,
        S: Storage,
    {
        let evicted = self.write_block_into_cache(ino, block_id, block);
        if let Some((BlockCoordinate(e_ino, e_block), evicted)) = evicted {
            self.backend
                .store(e_ino, e_block, IoBlock::new(evicted, 0, self.block_size));
        }
    }
}

impl<P, S> Storage for InMemoryCache<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    fn load(&self, ino: INum, block_id: usize) -> Option<Block> {
        let block_in_cache = self.get_block_from_cache(ino, block_id);

        match block_in_cache {
            None => {
                // If a fetching from backend still fails, just return a `None`.
                let block_from_backend = self.backend.load(ino, block_id)?;

                self.write_and_evict(ino, block_id, block_from_backend.clone());

                Some(block_from_backend)
            }
            Some(block) => {
                self.policy.touch(&BlockCoordinate(ino, block_id));
                Some(block)
            }
        }
    }

    fn store(&self, ino: INum, block_id: usize, block: IoBlock) {
        let start_offset = block.offset();
        let end_offset = block.end();

        // If the writing block is the whole block, then there is no need to fetch a block from cache or backend,
        // as the block in storage will be overwritten directly.
        if start_offset == 0 && end_offset == self.block_size {
            self.write_and_evict(ino, block_id, block.block().clone());
            self.backend.store(ino, block_id, block);
            return;
        }

        let inserted = self.merge_block_in_place(ino, block_id, &block);
        if inserted {
            return;
        }

        let mut to_be_inserted = {
            let block_from_backend = self.backend.load(ino, block_id);
            block_from_backend.unwrap_or_else(|| {
                if start_offset == 0 {
                    Block::new(self.block_size)
                } else {
                    panic!("Trying to write a block not existing in the storage.")
                }
            })
        };

        Self::merge_two_block(&mut to_be_inserted, &block);

        self.write_and_evict(ino, block_id, to_be_inserted);

        // Write through
        // TODO: Spawn it after load.
        self.backend.store(ino, block_id, block);
    }

    fn remove(&self, ino: INum) {
        self.map.remove(&ino);
        self.backend.remove(ino);
    }

    fn invalidate(&self, ino: INum) {
        // self.map.remove(&ino);
        self.backend.invalidate(ino);
    }

    fn flush(&self, ino: INum) {
        self.backend.flush(ino);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use super::{Block, BlockCoordinate, InMemoryCache, IoBlock, Storage};
    use crate::{mock::MemoryStorage, policy::LruPolicy};

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
    const CACHE_CAPACITY_IN_BLOCKS: usize = 4;

    /// Create an `IoBlock`
    macro_rules! create_block {
        ($content:expr, $start:expr, $end:expr) => {{
            let mut block = Block::new(BLOCK_SIZE_IN_BYTES);
            block.make_mut().copy_from_slice(($content).as_slice());
            let io_block = IoBlock::new(block, $start, $end);
            io_block
        }};
        ($content:expr) => {
            create_block!($content, 0, BLOCK_SIZE_IN_BYTES)
        };
    }

    #[test]
    fn test_store_load_block() {
        let ino = 0;
        let block_id = 0;

        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        // Write a whole block
        {
            let io_block = create_block!(BLOCK_CONTENT);

            cache.store(ino, block_id, io_block);

            let loaded_from_cache = cache.load(ino, block_id).map(IoBlock::from).unwrap();
            assert_eq!(loaded_from_cache.as_slice(), BLOCK_CONTENT);

            // test write through
            let loaded_from_backend = backend.load(ino, block_id).map(IoBlock::from).unwrap();
            assert_eq!(loaded_from_backend.as_slice(), loaded_from_cache.as_slice());
        }

        // Overwrite an existing block
        {
            let io_block = create_block!(b"xxx foo ", 4, 7);
            cache.store(ino, block_id, io_block);
            let loaded = cache.load(ino, block_id).map(IoBlock::from).unwrap();
            assert_eq!(loaded.as_slice(), b"foo foo ");

            let io_block = create_block!(b"bar xxx ", 0, 4);
            cache.store(ino, block_id, io_block);
            let loaded = cache.load(ino, block_id).map(IoBlock::from).unwrap();
            assert_eq!(loaded.as_slice(), b"bar foo ");
        }

        // Load an inexisting block
        {
            let block = cache.load(ino, 1);
            assert!(block.is_none());

            let block = cache.load(1, block_id);
            assert!(block.is_none());
        }

        // Append write
        {
            let io_block = create_block!(b"xxx foo ", 0, 4);
            cache.store(ino, 1, io_block);

            let loaded = cache.load(ino, 1).map(IoBlock::from).unwrap();
            assert_eq!(loaded.as_slice(), b"xxx \0\0\0\0");
        }

        // Remove
        {
            cache.remove(ino);
            assert!(!backend.contains(ino, 0));
            assert!(!backend.contains(ino, 1));

            let loaded = cache.load(ino, 0);
            assert!(loaded.is_none());
            let loaded = cache.load(ino, 1);
            assert!(loaded.is_none());
        }
    }

    #[test]
    fn test_evict() {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        // Fill the cache
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let io_block = create_block!(BLOCK_CONTENT);

            cache.store(0, block_id, io_block);
        }

        // Clear the backend
        backend.remove(0);

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Insert a block, and (0, 0) will be evicted
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(1, 0, io_block);
        let loaded = backend.load(0, 0).map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        // LRU in the cache: (0, 1) -> (0, 2) -> (0, 3) -> (1, 0)
        // Touch (0, 1) by loading
        let _ = cache.load(0, 1);

        // LRU in the cache: (0, 2) -> (0, 3) -> (1, 0) -> (0, 1)
        // Insert a block, and (0, 2) will be evicted
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(1, 1, io_block);
        assert!(!backend.contains(0, 1));
        let loaded = backend.load(0, 2).map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        // LRU in the cache: (0, 3) -> (1, 0) -> (0, 1) -> (1, 1)
        // Touch (0, 3) by storing
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(0, 3, io_block);
        backend.remove(0); // remove the (0, 3) block that is written through

        // LRU in the cache: (1, 0) -> (0, 1) -> (1, 1) -> (0, 3)
        // Insert a block, and (1, 0) will be evicted
        backend.remove(1);
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(1, 2, io_block);
        assert!(!backend.contains(0, 3));
        let loaded = backend.load(1, 0).map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[test]
    fn test_flush() {
        let ino = 0;
        let block_id = 0;

        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, block_id, io_block);

        cache.flush(ino);
        assert!(backend.flushed(ino));
    }

    #[test]
    fn test_load_from_backend() {
        let ino = 0;
        let block_id = 0;

        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, block_id, io_block);

        cache.invalidate(ino);
        let loaded_from_cache = cache.get_block_from_cache(ino, block_id);
        assert!(loaded_from_cache.is_none());

        let loaded = cache.load(ino, block_id).map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[test]
    // TODO: Remove this after the error handling
    #[should_panic(expected = "Trying to write a block not existing in the storage.")]
    fn test_write_missing_block_in_middle() {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, backend, BLOCK_SIZE_IN_BYTES);

        let io_block = IoBlock::new(Block::new(BLOCK_SIZE_IN_BYTES), 4, 7);
        cache.store(0, 0, io_block);
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_write_out_of_range() {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, backend, BLOCK_SIZE_IN_BYTES);

        let io_block = IoBlock::new(Block::new(16), 0, 16);
        cache.store(0, 0, io_block);
    }
}
