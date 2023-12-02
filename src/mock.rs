//! Mock storages for test and local nodes.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

use crate::INum;

use super::{Block, IoBlock, Storage};

/// A "persistent" storage layer in memory.
#[derive(Default)]
pub struct MemoryStorage {
    /// The inner map of this storage
    inner: Mutex<HashMap<INum, HashMap<usize, Block>>>,
    /// Records of the flushed files
    flushed: Mutex<HashSet<INum>>,
    /// The size of block
    block_size: usize,
}

impl MemoryStorage {
    /// Creates a memory storage with block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            ..Default::default()
        }
    }

    /// Tests if the storage contains the block of `(ino, block_id)`
    pub fn contains(&self, ino: INum, block_id: usize) -> bool {
        self.inner
            .lock()
            .get(&ino)
            .map_or(false, |file| file.contains_key(&block_id))
    }

    /// Tests if the file is flushed,
    /// and after being tested, the flushed status of the file will be set to `false` again.
    pub fn flushed(&self, ino: INum) -> bool {
        self.flushed.lock().remove(&ino)
    }
}

impl Storage for MemoryStorage {
    fn load(&self, ino: INum, block_id: usize) -> Option<Block> {
        self.inner
            .lock()
            .get(&ino)
            .and_then(|file| file.get(&block_id))
            .cloned()
    }

    fn store(&self, ino: INum, block_id: usize, block: IoBlock) {
        let start = block.offset();
        let end = block.end();

        self.inner
            .lock()
            .entry(ino)
            .or_default()
            .entry(block_id)
            .or_insert_with(|| Block::new(self.block_size))
            .make_mut()
            .get_mut(start..end)
            .unwrap_or_else(|| panic!("Out of range"))
            .copy_from_slice(block.as_slice());
    }

    fn remove(&self, ino: INum) {
        self.inner.lock().remove(&ino);
    }

    fn invalidate(&self, _: INum) {}

    fn flush(&self, ino: INum) {
        self.flushed.lock().insert(ino);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use clippy_utilities::OverflowArithmetic;

    use crate::block::{Block, IoBlock};

    use super::{MemoryStorage, Storage};

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";

    #[test]
    fn test_read_write() {
        let ino = 0;
        let block_id = 0;
        let mut block = Block::new(BLOCK_SIZE_IN_BYTES);
        block.make_mut().copy_from_slice(BLOCK_CONTENT.as_slice());
        let io_block = IoBlock::new(block, 0, BLOCK_SIZE_IN_BYTES);
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        storage.store(ino, block_id, io_block);

        assert!(storage.contains(ino, block_id));
        let block = storage
            .load(ino, block_id)
            .map(|b| IoBlock::new(b, 0, BLOCK_SIZE_IN_BYTES))
            .unwrap();
        assert_eq!(block.as_slice(), BLOCK_CONTENT);

        assert!(!storage.contains(ino, 1));
        let block = storage.load(ino, 1);
        assert!(block.is_none());

        assert!(!storage.contains(1, 1));
        let block = storage.load(1, 1);
        assert!(block.is_none());

        storage.remove(ino);
        assert!(!storage.contains(ino, block_id));
        let block = storage.load(ino, block_id);
        assert!(block.is_none());
    }

    #[test]
    #[should_panic(expected = "Out of range")]
    fn test_ioblock_out_of_range() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        let block = Block::new(BLOCK_SIZE_IN_BYTES);
        let io_block = IoBlock::new(block, 0, BLOCK_SIZE_IN_BYTES.overflow_mul(2));

        storage.store(0, 0, io_block);
    }

    #[test]
    fn test_flush() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        storage.flush(0);
        assert!(storage.flushed(0));
        assert!(!storage.flushed(0));
    }

    #[test]
    fn test_invalid() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        storage.invalidate(0);
    }
}
