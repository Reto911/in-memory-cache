//! The storage trait, as a abstraction of the storage layers.

use std::sync::Arc;

use crate::INum;

use super::{Block, IoBlock};

/// The `Storage` trait. It handles blocks with storage such as in-memory cache, in-disk cache and `S3Backend`.
pub trait Storage {
    /// Load a block from the storage.
    fn load(&self, ino: INum, block_id: usize) -> Option<Block>;
    /// Store a block to the storage.
    fn store(&self, ino: INum, block_id: usize, block: IoBlock);
    /// Remove a file from storage.
    fn remove(&self, ino: INum);
    /// Invalidate caches of a file, if the storage contains caches.
    fn invalidate(&self, ino: INum);
    /// Flush a file
    fn flush(&self, ino: INum);
}

impl<T> Storage for Arc<T>
where
    T: Storage + Send + Sync,
{
    fn load(&self, ino: INum, block_id: usize) -> Option<Block> {
        self.as_ref().load(ino, block_id)
    }

    fn store(&self, ino: INum, block_id: usize, block: IoBlock) {
        self.as_ref().store(ino, block_id, block);
    }

    fn remove(&self, ino: INum) {
        self.as_ref().remove(ino);
    }

    fn invalidate(&self, ino: INum) {
        self.as_ref().invalidate(ino);
    }

    fn flush(&self, ino: INum) {
        self.as_ref().flush(ino);
    }
}
