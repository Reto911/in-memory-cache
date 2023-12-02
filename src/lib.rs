//! This is the cache implementation for the memfs

// TODO: Remove this after the storage is ready for product env.
#![allow(dead_code)]

mod block;
mod memory_cache;
mod mock;
mod policy;
mod storage;
mod storage_manager;

pub use block::{Block, BlockCoordinate, IoBlock};
pub use memory_cache::InMemoryCache;
pub use storage::Storage;
pub use storage_manager::StorageManager;

/// The size of a block.
pub const BLOCK_SIZE_IN_BYTES: usize = 512 * 1024;
pub type INum = u64;
