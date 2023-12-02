//! Utilities for blocks.

use std::{fmt::Formatter, sync::Arc};

use aligned_utils::bytes::AlignedBytes;

use crate::INum;

/// Page Size
const PAGE_SIZE: usize = 4096;

/// A common coordinate to locate a block.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BlockCoordinate(pub INum, pub usize);

/// The minimum unit of data in the storage layers.
#[derive(Clone)]
pub struct Block {
    /// The underlying data of a block. Shared with `Arc`.
    inner: Arc<AlignedBytes>,
}

impl Block {
    /// Create a block with `capacity`, which usually equals the `block_size` of storage manager.
    pub fn new(capacity: usize) -> Self {
        Block {
            inner: Arc::new(AlignedBytes::new_zeroed(capacity, PAGE_SIZE)),
        }
    }

    /// Returns the length of the block, which usually equals the `block_size` of storage manager.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Get a mutable slice of the underlying data, copy them if there are other blocks hold the same data with `Arc`.
    /// See also [`Arc::make_mut`](fn@std::sync::Arc::make_mut).
    pub fn make_mut(&mut self) -> &mut [u8] {
        Arc::make_mut(&mut self.inner).as_mut()
    }
}

/// A wrapper for `IoMemBlock`, which is used for I/O operations
#[derive(Clone)]
pub struct IoBlock {
    /// The inner `MemBlock` that contains data
    inner: Block,
    /// The offset for this `MemBlock`
    offset: usize,
    /// The end offset for this `MemBlock`
    end: usize,
}

impl std::fmt::Debug for IoBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoMemBlock")
            .field("offset", &self.offset)
            .field("end", &self.end)
            .finish()
    }
}

impl IoBlock {
    /// The constructor of `IoMemBlock`
    pub const fn new(inner: Block, offset: usize, end: usize) -> Self {
        Self { inner, offset, end }
    }

    /// The inner block
    pub fn block(&self) -> &Block {
        &self.inner
    }

    /// The offset of valid bytes of the inner block
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// The end offset of valid bytes of the inner block
    pub const fn end(&self) -> usize {
        self.end
    }

    pub const fn len(&self) -> usize {
        self.end - self.offset
    }

    /// Turn `IoMemBlock` into slice
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.inner
            .inner
            .get(self.offset..self.end)
            .unwrap_or_else(|| {
                panic!(
                    "`{}..{}` is out of range of {}.",
                    self.offset,
                    self.end,
                    self.inner.len()
                )
            })
    }
}

impl From<Block> for IoBlock {
    fn from(block: Block) -> Self {
        let len = block.len();
        IoBlock::new(block, 0, len)
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, Block, IoBlock};

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";

    #[test]
    fn test_block() {
        let mut block = Block::new(BLOCK_SIZE_IN_BYTES);
        assert_eq!(block.len(), BLOCK_SIZE_IN_BYTES);

        block.make_mut().copy_from_slice(BLOCK_CONTENT);
        assert_eq!(block.inner.get(..), Some(BLOCK_CONTENT.as_slice()));

        let _another_block = block.clone();
        assert_eq!(Arc::strong_count(&block.inner), 2);
    }

    #[test]
    fn test_io_block() {
        let mut block = Block::new(BLOCK_SIZE_IN_BYTES);
        block.make_mut().copy_from_slice(BLOCK_CONTENT);

        let mut io_block = IoBlock::from(block);
        assert_eq!(io_block.len(), BLOCK_SIZE_IN_BYTES);
        assert_eq!(io_block.as_slice(), BLOCK_CONTENT);

        io_block.offset = 1;
        io_block.end = 5;
        assert_eq!(io_block.len(), 4);
        assert_eq!(io_block.as_slice(), b"oo b");

        assert_eq!(format!("{io_block:?}"), "IoMemBlock { offset: 1, end: 5 }");
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_io_block_out_of_range() {
        let io_block = IoBlock::new(Block::new(8), 0, 16);
        let _ = io_block.as_slice();
    }
}
