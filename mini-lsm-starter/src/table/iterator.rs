use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};
use anyhow::Result;
use std::sync::Arc;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut s = Self {
            table,
            blk_iter: BlockIterator::default(),
            blk_idx: 0,
        };
        s.seek_to_first()?;
        Ok(s)
    }

    fn seek_to_index(&mut self, idx: usize) -> Result<()> {
        let block = self.table.read_block_cached(idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_idx = idx;
        Ok(())
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to_index(0)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut s = Self {
            table,
            blk_iter: BlockIterator::default(),
            blk_idx: 0,
        };
        s.seek_to_key(key)?;
        Ok(s)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        self.blk_idx = self.table.find_block_idx(key);
        if self.blk_idx >= self.table.num_of_blocks() {
            self.blk_iter = BlockIterator::default();
        } else {
            self.seek_to_index(self.blk_idx)?;
            self.blk_iter.seek_to_key(key);
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.is_valid() {
            if self.blk_idx + 1 >= self.table.num_of_blocks() {
                return Ok(());
            }
            self.seek_to_index(self.blk_idx + 1)?;
            assert!(self.is_valid());
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
