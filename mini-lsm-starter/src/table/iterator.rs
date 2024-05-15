use super::{BlockMeta, SsTable};
use crate::block::Block;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};
use anyhow::Result;
use std::process::id;
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
        let block_offset = self
            .table
            .block_meta
            .get(idx + 1)
            .map(|m| m.offset)
            .unwrap_or(self.table.block_meta_offset as usize);
        let start = self.table.block_meta[idx].offset as u64;
        let block = self.table.file.read(start, block_offset as u64 - start)?;
        let block = Block::decode(block.as_slice());
        self.blk_iter = BlockIterator::create_and_seek_to_first(Arc::new(block));
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
        let result = self
            .table
            .block_meta
            .binary_search_by_key(&key, |m| m.first_key.as_key_slice());
        let idx = match result {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        };
        match self.table.block_meta.get(idx) {
            None => {
                // to indicate Self is invalid
                self.blk_idx = idx;
                self.blk_iter = BlockIterator::default();
                Ok(())
            }
            Some(block_meta) => {
                self.blk_idx = idx;
                if block_meta.last_key.as_key_slice() < key {
                    self.blk_idx += 1;
                    if self.blk_idx >= self.table.block_meta.len() {
                        self.blk_iter = BlockIterator::default();
                        return Ok(());
                    }
                }
                self.seek_to_index(self.blk_idx)?;
                self.blk_iter.seek_to_key(key);
                Ok(())
            }
        }
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
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
            if self.blk_idx + 1 >= self.table.block_meta.len() {
                return Ok(());
            }
            self.blk_idx += 1;
            let block_offset = self.table.block_meta[self.blk_idx].offset;
            let end_offset = self
                .table
                .block_meta
                .get(self.blk_idx + 1)
                .map(|s| s.offset)
                .unwrap_or(self.table.block_meta_offset as usize);
            let block = self
                .table
                .file
                .read(block_offset as u64, (end_offset - block_offset) as u64)?;
            let block = Block::decode(block.as_slice());
            self.blk_iter = BlockIterator::create_and_seek_to_first(Arc::new(block));
            assert!(self.is_valid());
        }

        Ok(())
    }
}
