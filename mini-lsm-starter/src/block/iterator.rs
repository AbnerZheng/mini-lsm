use super::Block;
use crate::block::builder::prefix_decoding;
use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use std::sync::Arc;

/// Iterates on a block.
#[derive(Default)]
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let has_elem = !block.offsets.is_empty();
        let mut block_iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };

        if has_elem {
            block_iter.seek_to_first();
        }

        block_iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        Self::new(block)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0);
    }

    fn seek_to_index(&mut self, idx: usize) {
        match self.block.offsets.get(idx) {
            None => {
                self.key = KeyVec::default();
            }
            Some(offset) => {
                if idx != 0 {
                    assert!(!self.first_key.is_empty());
                }

                let end_pos = self
                    .block
                    .offsets
                    .get(idx + 1)
                    .map(|u| *u as usize)
                    .unwrap_or(self.block.data.len());

                let entry_raw = &self.block.data[*offset as usize..end_pos];
                let (key, remaining) = prefix_decoding(&self.first_key, entry_raw);
                let value_len = remaining.as_slice().get_u16();
                let value_range = (end_pos - value_len as usize, end_pos);

                if idx == 0 {
                    self.first_key = key.clone();
                }
                self.key = key;
                self.idx = idx;
                self.value_range = value_range;
            }
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let seek_key_vec = key.to_key_vec();

        // println!(
        //     "{}, {:?}, seek to {:?}",
        //     self.is_valid(),
        //     Bytes::copy_from_slice(self.key.raw_ref()),
        //     Bytes::copy_from_slice(key.raw_ref())
        // );
        if !self.is_valid() {
            return;
        }

        let mut enumerate = self.block.offsets[..self.block.offsets.len() - 1]
            .iter()
            .zip(self.block.offsets[1..].iter())
            .enumerate()
            .collect::<Vec<_>>();
        let last_entry = (
            &self.block.offsets[self.block.offsets.len() - 1],
            &(self.block.data.len() as u16),
        );
        enumerate.push((self.block.offsets.len(), last_entry));
        for (idx, (start_pos, end_pos)) in enumerate {
            let start_pos = *start_pos as usize;
            let end_pos = *end_pos as usize;
            let entry_raw = &self.block.data[start_pos..end_pos];
            let (key, remaining) = prefix_decoding(&self.first_key, entry_raw);

            if idx == 0 {
                self.first_key = key.clone();
            }

            if key >= seek_key_vec {
                let value_len = remaining.as_slice().get_u16();
                self.value_range = (end_pos - value_len as usize, end_pos);
                self.key = key.clone();
                self.idx = idx;
                return;
            }
        }
    }
}
