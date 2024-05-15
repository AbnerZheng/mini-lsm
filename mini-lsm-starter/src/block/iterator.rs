use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use nom::InputTake;
use std::sync::Arc;

/// Iterates on a block.
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
        if block.offsets.is_empty() {
            Self {
                block,
                key: KeyVec::new(),
                value_range: (0, 0),
                idx: 0,
                first_key: KeyVec::new(),
            }
        } else {
            let first_entry_end = block
                .offsets
                .get(1)
                .map(|u| usize::from(*u))
                .unwrap_or(block.data.len());
            let mut entry_raw = &block.data[..first_entry_end];
            // deserialize
            let key_len = entry_raw.get_u16();
            let (mut remaining, key_raw) = entry_raw.take_split(key_len as usize);
            let key = KeyVec::from_vec(key_raw.to_vec());
            let value_len = remaining.get_u16();
            let value_range = (first_entry_end - value_len as usize, first_entry_end);

            Self {
                block,
                key: key.clone(),
                value_range,
                idx: 0,
                first_key: key,
            }
        }
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
            None => {}
            Some(offset) => {
                let end_pos = self
                    .block
                    .offsets
                    .get(idx + 1)
                    .map(|u| *u as usize)
                    .unwrap_or(self.block.data.len());

                let mut entry = &self.block.data[*offset as usize..end_pos];
                let key_len = entry.get_u16();
                let (key_raw, mut remaining) = entry.split_at(key_len as usize);
                let key = KeyVec::from_vec(key_raw.to_vec());
                let value_len = remaining.get_u16();
                let value_range = (end_pos - value_len as usize, end_pos);

                self.key = key.clone();
                self.first_key = key;
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
        let key_vec = key.to_key_vec();

        println!("{}, {:?}", self.is_valid(), self.key);
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
            let mut entry_raw = &self.block.data[start_pos..end_pos];
            let key_len = entry_raw.get_u16();
            let (key_raw, mut remaining) = entry_raw.split_at(key_len as usize);
            let key = KeyVec::from_vec(key_raw.to_vec());
            if key >= key_vec {
                let value_len = remaining.get_u16();
                self.value_range = (end_pos - value_len as usize, end_pos);
                self.key = key.clone();
                self.idx = idx;
                return;
            }
        }
    }
}
