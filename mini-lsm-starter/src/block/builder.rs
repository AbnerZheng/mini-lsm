use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let Ok(key_len) = u16::try_from(key.len()) else {
            return false;
        };
        let Ok(value_len) = u16::try_from(value.len()) else {
            return false;
        };

        if !self.is_empty()
            && self.data.len()
                + key_len as usize
                + value_len as usize
                + 2
                + 2
                + self.offsets.len() * 2
                + 2
                + 2
                > self.block_size
        {
            return false;
        }

        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        let old_len = self.data.len() as u16;
        self.data.put_u16(key_len);
        self.data.extend_from_slice(key.raw_ref());
        self.data.put_u16(value_len);
        self.data.extend_from_slice(value);

        self.offsets.push(old_len);
        return true;
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        return Block {
            data: self.data,
            offsets: self.offsets,
        };
    }
}
