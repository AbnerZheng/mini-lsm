use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::{Buf, BufMut};

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

/// key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | timestamp (u64)
pub fn prefix_encoding(first_key: &KeyVec, key: &KeyVec) -> Vec<u8> {
    let same_prefix = first_key
        .key_ref()
        .iter()
        .zip(key.key_ref().iter())
        .take_while(|(a, b)| **a == **b)
        .map(|(a, _b)| *a)
        .collect::<Vec<_>>();

    let mut res = vec![];
    let key_overlap_len = same_prefix.len() as u16;
    let rest_key_len = key.key_len() as u16 - key_overlap_len;
    res.put_u16(key_overlap_len);
    res.put_u16(rest_key_len);
    res.put_slice(&key.key_ref()[key_overlap_len as usize..]);
    res.put_u64(key.ts());
    res
}

pub fn prefix_decoding(first_key: &KeyVec, mut entry_raw: &[u8]) -> (KeyVec, Vec<u8>) {
    let overlap_len = entry_raw.get_u16();
    let rest_key_len = entry_raw.get_u16();
    let (rest_key_raw, mut remaining) = entry_raw.split_at(rest_key_len as usize);
    assert!(overlap_len <= first_key.key_len() as u16);

    let mut key = first_key.key_ref()[..overlap_len as usize].to_vec();
    key.extend_from_slice(rest_key_raw);
    let ts = remaining.get_u64();

    (KeyVec::from_vec_with_ts(key, ts), remaining.to_vec())
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
    ///
    /// -----------------------------------------------------------------------
    /// |                           Entry #1                            | ... |
    /// -----------------------------------------------------------------------
    /// | prefix encoded key     |    value_len (2B) |   value (varlen) | ... |
    /// -----------------------------------------------------------------------
    /// | encode_key.len()       +   2(B)            +  value.len()
    ///
    /// ---------------------------------------------------------------------------------------------------------
    /// |             Data Section             |              Offset Section             |      Extra             |
    /// ----------------------------------------------------------------------------------------------------------
    /// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1(u16) | Offset #2 (u16) | ...  |  num_of_elements (u16) |
    /// ----------------------------------------------------------------------------------------------------------
    /// | data.len() + key_len + 2 + value_len +  offset_len * 2  + 2                    +   2                    |
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // key prefix encoding
        let key = key.to_key_vec();
        let encode_key = prefix_encoding(&self.first_key, &key);
        let encode_key_len = encode_key.len();
        let value_len = value.len();

        if !self.is_empty()
            && self.data.len() + encode_key_len + value_len + 2 + self.offsets.len() * 2 + 2 + 2
                > self.block_size
        {
            return false;
        }

        if self.is_empty() {
            self.first_key = key.clone();
        }

        let old_len = self.data.len() as u16;
        self.data.extend_from_slice(encode_key.as_slice());
        self.data.put_u16(value_len as u16);
        self.data.extend_from_slice(value);

        self.offsets.push(old_len);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
