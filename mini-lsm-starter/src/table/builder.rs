#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyVec;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use bytes::BufMut;
use farmhash::{fingerprint32, fingerprint64};
use std::mem;
use std::path::Path;
use std::sync::Arc;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    pub(crate) key_hashes: Vec<u32>,
}

impl SsTableBuilder {}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::default(),
            last_key: KeyVec::default(),
            data: vec![],
            meta: vec![],
            key_hashes: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.key_hashes.push(fingerprint32(key.raw_ref()));
        let added = self.builder.add(key, value);
        if !added {
            // split a new block
            let mut new_builder = BlockBuilder::new(self.block_size);
            let added = new_builder.add(key, value);
            assert!(added, "Not able to add key pair to a new created block");

            let old_builder = mem::replace(&mut self.builder, new_builder);
            let block = old_builder.build().encode();
            let old_first_key = mem::replace(&mut self.first_key, key.to_key_vec());
            let old_last_key = mem::replace(&mut self.last_key, key.to_key_vec());

            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: old_first_key.into_key_bytes(),
                last_key: old_last_key.into_key_bytes(),
            });

            self.data.extend_from_slice(&block);
        } else {
            self.last_key.set_from_slice(key);
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // seal the currently active block
        let block = self.builder.build().encode();
        let old_first_key = mem::take(&mut self.first_key);
        let old_last_key = mem::take(&mut self.last_key);

        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: old_first_key.into_key_bytes(),
            last_key: old_last_key.into_key_bytes(),
        });
        self.data.extend_from_slice(&block);
        let block_meta_offset = self.data.len() as u64;
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);

        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(self.key_hashes.as_slice(), bits_per_key);

        // encode bloom into the file
        let bloom_filter_offset = self.data.len() as u32;
        bloom.encode(&mut self.data);
        self.data.put_u32(bloom_filter_offset);

        Ok(SsTable {
            block_meta_offset,
            file: FileObject::create(path.as_ref(), self.data)?,
            id,
            block_cache,
            first_key: self
                .meta
                .first()
                .map(|m| m.first_key.clone())
                .unwrap_or_default(),
            last_key: self
                .meta
                .last()
                .map(|m| m.last_key.clone())
                .unwrap_or_default(),
            block_meta: self.meta,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
