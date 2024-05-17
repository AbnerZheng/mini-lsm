use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;
use anyhow::{anyhow, Error, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;
use std::fs::File;
use std::mem;
use std::path::Path;
use std::sync::Arc;

use self::bloom::Bloom;

pub(crate) mod bloom;
mod builder;
mod iterator;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

const SIZE_OF_USIZE: usize = mem::size_of::<usize>();
const SIZE_OF_U32: usize = mem::size_of::<u32>();

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let meta_offset = buf.len() as u32;
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put(meta.last_key.raw_ref());
        }
        buf.put_u32(meta_offset);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut res = vec![];
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16();
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len as usize));
            let last_key_len = buf.get_u16();
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len as usize));

            res.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        }

        res
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: u64,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_filter_offset_raw = file.read(file.1 - SIZE_OF_U32 as u64, SIZE_OF_U32 as u64)?;
        let bloom_filter_offset = bloom_filter_offset_raw.as_slice().get_u32() as u64;

        let bloom_filter_raw = file.read(
            bloom_filter_offset,
            file.1 - SIZE_OF_U32 as u64 - bloom_filter_offset,
        )?;

        let bloom = Bloom::decode(bloom_filter_raw.as_slice())?;

        let block_meta_offset_raw =
            file.read(bloom_filter_offset - SIZE_OF_U32 as u64, SIZE_OF_U32 as u64)?;
        let block_meta_offset = block_meta_offset_raw.as_slice().get_u32() as u64;

        let meta_block_raw = file.read(
            block_meta_offset,
            bloom_filter_offset - SIZE_OF_U32 as u64 - block_meta_offset,
        )?;

        let block_meta = BlockMeta::decode_block_meta(meta_block_raw.as_slice());

        Ok(Self {
            file,
            first_key: block_meta
                .first()
                .map(|m| m.first_key.clone())
                .unwrap_or_default(),
            last_key: block_meta
                .last()
                .map(|m| m.last_key.clone())
                .unwrap_or_default(),
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start_offset = self
            .block_meta
            .get(block_idx)
            .map(|m| m.offset)
            .ok_or(Error::msg("index out of bound"))?;

        let end_offset = self
            .block_meta
            .get(block_idx + 1)
            .map(|m| m.offset)
            .unwrap_or(self.block_meta_offset as usize);

        let block = self
            .file
            .read(start_offset as u64, (end_offset - start_offset) as u64)?;
        Ok(Arc::new(Block::decode(block.as_slice())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match self.block_cache {
            None => self.read_block(block_idx),
            Some(ref cache) => cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{e}")),
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let result = self
            .block_meta
            .binary_search_by_key(&key, |m| m.first_key.as_key_slice());
        let idx = result.unwrap_or_else(|idx| idx.saturating_sub(1));
        match self.block_meta.get(idx) {
            None => {
                // to indicate Self is invalid
                idx
            }
            Some(block_meta) => {
                if block_meta.last_key.as_key_slice() < key {
                    return idx + 1;
                }
                idx
            }
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
