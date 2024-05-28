#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = match sstables.get(0) {
            None => None,
            Some(sst) => Some(SsTableIterator::create_and_seek_to_first(sst.clone())?),
        };
        let mut iter = SstConcatIterator {
            current,
            next_sst_idx: 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let sst_idx = 0;
        for (idx, sst) in sstables.iter().enumerate() {
            if sst.last_key().as_key_slice() >= key {
                let mut iter = Self {
                    current: Some(SsTableIterator::create_and_seek_to_key(sst.clone(), key)?),
                    next_sst_idx: idx + 1,
                    sstables,
                };
                iter.move_until_valid()?;
                return Ok(iter);
            }
        }

        Ok(SstConcatIterator {
            current: None,
            next_sst_idx: sstables.len(),
            sstables,
        })
    }

    fn move_until_valid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().map_or(false, |iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        match &mut self.current {
            None => Ok(()),
            Some(iter) => {
                iter.next()?;
                self.move_until_valid()?;
                Ok(())
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
