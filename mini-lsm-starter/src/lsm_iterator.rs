#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{bail, Result};
use bytes::Bytes;
use std::collections::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeyBytes, KeySlice, KeyVec};
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    SstConcatIterator,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: TwoMergeIterator<
            TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
            SstConcatIterator,
        >,
        upper_bound: Bound<Bytes>,
    ) -> Result<Self> {
        let mut inner = iter;
        while inner.is_valid() && inner.value().is_empty() {
            inner.next()?
        }

        let is_valid = match upper_bound.as_ref() {
            Bound::Included(key) => {
                inner.is_valid() && inner.key() <= KeySlice::from_slice(key.as_ref())
            }
            Bound::Excluded(key) => {
                inner.is_valid() && inner.key() < KeySlice::from_slice(key.as_ref())
            }
            Bound::Unbounded => inner.is_valid(),
        };
        Ok(Self {
            inner,
            upper_bound,
            is_valid,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid {
            return Ok(());
        }
        self.inner.next()?;
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        self.is_valid = match self.upper_bound.as_ref() {
            Bound::Included(key) => self.inner.key() <= KeySlice::from_slice(key.as_ref()),
            Bound::Excluded(key) => self.inner.key() < KeySlice::from_slice(key.as_ref()),
            Bound::Unbounded => self.inner.is_valid(),
        };
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
