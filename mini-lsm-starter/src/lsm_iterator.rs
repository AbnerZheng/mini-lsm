use std::collections::Bound;

use anyhow::{bail, Result};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeyBytes;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper_bound: Bound<KeyBytes>,
    is_valid: bool,
    read_ts: u64,
}

fn skip(inner: &mut LsmIteratorInner, read_ts: u64) -> Result<()> {
    while inner.is_valid() && inner.key().ts() > read_ts {
        inner.next()?;
    }
    Ok(())
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        upper_bound: Bound<KeyBytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut inner = iter;
        skip(&mut inner, read_ts)?;
        if inner.is_valid() && inner.value().is_empty() {
            let mut prev_key = inner.key().key_ref().to_vec();
            skip(&mut inner, read_ts)?;

            while inner.is_valid() {
                if inner.key().key_ref() == prev_key {
                    inner.next()?;
                    skip(&mut inner, read_ts)?;
                } else if inner.value().is_empty() {
                    // delete key
                    prev_key = inner.key().key_ref().to_vec();
                    inner.next()?;
                    skip(&mut inner, read_ts)?;
                } else {
                    break;
                }
            }
        }

        let is_valid = match upper_bound.as_ref() {
            Bound::Included(key) => {
                inner.is_valid() && inner.key().key_ref() <= key.as_key_slice().key_ref()
            }
            Bound::Excluded(key) => {
                inner.is_valid() && inner.key().key_ref() < key.as_key_slice().key_ref()
            }
            Bound::Unbounded => inner.is_valid(),
        };
        Ok(Self {
            inner,
            upper_bound,
            is_valid,
            read_ts,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid {
            return Ok(());
        }
        let mut prev_key = self.inner.key().key_ref().to_vec();
        self.inner.next()?;
        skip(&mut self.inner, self.read_ts)?;
        while self.inner.is_valid() {
            if self.inner.key().ts() > self.read_ts || prev_key == self.inner.key().key_ref() {
                // we will only read the latest versions of the keys below or equal to the read timestamp.
                self.inner.next()?;
                skip(&mut self.inner, self.read_ts)?;
            } else if self.inner.value().is_empty() {
                // delete key
                prev_key = self.inner.key().key_ref().to_vec();
                self.inner.next()?;
                skip(&mut self.inner, self.read_ts)?;
            } else {
                break;
            }
        }
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        self.is_valid = match self.upper_bound.as_ref() {
            Bound::Included(key) => self.inner.key().key_ref() <= key.as_key_slice().key_ref(),
            Bound::Excluded(key) => self.inner.key().key_ref() < key.as_key_slice().key_ref(),
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
