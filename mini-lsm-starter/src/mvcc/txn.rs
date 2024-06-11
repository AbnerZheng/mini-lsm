#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use log::warn;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::lsm_storage::WriteBatchRecord;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_committed();
        println!("get {key:?}, read_ts = {}", self.read_ts);
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            guard.1.insert(farmhash::hash32(key));
        }
        match self.local_storage.get(key) {
            Some(entry) => {
                return if entry.value().is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(entry.value().clone()))
                }
            }
            None => self.inner.get_with_ts(key, self.read_ts),
        }
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let lower = lower.map(Bytes::copy_from_slice);
        let upper = upper.map(Bytes::copy_from_slice);
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (Default::default(), Default::default()),
        }
        .build();
        local_iter.next()?;

        println!(
            "local_iter is valid? {}, outer_iter is valid? {}",
            local_iter.is_valid(),
            iter.is_valid()
        );

        TxnIterator::create(self.clone(), TwoMergeIterator::create(local_iter, iter)?)
    }

    fn check_committed(&self) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("can't operate on a committed txn");
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.check_committed();
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(key_hash) = &self.key_hashes {
            let mut guard = key_hash.lock();
            guard.0.insert(farmhash::hash32(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.check_committed();
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::from_static(b""));
        if let Some(key_hash) = &self.key_hashes {
            let mut guard = key_hash.lock();
            guard.0.insert(farmhash::hash32(key));
        }
    }

    pub fn commit(&self) -> Result<()> {
        let mvcc = self.inner.mvcc();
        let _guard = mvcc.commit_lock.lock();

        let (write_set, read_set) = self
            .key_hashes
            .as_ref()
            .map(|k| k.lock())
            .map(|g| (g.0.clone(), g.1.clone()))
            .unwrap_or_default();
        if !write_set.is_empty() {
            let expected_commit_ts = mvcc.latest_commit_ts() + 1;
            let committed_txs_guard = mvcc.committed_txns.lock();
            let mut range =
                committed_txs_guard.range((Excluded(self.read_ts), Excluded(expected_commit_ts)));
            let overlapped =
                range.any(|(ts, commited_data)| !commited_data.key_hashes.is_disjoint(&read_set));
            if overlapped {
                anyhow::bail!("abort transaction because failing the validation of isolation");
            }
        }

        self.committed.store(true, Ordering::SeqCst);
        // batch write
        let mut write_batch_record = Vec::with_capacity(self.local_storage.len());
        let mut commited_hashes = HashSet::with_capacity(self.local_storage.len());

        for entry in self.local_storage.iter() {
            let record = WriteBatchRecord::Put(entry.key().to_vec(), entry.value().to_vec());
            write_batch_record.push(record);
            let key = farmhash::hash32(entry.key());
            commited_hashes.insert(key);
        }
        let commit_ts = self.inner.write_batch_inner(&write_batch_record)?;
        let mut committed_txs_guard = mvcc.committed_txns.lock();
        committed_txs_guard.insert(
            commit_ts,
            CommittedTxnData {
                key_hashes: commited_hashes,
                read_ts: self.read_ts,
                commit_ts,
            },
        );

        let watermark = mvcc.watermark();
        committed_txs_guard.retain(|entry, _| *entry >= watermark);

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // remove read_ts from watermark
        let x = self.inner.mvcc();
        let mut guard = x.ts.lock();
        guard.1.remove_reader(self.read_ts);
        println!("watermark changed, {:?}", guard.1.watermark());
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|s| match s.iter.next() {
            None => *s.item = (Bytes::new(), Bytes::new()),
            Some(entry) => *s.item = (entry.key().clone(), entry.value().clone()),
        });

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        iter.skip_delete()?;

        if iter.is_valid() {
            if let Some(key_hashes) = &iter.txn.key_hashes {
                let mut guard = key_hashes.lock();
                guard.1.insert(farmhash::hash32(iter.key()));
            }
        }

        Ok(iter)
    }

    fn skip_delete(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_delete()?;

        if self.is_valid() {
            if let Some(key_hashes) = &self.txn.key_hashes {
                let mut guard = key_hashes.lock();
                guard.1.insert(farmhash::hash32(self.key()));
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
