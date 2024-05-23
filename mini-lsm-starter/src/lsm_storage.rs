use std::collections::HashMap;
use std::fmt::format;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::process::id;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use nom::combinator::iterator;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::lsm_storage::CompactionFilter::Prefix;
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

pub fn range_overlap(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    first_key: &KeyBytes,
    last_key: &KeyBytes,
) -> bool {
    match map_bound(lower) {
        Bound::Included(l) => {
            if KeyBytes::from_bytes(l) > *last_key {
                return false;
            }
        }
        Bound::Excluded(l) => {
            if KeyBytes::from_bytes(l) >= *last_key {
                return false;
            }
        }
        Bound::Unbounded => {}
    }

    match map_bound(upper) {
        Bound::Included(u) => {
            if KeyBytes::from_bytes(u) < *first_key {
                return false;
            }
        }
        Bound::Excluded(u) => {
            if KeyBytes::from_bytes(u) <= *first_key {
                return false;
            }
        }
        Bound::Unbounded => {}
    }
    true
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.flush_notifier.send(()).map_err(|e| anyhow!("{e}"))?;
        self.compaction_notifier
            .send(())
            .map_err(|e| anyhow!("{e}"))?;

        self.flush_thread
            .lock()
            .take()
            .map(|thread| {
                thread
                    .join()
                    .map_err(|e| anyhow!("failed to stop flush thread: {:?}", e))
            })
            .transpose()?;

        self.compaction_thread
            .lock()
            .take()
            .map(|thread| {
                thread
                    .join()
                    .map_err(|e| anyhow!("failed to stop compaction thread: {:?}", e))
            })
            .transpose()?;

        if !self.inner.options.enable_wal {
            // flush all memtables to the disk
            self.force_flush()?;
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            fs::create_dir(path)?;
        }

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let res = snapshot.memtable.get(key);
        match res {
            Some(b) => {
                if b.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(b))
                }
            }
            None => {
                let r = snapshot
                    .imm_memtables
                    .iter()
                    .find_map(|mem_table| mem_table.get(key));
                match r {
                    Some(b) => {
                        if b.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(b))
                        }
                    }
                    None => {
                        let key = KeySlice::from_slice(key);
                        // read from ssttable
                        for idx in &snapshot.l0_sstables {
                            let sstable = snapshot.sstables[idx].clone();
                            if !sstable.may_contain_key(key) {
                                continue;
                            }
                            let iter = SsTableIterator::create_and_seek_to_key(sstable, key)?;
                            if iter.is_valid() && iter.key() == key {
                                if iter.value().is_empty() {
                                    return Ok(None);
                                } else {
                                    return Ok(Some(Bytes::copy_from_slice(iter.value())));
                                }
                            }
                        }

                        for (level, idxs) in &snapshot.levels {
                            println!("try fetching key from l{level}");
                            for idx in idxs {
                                // those sstable in each levels are sorted
                                let sst = snapshot.sstables[idx].clone();
                                if sst.first_key().as_key_slice() > key {
                                    break;
                                }
                                if sst.last_key().as_key_slice() < key {
                                    continue;
                                }
                                if !sst.may_contain_key(key) {
                                    continue;
                                }

                                let iter = SsTableIterator::create_and_seek_to_key(sst, key)?;
                                if iter.is_valid() && iter.key() == key {
                                    if iter.value().is_empty() {
                                        return Ok(None);
                                    } else {
                                        return Ok(Some(Bytes::copy_from_slice(iter.value())));
                                    }
                                }
                            }
                        }

                        Ok(None)
                    }
                }
            }
        }
    }
    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        {
            let state = self.state.read();
            let _ = state.memtable.put(key, value);
        }
        self.try_freeze_memtable()
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable = MemTable::create(self.next_sst_id());
        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut state.memtable, Arc::new(memtable));
            state.imm_memtables.insert(0, old_memtable);
            *guard = Arc::new(state);
        }
        Ok(())
    }

    fn try_freeze_memtable(&self) -> Result<()> {
        if self.state.read().memtable.approximate_size() > self.options.target_sst_size {
            let state_lock_guard = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() > self.options.target_sst_size {
                return self.force_freeze_memtable(&state_lock_guard);
            }
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let memtable_to_flush = {
            let guard = self.state.read();
            guard
                .imm_memtables
                .last()
                .ok_or_else(|| anyhow!("No immutable memtable to flush"))?
                .clone()
        };
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;
        let sst_id = memtable_to_flush.id();
        let sstable = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        }

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            // remove it from imm_memtable
            let memtable = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(memtable.id(), sst_id);

            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            }
            snapshot.sstables.insert(sst_id, Arc::new(sstable));

            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let iterator = Box::new(snapshot.memtable.scan(lower, upper));
        let mut vec = snapshot
            .imm_memtables
            .iter()
            .map(|m| Box::new(m.scan(lower, upper)))
            .collect::<Vec<_>>();
        vec.insert(0, iterator);
        let mem_iter = MergeIterator::create(vec);

        let mut iters = Vec::new();
        for idx in &snapshot.l0_sstables {
            let sstable = snapshot.sstables[idx].clone();

            if !range_overlap(lower, upper, sstable.first_key(), sstable.last_key()) {
                continue;
            }

            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(sstable, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sstable,
                        KeySlice::from_slice(key),
                    )?;
                    iter.next()?;
                    iter
                }

                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sstable)?,
            };
            iters.push(Box::new(iter));
        }

        let l0_sst_iter = MergeIterator::create(iters);
        let memtable_merge_iter = TwoMergeIterator::create(mem_iter, l0_sst_iter)?;

        let mut levels_iter = Vec::with_capacity(snapshot.levels.len());
        for (_, idxs) in &snapshot.levels {
            let levels_sst = idxs
                .iter()
                .map(|idx| snapshot.sstables[idx].clone())
                .filter(|sst| range_overlap(lower, upper, sst.first_key(), sst.last_key()))
                .collect::<Vec<_>>();

            let levels_sst_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    levels_sst,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        levels_sst,
                        KeySlice::from_slice(key),
                    )?;
                    iter.next()?;
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(levels_sst)?,
            };
            levels_iter.push(Box::new(levels_sst_iter));
        }

        let levels_merge_iter = MergeIterator::create(levels_iter);

        let two_merge_iter = TwoMergeIterator::create(memtable_merge_iter, levels_merge_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(
            two_merge_iter,
            map_bound(upper),
        )?))
    }
}
