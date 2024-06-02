use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::fs::File;
use std::ops::Bound;
use std::ops::Bound::Included;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
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
use crate::key::{KeyBytes, KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

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
    lower: Bound<KeySlice>,
    upper: Bound<KeySlice>,
    first_key: &KeyBytes,
    last_key: &KeyBytes,
) -> bool {
    match map_bound(lower) {
        Bound::Included(l) => {
            if l.key_ref() > last_key.key_ref() {
                return false;
            }
        }
        Bound::Excluded(l) => {
            if l.key_ref() >= last_key.key_ref() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }

    match map_bound(upper) {
        Bound::Included(u) => {
            if u.key_ref() < first_key.key_ref() {
                return false;
            }
        }
        Bound::Excluded(u) => {
            if u.key_ref() <= first_key.key_ref() {
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
    #[allow(dead_code)]
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
        } else {
            // flush all wal
            self.sync()?;
        }
        self.inner.sync_dir()?;
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
            fs::create_dir_all(path)?;
        }

        let mut state = LsmStorageState::create(&options);

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

        let block_cache = Arc::new(BlockCache::new(1024));

        let manifest_file_path = path.join("MANIFEST");
        let mut max_sst_id = 0;

        let manifest = if manifest_file_path.exists() {
            // recover from file
            let (manifest, records) = Manifest::recover(manifest_file_path)?;
            let mut sst_need_load: BTreeSet<usize> = BTreeSet::new();
            let mut memtable_to_recover = Vec::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        if compaction_controller.flush_to_l0() {
                            println!("flush memtable{sst_id} to l0");
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            println!("flush memtable{sst_id} to new tier");
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        let option = memtable_to_recover.pop();
                        assert_eq!(option, Some(sst_id), "failed to flush memtable");
                        sst_need_load.insert(sst_id);
                        max_sst_id = max_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(sst_id) => {
                        println!("new memtable {sst_id}");
                        memtable_to_recover.insert(0, sst_id);
                        max_sst_id = max_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, sst_to_add) => {
                        sst_need_load.extend(sst_to_add.iter());
                        for x in &sst_to_add {
                            max_sst_id = max_sst_id.max(*x);
                        }

                        let (new_state, sst_to_remove) = compaction_controller
                            .apply_compaction_result(&state, &task, &sst_to_add);
                        for sst_id in sst_to_remove {
                            sst_need_load.remove(&sst_id);
                        }
                        state = new_state;
                    }
                }
            }
            if options.enable_wal {
                for sst_id in memtable_to_recover {
                    let mem_table =
                        MemTable::recover_from_wal(sst_id, Self::path_of_wal_static(path, sst_id))?;
                    state.imm_memtables.push(Arc::new(mem_table));
                }
            }
            for sst_id in sst_need_load {
                let ss_table = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(Self::path_of_sst_static(path, sst_id).as_path())?,
                )?;
                state.sstables.insert(sst_id, Arc::new(ss_table));
            }
            max_sst_id += 1;
            manifest
        } else {
            Manifest::create(manifest_file_path)?
        };

        state.memtable = if options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                max_sst_id,
                Self::path_of_wal_static(path, max_sst_id),
            )?)
        } else {
            Arc::new(MemTable::create(max_sst_id))
        };

        manifest.add_record_when_init(ManifestRecord::NewMemtable(max_sst_id))?;

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(max_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(0)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        let guard = self.state.read();
        guard.memtable.sync_wal()?;
        for x in &guard.imm_memtables {
            x.sync_wal()?;
        }

        Ok(())
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

        let mut mem_table_iters = Vec::with_capacity(1 + snapshot.imm_memtables.len());

        let key_low = KeySlice::from_slice(key, TS_RANGE_BEGIN);
        let key_upper = KeySlice::from_slice(key, TS_RANGE_END);
        let iter = snapshot
            .memtable
            .scan(Included(key_low), Included(key_upper));
        mem_table_iters.push(Box::new(iter));

        for imm in &snapshot.imm_memtables {
            let iter = imm.scan(Included(key_low), Included(key_upper));
            mem_table_iters.push(Box::new(iter));
        }

        let mem_table_merge = MergeIterator::create(mem_table_iters);

        let l0_sst = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| {
                let sst = snapshot.sstables[sst_id].clone();
                if sst.key_in_range(key) && sst.may_contain_key(key) {
                    Some(sst)
                } else {
                    None
                }
            })
            .map(|sst| {
                let iter = SsTableIterator::create_and_seek_to_key(sst, key_low)?;
                Ok(Box::new(iter))
            })
            .collect::<Result<Vec<_>>>()?;

        let l0_merge_iter = MergeIterator::create(l0_sst);
        let two_merge_iters = TwoMergeIterator::create(mem_table_merge, l0_merge_iter)?;

        let mut iters = vec![];
        for (_level, idxs) in &snapshot.levels {
            // println!("try fetching key from l{level}");
            let sst_iters = idxs
                .iter()
                .filter_map(|sst_id| {
                    let sst = snapshot.sstables[sst_id].clone();

                    if sst.key_in_range(key) && sst.may_contain_key(key) {
                        Some(sst)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let iter = SstConcatIterator::create_and_seek_to_key(sst_iters, key_low)?;
            iters.push(Box::new(iter));
        }
        let levels_iter = MergeIterator::create(iters);
        let iterator = LsmIterator::new(
            TwoMergeIterator::create(two_merge_iters, levels_iter)?,
            map_bound(Included(key_upper)),
        )?;
        if iterator.is_valid() && iterator.key() == key && !iterator.value().is_empty() {
            Ok(Some(Bytes::copy_from_slice(iterator.value())))
        } else {
            Ok(None)
        }
    }
    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let mvcc = self.mvcc.as_ref().unwrap();
        let _guard = mvcc.write_lock.lock();
        for x in batch {
            let ts = mvcc.latest_commit_ts() + 1;
            match x {
                WriteBatchRecord::Put(key, value) => {
                    let state = self.state.read();
                    state
                        .memtable
                        .put(KeySlice::from_slice(key.as_ref(), ts), value.as_ref())?;
                }
                WriteBatchRecord::Del(key) => {
                    let state = self.state.read();
                    state
                        .memtable
                        .put(KeySlice::from_slice(key.as_ref(), ts), &[])?;
                }
            }
            mvcc.update_commit_ts(ts);
            self.try_freeze_memtable()?;
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
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
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let sst_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            MemTable::create_with_wal(sst_id, self.path_of_wal(sst_id))?
        } else {
            MemTable::create(sst_id)
        };
        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut state.memtable, Arc::new(memtable));
            state.imm_memtables.insert(0, old_memtable);
            *guard = Arc::new(state);
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(state_lock_observer, ManifestRecord::NewMemtable(sst_id))?;
        }
        self.sync_dir()?;

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

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            // remove it from imm_memtable
            let memtable = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(memtable.id(), sst_id);

            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }

            println!("flushed {}.sst with size={}", sst_id, sstable.table_size());
            snapshot.sstables.insert(sst_id, Arc::new(sstable));

            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            fs::remove_file(self.path_of_wal(sst_id))?;
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        }

        self.sync_dir()?;

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

        let lower_bound_slice = lower.map(|b| KeySlice::from_slice(b, TS_RANGE_BEGIN));
        let upper_bound_slice = upper.map(|b| KeySlice::from_slice(b, TS_RANGE_END));

        let iterator = Box::new(snapshot.memtable.scan(lower_bound_slice, upper_bound_slice));
        let mut vec = snapshot
            .imm_memtables
            .iter()
            .map(|m| Box::new(m.scan(lower_bound_slice, upper_bound_slice)))
            .collect::<Vec<_>>();
        vec.insert(0, iterator);
        let mut mem_iter = MergeIterator::create(vec);
        if let Bound::Excluded(key) = lower {
            while mem_iter.is_valid() && mem_iter.key().key_ref() == key {
                mem_iter.next()?;
            }
        }

        let mut iters = Vec::new();
        for idx in &snapshot.l0_sstables {
            let sstable = snapshot.sstables[idx].clone();

            if !range_overlap(
                lower_bound_slice,
                upper_bound_slice,
                sstable.first_key(),
                sstable.last_key(),
            ) {
                continue;
            }

            let iter = match lower_bound_slice {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(sstable, key)?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(sstable, key)?;
                    while iter.is_valid() && iter.key().key_ref() == key.key_ref() {
                        iter.next()?;
                    }
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
                .filter(|sst| {
                    range_overlap(
                        lower_bound_slice,
                        upper_bound_slice,
                        sst.first_key(),
                        sst.last_key(),
                    )
                })
                .collect::<Vec<_>>();

            let levels_sst_iter = match lower_bound_slice {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(levels_sst, key)?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(levels_sst, key)?;
                    while iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
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
            map_bound(upper_bound_slice),
        )?))
    }
}
