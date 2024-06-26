use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::compact::CompactionTask::ForceFullCompaction;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

mod leveled;
mod simple_leveled;
mod tiered;

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_from_iter(
        &self,
        compact_to_bottom_level: bool,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut sst_to_add = vec![];

        let mut prev_key = None;
        let watermark = {
            let guard = self.mvcc().ts.lock();
            guard.1.watermark().unwrap_or(u64::MAX)
        };
        let compaction_filter_guard = self.compaction_filters.lock();
        let mut prefixs = Vec::with_capacity(compaction_filter_guard.len());
        for f in compaction_filter_guard.iter() {
            match f {
                CompactionFilter::Prefix(p) => prefixs.push(p.to_vec()),
            }
        }

        let mut latest_version_leq_added = false;
        while iter.is_valid() {
            // println!("merging {:?}/{:?}", iter.key(), iter.value());
            let cur_key = iter.key().key_ref().to_vec();
            let cur_ts = iter.key().ts();

            // If a version of a key is above watermark, keep it.
            // For all versions of a key below or equal to the watermark, keep the latest version.

            match &prev_key {
                None => {
                    latest_version_leq_added = cur_ts <= watermark;
                    if !(latest_version_leq_added
                        && compact_to_bottom_level
                        && iter.value().is_empty())
                    {
                        if latest_version_leq_added {
                            let mut removed = false;
                            for p in &prefixs {
                                if cur_key.starts_with(p) {
                                    removed = true;
                                    break;
                                }
                            }
                            if !removed {
                                sst_builder.add(iter.key(), iter.value());
                            }
                        } else {
                            sst_builder.add(iter.key(), iter.value());
                        }
                    }
                    prev_key = Some(cur_key);
                }
                Some(key) if *key != cur_key => {
                    latest_version_leq_added = cur_ts <= watermark;
                    if !(cur_ts <= watermark && compact_to_bottom_level && iter.value().is_empty())
                    {
                        let mut removed = false;
                        if latest_version_leq_added {
                            for p in &prefixs {
                                if cur_key.starts_with(p) {
                                    removed = true;
                                    break;
                                }
                            }
                        }
                        if !removed {
                            sst_builder.add(iter.key(), iter.value());

                            if sst_builder.estimated_size() > self.options.target_sst_size {
                                // split a new sst file
                                let sst_id = self.next_sst_id();
                                let sst_table = sst_builder.build(
                                    sst_id,
                                    Some(self.block_cache.clone()),
                                    self.path_of_sst(sst_id),
                                )?;
                                sst_to_add.push(Arc::new(sst_table));
                                sst_builder = SsTableBuilder::new(self.options.block_size);
                            }
                        }
                    }
                    prev_key = Some(cur_key);
                }
                Some(key) if *key == cur_key => {
                    if cur_ts > watermark {
                        sst_builder.add(iter.key(), iter.value());
                    } else if !latest_version_leq_added {
                        // keep the latest version for all version below or equal to the watermark
                        latest_version_leq_added = true;

                        if !(compact_to_bottom_level && iter.value().is_empty()) {
                            let mut removed = false;
                            for p in &prefixs {
                                if key.starts_with(p) {
                                    removed = true;
                                    break;
                                }
                            }
                            if !removed {
                                sst_builder.add(iter.key(), iter.value());
                            }
                        }
                    } else {
                        println!("remove version@{cur_ts} of {key:?}");
                    }
                }
                _ => {
                    // do nothing
                }
            }

            iter.next()?;
        }

        if !sst_builder.is_empty() {
            let sst_id = self.next_sst_id();
            let sst_table = sst_builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            );
            sst_to_add.push(Arc::new(sst_table.unwrap()));
        }
        Ok(sst_to_add)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        match task {
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included: _,
            }) => {
                let to_compact = tiers
                    .iter()
                    .map(|(_, sst_id)| {
                        let sst = sst_id
                            .iter()
                            .map(|sst_id| snapshot.sstables[sst_id].clone())
                            .collect::<Vec<_>>();
                        let iter = SstConcatIterator::create_and_seek_to_first(sst)?;
                        Ok(Box::new(iter))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let merge_iterator = MergeIterator::create(to_compact);
                self.compact_from_iter(task.compact_to_bottom_level(), merge_iterator)
            }
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                is_lower_level_bottom_level: _,
            })
            | CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                is_lower_level_bottom_level: _,
            }) => {
                let lower_tables = lower_level_sst_ids
                    .iter()
                    .map(|i| snapshot.sstables[i].clone())
                    .collect::<Vec<_>>();
                let lower_iterator = SstConcatIterator::create_and_seek_to_first(lower_tables)?;

                match upper_level {
                    None => {
                        let l0_tables = upper_level_sst_ids
                            .iter()
                            .map(|i| {
                                let sst = snapshot.sstables[i].clone();
                                let sst_iter = SsTableIterator::create_and_seek_to_first(sst)?;
                                Ok(Box::new(sst_iter))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let upper_iterator = MergeIterator::create(l0_tables);
                        self.compact_from_iter(
                            task.compact_to_bottom_level(),
                            TwoMergeIterator::create(upper_iterator, lower_iterator)?,
                        )
                    }
                    Some(_) => {
                        let upper_tables = upper_level_sst_ids
                            .iter()
                            .map(|i| snapshot.sstables[i].clone())
                            .collect::<Vec<_>>();
                        let upper_iterator =
                            SstConcatIterator::create_and_seek_to_first(upper_tables)?;
                        self.compact_from_iter(
                            task.compact_to_bottom_level(),
                            TwoMergeIterator::create(upper_iterator, lower_iterator)?,
                        )
                    }
                }
            }
            ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_tables = l0_sstables
                    .iter()
                    .map(|i| {
                        let sstable = snapshot.sstables[i].clone();
                        let sstable_iter = SsTableIterator::create_and_seek_to_first(sstable)?;
                        Ok(Box::new(sstable_iter))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let l0_merge_iterator = MergeIterator::create(l0_tables);

                let l1_tables = l1_sstables
                    .iter()
                    .map(|i| snapshot.sstables[i].clone())
                    .collect::<Vec<_>>();

                let concat_iterator = SstConcatIterator::create_and_seek_to_first(l1_tables)?;
                let merge_iterator = TwoMergeIterator::create(l0_merge_iterator, concat_iterator)?;
                self.compact_from_iter(task.compact_to_bottom_level(), merge_iterator)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let l0_sstables_to_compact = snapshot.l0_sstables.clone();
        assert_eq!(snapshot.levels[0].0, 1);
        let l1_sstables_to_compact = snapshot.levels[0].1.clone();

        let task = ForceFullCompaction {
            l0_sstables: l0_sstables_to_compact.clone(),
            l1_sstables: l1_sstables_to_compact.clone(),
        };
        let new_l1_levels = self.compact(&task)?;
        let sst_to_add_ids = new_l1_levels.iter().map(|sst| sst.sst_id()).collect();

        // update state of lsm
        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();

            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(task, sst_to_add_ids),
                )?;
            }

            // remove sstable
            l0_sstables_to_compact
                .iter()
                .chain(l1_sstables_to_compact.iter())
                .for_each(|sst_id| {
                    state.sstables.remove(sst_id);
                });

            state
                .l0_sstables
                .retain(|sst_id| !l0_sstables_to_compact.contains(sst_id));

            let mut level1 = Vec::with_capacity(new_l1_levels.len());
            for sst in new_l1_levels {
                let sst_id = sst.sst_id();
                level1.push(sst_id);
                let result = state.sstables.insert(sst_id, sst);
                assert!(result.is_none());
            }
            state.levels[0] = (1, level1);
            *self.state.write() = Arc::new(state);
        }

        // remove files
        for sst_id in l0_sstables_to_compact
            .iter()
            .chain(l1_sstables_to_compact.iter())
        {
            fs::remove_file(self.path_of_sst(*sst_id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let option = {
            let guard = self.state.read();
            self.compaction_controller.generate_compaction_task(&guard)
        };

        let Some(compaction_task) = option else {
            return Ok(());
        };

        self.dump_structure();
        println!("running compaction task: {:?}", compaction_task);

        let sst_to_add = self.compact(&compaction_task)?;
        let sst_to_add_ids = sst_to_add
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<_>>();

        let sst_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();

            for sst in sst_to_add {
                let prev = state.sstables.insert(sst.sst_id(), sst);
                assert!(prev.is_none());
            }

            let (mut new_state, sst_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&state, &compaction_task, &sst_to_add_ids);

            drop(state);

            for sst_id in &sst_to_remove {
                let res = new_state.sstables.remove(sst_id);
                assert!(res.is_some());
            }

            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(compaction_task, sst_to_add_ids.clone()),
                )?;
            }
            *self.state.write() = Arc::new(new_state);
            println!(
                "compaction finished: {} files removed, {} files added, output={:?}",
                sst_to_remove.len(),
                sst_to_add_ids.len(),
                sst_to_add_ids
            );
            sst_to_remove
        };

        for sst_id in &sst_to_remove {
            fs::remove_file(self.path_of_sst(*sst_id))?
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let imm_memtable_len = {
            let snapshot = self.state.read();
            snapshot.imm_memtables.len()
        };
        if imm_memtable_len >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()
        } else {
            Ok(())
        }
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                            eprintln!("flush failed: {}", e);
                        },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
