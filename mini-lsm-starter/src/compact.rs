#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::fs;
use std::os::macos::raw::stat;
use std::sync::Arc;
use std::time::Duration;

use crate::compact::CompactionTask::ForceFullCompaction;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use log::warn;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

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
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_) => {
                unimplemented!();
            }
            CompactionTask::Tiered(_) => {
                unimplemented!();
            }
            CompactionTask::Simple(_) => {
                unimplemented!();
            }
            ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = {
                    let guard = self.state.read();
                    guard.clone()
                };

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
                let mut merge_iterator =
                    TwoMergeIterator::create(l0_merge_iterator, concat_iterator)?;
                let mut sst_builder = SsTableBuilder::new(self.options.block_size);

                let mut sstable_to_add = vec![];
                while merge_iterator.is_valid() {
                    if !(task.compact_to_bottom_level() && merge_iterator.value().is_empty()) {
                        sst_builder.add(merge_iterator.key(), merge_iterator.value());
                        if sst_builder.estimated_size() > self.options.target_sst_size {
                            // split a new sst file
                            let sst_id = self.next_sst_id();
                            let sst_table = sst_builder.build(
                                sst_id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(sst_id),
                            )?;
                            sstable_to_add.push(Arc::new(sst_table));
                            sst_builder = SsTableBuilder::new(self.options.block_size);
                        }
                    }
                    merge_iterator.next()?;
                }

                if !sst_builder.is_empty() {
                    let sst_id = self.next_sst_id();
                    let sst_table = sst_builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    );
                    sstable_to_add.push(Arc::new(sst_table.unwrap()));
                }

                Ok(sstable_to_add)
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

        let new_l1_levels = self.compact(&ForceFullCompaction {
            l0_sstables: l0_sstables_to_compact.clone(),
            l1_sstables: l1_sstables_to_compact.clone(),
        })?;

        // update state of lsm
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();

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

        return Ok(());
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
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
        if imm_memtable_len + 1 >= self.options.num_memtable_limit {
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
