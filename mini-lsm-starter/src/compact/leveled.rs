use log::warn;
use serde::{Deserialize, Serialize};
use std::process::id;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        unimplemented!()
    }

    pub fn target_size(&self, max_level_size_mb: usize) -> Vec<usize> {
        let mut res = vec![0; self.options.max_levels];
        let mut cur = max_level_size_mb.max(self.options.base_level_size_mb);
        for i in (0..self.options.max_levels).rev() {
            res[i] = cur;
            if cur <= self.options.base_level_size_mb {
                break;
            }
            cur /= self.options.level_size_multiplier;
        }

        res
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        assert_eq!(snapshot.levels.len(), self.options.max_levels);
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let max_level_size = snapshot.levels[self.options.max_levels - 1]
                .1
                .iter()
                .map(|sst_id| snapshot.sstables[sst_id].file.size())
                .sum::<u64>()
                / 1024
                / 1024;
            let target_size = self.target_size(max_level_size as usize);
            let idx = target_size
                .iter()
                .position(|&x| x > 0)
                .expect("target size should have at least 1 positive value");

            println!("flush L0 SST to base level {}", idx + 1);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: idx,
                lower_level_sst_ids: snapshot.levels[idx].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let LeveledCompactionTask {
            upper_level,
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level,
        } = task;
        let mut files_to_remove = vec![];
        match upper_level {
            None => {
                // compact l0
                assert_eq!(
                    snapshot.l0_sstables, *upper_level_sst_ids,
                    "state change during compaction"
                );
                snapshot.l0_sstables = vec![];
                files_to_remove.extend_from_slice(upper_level_sst_ids);
                let (level_id, sst_ids) = &snapshot.levels[*lower_level];
                assert_eq!(
                    *sst_ids, *lower_level_sst_ids,
                    "state change during compaction"
                );
                files_to_remove.extend_from_slice(sst_ids);
                snapshot.levels[*lower_level] = (*level_id, output.to_vec());
            }
            Some(_) => {}
        }
        (snapshot, files_to_remove)
    }
}
