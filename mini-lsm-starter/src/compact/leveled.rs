use serde::{Deserialize, Serialize};

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
        let mut cur = max_level_size_mb;
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
        if snapshot.l0_sstables.len() > self.options.level0_file_num_compaction_trigger {
            // use number of file as file size to simplify problem
            let target_size =
                self.target_size(snapshot.levels[self.options.max_levels - 1].1.len());
            let idx = target_size.iter().position(|&x| x > 0);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: vec![],
                lower_level: 0,
                lower_level_sst_ids: vec![],
                is_lower_level_bottom_level: false,
            });
        }
        unimplemented!()
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
