use crate::iterators::merge_iterator::MergeIterator;
use serde::{Deserialize, Serialize};
use std::arch::aarch64::vabal_high_s8;
use std::process::id;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if self.options.level0_file_num_compaction_trigger <= snapshot.l0_sstables.len() {
            assert_eq!(snapshot.levels[0].0, 1);

            if snapshot.levels[0].1.len() * 100
                < self.options.size_ratio_percent * snapshot.l0_sstables.len()
            {
                println!(
                    "compaction triggered at level 0 and 1 with size ratio {}",
                    snapshot.levels[0].1.len() / snapshot.l0_sstables.len()
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: None,
                    upper_level_sst_ids: snapshot.l0_sstables.clone(),
                    lower_level: 1,
                    lower_level_sst_ids: snapshot.levels[0].1.clone(),
                    is_lower_level_bottom_level: false,
                });
            }
        }

        for upper_level in 1..self.options.max_levels {
            assert_eq!(snapshot.levels[upper_level - 1].0, upper_level);
            let lower_level = upper_level + 1;
            assert_eq!(snapshot.levels[lower_level - 1].0, lower_level);

            let upper_level_sst = &snapshot.levels[upper_level - 1].1;
            let lower_level_sst = &snapshot.levels[lower_level - 1].1;

            if lower_level_sst.len() * 100 < self.options.size_ratio_percent * upper_level_sst.len()
            {
                println!(
                    "compaction triggered at level {upper_level} and {lower_level} with size ratio {}",
                    upper_level_sst.len().checked_div(lower_level_sst.len()).unwrap_or_default()
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: upper_level_sst.clone(),
                    lower_level,
                    lower_level_sst_ids: lower_level_sst.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert_eq!(snapshot.levels[task.lower_level - 1].0, task.lower_level);
        let mut snapshot = snapshot.clone();
        let mut to_remove_idx = vec![];
        let lower_level_sst = &mut snapshot.levels[task.lower_level - 1].1;
        assert_eq!(
            *lower_level_sst, task.lower_level_sst_ids,
            "lower level sst ids mismatch"
        );
        to_remove_idx.extend_from_slice(lower_level_sst);
        *lower_level_sst = output.to_vec();
        if task.upper_level.is_none() || task.upper_level == Some(0) {
            snapshot
                .l0_sstables
                .retain(|idx| !task.upper_level_sst_ids.contains(idx));
        } else {
            let upper_level = task.upper_level.unwrap();
            assert_eq!(snapshot.levels[upper_level - 1].0, upper_level);
            snapshot.levels[upper_level - 1]
                .1
                .retain(|idx| !task.upper_level_sst_ids.contains(idx))
        }
        to_remove_idx.extend_from_slice(task.upper_level_sst_ids.as_slice());
        (snapshot, to_remove_idx)
    }
}
