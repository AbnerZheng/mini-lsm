use crate::lsm_storage::LsmStorageState;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

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
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        lower_level: usize,
    ) -> Vec<usize> {
        let min_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].first_key())
            .min()
            .unwrap();
        let max_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].last_key())
            .max()
            .unwrap();

        snapshot.levels[lower_level]
            .1
            .iter()
            .filter(|&sst_id| {
                let sst = snapshot.sstables[sst_id].clone();

                if sst.first_key() > max_key {
                    false
                } else {
                    sst.last_key() >= min_key
                }
            })
            .copied()
            .collect::<Vec<_>>()
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
        let max_level_size_bytes = snapshot.levels[self.options.max_levels - 1]
            .1
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].file.size())
            .sum::<u64>();
        let max_level_size_mb = max_level_size_bytes / 1024 / 1024;
        let target_size = self.target_size(max_level_size_mb as usize);
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let idx = target_size
                .iter()
                .position(|&x| x > 0)
                .expect("target size should have at least 1 positive value");

            println!("flush L0 SST to base level {}", idx + 1);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: idx,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    idx,
                ),
                is_lower_level_bottom_level: idx == self.options.max_levels - 1,
            });
        }

        let files_sizes = snapshot
            .levels
            .iter()
            .map(|(_, sst_ids)| {
                let file_in_bytes = sst_ids
                    .iter()
                    .map(|sst_id| snapshot.sstables[sst_id].file.size())
                    .sum::<u64>();
                file_in_bytes / 1024 / 1024
            })
            .collect::<Vec<_>>();

        let (idx, ratio) = files_sizes
            .iter()
            .zip(target_size)
            .map(|(&act, tgt)| act as f64 / tgt as f64)
            .enumerate()
            .max_by(|x, y| {
                // println!("{x:?}, {y:?}");
                x.1.partial_cmp(&y.1).unwrap_or(Ordering::Less)
            })
            .unwrap();

        if ratio > 1.0 {
            let oldest = snapshot.levels[idx].1.iter().min().unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(idx),
                upper_level_sst_ids: vec![*oldest],
                lower_level: idx + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &[*oldest], idx + 1),
                is_lower_level_bottom_level: idx + 1 == self.options.max_levels - 1,
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
            ..
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
            }
            Some(upper_level) => {
                assert_eq!(upper_level_sst_ids.len(), 1);
                snapshot.levels[*upper_level]
                    .1
                    .retain(|i| *i != upper_level_sst_ids[0]);
            }
        }

        files_to_remove.extend_from_slice(upper_level_sst_ids);
        files_to_remove.extend_from_slice(lower_level_sst_ids);

        let (level_id, sst_ids) = &snapshot.levels[*lower_level];
        let mut lower_level_kept = if lower_level_sst_ids.is_empty() {
            // no need to remove
            sst_ids.clone()
        } else {
            let mut found = false;
            let mut res = vec![];
            for (i, sst_id) in sst_ids.iter().enumerate() {
                if lower_level_sst_ids[0] == *sst_id {
                    assert_eq!(
                        *lower_level_sst_ids,
                        sst_ids[i..i + lower_level_sst_ids.len()],
                        "state change during compaction"
                    );
                    found = true;

                    res.extend_from_slice(&sst_ids[..i]);
                    res.extend_from_slice(&sst_ids[i + lower_level_sst_ids.len()..]);
                }
            }
            if !found {
                panic!("state has changed");
            }
            res
        };
        // find the right place to insert compacted SsTable
        let first_key = snapshot.sstables[&output[0]].first_key();
        if lower_level_kept.is_empty() {
            snapshot.levels[*lower_level] = (*level_id, output.to_vec());
        } else {
            let insert_idx = lower_level_kept
                .iter()
                .position(|sst_id| snapshot.sstables[sst_id].last_key() > first_key);
            match insert_idx {
                None => {
                    lower_level_kept.extend_from_slice(output);
                    snapshot.levels[*lower_level] = (*level_id, lower_level_kept.to_vec());
                }
                Some(idx) => {
                    let (prev, post) = lower_level_kept.split_at(idx);
                    let mut res = prev.to_vec();
                    res.extend_from_slice(output);
                    res.extend_from_slice(post);
                    snapshot.levels[*lower_level] = (*level_id, res);
                }
            }
        }
        (snapshot, files_to_remove)
    }
}
