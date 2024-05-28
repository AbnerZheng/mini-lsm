use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Space Amplification Ratio
        let all_level_except_last_level = snapshot.levels[..snapshot.levels.len() - 1]
            .iter()
            .map(|(_idx, sst_ids)| sst_ids.len())
            .sum::<usize>();
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        if all_level_except_last_level * 100
            >= self.options.max_size_amplification_percent * last_level_size
        {
            println!(
                "compaction triggered by space amplification ratio: {}",
                all_level_except_last_level * 100 / last_level_size
            );

            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // Triggered by Size Ratio
        let mut acc = snapshot.levels[0].1.len();
        for n in 1..snapshot.levels.len() {
            let this_tire = snapshot.levels[n].1.len();
            if 100 * acc >= this_tire * (100 + self.options.size_ratio) {
                println!(
                    "compaction triggered by size ratio: {}",
                    acc * 100 / this_tire
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..=n].to_vec(),
                    bottom_tier_included: n == snapshot.levels.len() - 1,
                });
            }
            acc += this_tire;
        }

        // Reduce Sorted Runs
        println!("compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: vec![snapshot.levels[0].clone(), snapshot.levels[1].clone()],
            bottom_tier_included: false,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut tiers_to_remove = task
            .tiers
            .iter()
            .map(|(tier_id, sst_ids)| (*tier_id, sst_ids))
            .collect::<HashMap<_, _>>();

        let mut files_to_remove = Vec::new();
        let mut level_kept = Vec::new();

        for (tier_id, sst_ids) in snapshot.levels {
            if let Some(to_remove) = tiers_to_remove.remove(&tier_id) {
                assert_eq!(*to_remove, sst_ids, "state changed during compaction");
                files_to_remove.extend_from_slice(to_remove);
                if tiers_to_remove.is_empty() {
                    level_kept.push((output[0], output.to_vec()));
                }
            } else {
                level_kept.push((tier_id, sst_ids));
            }
        }
        assert!(
            tiers_to_remove.is_empty(),
            "Should be able to remove all compacted tiers"
        );

        snapshot.levels = level_kept;
        (snapshot, files_to_remove)
    }
}
