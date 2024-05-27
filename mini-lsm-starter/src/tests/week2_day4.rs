use tempfile::tempdir;

use crate::compact::LeveledCompactionController;
use crate::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
};

use super::harness::{check_compaction_ratio, compaction_bench};

#[test]
fn test_integration() {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(
            LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2,
                level_size_multiplier: 2,
                base_level_size_mb: 1,
                max_levels: 4,
            },
        )),
    )
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}

#[test]
fn test_target_size() {
    let controller = LeveledCompactionController::new(LeveledCompactionOptions {
        level0_file_num_compaction_trigger: 2,
        level_size_multiplier: 10,
        base_level_size_mb: 200,
        max_levels: 6,
    });
    assert_eq!(controller.target_size(200), vec![0, 0, 0, 0, 0, 200]);
    assert_eq!(controller.target_size(300), vec![0, 0, 0, 0, 30, 300]);
    assert_eq!(
        controller.target_size(30 * 1000),
        vec![0, 0, 30, 300, 3 * 1000, 30 * 1000]
    );
}
