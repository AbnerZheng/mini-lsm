#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = fs::OpenOptions::new().create(true).write(true).open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = fs::OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let records = serde_json::from_slice::<ManifestRecord>(&buf)
            .into_iter()
            .collect::<Vec<_>>();

        Ok((
            Manifest {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let bytes = serde_json::to_vec(&record)?;
        let mut guard = self.file.lock();
        guard.write_all(bytes.as_slice())?;
        guard.flush()?;
        Ok(())
    }
}
