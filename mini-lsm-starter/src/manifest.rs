use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut};
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
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = fs::OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let mut buf_slice = buf.as_slice();

        let mut records = vec![];
        while buf_slice.has_remaining() {
            let len = buf_slice.get_u16();
            let (json_record_raw, mut rem) = buf_slice.split_at(len as usize);
            let crc_calculated = crc32fast::hash(json_record_raw);
            let crc_expected = rem.get_u32();
            assert_eq!(crc_calculated, crc_expected, "corrupted manifest");
            buf_slice = rem;
            let record = serde_json::from_slice::<ManifestRecord>(json_record_raw)?;
            records.push(record);
        }

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
        let mut vec: Vec<u8> = Vec::with_capacity(4 + 2 + bytes.len());
        let crc = crc32fast::hash(&bytes);

        vec.put_u16(bytes.len() as u16);
        vec.extend_from_slice(&bytes);
        vec.put_u32(crc);
        let mut guard = self.file.lock();
        guard.write_all(&vec)?;
        guard.flush()?;
        Ok(())
    }
}
