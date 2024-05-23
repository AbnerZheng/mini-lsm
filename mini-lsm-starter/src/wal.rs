#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().write(true).create(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(writer)),
        })
    }

    pub fn recover(
        path: impl AsRef<Path>,
        skiplist: &SkipMap<Bytes, Bytes>,
    ) -> Result<(Self, usize)> {
        let mut file = OpenOptions::new().write(true).read(true).open(path)?;
        let mut buf = Vec::new();
        let size = file.read_to_end(&mut buf)?;
        let mut buf_slice = buf.as_slice();

        while buf_slice.has_remaining() {
            let key_len = buf_slice.get_u16();
            let (key, mut rem) = buf_slice.split_at(key_len as usize);
            let value_len = rem.get_u16();
            let (value, rem) = rem.split_at(value_len as usize);

            skiplist.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
            buf_slice = rem;
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(BufWriter::new(file))),
            },
            size,
        ))
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data: Vec<u8> = Vec::with_capacity(key.len() + value.len() + 4);
        data.put_u16(key.len() as u16);
        data.put_slice(key);
        data.put_u16(value.len() as u16);
        data.put_slice(value);

        let mut guard = self.file.lock();
        guard.write_all(&data)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
