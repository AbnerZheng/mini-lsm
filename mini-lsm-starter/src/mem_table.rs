use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

type MemMap = Arc<SkipMap<KeyBytes, Bytes>>;

/// A basic mem-table based on crossbeam-skiplist.
pub struct MemTable {
    map: MemMap,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    bound.map(|x| KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(x.key_ref()), x.ts()))
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(Default::default()),
            wal: None,
            id,
            approximate_size: Arc::new(Default::default()),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<MemTable> {
        let wal = Wal::create(path)?;
        Ok(Self {
            map: Arc::new(Default::default()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(Default::default()),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let skip_map = SkipMap::new();
        let (wal, approximate_size) = Wal::recover(path, &skip_map)?;
        Ok(Self {
            map: Arc::new(skip_map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(approximate_size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::for_testing_from_slice_default_ts(key), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::for_testing_from_slice_default_ts(key))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            lower.map(|s| KeySlice::from_slice(s, TS_DEFAULT)),
            upper.map(|s| KeySlice::from_slice(s, TS_DEFAULT)),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        self.map
            .get(&KeyBytes::from_bytes_with_ts(
                Bytes::from_static(unsafe { std::mem::transmute(key.key_ref()) }),
                key.ts(),
            ))
            .map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.map.insert(
            key.to_key_vec().into_key_bytes(),
            Bytes::copy_from_slice(value),
        );
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }
        self.approximate_size
            .fetch_add(key.key_len() + value.len(), Ordering::Relaxed);
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let bound = (map_bound(lower), map_bound(upper));
        let mut iterator = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range(bound),
            item: (Default::default(), Default::default()),
        }
        .build();
        // Move to first element
        iterator.next().unwrap();
        iterator
    }

    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        self.map
            .iter()
            .for_each(|entry| builder.add(entry.key().as_key_slice(), entry.value()));
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: MemMap,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        let x = self.borrow_item();
        x.1.as_ref()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|s| match s.iter.next() {
            None => {
                *s.item = (KeyBytes::new(), Bytes::new());
            }
            Some(entry) => *s.item = (entry.key().clone(), entry.value().clone()),
        });
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
