use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|s| *s += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        match self.readers.entry(ts) {
            Entry::Vacant(_) => {
                // do nothing
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                if *v <= 1 {
                    e.remove_entry();
                } else {
                    *v -= 1;
                }
            }
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(ts, _)| *ts)
    }

    pub(crate) fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
