#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use log::warn;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    flipped: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut s = Self {
            a,
            b,
            flipped: false,
        };
        s.update_flipped_and_skip_b();
        Ok(s)
    }

    fn update_flipped_and_skip_b(&mut self) {
        self.flipped = if !self.a.is_valid() {
            true
        } else if !self.b.is_valid() {
            false
        } else if self.b.key() == self.a.key() {
            let _ = self.b.next();
            false
        } else {
            self.b.key() < self.a.key()
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn value(&self) -> &[u8] {
        if self.flipped {
            self.b.value()
        } else {
            self.a.value()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.flipped {
            self.b.key()
        } else {
            self.a.key()
        }
    }

    fn is_valid(&self) -> bool {
        if self.flipped {
            self.b.is_valid()
        } else {
            self.a.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if !self.a.is_valid() && !self.b.is_valid() {
            return Ok(());
        }
        if self.flipped {
            self.b.next()?;
        } else {
            self.a.next()?;
        }
        self.update_flipped_and_skip_b();

        Ok(())
    }
}
