#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::thread::current;

use anyhow::Result;
use log::debug;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut binary_heap = iters
            .into_iter()
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(idx, h)| HeapWrapper(idx, h))
            .collect::<BinaryHeap<_>>();

        let top = binary_heap.pop();

        Self {
            iters: binary_heap,
            current: top,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        let option = self.current.as_ref();
        match option {
            None => KeySlice::default(),
            Some(x) => *&x.1.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.current.as_ref() {
            None => Default::default(),
            Some(x) => x.1.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        while let Some(mut iter) = self.iters.pop() {
            assert!(current.1.key() <= iter.1.key(), "heap invariant");
            if iter.1.key() == current.1.key() {
                iter.1.next()?;

                // put it back if there is still some valid item inside the iter
                if iter.1.is_valid() {
                    self.iters.push(iter)
                }
                // keep consuming if the key is the same
                continue;
            } else {
                self.iters.push(iter);
                // ok, we have deal with all item containing the same key
                break;
            }
        }
        // start deal with another key
        current.1.next()?;
        if !current.1.is_valid() {
            self.current = self.iters.pop();
        } else {
            let current = self.current.take().unwrap();
            self.iters.push(current);
            self.current = self.iters.pop();
        }

        Ok(())
    }
}
