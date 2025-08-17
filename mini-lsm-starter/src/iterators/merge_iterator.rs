// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
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
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }
        Self {
            iters: heap,
            current: None,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        assert!(self.is_valid(), "called key() on invalid MergeIterator");
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid(), "called value() on invalid MergeIterator");
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .is_some_and(|wrapper| wrapper.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        // Pop the "winner" iterator (smallest key)
        let mut winner = match self.iters.pop() {
            Some(wrapper) => wrapper,
            None => {
                self.current = None;
                return Ok(());
            }
        };

        let current_key = winner.1.key();

        // Process all other iterators in the heap
        let mut temp_heap = Vec::new();
        while let Some(mut wrapper) = self.iters.pop() {
            if wrapper.1.key() == current_key {
                // Duplicate key: advance it
                wrapper.1.next()?;
            }
            if wrapper.1.is_valid() {
                temp_heap.push(wrapper);
            }
        }

        // Push all processed iterators back into the heap
        for wrapper in temp_heap {
            self.iters.push(wrapper);
        }

        // Set current to the winner for key/value access
        self.current = Some(HeapWrapper(winner.0, winner.1));

        // Advance the winner iterator and push back if still valid
        if let Some(current) = &mut self.current {
            current.1.next()?;
            if current.1.is_valid() {
                // Move it back into the heap
                let to_push = self.current.take().unwrap();
                self.iters.push(to_push);
            }
        }

        Ok(())
    }
}
