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

use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::BufMut;

use super::{BlockMeta, SsTable};
use crate::key::Key;
use crate::table::FileObject;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.clear();
            self.first_key.extend_from_slice(key.raw_ref());
        }
        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend_from_slice(key.raw_ref());
            return;
        }

        self.rotate_block();
        assert!(self.builder.add(key, value));

        self.first_key.clear();
        self.last_key.clear();
        self.first_key.extend_from_slice(key.raw_ref());
        self.last_key.extend_from_slice(key.raw_ref());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.rotate_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.first().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            bloom: None,
            max_ts: u64::MAX,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    fn rotate_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_vec(std::mem::take(&mut self.first_key)).into_key_bytes(),
            last_key: Key::from_vec(std::mem::take(&mut self.last_key)).into_key_bytes(),
        });

        self.data.extend(encoded);
    }
}
