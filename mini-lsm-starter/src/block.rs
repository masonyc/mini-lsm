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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        buf.extend_from_slice(&self.data);
        for &off in &self.offsets {
            buf.put_u16(off);
        }

        buf.put_u16(self.offsets.len() as u16);
        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let total_len = data.len();

        let num_offsets = u16::from_be_bytes([data[total_len - 2], data[total_len - 1]]) as usize;

        let offsets_len = num_offsets * 2;
        let offsets_start = total_len - 2 - offsets_len;

        let data_region = &data[..offsets_start];
        let offsets_region = &data[offsets_start..total_len - 2];

        let mut offsets = Vec::with_capacity(num_offsets);
        let mut slice = offsets_region;
        while slice.len() >= 2 {
            let off = u16::from_be_bytes([slice[0], slice[1]]);
            offsets.push(off);
            slice = &slice[2..];
        }
        Self {
            data: data_region.to_vec(),
            offsets,
        }
    }
}
