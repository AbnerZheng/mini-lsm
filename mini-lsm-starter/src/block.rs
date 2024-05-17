mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use builder::{prefix_decoding, prefix_encoding};
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Default)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut res = self.data.clone();
        for x in &self.offsets {
            res.put_u16(*x);
        }
        res.put_u16(self.offsets.len() as u16);
        Bytes::from(res)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;

        let offset = data[data.len() - SIZEOF_U16 * (num_of_elements + 1)..data.len() - SIZEOF_U16]
            .chunks(2)
            .map(|mut chunk| chunk.get_u16())
            .collect::<Vec<_>>();

        let data = &data[..data.len() - SIZEOF_U16 * (num_of_elements + 1)];
        Self {
            data: data.to_vec(),
            offsets: offset,
        }
    }
}
