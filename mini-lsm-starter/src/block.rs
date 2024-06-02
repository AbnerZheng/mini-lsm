mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use builder::{prefix_decoding, prefix_encoding};
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZE_OF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZE_OF_U32: usize = std::mem::size_of::<u32>();
pub(crate) const SIZE_OF_U64: usize = std::mem::size_of::<u64>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Default)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

/// Data layout
///
/// --------------------------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section                  |      Extra      | checksum |
/// --------------------------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1(u16) | Offset #2 | ... | Offset #N | num_of_elements |  u32     |
/// -------------------------------------------------------------------------------------------------------------------
impl Block {
    pub fn encode(&self) -> Bytes {
        // data section
        let mut res = self.data.clone();

        // offset section
        for offset in &self.offsets {
            res.put_u16(*offset);
        }

        // num of element
        res.put_u16(self.offsets.len() as u16);

        // checksum
        let crc = crc32fast::hash(&res);
        res.put_u32(crc);

        res.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let end_position_num_of_elem = data.len() - SIZE_OF_U32;
        // verify checksum
        let crc_expected = (&data[end_position_num_of_elem..]).get_u32();
        let crc_actual = crc32fast::hash(&data[..end_position_num_of_elem]);
        assert_eq!(crc_expected, crc_actual, "corrupted block");

        let end_position_offset = end_position_num_of_elem - SIZE_OF_U16;
        let num_of_elements =
            (&data[end_position_offset..end_position_num_of_elem]).get_u16() as usize;

        let end_position_data = end_position_offset - num_of_elements * SIZE_OF_U16;

        let offset = data[end_position_data..end_position_offset]
            .chunks(2)
            .map(|mut chunk| chunk.get_u16())
            .collect::<Vec<_>>();

        let data = &data[..end_position_data];
        Self {
            data: data.to_vec(),
            offsets: offset,
        }
    }
}
