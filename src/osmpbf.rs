use byteorder::{ByteOrder, NetworkEndian};
use failure::Error;
use flate2::read::ZlibDecoder;
use memmap::Mmap;
use prost::{self, Message};

use std::fs::File;
use std::io::{self, Cursor, Read};
use std::path::Path;

include!(concat!(env!("OUT_DIR"), "/osmpbf.rs"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockType {
    Header,
    Nodes,
    DenseNodes,
    Ways,
    Relations,
}

impl BlockType {
    /// Decode block type from PrimitiveBlock protobuf message
    ///
    /// This does not decode any fields, it just checks which tags are present
    /// in PrimitiveGroup fields of the message.
    ///
    /// `blob` should contain decompressed data of an OSMData PrimitiveBlock.
    ///
    /// Note: We use public API of `prost` crate, which though is not exposed in
    /// the crate and marked with comment that it should be only used from
    /// `prost::Message`.
    pub fn from_osmdata_blob(blob: &[u8]) -> Result<BlockType, io::Error> {
        const PRIMITIVE_GROUP_TAG: u32 = 2;
        const NODES_TAG: u32 = 1;
        const DENSE_NODES_TAG: u32 = 2;
        const WAY_STAG: u32 = 3;
        const RELATIONS_TAG: u32 = 4;
        const CHANGESETS_TAG: u32 = 5;

        let mut cursor = Cursor::new(&blob[..]);
        loop {
            // decode fields of PrimitiveBlock
            let (key, wire_type) = prost::encoding::decode_key(&mut cursor)?;
            if key != PRIMITIVE_GROUP_TAG {
                // primitive group
                prost::encoding::skip_field(wire_type, &mut cursor)?;
                continue;
            }

            // We found a PrimitiveGroup field. There could be several of them, but
            // follwoing the specs of OSMPBF, all of them will have the same single
            // optional field, which defines the type of the block.

            // Decode the number of primitive groups.
            let _ = prost::encoding::decode_varint(&mut cursor)?;
            // Decode the tag of the first primitive group defining the type.
            let (tag, _wire_type) = prost::encoding::decode_key(&mut cursor)?;
            let block_type = match tag {
                NODES_TAG => BlockType::Nodes,
                DENSE_NODES_TAG => BlockType::DenseNodes,
                WAY_STAG => BlockType::Ways,
                RELATIONS_TAG => BlockType::Relations,
                CHANGESETS_TAG => {
                    panic!("found block containing unsupported changesets");
                }
                _ => {
                    panic!("invalid input data: malformed primitive block");
                }
            };
            return Ok(block_type);
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockIndex {
    pub block_type: BlockType,
    pub blob_start: usize,
    pub blob_len: usize,
    pub blob_header_len: usize,
}

pub struct BlockReader {
    _file: File,
    mmap: Mmap,
    block_buf: Vec<u8>, // buffer for the decompressed block data from the blob
}

impl BlockReader {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path.as_ref())?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            _file: file,
            mmap,
            block_buf: Vec::new(),
        })
    }

    pub fn data(&self) -> &[u8] {
        &self.mmap[..]
    }

    /// Reads the pbf file at the given path and builds an index of block types.
    ///
    /// The index is sorted lexicographically by block type and position in the
    /// pbf file.
    pub fn build_block_index(&self) -> Result<Vec<BlockIndex>, Error> {
        let mut index: Vec<_> = BlockIndexIterator::new(&self)?
            .filter_map(|block| match block {
                Ok(b) => Some(b),
                Err(e) => {
                    warn!("Skipping block due to error: {}", e);
                    None
                }
            })
            .collect();
        index.sort();
        Ok(index)
    }

    pub fn read_block<T: Message + Default>(&mut self, idx: &BlockIndex) -> Result<T, Error> {
        let blob = Blob::decode(&self.mmap[idx.blob_start..idx.blob_start + idx.blob_len])?;
        let blob_data = if blob.raw.is_some() {
            blob.raw.as_ref().unwrap()
        } else if blob.zlib_data.is_some() {
            // decompress zlib data
            self.block_buf.clear();
            let data: &Vec<u8> = blob.zlib_data.as_ref().unwrap();
            let mut decoder = ZlibDecoder::new(&data[..]);
            decoder.read_to_end(&mut self.block_buf)?;
            &self.block_buf
        } else {
            return Err(format_err!("invalid input data: unknown compression"));
        };
        Ok(T::decode(blob_data)?)
    }
}

struct BlockIndexIterator<'a> {
    data: &'a [u8],
    cursor: usize,
    blob_buf: Vec<u8>,
}

impl<'a> BlockIndexIterator<'a> {
    fn new(block_reader: &'a BlockReader) -> Result<Self, Error> {
        Ok(Self {
            data: block_reader.data(),
            cursor: 0,
            blob_buf: Vec::new(),
        })
    }

    fn read_next(&mut self) -> Result<BlockIndex, io::Error> {
        // read size of blob header
        let blob_header_len =
            NetworkEndian::read_i32(&self.data[self.cursor..self.cursor + 4]) as usize;
        self.cursor += 4;

        // read blob header
        let blob_header =
            BlobHeader::decode(&self.data[self.cursor..self.cursor + blob_header_len])?;
        self.cursor += blob_header_len;

        let blob_start = self.cursor;
        let blob_len = blob_header.datasize as usize;
        self.cursor += blob_len;

        if blob_header.type_ == "OSMHeader" {
            Ok(BlockIndex {
                block_type: BlockType::Header,
                blob_start,
                blob_len,
                blob_header_len,
            })
        } else if blob_header.type_ == "OSMData" {
            // read blob
            let blob = Blob::decode(&self.data[blob_start..blob_start + blob_len])?;
            let blob_data = if blob.raw.is_some() {
                // use raw bytes
                blob.raw.as_ref().unwrap()
            } else if blob.zlib_data.is_some() {
                // decompress zlib data
                self.blob_buf.clear();
                let data: &Vec<u8> = blob.zlib_data.as_ref().unwrap();
                let mut decoder = ZlibDecoder::new(&data[..]);
                decoder.read_to_end(&mut self.blob_buf)?;
                &self.blob_buf
            } else {
                panic!("can only read raw or zlib compressed blob");
            };
            assert_eq!(
                blob_data.len(),
                blob.raw_size.unwrap_or_else(|| blob_data.len() as i32) as usize
            );

            Ok(BlockIndex {
                block_type: BlockType::from_osmdata_blob(&blob_data[..])?,
                blob_start,
                blob_len,
                blob_header_len,
            })
        } else {
            panic!("unknown blob type");
        }
    }
}

impl<'a> Iterator for BlockIndexIterator<'a> {
    type Item = Result<BlockIndex, io::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.data.len() {
            Some(self.read_next())
        } else {
            None
        }
    }
}
