#![allow(clippy::needless_borrow)]
// FIXME: Remove once this is used in Header.
#![allow(dead_code)]
use rustler::{NifMap, NifRecord, NifUnitEnum};

#[derive(Debug, NifRecord)]
#[tag = "record_batch"]
pub struct RecordBatch {
    pub length: i64,
    pub nodes: Vec<FieldNode>,
    pub buffers: Vec<Buffer>,
    pub compression: Compression,
}

#[derive(Debug, NifMap)]
pub struct FieldNode {
    pub length: i64,
    pub null_count: i64,
}

#[derive(Debug, NifMap)]
pub struct Buffer {
    pub offset: i64,
    pub length: i64,
}

#[derive(Debug, NifUnitEnum)]
pub enum Compression {
    Undefined,
    Lz4Frame,
    Zstd,
}

impl RecordBatch {
    pub fn serialize(&self) -> arrow_format::ipc::RecordBatch {
        arrow_format::ipc::RecordBatch {
            length: self.length,
            nodes: Some(
                self.nodes
                    .iter()
                    .map(|node| node.serialize())
                    .collect::<Vec<_>>(),
            ),
            buffers: Some(
                self.buffers
                    .iter()
                    .map(|buffer| buffer.serialize())
                    .collect::<Vec<_>>(),
            ),
            compression: self.compression.serialize(),
        }
    }
}

impl FieldNode {
    fn serialize(&self) -> arrow_format::ipc::FieldNode {
        arrow_format::ipc::FieldNode {
            length: self.length,
            null_count: self.null_count,
        }
    }
}

impl Buffer {
    fn serialize(&self) -> arrow_format::ipc::Buffer {
        arrow_format::ipc::Buffer {
            offset: self.offset,
            length: self.length,
        }
    }
}

impl Compression {
    fn serialize(&self) -> Option<Box<arrow_format::ipc::BodyCompression>> {
        match &self {
            Compression::Zstd => Some(Box::new(arrow_format::ipc::BodyCompression {
                codec: arrow_format::ipc::CompressionType::Zstd,
                method: arrow_format::ipc::BodyCompressionMethod::Buffer,
            })),
            Compression::Lz4Frame => Some(Box::new(arrow_format::ipc::BodyCompression {
                codec: arrow_format::ipc::CompressionType::Lz4Frame,
                method: arrow_format::ipc::BodyCompressionMethod::Buffer,
            })),
            Compression::Undefined => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::utils;

    #[test]
    fn test_record_batch_serialize() {
        // Test with a non-null compression
        let crate::message::Header::RecordBatch(mut record_batch) = utils::record_batch().header else {panic!("This is a pointless panic")};
        record_batch.compression = Compression::Zstd;

        let mut arrow_format_record_batch = match utils::arrow_record_batch().header.unwrap() {
            arrow_format::ipc::MessageHeader::RecordBatch(boxed_rb) => *boxed_rb,
            _ => panic!("This is a pointless panic"),
        };

        arrow_format_record_batch.compression =
            Some(Box::new(arrow_format::ipc::BodyCompression {
                codec: arrow_format::ipc::CompressionType::Zstd,
                method: arrow_format::ipc::BodyCompressionMethod::Buffer,
            }));

        assert_eq!(record_batch.serialize(), arrow_format_record_batch);
    }

    #[test]
    fn test_field_node_serialize() {
        let field_node = FieldNode {
            length: 4,
            null_count: 1,
        };

        let arrow_format_field_node = arrow_format::ipc::FieldNode {
            length: 4,
            null_count: 1,
        };

        assert_eq!(field_node.serialize(), arrow_format_field_node)
    }

    #[test]
    fn test_buffer_serialize() {
        let buffer = Buffer {
            offset: 0,
            length: 1,
        };

        let arrow_format_buffer = arrow_format::ipc::Buffer {
            offset: 0,
            length: 1,
        };

        assert_eq!(buffer.serialize(), arrow_format_buffer)
    }

    #[test]
    fn test_compression_serialize() {
        // Zstd Compression
        let zstd = Compression::Zstd.serialize().unwrap();
        assert_eq!(zstd.codec, arrow_format::ipc::CompressionType::Zstd);
        assert_eq!(
            zstd.method,
            arrow_format::ipc::BodyCompressionMethod::Buffer
        );

        // LZ4 Frame Compression
        let lz4_frame = Compression::Lz4Frame.serialize().unwrap();
        assert_eq!(
            lz4_frame.codec,
            arrow_format::ipc::CompressionType::Lz4Frame
        );
        assert_eq!(
            lz4_frame.method,
            arrow_format::ipc::BodyCompressionMethod::Buffer
        );

        // No Compression
        let undefined = Compression::Undefined.serialize();
        assert_eq!(undefined, None);
    }
}
