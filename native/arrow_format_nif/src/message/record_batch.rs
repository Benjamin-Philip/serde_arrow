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
    fn serialize(&self) -> arrow_format::ipc::RecordBatch {
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

    #[test]
    fn test_record_batch_serialize() {
        // Our format data

        let record_batch = RecordBatch {
            length: 4,
            nodes: vec![
                FieldNode {
                    length: 4,
                    null_count: 1,
                },
                FieldNode {
                    length: 4,
                    null_count: 1,
                },
                FieldNode {
                    length: 4,
                    null_count: 1,
                },
                FieldNode {
                    length: 4,
                    null_count: 1,
                },
            ],
            buffers: vec![
                Buffer {
                    offset: 0,
                    length: 1,
                },
                Buffer {
                    offset: 8,
                    length: 4,
                },
                Buffer {
                    offset: 16,
                    length: 1,
                },
                Buffer {
                    offset: 24,
                    length: 20,
                },
                Buffer {
                    offset: 48,
                    length: 15,
                },
                Buffer {
                    offset: 64,
                    length: 1,
                },
                Buffer {
                    offset: 72,
                    length: 4,
                },
                Buffer {
                    offset: 80,
                    length: 1,
                },
                Buffer {
                    offset: 88,
                    length: 2,
                },
                Buffer {
                    offset: 96,
                    length: 10,
                },
            ],
            compression: Compression::Zstd,
        }
        .serialize();

        // Arrow Format Data

        let length = 4;
        let nodes = Some(vec![
            arrow_format::ipc::FieldNode {
                length: 4,
                null_count: 1,
            },
            arrow_format::ipc::FieldNode {
                length: 4,
                null_count: 1,
            },
            arrow_format::ipc::FieldNode {
                length: 4,
                null_count: 1,
            },
            arrow_format::ipc::FieldNode {
                length: 4,
                null_count: 1,
            },
        ]);
        let buffers = Some(vec![
            arrow_format::ipc::Buffer {
                offset: 0,
                length: 1,
            },
            arrow_format::ipc::Buffer {
                offset: 8,
                length: 4,
            },
            arrow_format::ipc::Buffer {
                offset: 16,
                length: 1,
            },
            arrow_format::ipc::Buffer {
                offset: 24,
                length: 20,
            },
            arrow_format::ipc::Buffer {
                offset: 48,
                length: 15,
            },
            arrow_format::ipc::Buffer {
                offset: 64,
                length: 1,
            },
            arrow_format::ipc::Buffer {
                offset: 72,
                length: 4,
            },
            arrow_format::ipc::Buffer {
                offset: 80,
                length: 1,
            },
            arrow_format::ipc::Buffer {
                offset: 88,
                length: 2,
            },
            arrow_format::ipc::Buffer {
                offset: 96,
                length: 10,
            },
        ]);
        let compression = Some(Box::new(arrow_format::ipc::BodyCompression {
            codec: arrow_format::ipc::CompressionType::Zstd,
            method: arrow_format::ipc::BodyCompressionMethod::Buffer,
        }));
        let arrow_format_record_batch = arrow_format::ipc::RecordBatch {
            length: 4,
            nodes: nodes,
            buffers: buffers,
            compression: compression,
        };

        // Actual Test Cases

        // TODO Specifically compare nodes, buffers and compression before a
        // full comparison.
        assert_eq!(record_batch.length, length);
        assert_eq!(record_batch, arrow_format_record_batch);
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
