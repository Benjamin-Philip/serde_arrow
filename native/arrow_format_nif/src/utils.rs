use std::collections::HashMap;

use crate::message::record_batch::{Buffer, Compression, FieldNode, RecordBatch};
use crate::message::schema::field::{Dictionary, Field, Name};
use crate::message::schema::types::{FixedSizeList, Int, Type};
use crate::message::schema::{Endianness, Feature, Schema};
use crate::message::{Body, Header, Message, Version};

pub type CustomMetadata = Vec<HashMap<String, String>>;

/// Returns an example `Message` of a `Schema`
pub fn schema() -> Message {
    Message {
        version: Version::V5,
        header: Header::Schema(Schema {
            endianness: Endianness::Little,
            fields: vec![
                Field {
                    name: Name::from("id"),
                    nullable: true,
                    r#type: Type::Int(Int {
                        bit_width: 8,
                        is_signed: true,
                    }),
                    dictionary: Dictionary::Undefined,
                    children: vec![],
                    custom_metadata: vec![],
                },
                Field {
                    name: Name::from("name"),
                    nullable: true,
                    r#type: Type::LargeBinary,
                    dictionary: Dictionary::Undefined,
                    children: vec![],
                    custom_metadata: vec![],
                },
                Field {
                    name: Name::from("age"),
                    nullable: true,
                    r#type: Type::Int(Int {
                        bit_width: 8,
                        is_signed: false,
                    }),
                    dictionary: Dictionary::Undefined,
                    children: vec![],
                    custom_metadata: vec![],
                },
                Field {
                    name: Name::from("annual_marks"),
                    nullable: true,
                    r#type: Type::FixedSizeList(FixedSizeList { list_size: 3 }),
                    dictionary: Dictionary::Undefined,
                    children: vec![Field {
                        name: Name::Undefined,
                        nullable: true,
                        r#type: Type::Int(Int {
                            bit_width: 8,
                            is_signed: false,
                        }),
                        dictionary: Dictionary::Undefined,
                        children: vec![],
                        custom_metadata: vec![],
                    }],
                    custom_metadata: vec![],
                },
            ],
            custom_metadata: vec![],
            features: vec![Feature::Unused],
        }),
        body_length: 0,
        custom_metadata: vec![],
        body: Body::Undefined,
    }
}

/// Returns an example `Message` of a `Schema`
pub fn record_batch() -> Message {
    Message {
        version: Version::V5,
        header: Header::RecordBatch(RecordBatch {
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
            compression: Compression::Undefined,
        }),
        body_length: 640,
        custom_metadata: vec![],
        body: Body::Undefined,
    }
}
