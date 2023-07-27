use rustler::Atom;

mod message;
mod utils;

use message::record_batch::{Buffer, Compression, FieldNode, RecordBatch};
use message::schema::field::{Field, Name};
use message::schema::{Endianness, Feature, Schema};
use message::{Header, Message, Version};

mod atoms {
    rustler::atoms! {
        // TODO Organize atoms in a more systematic order

        // Versions

        v1,
        v2,
        v3,
        v4,
        v5,

        // Endiannes

        little,
        big,

        // Layouts

        fixed_primitive,
        variable_binary,
        fixed_list,
        variable_list,

        // Message Types

        schema,
        record_batch,

        // Features

        unused,

        // Built-ins

        ok,
        undefined
    }
}

/// Returns `:ok` on successful decodes.
///
/// This function tests Message's (and its components')
/// `rustler::types::Decoder` implementation(s), and has only been for testing
/// purposes.
#[rustler::nif]
fn test_decode(_msg: Message) -> Atom {
    atoms::ok()
}

/// Returns an example `Message` of a `Schema` or a `RecordBatch`.
///
/// This function tests Message's (and its components')
/// `rustler::types::Encoder` implementation(s), and has only been for testing
/// purposes.
#[rustler::nif]
fn test_encode(msg_type: Atom) -> Message {
    if msg_type == atoms::schema() {
        Message {
            version: Version::V5,
            header: Header::Schema(Schema {
                endianness: Endianness::Little,
                fields: vec![
                    Field {
                        name: Name::String("id".to_string()),
                        nullable: true,
                        r#type: atoms::fixed_primitive(),
                        dictionary: atoms::undefined(),
                        children: vec![],
                        custom_metadata: vec![],
                    },
                    Field {
                        name: Name::String("name".to_string()),
                        nullable: true,
                        r#type: atoms::variable_binary(),
                        dictionary: atoms::undefined(),
                        children: vec![],
                        custom_metadata: vec![],
                    },
                    Field {
                        name: Name::String("age".to_string()),
                        nullable: true,
                        r#type: atoms::fixed_primitive(),
                        dictionary: atoms::undefined(),
                        children: vec![],
                        custom_metadata: vec![],
                    },
                    Field {
                        name: Name::String("annual_marks".to_string()),
                        nullable: true,
                        r#type: atoms::fixed_list(),
                        dictionary: atoms::undefined(),
                        children: vec![Field {
                            name: Name::Atom(atoms::undefined()),
                            nullable: true,
                            r#type: atoms::fixed_primitive(),
                            dictionary: atoms::undefined(),
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
            body: atoms::undefined(),
        }
    } else {
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
            body: atoms::undefined(),
        }
    }
}

rustler::init!("arrow_format_nif", [test_decode, test_encode]);
