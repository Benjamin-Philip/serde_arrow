// use arrow_format::ipc::planus::Builder;
use crate::utils::CustomMetadata;
use rustler::{Atom, NifRecord};

pub mod header;
pub mod schema;

use header::Header;

#[derive(Debug, NifRecord)]
#[tag = "message"]
pub struct Message {
    version: Atom, // TODO Limit this to just v1..v5.
    header: Header,
    body_length: i32,
    custom_metadata: CustomMetadata,
    body: Atom,
}

// TODO arrow_format::ipc::MetadataVersion::V5,
// #[rustler::nif]
// fn serialize_message<'a>(env: rustler::env::Env<'a>) -> rustler::types::Binary<'a> {
//     let schema = serialize_schema();

//     let message = arrow_format::ipc::Message {
//         version: arrow_format::ipc::MetadataVersion::V5,
//         header: Some(arrow_format::ipc::MessageHeader::Schema(Box::new(schema))),
//         body_length: 0,
//         custom_metadata: None, // todo: allow writing custom metadata
//     };

//     let mut builder = Builder::new();
//     let footer_data = builder.finish(&message, None);

//     let mut erl_bin = rustler::types::OwnedBinary::new(footer_data.len()).unwrap();
//     erl_bin.as_mut_slice().copy_from_slice(&footer_data);
//     erl_bin.release(env)
// }

// fn serialize_schema() -> arrow_format::ipc::Schema {
//     let endianness = arrow_format::ipc::Endianness::Little;
//     let custom_metadata = None;
//     let fields = vec![];

//     arrow_format::ipc::Schema {
//         endianness,
//         fields: Some(fields),
//         custom_metadata,
//         features: None, // todo add this one
//     }
// }
