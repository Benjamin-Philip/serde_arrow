use rustler::types::Binary;
use rustler::{Atom, Env};

mod message;
mod utils;

use message::Message;

mod atoms {
    rustler::atoms! {
        // TODO Organize atoms in a more systematic order

        // Versions

        v1,
        v2,
        v3,
        v4,
        v5,

        // Endianness

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
/// `rustler::types::Decoder` implementation(s), and is only used for testing
/// purposes.
#[rustler::nif]
fn test_decode(_msg: Message) -> Atom {
    atoms::ok()
}

/// Returns an example `Message` of a `Schema` or a `RecordBatch`.
///
/// This function tests Message's (and its components')
/// `rustler::types::Encoder` implementation(s), and is only used for testing
/// purposes.
#[rustler::nif]
fn test_encode(msg_type: Atom) -> Message {
    if msg_type == atoms::schema() {
        utils::schema()
    } else {
        utils::record_batch()
    }
}

/// Serializes a message into its correspondding flatbuffers.
///
/// This function serializes a message into its correspondding flatbuffers and
/// returns a binary
#[rustler::nif]
fn serialize_message(env: Env, message: Message) -> Binary {
    let flatbuffers = message.serialize_to_ipc();

    let mut erl_bin = rustler::types::OwnedBinary::new(flatbuffers.len()).unwrap();
    erl_bin.as_mut_slice().copy_from_slice(&flatbuffers);
    erl_bin.release(env)
}

rustler::init!(
    "arrow_format_nif",
    [test_decode, test_encode, serialize_message]
);
