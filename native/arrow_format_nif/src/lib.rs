use rustler::Atom;

mod message;
mod utils;

use message::Message;

mod atoms {
    rustler::atoms! {
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
        variable_list
    }
}

/// Returns `:ok` on successful decodes.
///
/// This function tests Message's (and its components')
/// `rustler::types::Decoder` implementation(s), and has only been for testing
/// purposes.
#[rustler::nif]
fn test_decode(msg: Message) -> Atom {
    rustler::types::atom::ok()
}

rustler::init!("arrow_format_nif", [test_decode]);
