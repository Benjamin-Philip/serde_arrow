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

#[rustler::nif]
fn print(msg: Message) -> Atom {
    println!("{:?}", msg);
    rustler::types::atom::ok()
}

rustler::init!("arrow_format_nif", [print]);
