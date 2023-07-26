use crate::utils::CustomMetadata;
use rustler::{Atom, NifRecord};

pub mod header;
pub mod record_batch;
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
