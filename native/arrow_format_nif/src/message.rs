use crate::utils::CustomMetadata;
use rustler::{Atom, NifRecord};

pub mod header;
pub mod record_batch;
pub mod schema;

use header::Header;

#[derive(Debug, NifRecord)]
#[tag = "message"]
pub struct Message {
    pub version: Atom, // TODO Limit this to just v1..v5.
    pub header: Header,
    pub body_length: i32,
    pub custom_metadata: CustomMetadata,
    pub body: Atom,
}
