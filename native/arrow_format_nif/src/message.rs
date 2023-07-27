use crate::utils::CustomMetadata;
use rustler::{Atom, NifRecord, NifUnitEnum, NifUntaggedEnum};

pub mod record_batch;
pub mod schema;

use record_batch::RecordBatch;
use schema::Schema;

#[derive(Debug, NifRecord)]
#[tag = "message"]
pub struct Message {
    pub version: Version,
    pub header: Header,
    pub body_length: i32,
    pub custom_metadata: CustomMetadata,
    pub body: Atom,
}

#[derive(Debug, NifUnitEnum)]
pub enum Version {
    V1,
    V2,
    V3,
    V4,
    V5,
}

#[derive(Debug, NifUntaggedEnum)]
pub enum Header {
    Schema(Schema),
    RecordBatch(RecordBatch),
}
