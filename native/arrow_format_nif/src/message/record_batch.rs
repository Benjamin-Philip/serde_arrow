#![allow(clippy::needless_borrow)]
use rustler::{Atom, NifMap, NifRecord};

#[derive(Debug, NifRecord)]
#[tag = "record_batch"]
pub struct RecordBatch {
    pub length: i64,
    pub nodes: Vec<FieldNode>,
    pub buffers: Vec<Buffer>,
    pub compression: Atom, // TODO limit this to undefined, lz4_frame, and zstd
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
