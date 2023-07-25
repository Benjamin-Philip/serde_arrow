use rustler::{Atom, NifMap, NifRecord};

#[derive(Debug, NifRecord)]
#[tag = "record_batch"]
pub struct RecordBatch {
    length: i64,
    nodes: Vec<FieldNode>,
    buffers: Vec<Buffer>,
    compression: Atom, // TODO limit this to undefined, lz4_frame, and zstd
}

#[derive(Debug, NifMap)]
pub struct FieldNode {
    length: i64,
    null_count: i64,
}

#[derive(Debug, NifMap)]
pub struct Buffer {
    offset: i64,
    length: i64,
}
