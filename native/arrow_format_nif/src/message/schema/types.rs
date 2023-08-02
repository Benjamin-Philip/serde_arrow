#![allow(clippy::needless_borrow)]
use rustler::{NifMap, NifTaggedEnum};

#[derive(Debug, NifTaggedEnum)]
pub enum Type {
    Int(Int),
    FloatingPoint(FloatingPoint),
    FixedSizeList(FixedSizeList),
    LargeBinary,
    LargeList,
}

#[derive(Debug, NifMap)]
pub struct Int {
    pub bit_width: i64,
    pub is_signed: bool,
}

#[derive(Debug, NifMap)]
pub struct FloatingPoint {
    pub precision: Precision,
}

#[derive(Debug, NifTaggedEnum)]
pub enum Precision {
    Half,
    Single,
    Double,
}

#[derive(Debug, NifMap)]
pub struct FixedSizeList {
    pub list_size: i64,
}
