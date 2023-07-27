use crate::utils::CustomMetadata;
use rustler::{NifRecord, NifUnitEnum};

pub mod field;

use field::Field;

#[derive(Debug, NifRecord)]
#[tag = "schema"]
pub struct Schema {
    pub endianness: Endianness,
    pub fields: Vec<Field>,
    pub custom_metadata: CustomMetadata,
    pub features: Vec<Feature>,
}

#[derive(Debug, NifUnitEnum)]
pub enum Endianness {
    Little,
    Big,
}

#[derive(Debug, NifUnitEnum)]
pub enum Feature {
    Unused,
    DictionaryReplacement,
    CompressedBody,
}
