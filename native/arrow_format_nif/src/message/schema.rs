use crate::utils::CustomMetadata;
use rustler::{Atom, NifRecord};

pub mod field;

use field::Field;

#[derive(Debug, NifRecord)]
#[tag = "schema"]
pub struct Schema {
    pub endianness: Atom, // TODO limit this to little and big
    pub fields: Vec<Field>,
    pub custom_metadata: CustomMetadata,
    pub features: Vec<Atom>,
}
