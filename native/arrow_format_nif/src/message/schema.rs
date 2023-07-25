use crate::utils::CustomMetadata;
use rustler::{Atom, NifRecord};

pub mod field;

use field::Field;

#[derive(Debug, NifRecord)]
#[tag = "schema"]
pub struct Schema {
    endianness: Atom, // TODO limit this to little and big
    fields: Vec<Field>,
    custom_metadata: CustomMetadata,
    features: Vec<Atom>,
}
