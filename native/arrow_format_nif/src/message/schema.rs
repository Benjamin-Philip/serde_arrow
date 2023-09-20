// FIXME: Remove once this is used in Header.
#![allow(dead_code)]
use crate::utils::CustomMetadata;
use rustler::{NifRecord, NifUnitEnum};

pub mod field;
pub mod types;

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

impl Schema {
    pub fn serialize(&self) -> arrow_format::ipc::Schema {
        arrow_format::ipc::Schema {
            endianness: self.endianness.serialize(),
            fields: Some(self.fields.iter().map(|field| field.serialize()).collect()),
            custom_metadata: None,
            features: Some(
                self.features
                    .iter()
                    .map(|feature| feature.serialize())
                    .collect(),
            ),
        }
    }
}

impl Endianness {
    fn serialize(&self) -> arrow_format::ipc::Endianness {
        match &self {
            Endianness::Little => arrow_format::ipc::Endianness::Little,
            Endianness::Big => arrow_format::ipc::Endianness::Big,
        }
    }
}

impl Feature {
    fn serialize(&self) -> arrow_format::ipc::Feature {
        match &self {
            Feature::Unused => arrow_format::ipc::Feature::Unused,
            Feature::DictionaryReplacement => arrow_format::ipc::Feature::DictionaryReplacement,
            Feature::CompressedBody => arrow_format::ipc::Feature::CompressedBody,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::message::Header;
    use crate::utils;
    use crate::test::fixtures;

    use arrow_format::ipc;

    #[test]
    fn test_schema_serialize() {
        let Header::Schema(schema) = utils::schema().header else {
            panic!("This is a pointless panic!")
        };
        let ipc::MessageHeader::Schema(arrow_schema) = fixtures::arrow_schema().header.unwrap()
        else {
            panic!("This is a pointless panic!")
        };

        assert_eq!(schema.serialize(), *arrow_schema);
    }

    #[test]
    fn test_endianness_serialize() {
        let little = Endianness::Little.serialize();
        assert_eq!(little, ipc::Endianness::Little);

        let big = Endianness::Big.serialize();
        assert_eq!(big, ipc::Endianness::Big);
    }

    #[test]
    fn test_feature_serialize() {
        let unused = Feature::Unused.serialize();
        assert_eq!(unused, ipc::Feature::Unused);

        let dictionary_replacement = Feature::DictionaryReplacement.serialize();
        assert_eq!(dictionary_replacement, ipc::Feature::DictionaryReplacement);

        let compressed_body = Feature::CompressedBody.serialize();
        assert_eq!(compressed_body, ipc::Feature::CompressedBody);
    }
}
