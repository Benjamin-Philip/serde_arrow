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
    fn serialize(&self) -> arrow_format::ipc::Schema {
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
    use super::field::*;
    use super::types::*;
    use super::*;
    use arrow_format::ipc;

    #[test]
    fn test_schema_serialize() {
        let schema = Schema {
            endianness: Endianness::Little,
            fields: vec![
                Field {
                    name: Name::from("id"),
                    nullable: true,
                    r#type: Type::Int(Int {
                        bit_width: 8,
                        is_signed: true,
                    }),
                    dictionary: Dictionary::Undefined,
                    children: vec![],
                    custom_metadata: vec![],
                },
                Field {
                    name: Name::from("name"),
                    nullable: true,
                    r#type: Type::LargeBinary,
                    dictionary: Dictionary::Undefined,
                    children: vec![],
                    custom_metadata: vec![],
                },
                Field {
                    name: Name::from("age"),
                    nullable: true,
                    r#type: Type::Int(Int {
                        bit_width: 8,
                        is_signed: false,
                    }),
                    dictionary: Dictionary::Undefined,
                    children: vec![],
                    custom_metadata: vec![],
                },
                Field {
                    name: Name::from("annual_marks"),
                    nullable: true,
                    r#type: Type::FixedSizeList(FixedSizeList { list_size: 3 }),
                    dictionary: Dictionary::Undefined,
                    children: vec![Field {
                        name: Name::Undefined,
                        nullable: true,
                        r#type: Type::Int(Int {
                            bit_width: 8,
                            is_signed: false,
                        }),
                        dictionary: Dictionary::Undefined,
                        children: vec![],
                        custom_metadata: vec![],
                    }],
                    custom_metadata: vec![],
                },
            ],
            custom_metadata: vec![],
            features: vec![Feature::Unused],
        };

        let arrow_schema = ipc::Schema {
            endianness: arrow_format::ipc::Endianness::Little,
            fields: Some(vec![
                ipc::Field {
                    name: Some("id".to_string()),
                    nullable: true,
                    type_: Some(ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 8,
                        is_signed: true,
                    }))),
                    dictionary: None,
                    children: None,
                    custom_metadata: None,
                },
                ipc::Field {
                    name: Some("name".to_string()),
                    nullable: true,
                    type_: Some(ipc::Type::LargeBinary(Box::new(ipc::LargeBinary {}))),
                    dictionary: None,
                    children: None,
                    custom_metadata: None,
                },
                ipc::Field {
                    name: Some("age".to_string()),
                    nullable: true,
                    type_: Some(ipc::Type::Int(Box::new(ipc::Int {
                        bit_width: 8,
                        is_signed: false,
                    }))),
                    dictionary: None,
                    children: None,
                    custom_metadata: None,
                },
                ipc::Field {
                    name: Some("annual_marks".to_string()),
                    nullable: true,
                    type_: Some(ipc::Type::FixedSizeList(Box::new(ipc::FixedSizeList {
                        list_size: 3,
                    }))),
                    dictionary: None,
                    children: Some(vec![ipc::Field {
                        name: None,
                        nullable: true,
                        type_: Some(ipc::Type::Int(Box::new(ipc::Int {
                            bit_width: 8,
                            is_signed: false,
                        }))),
                        dictionary: None,
                        children: None,
                        custom_metadata: None,
                    }]),
                    custom_metadata: None,
                },
            ]),
            custom_metadata: None,
            features: Some(vec![arrow_format::ipc::Feature::Unused]),
        };

        assert_eq!(schema.serialize(), arrow_schema);
    }

    #[test]
    fn test_endianness_serialize() {
        let little = Endianness::Little.serialize();
        assert_eq!(little, arrow_format::ipc::Endianness::Little);

        let big = Endianness::Big.serialize();
        assert_eq!(big, arrow_format::ipc::Endianness::Big);
    }

    #[test]
    fn test_feature_serialize() {
        let unused = Feature::Unused.serialize();
        assert_eq!(unused, arrow_format::ipc::Feature::Unused);

        let dictionary_replacement = Feature::DictionaryReplacement.serialize();
        assert_eq!(
            dictionary_replacement,
            arrow_format::ipc::Feature::DictionaryReplacement
        );

        let compressed_body = Feature::CompressedBody.serialize();
        assert_eq!(compressed_body, arrow_format::ipc::Feature::CompressedBody);
    }
}
