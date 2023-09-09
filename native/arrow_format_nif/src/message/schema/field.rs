use crate::utils::CustomMetadata;
use arrow_format::ipc;
use rustler::{Decoder, Encoder, NifRecord, NifUnitEnum, Term};

use super::types::Type;

#[derive(Debug, NifRecord)]
#[tag = "field"]
pub struct Field {
    pub name: Name,
    pub nullable: bool,
    pub r#type: Type,
    pub dictionary: Dictionary,
    pub children: Vec<Field>,
    pub custom_metadata: CustomMetadata,
}

#[derive(Debug)]
pub enum Name {
    Undefined,
    String(String),
}

#[derive(Debug, NifUnitEnum)]
pub enum Dictionary {
    Undefined,
}

impl Encoder for Name {
    fn encode<'a>(&self, env: rustler::env::Env<'a>) -> Term<'a> {
        match &self {
            Name::Undefined => crate::atoms::undefined().encode(env),
            Name::String(name) => name.clone().into_bytes().encode(env),
        }
    }
}

impl<'a> Decoder<'a> for Name {
    fn decode(term: Term<'a>) -> rustler::NifResult<Name> {
        if term.is_atom() {
            Ok(Name::Undefined)
        } else {
            let vector: Vec<u8> = term.decode()?;
            let string = String::from_utf8(vector).unwrap();
            Ok(Name::String(string))
        }
    }
}

impl From<&str> for Name {
    fn from(value: &str) -> Self {
        Name::String(value.to_string())
    }
}

impl Field {
    fn serialize(&self) -> arrow_format::ipc::Field {
        let name = self.name.serialize();
        let r#type = Some(self.r#type.serialize());
        let children = if self.children.is_empty() {
            None
        } else {
            Some(
                self.children
                    .iter()
                    .map(|child| child.serialize())
                    .collect(),
            )
        };

        ipc::Field {
            name,
            nullable: self.nullable,
            type_: r#type,
            dictionary: None,
            children,
            custom_metadata: None,
        }
    }
}

impl Name {
    fn serialize(&self) -> Option<String> {
        match &self {
            Name::Undefined => None,
            Name::String(name) => Some(name.clone()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::types::*;
    use super::*;
    use arrow_format::ipc;

    #[test]
    fn test_field_serialize() {
        let name = Field {
            name: Name::from("id"),
            nullable: true,
            r#type: Type::Int(Int {
                bit_width: 8,
                is_signed: true,
            }),
            dictionary: Dictionary::Undefined,
            children: vec![],
            custom_metadata: vec![],
        };

        let arrow_format_name = ipc::Field {
            name: Some("id".to_string()),
            nullable: true,
            type_: Some(ipc::Type::Int(Box::new(ipc::Int {
                bit_width: 8,
                is_signed: true,
            }))),
            dictionary: None,
            children: None,
            custom_metadata: None,
        };

        assert_eq!(name.serialize(), arrow_format_name);

        let annual_marks = Field {
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
        };

        let arrow_format_annual_marks = ipc::Field {
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
        };

        assert_eq!(annual_marks.serialize(), arrow_format_annual_marks);
    }

    #[test]
    fn test_name_serialize() {
        // Linking this seems to fail
        // TODO: Find a way to test this.
        // assert_eq!(Name::Atom(crate::atoms::undefined()).serialize(), None);
        assert_eq!(
            Name::from("named_field").serialize(),
            Some("named_field".to_string())
        );
    }
}
