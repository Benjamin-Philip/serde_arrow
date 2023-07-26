use crate::utils::CustomMetadata;
use rustler::{Atom, Decoder, Encoder, NifRecord, Term};

#[derive(Debug, NifRecord)]
#[tag = "field"]
pub struct Field {
    pub name: Name,
    pub nullable: bool,
    pub r#type: Atom,
    pub dictionary: Atom,
    pub children: Vec<Field>,
    pub custom_metadata: CustomMetadata,
}

// TODO: Update Option<T>'s Encoder and Decoder implementations to treat
// undefined as None in rustler main.
#[derive(Debug)]
pub enum Name {
    Atom(Atom),
    String(String),
}

impl Encoder for Name {
    fn encode<'a>(&self, env: rustler::env::Env<'a>) -> Term<'a> {
        match &self {
            Name::Atom(_) => crate::atoms::undefined().encode(env),
            Name::String(name) => name.clone().into_bytes().encode(env),
        }
    }
}

impl<'a> Decoder<'a> for Name {
    fn decode(term: Term<'a>) -> rustler::NifResult<Name> {
        if term.is_atom() {
            Ok(Name::Atom(term.decode()?))
        } else {
            let vector: Vec<u8> = term.decode()?;
            let string = String::from_utf8(vector).unwrap();
            Ok(Name::String(string))
        }
    }
}
