use super::record_batch::RecordBatch;
use super::schema::Schema;
use rustler::{Decoder, Encoder, Term};

#[derive(Debug)]
pub enum Header {
    Schema(Schema),
    RecordBatch(RecordBatch),
}

// TODO: Remove stub implementation
impl Encoder for Header {
    fn encode<'a>(&self, env: rustler::env::Env<'a>) -> Term<'a> {
        let msg = match &self {
            Header::Schema(_) => "foo",
            Header::RecordBatch(_) => "Bar",
        };

        let mut msg_binary = rustler::NewBinary::new(env, msg.len());
        msg_binary.as_mut_slice().clone_from_slice(msg.as_bytes());

        msg_binary.into()
    }
}

impl<'a> Decoder<'a> for Header {
    fn decode(term: Term<'a>) -> rustler::NifResult<Header> {
        // TODO Find a way to differentiate between invalid schemas and record
        // batches
        match term.decode::<Schema>() {
            Ok(schema) => Ok(Header::Schema(schema)),
            Err(_) => Ok(Header::RecordBatch(term.decode::<RecordBatch>()?)),
        }
    }
}
