use super::record_batch::RecordBatch;
use super::schema::Schema;
use rustler::{Decoder, Encoder, Term};

#[derive(Debug)]
pub enum Header {
    Schema(Schema),
    RecordBatch(RecordBatch),
}

impl Encoder for Header {
    fn encode<'a>(&self, env: rustler::env::Env<'a>) -> Term<'a> {
        match &self {
            Header::Schema(schema) => schema.encode(env),
            Header::RecordBatch(record_batch) => record_batch.encode(env),
        }
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
