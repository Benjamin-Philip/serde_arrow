use crate::utils::CustomMetadata;

use arrow_format::ipc;
use rustler::{NifRecord, NifUnitEnum, NifUntaggedEnum};

pub mod record_batch;
pub mod schema;

use record_batch::RecordBatch;
use schema::Schema;

#[derive(Debug, NifRecord)]
#[tag = "message"]
pub struct Message {
    pub version: Version,
    pub header: Header,
    pub body_length: i32,
    pub custom_metadata: CustomMetadata,
    pub body: Body,
}

#[derive(Debug, NifUnitEnum)]
pub enum Version {
    V1,
    V2,
    V3,
    V4,
    V5,
}

#[derive(Debug, NifUntaggedEnum)]
pub enum Header {
    Schema(Schema),
    RecordBatch(RecordBatch),
}

#[derive(Debug, NifUnitEnum)]
pub enum Body {
    Undefined,
}

 impl Header {
    pub fn serialize(&self) -> ipc::MessageHeader {
        match self {
            Header::Schema(schema) => ipc::MessageHeader::Schema(Box::new(schema.serialize())),
            Header::RecordBatch(record_batch) => {
                ipc::MessageHeader::RecordBatch(Box::new(record_batch.serialize()))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::utils;

    #[test]
    fn test_header_serialize() {
        // Schema

        let schema = utils::schema().header;
        let arrow_schema = utils::arrow_schema().header.unwrap();

        assert_eq!(schema.serialize(), arrow_schema);

        // RecordBatch

        let record_batch = utils::record_batch().header;
        let arrow_record_batch = utils::arrow_record_batch().header.unwrap();

        assert_eq!(record_batch.serialize(), arrow_record_batch);
    }
}
