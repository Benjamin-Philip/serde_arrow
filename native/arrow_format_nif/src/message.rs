use crate::utils::CustomMetadata;

use arrow_format::ipc;
use arrow_format::ipc::planus::Builder;
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

impl Message {
    // A Word on Terminology:
    //
    // The function `serialize_to_ipc` is what actually serializes a function to
    // its final binary/flatbuffer form.
    //
    // The function `serialize` just "serializes" (in this case converts) our
    // message struct to `arrow_format`'s message struct. A similar "naming
    // convention" was followed in all the other traits which implement
    // `serialize`

    pub fn serialize_to_ipc(&self) -> Vec<u8> {
        let message = self.serialize();

        let mut builder = Builder::new();
        builder.finish(message, None).to_vec()
    }

    pub fn serialize(&self) -> ipc::Message {
        ipc::Message {
            version: self.version.serialize(),
            header: Some(self.header.serialize()),
            body_length: 0i64,
            custom_metadata: None,
        }
    }
}

impl Version {
    pub fn serialize(&self) -> ipc::MetadataVersion {
        match self {
            Version::V1 => ipc::MetadataVersion::V1,
            Version::V2 => ipc::MetadataVersion::V2,
            Version::V3 => ipc::MetadataVersion::V3,
            Version::V4 => ipc::MetadataVersion::V4,
            Version::V5 => ipc::MetadataVersion::V5,
        }
    }
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
    use crate::test::fixtures;

    #[test]
    fn test_message_serialize() {
        assert_eq!(utils::schema().serialize(), fixtures::arrow_schema());
        assert_eq!(
            utils::record_batch().serialize(),
            fixtures::arrow_record_batch()
        );
    }

    #[test]
    fn test_version_serialize() {
        assert_eq!(Version::V1.serialize(), ipc::MetadataVersion::V1);
        assert_eq!(Version::V2.serialize(), ipc::MetadataVersion::V2);
        assert_eq!(Version::V3.serialize(), ipc::MetadataVersion::V3);
        assert_eq!(Version::V4.serialize(), ipc::MetadataVersion::V4);
        assert_eq!(Version::V5.serialize(), ipc::MetadataVersion::V5);
    }

    #[test]
    fn test_header_serialize() {
        // Schema

        let schema = utils::schema().header;
        let arrow_schema = fixtures::arrow_schema().header.unwrap();

        assert_eq!(schema.serialize(), arrow_schema);

        // RecordBatch

        let record_batch = utils::record_batch().header;
        let arrow_record_batch = fixtures::arrow_record_batch().header.unwrap();

        assert_eq!(record_batch.serialize(), arrow_record_batch);
    }
}
