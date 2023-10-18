use super::message::schema::Schema;
use super::message::Version;
use crate::utils::CustomMetadata;

use arrow_format::ipc;
use arrow_format::ipc::planus::Builder;
use rustler::NifRecord;

#[derive(Debug, NifRecord)]
#[tag = "footer"]
pub struct Footer {
    pub version: Version,
    pub schema: Schema,
    pub dictionaries: Vec<Block>,
    pub record_batches: Vec<Block>,
    pub custom_metadata: CustomMetadata,
}

#[derive(Debug, NifRecord)]
#[tag = "block"]
pub struct Block {
    pub offset: i64,
    pub metadata_length: i32,
    pub body_length: i64,
}

impl Footer {
    // A Word on Terminology:
    //
    // The function `serialize_to_ipc` is what actually serializes a footer to
    // its final binary/flatbuffer form.
    //
    // The function `serialize` just "serializes" (in this case converts) our
    // footer struct to `arrow_format`'s file struct. A similar "naming
    // convention" was followed in all the other traits which implement
    // `serialize`

    pub fn serialize_to_ipc(&self) -> Vec<u8> {
        let file = self.serialize();

        let mut builder = Builder::new();
        builder.finish(file, None).to_vec()
    }

    pub fn serialize(&self) -> ipc::Footer {
        ipc::Footer {
            version: self.version.serialize(),
            schema: Some(Box::new(self.schema.serialize())),
            dictionaries: self
                .dictionaries
                .iter()
                .map(|dictionary| dictionary.serialize())
                .collect::<Vec<_>>()
                .into(),
            record_batches: self
                .record_batches
                .iter()
                .map(|record_batch| record_batch.serialize())
                .collect::<Vec<_>>()
                .into(),
            custom_metadata: None,
        }
    }
}

impl Block {
    pub fn serialize(&self) -> ipc::Block {
        ipc::Block {
            offset: self.offset,
            meta_data_length: self.metadata_length,
            body_length: self.body_length,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::fixtures;
    // use crate::utils;
    use arrow_format::ipc;

    #[test]
    fn test_block_serialize() {
        let block = Block {
            offset: 10,
            metadata_length: 10,
            body_length: 100,
        };
        let arrow_block = ipc::Block {
            offset: 10,
            meta_data_length: 10,
            body_length: 100,
        };
        assert_eq!(block.serialize(), arrow_block);
    }

    #[test]
    fn test_footer_serialize() {
        let footer = fixtures::footer();
        let arrow_footer = fixtures::arrow_footer();
        assert_eq!(footer.serialize(), arrow_footer);
    }
}
