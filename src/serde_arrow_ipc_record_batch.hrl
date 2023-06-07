-record(record_batch, {
    length :: pos_integer(),
    nodes :: [serde_arrow_ipc_record_batch:field_node()],
    buffers :: [serde_arrow_ipc_record_batch:buffer()],
    compression = undefined :: undefined | lz4_frame | zstd
}).
