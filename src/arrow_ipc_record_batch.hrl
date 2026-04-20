-record(record_batch, {
    length :: pos_integer(),
    nodes :: [arrow_ipc_record_batch:field_node()],
    buffers :: [arrow_ipc_record_batch:buffer()],
    compression = undefined :: undefined | lz4_frame | zstd
}).
