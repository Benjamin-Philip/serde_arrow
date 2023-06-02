-record(record_batch, {
    length :: pos_integer(),
    nodes :: [#{length => pos_integer(), null_count => non_neg_integer()}],
    buffers :: [#{offset => non_neg_integer(), length => pos_integer()}],
    compression = undefined :: undefined | lz4_frame | zstd
}).
