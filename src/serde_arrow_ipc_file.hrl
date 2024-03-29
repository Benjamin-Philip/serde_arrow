-record(block, {
    offset :: non_neg_integer(),
    metadata_length :: non_neg_integer(),
    body_length :: non_neg_integer()
}).

-record(footer, {
    version = v5 :: serde_arrow_ipc_message:metadata_version(),
    schema :: #schema{},
    dictionaries = [] :: [#block{}],
    record_batches :: [#block{}],
    custom_metadata = [] :: [serde_arrow_ipc_message:key_value()]
}).

-record(file, {footer :: #footer{}, body :: binary()}).
