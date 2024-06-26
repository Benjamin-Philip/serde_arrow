-include("serde_arrow_ipc_schema.hrl").
-include("serde_arrow_ipc_record_batch.hrl").
-include("serde_arrow_array.hrl").

-record(message, {
    version = v5 :: serde_arrow_ipc_message:metadata_version(),
    header :: #schema{} | #record_batch{},
    body_length :: non_neg_integer(),
    custom_metadata = [] :: [serde_arrow_ipc_message:key_value()],

    %% This field is unique to serde_arrow.
    %% The rest are from the flatbuffers definitions.
    body :: binary() | undefined
}).
