-include("arrow_ipc_schema.hrl").
-include("arrow_ipc_record_batch.hrl").
-include("arrow_array.hrl").

-record(message, {
    version = v5 :: arrow_ipc_message:metadata_version(),
    header :: #schema{} | #record_batch{},
    body_length :: non_neg_integer(),
    custom_metadata = [] :: [arrow_ipc_message:key_value()],

    %% This field is unique to arrow.
    %% The rest are from the flatbuffers definitions.
    body :: binary() | undefined
}).
