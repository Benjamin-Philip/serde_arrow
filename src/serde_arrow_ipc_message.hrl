-include("serde_arrow_ipc_schema.hrl").

-record(message, {
    version = v5 :: serde_arrow_ipc_message:metadata_version(),
    header :: #schema{},
    body_length :: non_neg_integer(),
    custom_metadata = [] :: [serde_arrow_ipc_message:key_value()],

    %% This field is unique to serde_arrow.
    %% The rest are from the flatbuffers definitions.
    body :: binary() | undefined
}).
