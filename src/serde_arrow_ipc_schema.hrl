-include("serde_arrow_ipc_field.hrl").

-record(schema, {
    endianness = little :: serde_arrow_ipc_schema:endianness(),
    fields = [#field{}],
    custom_metadata = [] :: [serde_arrow_ipc_message:key_value()],
    features = [unused] :: [serde_arrow_ipc_schema:feature()]
}).
