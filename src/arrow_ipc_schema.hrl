-include("arrow_ipc_field.hrl").

-record(schema, {
    endianness = little :: arrow_ipc_schema:endianness(),
    fields = [#field{}],
    custom_metadata = [] :: [arrow_ipc_message:key_value()],
    features = [unused] :: [arrow_ipc_schema:feature()]
}).
