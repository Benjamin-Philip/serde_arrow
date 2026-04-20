-record(field, {
    name :: string() | undefined,
    nullable = true :: boolean(),
    type :: arrow_ipc_type:ipc_type(),
    dictionary = undefined,
    children = [] :: [#field{}],
    custom_metadata = [] :: [arrow_ipc_message:key_value()]
}).
