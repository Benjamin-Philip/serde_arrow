-record(field, {
    name :: string() | undefined,
    nullable = true :: boolean(),
    type :: serde_arrow_array:layout(),
    dictionary = undefined,
    children = [] :: [#field{}],
    custom_metadata = [] :: [serde_arrow_ipc_message:key_value()]
}).
