-record(buffer, {
    type :: serde_arrow_type:arrow_type() | byte,
    length :: pos_integer(),
    element_length :: pos_integer(),
    data :: binary()
}).
