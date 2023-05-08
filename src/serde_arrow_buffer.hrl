-record(buffer, {
    type :: serde_arrow_type:arrow_longhand_type(),
    length :: pos_integer(),
    data :: binary()
}).
