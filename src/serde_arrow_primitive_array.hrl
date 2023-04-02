-record(primitive_array, {
    type :: serde_arrow_type:arrow_type(),
    len :: pos_integer(),
    null_count :: pos_integer(),
    validity_bitmap :: binary() | nil,
    value :: binary()
}).
