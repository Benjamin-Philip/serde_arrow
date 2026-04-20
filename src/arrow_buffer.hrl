-record(buffer, {
    type :: arrow_type:arrow_longhand_type(),
    length :: pos_integer(),
    data :: [arrow_type:native_type()] | binary()
}).
