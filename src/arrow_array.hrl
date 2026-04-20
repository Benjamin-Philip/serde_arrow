-include("arrow_buffer.hrl").

-record(array, {
    layout :: arrow_array:layout(),
    type :: arrow_type:arrow_longhand_type() | arrow_type:arrow_nested_type(),
    len :: pos_integer(),
    element_len :: pos_integer() | undefined,
    null_count :: non_neg_integer(),
    validity_bitmap :: #buffer{} | undefined,
    offsets :: #buffer{} | undefined,
    data :: #buffer{} | #array{}
}).
