-include("serde_arrow_buffer.hrl").

-record(array, {
    layout :: serde_arrow_array:layout(),
    type :: serde_arrow_type:arrow_longhand_type() | serde_arrow_type:arrow_nested_type(),
    len :: pos_integer(),
    element_len :: pos_integer() | undefined,
    null_count :: non_neg_integer(),
    validity_bitmap :: #buffer{} | undefined,
    offsets :: #buffer{} | undefined,
    data :: #buffer{} | #array{}
}).
