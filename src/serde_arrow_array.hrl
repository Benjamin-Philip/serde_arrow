-include("serde_arrow_buffer.hrl").

-record(array, {
    layout :: serde_arrow_array:layout(),
    type :: serde_arrow_type:arrow_type(),
    len :: pos_integer(),
    null_count :: pos_integer(),
    validity_bitmap :: #buffer{} | undefined,
    offsets :: #buffer{} | undefined,
    data :: #buffer{}
}).
