-module(serde_arrow_ipc_record_batch).
-export([from_erlang/1]).

-include("serde_arrow_ipc_record_batch.hrl").
-include("serde_arrow_array.hrl").

-spec from_erlang(Arrays :: [#array{}]) -> RecordBatch :: #record_batch{}.
from_erlang(Arrays) ->
    FieldNodes = lists:map(
        fun(X) -> #{length => X#array.len, null_count => X#array.null_count} end, Arrays
    ),

    [Array | _] = Arrays,
    Length = Array#array.len,
    #record_batch{length = Length, nodes = FieldNodes}.
