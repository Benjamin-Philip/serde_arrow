-module(serde_arrow_ipc_field_node).
-export([from_erlang/1]).

-include("serde_arrow_array.hrl").
-include("serde_arrow_ipc_field_node.hrl").

-spec from_erlang(Array :: #array{}) -> FieldNode :: #field_node{}.
from_erlang(Array) ->
    #field_node{length = Array#array.len, null_count = Array#array.null_count}.
