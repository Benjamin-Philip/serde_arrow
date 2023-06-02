-module(serde_arrow_ipc_field_node_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_field_node.hrl").

all() ->
    [valid_field_node_from_erlang].

valid_field_node_from_erlang(_Config) ->
    Array = serde_arrow_array:from_erlang(fixed_primitive, [1, 2, undefined], {s, 8}),
    FieldNode = #field_node{length = 3, null_count = 1},
    ?assertEqual(serde_arrow_ipc_field_node:from_erlang(Array), FieldNode).
