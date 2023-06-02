-module(serde_arrow_ipc_record_batch_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_record_batch.hrl").
-include("serde_arrow_array.hrl").

-define(ID, serde_arrow_array:from_erlang(fixed_primitive, [0, 1, 2, undefined], {s, 8})).
-define(Name,
    serde_arrow_array:from_erlang(
        variable_binary, [<<"alice">>, <<"bob">>, <<"charlie">>, undefined], {bin, undefined}
    )
).
-define(Age, serde_arrow_array:from_erlang(fixed_primitive, [10, 20, 30, undefined], {s, 8})).

-define(Fields, [?ID, ?Name, ?Age]).

all() ->
    [
        valid_length_on_from_erlang,
        valid_nodes_on_from_erlang,
        valid_buffers_on_from_erlang,
        valid_compression_on_from_erlang
    ].

valid_length_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Fields))#record_batch.length, 4).

valid_nodes_on_from_erlang(_Config) ->
    FieldNodes = lists:duplicate(3, #{length => 4, null_count => 1}),
    ?assertEqual((from_erlang(?Fields))#record_batch.nodes, FieldNodes).

valid_buffers_on_from_erlang(_Config) ->
    Buffers = [#{offset => 0, length => 1}, #{offset => 1, length => 3}],
    erlang:display(?ID),
    ?assertEqual((from_erlang(?Fields))#record_batch.buffers, Buffers).

valid_compression_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Fields))#record_batch.compression, undefined).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

from_erlang(X) ->
    serde_arrow_ipc_record_batch:from_erlang(X).
