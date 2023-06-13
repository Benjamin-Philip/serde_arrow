-module(serde_arrow_ipc_record_batch_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_record_batch.hrl").
-include("serde_arrow_array.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

all() ->
    [
        valid_length_on_from_erlang,
        valid_nodes_on_from_erlang,
        valid_buffers_on_from_erlang,
        valid_compression_on_from_erlang
    ].

valid_length_on_from_erlang(_Config) ->
    ?assertEqual((?RecordBatch)#record_batch.length, 4).

valid_nodes_on_from_erlang(_Config) ->
    FieldNodes = lists:duplicate(4, #{length => 4, null_count => 1}),
    ?assertEqual((?RecordBatch)#record_batch.nodes, FieldNodes).

valid_buffers_on_from_erlang(_Config) ->
    ID = [#{offset => 0, length => 1}, #{offset => 8, length => 4}],
    Name = [
        #{offset => 16, length => 1}, #{offset => 24, length => 20}, #{offset => 48, length => 15}
    ],
    Age = [#{offset => 64, length => 1}, #{offset => 72, length => 4}],
    Marks =
        [#{offset => 80, length => 1}] ++
            [#{offset => 88, length => 2}, #{offset => 96, length => 10}],
    Buffers = ID ++ Name ++ Age ++ Marks,

    ?assertEqual((?RecordBatch)#record_batch.buffers, Buffers).

valid_compression_on_from_erlang(_Config) ->
    ?assertEqual((?RecordBatch)#record_batch.compression, undefined).
