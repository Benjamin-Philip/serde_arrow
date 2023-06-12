-module(serde_arrow_ipc_schema_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_schema.hrl").

-include("serde_arrow_ipc_marks_data.hrl").

all() ->
    [
        valid_endianness_on_from_erlang,
        valid_fields_on_from_erlang,
        valid_custom_metadata_on_from_erlang,
        valid_features_on_from_erlang
    ].

valid_endianness_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.endianness, little).

valid_fields_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.fields, ?Fields).

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.custom_metadata, []).

valid_features_on_from_erlang(_Config) ->
    ?assertEqual((?Schema)#schema.features, [unused]).
