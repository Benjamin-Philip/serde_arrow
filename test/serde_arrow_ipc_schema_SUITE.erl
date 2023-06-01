-module(serde_arrow_ipc_schema_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("serde_arrow_ipc_schema.hrl").

-define(ID, serde_arrow_ipc_field:from_erlang({s, 8}, "id")).
-define(Name, serde_arrow_ipc_field:from_erlang({bin, undefined}, "name")).
-define(Age, serde_arrow_ipc_field:from_erlang({u, 8}, "age")).

-define(Fields, [?ID, ?Name, ?Age]).

all() ->
    [
        valid_endianness_on_from_erlang,
        valid_fields_on_from_erlang,
        valid_custom_metadata_on_from_erlang,
        valid_features_on_from_erlang
    ].

valid_endianness_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Fields))#schema.endianness, little).

valid_fields_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Fields))#schema.fields, ?Fields).

valid_custom_metadata_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Fields))#schema.custom_metadata, []).

valid_features_on_from_erlang(_Config) ->
    ?assertEqual((from_erlang(?Fields))#schema.features, [unused]).

%%%%%%%%%%%
%% Utils %%
%%%%%%%%%%%

from_erlang(X) ->
    serde_arrow_ipc_schema:from_erlang(X).
